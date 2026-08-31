"""
The Raft consensus state machine: Follower / Candidate / Leader.

=============================================================================
THE PROBLEM THIS SOLVES
=============================================================================
We have N machines. We want exactly ONE of them to be "the leader" that
accepts writes, and we want the cluster to pick a new one automatically
when that leader dies. Nobody is in charge of deciding this — there's no
external coordinator. The machines have to work it out among themselves,
over an unreliable network, with no shared memory and no shared clock.

Two ideas do almost all of the work:

  TERMS — Raft chops all of time into numbered "terms" (1, 2, 3, ...).
  A term is at most one leader's reign. Every message carries the term
  of its sender. This is a LOGICAL CLOCK: we can't trust wall-clock time
  across machines (clocks drift, and a paused/slow machine can wake up
  believing no time has passed), but we CAN totally order events by term.
  The rule is beautifully simple and appears everywhere below:

      * If someone tells me about a term HIGHER than mine, I am stale.
        I immediately adopt their term and become a follower — no
        exceptions, no matter what I was doing, even if I'm the leader.
      * If someone tells me about a term LOWER than mine, THEY are stale.
        I reject their message and tell them my term so they step down.

  MAJORITIES — a candidate needs votes from a strict majority (N/2 + 1)
  to become leader. The reason this is safe is a counting argument: any
  two majorities of the same set MUST overlap in at least one node. Since
  each node votes at most once per term, and any two would-be leaders in
  the same term would need overlapping majorities, that shared node would
  have had to vote twice — which it never does. Therefore at most one
  leader per term. That's the entire safety guarantee, and it's why the
  vote must be written to DISK before it's sent (Phase 1's rule).

=============================================================================
WHY ELECTION TIMEOUTS ARE RANDOMIZED
=============================================================================
A follower starts an election when it hasn't heard from a leader "in a
while." If every node used the SAME timeout, then when a leader dies,
every follower would time out at the same instant, all become candidates
at once, and all vote for themselves — splitting the vote so nobody gets
a majority. They'd time out together again, and again. This is a "split
vote" and it can livelock the cluster indefinitely.

The fix is almost embarrassingly simple: each node picks a RANDOM timeout
from a range (here 1.5-3.0s). Whoever draws the shortest one wakes up
first, becomes a candidate alone, and usually wins before anyone else
even wakes up. Randomness breaks the symmetry. This is a genuinely
important idea — a hard distributed coordination problem dissolved by
adding noise instead of by adding more protocol.

=============================================================================
WHY A CANDIDATE CAN BE REFUSED EVEN IF IT'S IN A NEW TERM
=============================================================================
See `_log_is_up_to_date` below. A node will only vote for a candidate
whose log is at least as complete as its own. This doesn't matter much
in Phase 2 (all logs are empty), but it's the rule that makes Phase 3
safe: it guarantees a leader can never be elected while missing an entry
that's already committed, so committed data can never be lost. Building
it in now means Phase 3 doesn't have to retrofit it.
"""

import random
import threading
import time

from .rpc import send_rpc
from .state import PersistentState

FOLLOWER = "follower"
CANDIDATE = "candidate"
LEADER = "leader"

# Deliberately slow timings so a human can watch an election happen in
# real time. Production Raft typically uses 150-300ms election timeouts.
# The invariant that actually matters is the ORDERING:
#
#     RPC timeout  <  heartbeat interval  <<  election timeout
#
# A leader must get several heartbeats in before the most impatient
# follower loses faith. If heartbeats were as slow as election timeouts,
# the cluster would depose a perfectly healthy leader constantly.
ELECTION_TIMEOUT_MIN = 1.5
ELECTION_TIMEOUT_MAX = 3.0
HEARTBEAT_INTERVAL = 0.5


class RaftNode:
    def __init__(self, node_id: str, peers: dict[str, str], storage_path: str):
        """
        node_id: this node's name, e.g. "node1"
        peers:   {other_node_id: "http://host:port"} — NOT including self
        """
        self.node_id = node_id
        self.peers = peers
        self.state = PersistentState(storage_path)

        # --- Volatile state: rebuilt on restart, never written to disk ---
        # Contrast with PersistentState. If we crash, it's fine to forget
        # our role: we just come back as a follower and either find the
        # existing leader or start an election. It is NOT fine to forget
        # our term or our vote, which is why those live on disk.
        self.role = FOLLOWER
        self.leader_id: str | None = None
        self.votes_received: set[str] = set()

        # One lock guarding ALL mutable state above. Inbound RPCs arrive
        # on HTTP server threads while the ticker thread runs concurrently,
        # so every read-modify-write of node state must be serialized or
        # we'd get races (e.g. two threads both deciding they won the same
        # election). Held only briefly, and NEVER while doing network I/O.
        self.lock = threading.RLock()

        self._election_deadline = 0.0
        self._next_heartbeat = 0.0
        self._running = False
        self._ticker: threading.Thread | None = None
        self._reset_election_timer()

    # ------------------------------------------------------------------
    # Logging — the point of Phase 2 is to WATCH this happen, so the
    # node narrates every state transition to stdout.
    # ------------------------------------------------------------------
    def log(self, message: str) -> None:
        stamp = time.strftime("%H:%M:%S")
        print(f"[{stamp}] {self.node_id} (term {self.state.current_term}) {message}", flush=True)

    # ------------------------------------------------------------------
    # Timers
    # ------------------------------------------------------------------
    def _reset_election_timer(self) -> None:
        """
        Called whenever we hear from a legitimate leader (or grant a vote).

        Resetting on a granted vote matters: if we just voted for someone,
        they deserve a fair chance to finish winning before we get
        impatient and start a competing election ourselves.
        """
        timeout = random.uniform(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)
        self._election_deadline = time.monotonic() + timeout

    # ------------------------------------------------------------------
    # Role transitions
    # ------------------------------------------------------------------
    def _become_follower(self, term: int, leader_id: str | None = None) -> None:
        """
        Step down. This is the single most-invoked rule in Raft: seeing a
        higher term than your own means you are, by definition, out of
        date — so you stop whatever you were doing and follow.
        """
        was = self.role
        if term > self.state.current_term:
            # Persist BEFORE acting, per Phase 1's rule. New term means a
            # fresh vote is available, so votedFor resets to None.
            self.state.current_term = term
            self.state.voted_for = None
            self.state._save()
        self.role = FOLLOWER
        self.leader_id = leader_id
        self.votes_received = set()
        if was != FOLLOWER:
            self.log(f"stepping down: {was} -> follower")

    def _become_candidate(self) -> None:
        """
        Start an election for the NEXT term.

        Three things happen atomically-ish, and the order matters:
          1. bump the term (a candidate always runs in a brand-new term)
          2. vote for self  (you always vote for yourself; a 1-node
             cluster must be able to elect itself with zero RPCs)
          3. persist both BEFORE sending a single RequestVote — otherwise
             a crash here could let us vote again in this same term after
             restarting, which is precisely the double-vote that breaks
             the majority-overlap safety argument.
        """
        self.state.current_term += 1
        self.state.voted_for = self.node_id
        self.state._save()

        self.role = CANDIDATE
        self.leader_id = None
        self.votes_received = {self.node_id}
        self._reset_election_timer()
        self.log(f"election timeout -> becoming CANDIDATE, requesting votes")

        # The self-vote might ALREADY be a majority — that's the case in a
        # single-node cluster, where majority is 1. If we only ever counted
        # votes when an RPC reply came back, such a node would campaign
        # forever without noticing it had already won, because there are no
        # peers to reply. Check the tally here, not just in the reply path.
        if len(self.votes_received) >= self._majority():
            self._become_leader()

    def _become_leader(self) -> None:
        self.role = LEADER
        self.leader_id = self.node_id
        self.log(f"*** WON ELECTION with {len(self.votes_received)} votes "
                 f"({sorted(self.votes_received)}) -> now LEADER ***")
        # Send a heartbeat immediately rather than waiting up to
        # HEARTBEAT_INTERVAL. A brand-new leader wants to announce itself
        # as fast as possible, to stop other nodes from timing out and
        # starting a competing election in a higher term.
        self._next_heartbeat = 0.0

    def _majority(self) -> int:
        cluster_size = len(self.peers) + 1
        return cluster_size // 2 + 1

    # ==================================================================
    # INBOUND RPCs — things other nodes call on us
    # ==================================================================

    def handle_request_vote(self, msg: dict) -> dict:
        """
        RequestVote RPC. A candidate is asking us to vote for it.

        We grant the vote only if ALL of these hold:
          1. Their term is not older than ours.
          2. We haven't already voted for someone ELSE this term.
             (Re-voting for the SAME candidate is allowed and necessary:
             if our reply got lost and they retried, we must give the
             same answer again. RPCs must be idempotent, because the
             network will duplicate them.)
          3. Their log is at least as up-to-date as ours.
        """
        with self.lock:
            term = msg["term"]
            candidate_id = msg["candidate_id"]

            # Rule: a message from an OLDER term is from a stale node.
            # Reject and report our term so they step down.
            if term < self.state.current_term:
                self.log(f"rejecting vote for {candidate_id}: their term "
                         f"{term} < ours {self.state.current_term}")
                return {"term": self.state.current_term, "vote_granted": False}

            # Rule: a message from a NEWER term means we're stale.
            if term > self.state.current_term:
                self._become_follower(term)

            already_voted_elsewhere = (
                self.state.voted_for is not None
                and self.state.voted_for != candidate_id
            )
            if already_voted_elsewhere:
                self.log(f"rejecting vote for {candidate_id}: already voted "
                         f"for {self.state.voted_for} this term")
                return {"term": self.state.current_term, "vote_granted": False}

            if not self._log_is_up_to_date(msg["last_log_index"], msg["last_log_term"]):
                self.log(f"rejecting vote for {candidate_id}: their log is behind ours")
                return {"term": self.state.current_term, "vote_granted": False}

            # Persist the vote to disk BEFORE replying. If we crash after
            # replying but before saving, we could wake up and vote again
            # in this term -> two leaders. This one line is load-bearing.
            self.state.set_voted_for(candidate_id)
            self._reset_election_timer()
            self.log(f"granting vote to {candidate_id}")
            return {"term": self.state.current_term, "vote_granted": True}

    def _log_is_up_to_date(self, candidate_last_index: int, candidate_last_term: int) -> bool:
        """
        Is the candidate's log at least as complete as ours?

        Raft compares logs by (term of last entry, then index):
          - a HIGHER last term always wins — entries from a later term
            supersede a longer log from an older term
          - same last term -> the LONGER log wins

        Why term first? A long log full of entries from an old term may
        contain uncommitted junk from a leader that died mid-write. A
        shorter log ending in a newer term reflects more recent consensus.
        """
        our_index = self.state.last_log_index()
        our_term = self.state.last_log_term()
        if candidate_last_term != our_term:
            return candidate_last_term > our_term
        return candidate_last_index >= our_index

    def handle_append_entries(self, msg: dict) -> dict:
        """
        AppendEntries RPC.

        In Phase 3 this carries real log entries. In Phase 2 it's used
        purely as a HEARTBEAT with an empty entries list — the leader
        saying "I'm alive, don't start an election."

        Raft reuses one RPC for both jobs on purpose: the heartbeat isn't
        a separate mechanism bolted on, it's just the replication RPC with
        nothing to replicate. Fewer message types, fewer edge cases.
        """
        with self.lock:
            term = msg["term"]
            leader_id = msg["leader_id"]

            # Stale leader talking. Reject; our higher term in the reply
            # tells them to step down.
            if term < self.state.current_term:
                return {"term": self.state.current_term, "success": False}

            # A valid leader exists for this term. Adopt it and reset our
            # election timer — this is the heartbeat doing its actual job.
            if term > self.state.current_term:
                self._become_follower(term, leader_id)
            elif self.role == CANDIDATE:
                # Same term, but someone else already won this election.
                # We lost. Step down gracefully rather than keep campaigning.
                self.log(f"lost election: {leader_id} is leader for this term")
                self._become_follower(term, leader_id)
            else:
                self.leader_id = leader_id

            self._reset_election_timer()
            return {"term": self.state.current_term, "success": True}

    def handle_status(self, _msg: dict) -> dict:
        """Read-only introspection, for the demo script and for curl."""
        with self.lock:
            return {
                "node_id": self.node_id,
                "role": self.role,
                "term": self.state.current_term,
                "voted_for": self.state.voted_for,
                "leader_id": self.leader_id,
                "log_length": len(self.state.log),
            }

    # ==================================================================
    # OUTBOUND — things we do to other nodes
    # ==================================================================

    def _start_election(self) -> None:
        with self.lock:
            self._become_candidate()
            # Snapshot everything the RPC needs WHILE holding the lock,
            # then release it before touching the network. Doing I/O under
            # a lock would let one dead peer stall every other thread in
            # the process, including the ticker.
            payload = {
                "term": self.state.current_term,
                "candidate_id": self.node_id,
                "last_log_index": self.state.last_log_index(),
                "last_log_term": self.state.last_log_term(),
            }
            election_term = self.state.current_term

        # Fan out to all peers in parallel. Sequential requests would mean
        # one unreachable peer delays the votes from every peer after it,
        # possibly past our own election timeout.
        for peer_id, url in self.peers.items():
            threading.Thread(
                target=self._request_vote_from,
                args=(peer_id, url, payload, election_term),
                daemon=True,
            ).start()

    def _request_vote_from(self, peer_id: str, url: str, payload: dict, election_term: int) -> None:
        reply = send_rpc(f"{url}/request_vote", payload)
        if reply is None:
            return  # Peer down or slow. Perfectly normal; just no vote.

        with self.lock:
            # CRITICAL STALENESS CHECK. This reply may have taken seconds
            # to arrive, during which we might have stepped down, or moved
            # on to a later election. Counting a vote from an old election
            # toward a current one would be a real bug — so we verify that
            # the world hasn't changed underneath us before acting.
            if self.role != CANDIDATE or self.state.current_term != election_term:
                return

            if reply["term"] > self.state.current_term:
                # Somebody out there is ahead of us. Abandon the election.
                self.log(f"discovered higher term {reply['term']} from {peer_id}")
                self._become_follower(reply["term"])
                return

            if reply.get("vote_granted"):
                self.votes_received.add(peer_id)
                self.log(f"received vote from {peer_id} "
                         f"({len(self.votes_received)}/{self._majority()} needed)")
                if len(self.votes_received) >= self._majority():
                    self._become_leader()

    def _send_heartbeats(self) -> None:
        with self.lock:
            if self.role != LEADER:
                return
            payload = {
                "term": self.state.current_term,
                "leader_id": self.node_id,
                # Phase 3 fills these in for real; empty = pure heartbeat.
                "prev_log_index": self.state.last_log_index(),
                "prev_log_term": self.state.last_log_term(),
                "entries": [],
                "leader_commit": 0,
            }
            leader_term = self.state.current_term

        for peer_id, url in self.peers.items():
            threading.Thread(
                target=self._heartbeat_one,
                args=(peer_id, url, payload, leader_term),
                daemon=True,
            ).start()

    def _heartbeat_one(self, peer_id: str, url: str, payload: dict, leader_term: int) -> None:
        reply = send_rpc(f"{url}/append_entries", payload)
        if reply is None:
            return
        with self.lock:
            # A follower can inform a leader that it's been deposed — this
            # is how a leader that was network-partitioned away rejoins the
            # cluster and stops acting as leader.
            if reply["term"] > self.state.current_term:
                self.log(f"heartbeat rejected by {peer_id}: term "
                         f"{reply['term']} > ours. Stepping down.")
                self._become_follower(reply["term"])

    # ==================================================================
    # THE TICKER — the node's heartbeat of activity
    # ==================================================================

    def _tick(self) -> None:
        """
        One clock tick: check whether it's time to act.

        Everything time-driven in Raft reduces to these two checks:
          - "Am I a follower/candidate whose election timer expired?"
              -> start an election
          - "Am I the leader and due to send heartbeats?"
              -> send heartbeats
        """
        now = time.monotonic()

        with self.lock:
            role = self.role
            election_expired = now >= self._election_deadline
            heartbeat_due = now >= self._next_heartbeat

        if role == LEADER:
            if heartbeat_due:
                with self.lock:
                    self._next_heartbeat = now + HEARTBEAT_INTERVAL
                self._send_heartbeats()
        elif election_expired:
            # Applies to followers (no leader contact) AND candidates
            # (election ended in a split vote with no winner). A candidate
            # that times out simply starts a NEW election in a higher term
            # with a fresh random timeout — which is how split votes
            # resolve themselves instead of deadlocking.
            self._start_election()

    def start(self) -> None:
        self._running = True
        self._ticker = threading.Thread(target=self._run_ticker, daemon=True)
        self._ticker.start()

    def _run_ticker(self) -> None:
        while self._running:
            self._tick()
            # 20ms granularity: fine enough that timing is accurate
            # relative to our 1.5-3.0s timeouts, coarse enough to be
            # effectively free CPU-wise.
            time.sleep(0.02)

    def stop(self) -> None:
        self._running = False
