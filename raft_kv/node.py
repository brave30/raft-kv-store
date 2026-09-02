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
even wakes up. Randomness breaks the symmetry.

=============================================================================
PHASE 3: LOG REPLICATION — THE LOG MATCHING PROPERTY
=============================================================================
Electing a leader was the easy half. The hard half is getting the same
log onto every machine, given that followers can be missing entries, can
hold WRONG entries left behind by a leader that died mid-broadcast, and
can be offline for arbitrary stretches.

Raft maintains one invariant, the LOG MATCHING PROPERTY:

    If two logs contain an entry with the same INDEX and the same TERM,
    then the logs are IDENTICAL in every entry up through that index.

That's an extraordinarily strong claim — agreement about one entry
implies agreement about all of history before it. It's maintained by
induction, and the inductive step is a single check in AppendEntries:

    Every AppendEntries carries prev_log_index / prev_log_term: the
    position and term of the entry immediately BEFORE the new ones. A
    follower refuses the whole request unless its own log matches there.

Base case: both logs are empty and trivially agree at index 0. Inductive
step: a follower only ever appends at a point where it has confirmed
agreement, so agreement extends forward one entry at a time and can never
be silently violated.

When the check fails, the leader steps `next_index` backward for that
follower and tries again with an earlier position — walking back until it
finds the last point where the two logs agree, then overwriting
everything after it. The leader never modifies its OWN log to match a
follower. Leaders are always right, by definition; that's what the
election's up-to-dateness check bought us in Phase 2, and it's why a
leader can never be elected while missing a committed entry.
"""

import random
import threading
import time

from .log import LogEntry
from .rpc import send_rpc
from .state import PersistentState
from .store import KeyValueStore

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

# How long a client write waits for a majority to acknowledge before we
# give up and report failure. Generous relative to the heartbeat interval,
# because a write may need a round or two of log repair on a lagging
# follower before a majority has it.
COMMIT_WAIT_TIMEOUT = 3.0

# How long a linearizable read waits for a majority to confirm we are
# still leader. Shorter than COMMIT_WAIT_TIMEOUT: this is one round trip
# with no disk work, so if it hasn't happened quickly, something is wrong
# and the client is better served by a fast failure than a slow answer.
LEADERSHIP_CONFIRM_TIMEOUT = 1.5

# A forwarded request gets a longer deadline than a direct one, because it
# includes the leader's own commit wait on the far side. Without the extra
# headroom the follower would give up while the leader was still correctly
# working on the request.
FORWARD_TIMEOUT = COMMIT_WAIT_TIMEOUT + 2.0


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

        # --- Phase 3: commit tracking (volatile on every node) ---
        # commit_index: highest log index known to be on a majority, and
        #   therefore permanent — it can never be lost or overwritten.
        # last_applied: highest index actually fed into the store below.
        #   Always <= commit_index; the gap is entries decided but not yet
        #   replayed. These are volatile because they're both recoverable:
        #   on restart we replay the persisted log from the start, and the
        #   leader re-tells us the commit index in its next heartbeat.
        self.commit_index = 0
        self.last_applied = 0
        self.store = KeyValueStore()

        # --- Phase 3: leader-only state, reset at every election ---
        # next_index[peer]:  the next log index we intend to SEND them.
        #                    A GUESS — starts optimistic (our own last
        #                    index + 1) and walks backward on rejection.
        # match_index[peer]: the highest index we have CONFIRMED they
        #                    store. A FACT — only ever moves forward, and
        #                    only on a successful reply.
        #
        # Two variables rather than one because guesses and facts must not
        # be conflated: commit decisions are counted from match_index
        # only. Committing based on an optimistic guess would mean
        # declaring data safe that no follower actually has.
        self.next_index: dict[str, int] = {}
        self.match_index: dict[str, int] = {}

        # One lock guarding ALL mutable state above. Inbound RPCs arrive
        # on HTTP server threads while the ticker thread runs concurrently,
        # so every read-modify-write of node state must be serialized or
        # we'd get races (e.g. two threads both deciding they won the same
        # election). Held only briefly, and NEVER while doing network I/O.
        self.lock = threading.RLock()

        # Signalled whenever commit_index advances, so client writes
        # blocked in submit() wake immediately on commit instead of
        # polling for it.
        self._commit_changed = threading.Condition(self.lock)

        self._election_deadline = 0.0
        self._next_heartbeat = 0.0
        self._running = False
        self._ticker: threading.Thread | None = None
        self._reset_election_timer()

    # ------------------------------------------------------------------
    # Logging — the point of the demo is to WATCH this happen, so the
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
        self.next_index = {}
        self.match_index = {}
        if was != FOLLOWER:
            self.log(f"stepping down: {was} -> follower")
            # A deposed leader may have clients blocked in submit(). Wake
            # them so they fail fast rather than waiting for the full
            # timeout on a commit that is never going to happen.
            self._commit_changed.notify_all()

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

        # Reset our bookkeeping about every follower. We know nothing
        # about their logs yet, so:
        #   next_index = our last index + 1  — the optimistic guess that
        #       they're fully caught up. If wrong, the consistency check
        #       fails and we walk it back. Optimism costs one round trip
        #       when wrong; pessimism (starting at 1) would cost a full
        #       log replay for every follower at every election.
        #   match_index = 0 — we have confirmed NOTHING. Starting this
        #       optimistically would be a correctness bug, not just a
        #       slow path: we could commit an entry no follower has.
        last = self.state.last_log_index()
        self.next_index = {peer: last + 1 for peer in self.peers}
        self.match_index = {peer: 0 for peer in self.peers}

        # Append a no-op entry from OUR term. See _advance_commit_index
        # for the full explanation — briefly, a leader may not commit
        # entries from previous terms by majority count alone, so without
        # an entry of its own a new leader could be unable to commit
        # anything (including old, stranded entries) until a client
        # happens to write. The no-op removes that dependency on luck.
        self.state.append_entries([
            LogEntry(term=self.state.current_term,
                     index=last + 1,
                     command={"op": "NOOP"})
        ])

        # The no-op may be immediately committable (single-node cluster),
        # for the same reason as in submit(): we are a replica ourselves.
        self._advance_commit_index()

        # Send immediately rather than waiting up to HEARTBEAT_INTERVAL.
        # A brand-new leader wants to announce itself as fast as possible,
        # to stop other nodes timing out and starting a competing election.
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

        THIS IS WHAT MAKES PHASE 3 SAFE. Because a committed entry is on a
        majority, and a winner needs votes from a majority, and any two
        majorities overlap, at least one voter holds every committed
        entry — and this check makes that voter refuse anyone missing it.
        So a node lacking committed data can never become leader, and a
        leader is therefore always free to overwrite whatever followers
        disagree with. Election safety is what licenses leader authority.
        """
        our_index = self.state.last_log_index()
        our_term = self.state.last_log_term()
        if candidate_last_term != our_term:
            return candidate_last_term > our_term
        return candidate_last_index >= our_index

    def handle_append_entries(self, msg: dict) -> dict:
        """
        AppendEntries RPC — replication and heartbeat in one message.

        With an empty `entries` list this is a pure heartbeat (Phase 2).
        With entries, it's the leader replicating its log to us. Raft
        reuses one RPC for both jobs on purpose: the heartbeat isn't a
        separate mechanism bolted on, it's just the replication RPC with
        nothing to replicate. Fewer message types, fewer edge cases.

        The steps below are ordered deliberately; each depends on the
        previous one having been checked.
        """
        with self.lock:
            term = msg["term"]
            leader_id = msg["leader_id"]

            # --- Step 1: is the sender still a legitimate leader? ---
            # Stale leader talking. Reject; our higher term in the reply
            # tells them to step down.
            if term < self.state.current_term:
                return {"term": self.state.current_term, "success": False,
                        "conflict_index": 0}

            if term > self.state.current_term:
                self._become_follower(term, leader_id)
            elif self.role == CANDIDATE:
                # Same term, but someone else already won this election.
                # We lost. Step down gracefully rather than keep campaigning.
                self.log(f"lost election: {leader_id} is leader for this term")
                self._become_follower(term, leader_id)
            else:
                self.leader_id = leader_id

            # We've heard from the leader, so we're not starting an
            # election. Reset the timer BEFORE the consistency check
            # below: even a request we're about to reject came from a
            # live, legitimate leader, and deposing it for slowness while
            # it's actively repairing our log would be self-defeating.
            self._reset_election_timer()

            prev_index = msg["prev_log_index"]
            prev_term = msg["prev_log_term"]

            # --- Step 2: the consistency check (the inductive step) ---
            # Do we agree with the leader about the entry immediately
            # before the ones it's sending? If not, we must refuse the
            # whole batch — appending here would create a gap or splice
            # incompatible histories together, breaking the Log Matching
            # Property for every entry that follows.
            if prev_index > self.state.last_log_index():
                # We're simply too short. Tell the leader where our log
                # actually ends so it can skip straight there instead of
                # decrementing next_index one entry per round trip. With
                # a follower 10,000 entries behind, that difference is
                # 10,000 round trips versus one.
                self.log(f"rejecting append from {leader_id}: my log ends at "
                         f"{self.state.last_log_index()}, they assumed {prev_index}")
                return {"term": self.state.current_term, "success": False,
                        "conflict_index": self.state.last_log_index() + 1}

            if self.state.term_at(prev_index) != prev_term:
                # We have an entry there, but from a DIFFERENT term — it
                # came from a leader that has since been superseded, and
                # everything from that point on in our log is suspect.
                conflicting_term = self.state.term_at(prev_index)
                # Skip back over the entire run of entries from that bad
                # term: if one of them is wrong, all of them are, since
                # they came from the same deposed leader.
                conflict_index = prev_index
                while (conflict_index > 1
                       and self.state.term_at(conflict_index - 1) == conflicting_term):
                    conflict_index -= 1
                self.log(f"rejecting append from {leader_id}: term mismatch at "
                         f"{prev_index} (mine {conflicting_term}, theirs {prev_term})")
                return {"term": self.state.current_term, "success": False,
                        "conflict_index": conflict_index}

            # --- Step 3: splice in the new entries ---
            entries = [LogEntry.from_dict(e) for e in msg["entries"]]
            if entries:
                self._merge_entries(entries)

            # --- Step 4: adopt the leader's commit index ---
            # The leader piggybacks its commit index on every message, so
            # followers learn what's permanent without a separate RPC.
            leader_commit = msg["leader_commit"]
            if leader_commit > self.commit_index:
                # min() with our own last index is essential: the leader
                # may have committed entries it hasn't sent us yet, and we
                # must never claim to have committed an entry we don't
                # physically hold.
                self.commit_index = min(leader_commit, self.state.last_log_index())
                self._apply_committed()

            return {"term": self.state.current_term, "success": True,
                    "conflict_index": 0}

    def _merge_entries(self, entries: list[LogEntry]) -> None:
        """
        Append the leader's entries, truncating ours only where they
        genuinely conflict.

        THE SUBTLE BUG THIS AVOIDS: the obvious implementation is "delete
        everything from prev_index+1 onward, then append." That is wrong,
        and dangerously so. The network can deliver a DUPLICATE or DELAYED
        AppendEntries — say an old one carrying entries 5-6 arriving after
        we've already accepted 5-10. Blind truncation would delete 7-10,
        which may already be COMMITTED and applied on other nodes. We'd be
        destroying permanent data because a stale packet showed up late.

        So we only truncate at a position where the terms actually differ.
        Entries we already hold with a matching term are left untouched
        (by the Log Matching Property they're identical anyway), which
        makes this RPC idempotent — replaying it changes nothing.
        """
        for offset, entry in enumerate(entries):
            existing_term = self.state.term_at(entry.index)
            if entry.index > self.state.last_log_index():
                # Past the end of our log — everything from here is new.
                self.state.append_entries(entries[offset:])
                self.log(f"appended entries {entry.index}.."
                         f"{entries[-1].index} from leader")
                return
            if existing_term != entry.term:
                # A real conflict. Now truncation IS correct: this entry
                # and everything after it came from a superseded leader.
                self.state.overwrite_from(entry.index, entries[offset:])
                self.log(f"conflict at {entry.index} (mine term {existing_term}, "
                         f"leader term {entry.term}) -> overwrote from there")
                return
        # Fell through: we already had every entry, at matching terms.
        # A duplicate or retried request. Correctly a no-op.

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
                "commit_index": self.commit_index,
                "last_applied": self.last_applied,
                "store": self.store.snapshot(),
            }

    # ==================================================================
    # APPLYING COMMITTED ENTRIES
    # ==================================================================

    def _apply_committed(self) -> None:
        """
        Feed newly-committed entries into the key-value store, in order.

        Called on every node — followers apply too, which is what keeps
        their stores identical to the leader's and lets them serve reads
        (and take over instantly on failover) without replaying anything.

        Takes the lock itself. self.lock is an RLock precisely so that
        methods like this can be safely re-entered by callers that already
        hold it, rather than every call site having to remember an
        undocumented "you must hold the lock" contract.
        """
        with self.lock:
            self._apply_committed_locked()

    def _apply_committed_locked(self) -> None:
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.state.entry_at(self.last_applied)
            if entry is None:
                # Can't happen if commit_index is maintained correctly —
                # so if it does, we want to know immediately rather than
                # apply a silently wrong prefix of the log.
                raise RuntimeError(
                    f"commit_index {self.commit_index} exceeds log "
                    f"(len {len(self.state.log)}) — commit tracking is broken")
            self.store.apply(entry.command)
            if entry.command.get("op") != "NOOP":
                self.log(f"applied #{entry.index}: {entry.command}")

    def _advance_commit_index(self) -> None:
        """
        LEADER ONLY: figure out how far the log is now safely committed.

        An entry is committed once it's stored on a MAJORITY of nodes.
        We find the highest index N replicated on a majority (counting
        ourselves, since we obviously have our own entries) and commit
        up to it. Because of the Log Matching Property, committing N
        implicitly commits everything before it — no need to check each.

        =====================================================================
        THE ONE-LINE RULE THAT LOOKS WRONG AND ISN'T
        =====================================================================
        Notice the extra condition: we only commit N if the entry at N is
        from our OWN CURRENT TERM. An entry from an earlier term is never
        committed by majority count alone, even when a clear majority
        physically holds it. This is Figure 8 of the Raft paper, and it is
        the single most counter-intuitive rule in the algorithm.

        Why isn't "a majority has it" enough? Because for an OLD entry,
        majority storage is not yet permanent — it can still be overwritten:

          1. S1 is leader in term 2. It appends entry X at index 3 and
             replicates it to S2. Now 2 of 5 nodes have X. S1 crashes.
          2. S5 is elected leader for term 3 (S3, S4 and itself vote for
             it — none of them have X, but their logs are legal choices
             since X was never committed). S5 crashes before doing much.
          3. S1 comes back and is elected leader for term 4. It continues
             replicating its OLD term-2 entry X, and now S3 has it too.
             X is now on a MAJORITY (S1, S2, S3). If S1 committed X here
             on the strength of that count alone, X would be permanent.
          4. But S1 crashes again, and S5 gets elected for term 5 — legally,
             because its log ends in term 3, which beats the term-2 entries
             on S2 and S3 under the up-to-dateness rule. S5 now overwrites
             index 3 on everyone with its own entry.

          X was on a majority in step 3, and is GONE in step 4. If we had
          reported "committed" to a client, we would have lied.

        The fix: commit an entry from the current term instead. Doing so
        commits everything before it as a side effect — safely, because a
        current-term entry on a majority guarantees any future leader must
        have it (up-to-dateness makes a log ending in an older term
        unelectable), so nothing at or before it can ever be overwritten.

        This is exactly why _become_leader appends a NOOP: it guarantees
        the leader always has a current-term entry available to commit,
        so old stranded entries get carried to safety immediately rather
        than waiting for a client write that might never arrive.

        Takes the lock itself (see _apply_committed).
        """
        with self.lock:
            if self.role != LEADER:
                return

            for n in range(self.state.last_log_index(), self.commit_index, -1):
                if self.state.term_at(n) != self.state.current_term:
                    # Old-term entry: not committable on its own. Keep
                    # scanning downward — but any lower index is also
                    # older, so in practice this ends the useful search.
                    continue
                # Count ourselves plus every follower confirmed to hold n.
                replicas = 1 + sum(1 for m in self.match_index.values() if m >= n)
                if replicas >= self._majority():
                    self.commit_index = n
                    self.log(f"committed up to #{n} "
                             f"({replicas}/{len(self.peers) + 1} replicas)")
                    self._apply_committed_locked()
                    # Wake any client blocked in submit() waiting on this.
                    self._commit_changed.notify_all()
                    return

    # ==================================================================
    # CLIENT WRITES
    # ==================================================================

    def submit(self, command: dict) -> dict:
        """
        Accept a client command, replicate it, and block until it commits.

        Returns a dict with ok=True once a majority has the entry durably
        on disk. Only at that point is it honest to tell a client the
        write succeeded — that's the whole promise of the system.

        Non-leaders refuse and name the leader instead of forwarding.
        Forwarding is Phase 4's job; refusing with a hint keeps the data
        path here honest about who is actually allowed to accept writes.
        """
        with self.lock:
            if self.role != LEADER:
                return {"ok": False, "error": "not_leader", "leader_id": self.leader_id}

            index = self.state.last_log_index() + 1
            term = self.state.current_term
            # Persist to OUR disk first. We're one of the replicas that
            # counts toward the majority, so our own durability is part
            # of the guarantee, not an afterthought.
            self.state.append_entries([
                LogEntry(term=term, index=index, command=command)
            ])
            self.log(f"accepted write #{index}: {command}")

        # Push it out immediately instead of waiting for the next
        # scheduled heartbeat — that's up to HEARTBEAT_INTERVAL of pure
        # latency on every write, for no reason.
        self._replicate_to_all()

        # Our own disk write counts toward the majority, so check whether
        # the entry is ALREADY committed without anyone replying. In a
        # single-node cluster (majority = 1) that's always true, and if we
        # only advanced the commit index when a follower replied, such a
        # node would block here forever waiting for peers that don't
        # exist. Same shape of bug as a candidate not counting its own
        # vote: the leader is a replica too, and forgetting that strands
        # the degenerate case.
        self._advance_commit_index()

        deadline = time.monotonic() + COMMIT_WAIT_TIMEOUT
        with self.lock:
            while self.commit_index < index:
                # Re-check leadership on every wakeup. If we were deposed
                # while waiting, this entry may be overwritten by the new
                # leader, so we must NOT report success.
                if self.role != LEADER or self.state.current_term != term:
                    return {"ok": False, "error": "leadership_lost",
                            "leader_id": self.leader_id}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    # Timed out. Note carefully: this does NOT mean the
                    # write failed — it may commit moments later. It means
                    # we cannot yet CONFIRM it. An honest client retries,
                    # which is safe as long as commands are idempotent
                    # (SET is; a hypothetical INCREMENT would not be).
                    return {"ok": False, "error": "commit_timeout", "index": index}
                self._commit_changed.wait(remaining)

            return {"ok": True, "index": index, "term": term}

    def handle_client_write(self, msg: dict) -> dict:
        """
        HTTP entry point for a write.

        If we're not the leader we FORWARD to whoever is, rather than
        bouncing the client back with a redirect. From the client's point
        of view any node accepts writes, which is what you want from a
        cluster — the client shouldn't have to track leadership, which can
        change between the redirect and the retry anyway.
        """
        command = msg.get("command")
        if not isinstance(command, dict) or "op" not in command:
            return {"ok": False, "error": "bad_command"}

        with self.lock:
            is_leader = self.role == LEADER
            leader_id = self.leader_id

        if is_leader:
            return self.submit(command)

        return self._forward("/write", msg, leader_id,
                             already_forwarded=msg.get("forwarded", False))

    def _forward(self, path: str, msg: dict, leader_id: str | None,
                 already_forwarded: bool) -> dict:
        """
        Pass a client request along to the leader and relay its answer.

        The `forwarded` flag prevents an infinite loop. Consider two nodes
        that each believe the other is leader — briefly possible during a
        changeover. Without the flag they would bounce the request back
        and forth until something timed out, consuming a thread on each
        node per hop. One hop is always enough: if the node we forward to
        isn't the leader either, the client should retry rather than have
        us chase leadership around the cluster.
        """
        if already_forwarded:
            # We were forwarded to, but we're not the leader either. Don't
            # forward again — report honestly and let the client retry.
            return {"ok": False, "error": "not_leader", "leader_id": leader_id}

        if leader_id is None or leader_id not in self.peers:
            # No known leader: either an election is in progress right now,
            # or we're partitioned away from the cluster. Both are genuinely
            # "try again shortly" conditions, not permanent failures.
            return {"ok": False, "error": "no_known_leader", "leader_id": None}

        url = self.peers[leader_id]
        forwarded = dict(msg)
        forwarded["forwarded"] = True
        reply = send_rpc(f"{url}{path}", forwarded, timeout=FORWARD_TIMEOUT)
        if reply is None:
            # The leader we knew about didn't answer — it may have just
            # died. Our election timer will notice shortly.
            return {"ok": False, "error": "leader_unreachable",
                    "leader_id": leader_id}
        reply["forwarded_by"] = self.node_id
        return reply

    def handle_client_read(self, msg: dict) -> dict:
        """
        Read a key.

        Two consistency levels, chosen by the caller:

          "linearizable" (default) — the real guarantee. The read is
              served by the leader, and only after the leader has proven
              it is STILL the leader. Any value returned reflects every
              write that had been acknowledged before the read began.

          "local" — read whatever this node happens to have applied. Fast
              and needs no network, but a follower may be behind and a
              deposed leader may not know it yet. Useful when staleness is
              acceptable (dashboards, caches) and worth offering
              explicitly so the choice is visible rather than accidental.

        Offering both, and defaulting to the safe one, is the point: the
        weaker mode is a decision the caller makes on purpose.
        """
        key = msg.get("key")
        consistency = msg.get("consistency", "linearizable")

        if consistency == "local":
            with self.lock:
                return {
                    "ok": True, "key": key, "value": self.store.get(key),
                    "consistency": "local", "role": self.role,
                    "leader_id": self.leader_id,
                    "applied_index": self.last_applied,
                }

        if consistency != "linearizable":
            return {"ok": False, "error": "bad_consistency_level"}

        with self.lock:
            is_leader = self.role == LEADER
            leader_id = self.leader_id

        if not is_leader:
            return self._forward("/read", msg, leader_id,
                                 already_forwarded=msg.get("forwarded", False))

        return self._linearizable_read(key)

    def _linearizable_read(self, key: str) -> dict:
        """
        The ReadIndex algorithm. Three steps, each necessary:

          1. Make sure we've committed an entry from our OWN term. A
             brand-new leader may not yet know which older entries are
             committed — it knows its log is complete (the election
             guaranteed that), but not how far commitment reached under
             the previous leader. Committing one of our own entries
             resolves that. The no-op appended at election time is exactly
             this entry, so in practice this wait is already satisfied.

          2. Record the current commit index, THEN confirm leadership with
             a majority. The order matters: capturing the index first and
             confirming after means the confirmed round vouches for an
             index that was already decided when we started, so no write
             can sneak in behind our answer.

          3. Wait until we've APPLIED up to that index before reading the
             store. commit_index is what the cluster has decided;
             last_applied is what our local store actually reflects.
             Reading between the two would miss a write that is already
             committed and therefore already acknowledged to some client.
        """
        with self.lock:
            if self.role != LEADER:
                return {"ok": False, "error": "not_leader",
                        "leader_id": self.leader_id}
            # Step 1
            if self.state.term_at(self.commit_index) != self.state.current_term:
                return {"ok": False, "error": "leader_not_ready",
                        "detail": "no entry from the current term is committed yet"}
            # Step 2 (capture before confirming)
            read_index = self.commit_index
            term = self.state.current_term

        if not self._confirm_still_leader():
            # We could not prove current leadership. We may well still be
            # leader — but "probably" is not a guarantee, and the whole
            # point of this path is to never return a value we can't
            # stand behind. Fail instead.
            return {"ok": False, "error": "leadership_not_confirmed",
                    "detail": "could not reach a majority; may have been partitioned"}

        # Step 3
        deadline = time.monotonic() + LEADERSHIP_CONFIRM_TIMEOUT
        with self.lock:
            while self.last_applied < read_index:
                if self.role != LEADER or self.state.current_term != term:
                    return {"ok": False, "error": "leadership_lost",
                            "leader_id": self.leader_id}
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return {"ok": False, "error": "apply_timeout"}
                self._commit_changed.wait(remaining)

            return {
                "ok": True, "key": key, "value": self.store.get(key),
                "consistency": "linearizable", "role": self.role,
                "leader_id": self.node_id, "read_index": read_index,
                "applied_index": self.last_applied,
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

    def _replicate_to_all(self) -> None:
        """
        Send AppendEntries to every follower.

        Each follower gets a DIFFERENT message, because each is at a
        different point in the log: a caught-up follower gets an empty
        heartbeat, while one that's behind gets the entries it's missing.
        This is why next_index is per-peer.
        """
        with self.lock:
            if self.role != LEADER:
                return
            work = [(peer_id, self.peers[peer_id], self._build_append_payload(peer_id))
                    for peer_id in self.peers]
            leader_term = self.state.current_term

        for peer_id, url, payload in work:
            threading.Thread(
                target=self._replicate_one,
                args=(peer_id, url, payload, leader_term),
                daemon=True,
            ).start()

    def _build_append_payload(self, peer_id: str) -> dict:
        """
        Construct the AppendEntries message for one specific follower.

        Caller must hold the lock (this reads next_index and the log
        together, and they must not change between the two reads).
        """
        next_idx = self.next_index.get(peer_id, 1)
        prev_index = next_idx - 1
        return {
            "term": self.state.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_index,
            "prev_log_term": self.state.term_at(prev_index),
            "entries": [e.to_dict() for e in self.state.log if e.index >= next_idx],
            "leader_commit": self.commit_index,
        }

    def _replicate_one(self, peer_id: str, url: str, payload: dict,
                       leader_term: int) -> bool:
        """
        Send one AppendEntries and process the reply.

        Returns True if the follower ACCEPTED it — meaning it recognises
        us as leader for this term. _confirm_still_leader counts those
        acceptances to prove our leadership is current.
        """
        reply = send_rpc(f"{url}/append_entries", payload)
        if reply is None:
            return False  # Follower down. We'll retry on the next heartbeat.

        with self.lock:
            # Same staleness check as in the vote path: this reply may
            # describe a world we've already left.
            if self.role != LEADER or self.state.current_term != leader_term:
                return False

            # A follower can inform a leader that it's been deposed — this
            # is how a leader that was network-partitioned away rejoins the
            # cluster and stops acting as leader.
            if reply["term"] > self.state.current_term:
                self.log(f"append rejected by {peer_id}: term "
                         f"{reply['term']} > ours. Stepping down.")
                self._become_follower(reply["term"])
                return False

            if reply["success"]:
                # They now hold everything we sent. Record it as FACT.
                # Computed from what we sent, not from their last index —
                # they may be ahead of us with junk from a dead leader
                # that we haven't overwritten yet.
                sent = payload["entries"]
                if sent:
                    new_match = sent[-1]["index"]
                    # max() because replies can arrive out of order; a
                    # stale reply must never drag match_index backward.
                    self.match_index[peer_id] = max(
                        self.match_index.get(peer_id, 0), new_match)
                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                else:
                    self.match_index[peer_id] = max(
                        self.match_index.get(peer_id, 0), payload["prev_log_index"])
                # A follower catching up may have just made a majority.
                self._advance_commit_index()
                return True
            else:
                # Consistency check failed: our guess about where their
                # log ends was too optimistic. Back off and retry from the
                # position they suggested.
                hint = reply.get("conflict_index") or 1
                old = self.next_index.get(peer_id, 1)
                self.next_index[peer_id] = max(1, min(old - 1, hint))
                self.log(f"{peer_id} rejected append; next_index "
                         f"{old} -> {self.next_index[peer_id]}, retrying")
                # Retry immediately rather than waiting for the next
                # heartbeat — repairing a badly lagging follower one
                # heartbeat per step would take a very long time.
                threading.Thread(target=self._replicate_to_peer,
                                 args=(peer_id,), daemon=True).start()
                # A rejection still proves this follower accepts our
                # TERM (it would have reported a higher one otherwise),
                # so it counts as a leadership acknowledgement even
                # though the log entries weren't accepted.
                return True

    def _replicate_to_peer(self, peer_id: str) -> None:
        """Send AppendEntries to ONE follower, using its current next_index."""
        with self.lock:
            if self.role != LEADER or peer_id not in self.peers:
                return
            payload = self._build_append_payload(peer_id)
            leader_term = self.state.current_term
            url = self.peers[peer_id]

        self._replicate_one(peer_id, url, payload, leader_term)

    def _confirm_still_leader(self, timeout: float = LEADERSHIP_CONFIRM_TIMEOUT) -> bool:
        """
        Prove, right now, that we are still the leader — by getting a
        majority of the cluster to accept an AppendEntries from us.

        WHY A LEADER CANNOT JUST TRUST ITSELF:
        Nothing tells a leader it has been deposed. If the network
        partitions us away from the cluster, we keep believing we're
        leader — our own term never changes, and no message arrives to
        correct us. Meanwhile the majority on the other side elects a new
        leader and starts committing writes. We are now a "zombie leader":
        confident, and wrong. Serving a read from our own state here would
        return data that is out of date by an unbounded amount.

        The proof: if a MAJORITY still accepts a message at our term, then
        no leader of a higher term can have been elected — electing one
        would itself require a majority, and any two majorities overlap,
        so at least one node would have had to both accept us and vote for
        a higher term, which it cannot do. So a successful round here means
        that as of the moment it started, we really were the only leader.

        Note this costs a network round trip per read. That's the honest
        price of a linearizable read. Real systems soften it by batching
        concurrent reads into one confirmation round, or by using a
        "leader lease" — trusting an election-timeout-long lease instead
        of confirming, which is faster but trades a safety proof for a
        clock-drift assumption. We do the simple, provably-correct thing.
        """
        with self.lock:
            if self.role != LEADER:
                return False
            term = self.state.current_term
            work = [(peer_id, self.peers[peer_id], self._build_append_payload(peer_id))
                    for peer_id in self.peers]
            needed = self._majority()

        # We count as one acknowledgement — we're a member of the cluster.
        # In a single-node cluster this is already a majority, so the loop
        # below never runs and the read costs nothing.
        acks = 1
        acks_lock = threading.Lock()
        reached = threading.Event()
        if acks >= needed:
            reached.set()

        def probe(peer_id: str, url: str, payload: dict) -> None:
            nonlocal acks
            # Reuse the normal replication path so this round also does
            # useful work: it carries entries, updates match_index, and
            # steps us down if a follower reports a higher term.
            if self._replicate_one(peer_id, url, payload, term):
                with acks_lock:
                    acks += 1
                    if acks >= needed:
                        reached.set()

        for peer_id, url, payload in work:
            threading.Thread(target=probe, args=(peer_id, url, payload),
                             daemon=True).start()

        reached.wait(timeout)

        with self.lock:
            # Even with a majority of acks, re-check that we didn't step
            # down partway through (a follower may have reported a higher
            # term while other probes were succeeding).
            return (reached.is_set()
                    and self.role == LEADER
                    and self.state.current_term == term)

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
              -> replicate (which doubles as the heartbeat)
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
                self._replicate_to_all()
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
