"""
Chaos test: hammer the cluster with writes while killing nodes at random,
then prove that nothing acknowledged was ever lost.

    python scripts/chaos_test.py                    # 45s default run
    python scripts/chaos_test.py --duration 120     # longer = more interleavings
    python scripts/chaos_test.py --seed 42          # reproduce a specific run

=============================================================================
WHAT THIS IS ACTUALLY TESTING
=============================================================================
The unit tests check rules in isolation: does a node refuse a second vote,
does it truncate correctly, does a majority commit. They're fast and
precise, but they only cover situations someone thought to write down.

Consensus bugs don't live there. They live in interleavings nobody
imagined — a leader dying between appending an entry and replicating it,
a node restarting mid-election, a write landing exactly as leadership
changes hands. You cannot enumerate those cases. What you CAN do is
generate them randomly, at volume, and check an invariant that must hold
no matter what happened in between.

=============================================================================
THE ORACLE: WHAT WE CHECK, AND WHY IT'S THE HARD PART
=============================================================================
Randomly killing processes is easy. Deciding what "correct" means
afterwards is the part that takes thought — a weak oracle silently passes
on real bugs, and a wrong one cries wolf on correct behaviour.

The single most important insight here:

    A FAILED REQUEST IS NOT PROOF THE OPERATION DIDN'T HAPPEN.

If a write times out, the entry may already be on a majority and commit a
moment later. The client genuinely cannot tell "never happened" from
"happened but I didn't hear back." (We watched this occur for real in
Phase 4: an isolated leader's write timed out, then committed once quorum
returned.) So every outcome falls into one of three buckets:

  MUST be present   — the client received ok. This is the durability
                      promise, and a violation is a serious bug: we told
                      someone their data was safe and then lost it.

  MAY be present    — timeout, error, or no answer at all. Either result
                      is correct. Asserting these are ABSENT would be the
                      classic mistake, and would fail constantly on a
                      perfectly healthy system.

  MUST NOT exist    — a key that was never written by anyone. Its
                      appearance means fabricated or corrupted data.

We also check, at the end, that every node holds byte-identical data.
That's the state machine safety property: identical logs, applied in
identical order, must produce identical state.

Writes use unique keys per writer, which keeps the oracle exact — no
ambiguity about which of several concurrent writes to the same key
"should" have won. One shared hot key is tracked separately (see
HOT_KEY below) to exercise contention on a single entry.
"""

import argparse
import json
import os
import random
import subprocess
import sys
import threading
import time
import urllib.request

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")
sys.path.insert(0, ROOT)

from raft_kv.client import (RaftClient, ClusterUnavailable,
                            WriteOutcomeUnknown)

NODES = {"node1": 5001, "node2": 5002, "node3": 5003}

# A single key every writer competes over, on top of their private keys.
# Private keys prove nothing is lost; the hot key exercises many writes
# landing on the same log position under leadership churn.
HOT_KEY = "hot"


class Cluster:
    """Starts, kills and restarts node processes."""

    def __init__(self, quiet: bool = True):
        self.procs: dict[str, subprocess.Popen] = {}
        self.quiet = quiet
        self.lock = threading.Lock()

    def _peers(self, node_id: str) -> str:
        return ",".join(f"{o}=http://127.0.0.1:{p}"
                        for o, p in NODES.items() if o != node_id)

    def start(self, node_id: str) -> None:
        with self.lock:
            existing = self.procs.get(node_id)
            if existing and existing.poll() is None:
                return
            proc = subprocess.Popen(
                [sys.executable, "-u", "-m", "raft_kv.server",
                 "--id", node_id, "--port", str(NODES[node_id]),
                 "--peers", self._peers(node_id), "--data-dir", DATA_DIR],
                cwd=ROOT,
                stdout=subprocess.DEVNULL if self.quiet else None,
                stderr=subprocess.DEVNULL if self.quiet else None,
            )
            self.procs[node_id] = proc

    def kill(self, node_id: str) -> bool:
        """
        Hard kill — SIGKILL equivalent, no cleanup, no goodbye to peers.

        Deliberately brutal. A graceful shutdown would let a node hand
        off or flush state, which is exactly the courtesy a real crash
        never extends. The whole point is to verify that data already
        written to disk survives without any cooperation from the dying
        process.
        """
        with self.lock:
            proc = self.procs.get(node_id)
            if proc is None or proc.poll() is not None:
                return False
            proc.kill()
            proc.wait()
            return True

    def alive(self) -> list[str]:
        with self.lock:
            return [n for n, p in self.procs.items() if p.poll() is None]

    def stop_all(self) -> None:
        with self.lock:
            for proc in self.procs.values():
                if proc.poll() is None:
                    proc.kill()
                    proc.wait()


def node_status(node_id: str, timeout: float = 1.0) -> dict | None:
    try:
        url = f"http://127.0.0.1:{NODES[node_id]}/status"
        with urllib.request.urlopen(url, timeout=timeout) as response:
            return json.loads(response.read().decode())
    except Exception:
        return None


# ======================================================================
# The workload
# ======================================================================

class Writer(threading.Thread):
    """
    One client writing as fast as it can, recording what it was told.

    Each writer owns a private key namespace (w0-, w1-, ...) so that its
    writes can be verified exactly. Every outcome is recorded — including
    failures, because "was allowed to fail" is part of what we verify.
    """

    def __init__(self, writer_id: int, client: RaftClient, stop_event: threading.Event):
        super().__init__(daemon=True)
        self.writer_id = writer_id
        self.client = client
        self.stop_event = stop_event

        # key -> value that the cluster CONFIRMED. Must survive.
        self.acknowledged: dict[str, str] = {}
        # key -> value where we never got a definitive answer. May or may
        # not be present at the end; both are correct.
        self.unknown: dict[str, str] = {}
        self.hot_writes: list[tuple[str, bool]] = []  # (value, acknowledged)

        self.counts = {"ok": 0, "unknown": 0, "unavailable": 0}

    def run(self) -> None:
        seq = 0
        while not self.stop_event.is_set():
            seq += 1
            key = f"w{self.writer_id}-{seq}"
            value = f"v{seq}"
            try:
                self.client.set(key, value)
                self.acknowledged[key] = value
                self.counts["ok"] += 1
            except WriteOutcomeUnknown:
                # Appended by a leader but not confirmed committed. It may
                # yet appear. Recording it as "unknown" rather than
                # "failed" is the whole point of the oracle.
                self.unknown[key] = value
                self.counts["unknown"] += 1
            except ClusterUnavailable:
                # No node could service it within the retry window. The
                # last attempt may still have reached a leader that
                # committed it after we stopped listening, so this is ALSO
                # only "unknown" — never "definitely didn't happen."
                self.unknown[key] = value
                self.counts["unavailable"] += 1

            # The contended key. Tracked separately because concurrent
            # writers overwrite each other, so no single value is
            # guaranteed at the end — only that the final value is one
            # somebody actually wrote.
            hot_value = f"w{self.writer_id}-{seq}"
            try:
                self.client.set(HOT_KEY, hot_value)
                self.hot_writes.append((hot_value, True))
            except (WriteOutcomeUnknown, ClusterUnavailable):
                self.hot_writes.append((hot_value, False))

            time.sleep(0.01)


class ChaosMonkey(threading.Thread):
    """
    Kills and restarts nodes at random while the writers work.

    Two modes, chosen at random each round:

      MINORITY FAILURE (usual) — kill one node. A majority survives, so
      the cluster should keep accepting writes throughout. Any data loss
      here would be a serious bug.

      MAJORITY FAILURE (occasional) — kill two of three. The survivor
      cannot reach a majority, so writes MUST stall. This is not a bug,
      it's Raft choosing consistency over availability; we include it to
      confirm the cluster refuses rather than inventing an answer, and
      that everything recovers once quorum returns.
    """

    def __init__(self, cluster: Cluster, rng: random.Random,
                 stop_event: threading.Event, log: list):
        super().__init__(daemon=True)
        self.cluster = cluster
        self.rng = rng
        self.stop_event = stop_event
        self.log = log
        self.kills = 0
        self.leader_kills = 0
        self.quorum_losses = 0

    def _current_leader(self, alive: list[str]) -> str | None:
        for node_id in alive:
            status = node_status(node_id, timeout=0.5)
            if status and status["role"] == "leader":
                return node_id
        return None

    def run(self) -> None:
        # Let the cluster elect a leader and take some writes first, so
        # we're disrupting a working system rather than the startup path.
        self.stop_event.wait(3.0)

        while not self.stop_event.is_set():
            alive = self.cluster.alive()
            if len(alive) < len(NODES):
                # Something is down: restore it before causing more havoc.
                down = [n for n in NODES if n not in alive]
                node = self.rng.choice(down)
                self.cluster.start(node)
                self.log.append((time.time(), f"restart {node}"))
                self.stop_event.wait(self.rng.uniform(2.0, 4.0))
                continue

            if self.rng.random() < 0.2:
                # Majority failure: take out two, briefly. Include the
                # leader when we can — losing quorum AND the leader at
                # once is the harshest realistic combination.
                leader = self._current_leader(alive)
                others = [n for n in alive if n != leader]
                if leader and others:
                    victims = [leader, self.rng.choice(others)]
                else:
                    victims = self.rng.sample(sorted(alive), min(2, len(alive)))
                for victim in victims:
                    if self.cluster.kill(victim):
                        self.kills += 1
                self.quorum_losses += 1
                self.log.append((time.time(), f"KILL {' + '.join(victims)} "
                                              f"(quorum lost - writes must stall)"))
                self.stop_event.wait(self.rng.uniform(1.5, 3.0))
            else:
                # Bias hard toward killing the LEADER. A uniformly random
                # victim mostly hits followers, which barely disturbs the
                # cluster — an early version of this test ran to
                # completion without ever triggering a single election,
                # and so proved far less than it appeared to. Failover is
                # the interesting path: it's where a half-replicated entry
                # from a dead leader either survives correctly or is
                # correctly discarded.
                leader = self._current_leader(alive)
                if leader and self.rng.random() < 0.75:
                    victim, targeted = leader, True
                else:
                    victim, targeted = self.rng.choice(alive), False

                if self.cluster.kill(victim):
                    self.kills += 1
                    if targeted or victim == leader:
                        self.leader_kills += 1
                        self.log.append((time.time(),
                                         f"kill {victim} (THE LEADER - forces an election)"))
                    else:
                        self.log.append((time.time(), f"kill {victim}"))
                self.stop_event.wait(self.rng.uniform(1.0, 3.0))


# ======================================================================
# Verification
# ======================================================================

def wait_for_convergence(timeout: float = 45.0) -> tuple[bool, dict]:
    """
    Wait until every node is up, has a leader, and holds identical data.

    Convergence is not instant after chaos stops: a restarted node has to
    catch up on everything it missed. Polling until it settles (rather
    than sleeping a fixed guess) is both faster and more honest — and if
    it never settles, that itself is the bug.
    """
    deadline = time.monotonic() + timeout
    last = {}
    while time.monotonic() < deadline:
        statuses = {n: node_status(n) for n in NODES}
        last = statuses
        if any(s is None for s in statuses.values()):
            time.sleep(0.5)
            continue
        if not any(s["role"] == "leader" for s in statuses.values()):
            time.sleep(0.5)
            continue
        stores = [json.dumps(s["store"], sort_keys=True) for s in statuses.values()]
        commits = {s["commit_index"] for s in statuses.values()}
        if len(set(stores)) == 1 and len(commits) == 1:
            return True, statuses
        time.sleep(0.5)
    return False, last


def verify(writers: list[Writer], statuses: dict, client: RaftClient) -> list[str]:
    """
    Check every invariant. Returns a list of violations (empty == pass).
    """
    violations: list[str] = []

    # ---- 1. Every node holds identical data ----------------------------
    stores = {n: s["store"] for n, s in statuses.items()}
    reference_node, reference = next(iter(stores.items()))
    for node_id, store in stores.items():
        if store != reference:
            only_here = set(store) - set(reference)
            only_there = set(reference) - set(store)
            differing = {k for k in set(store) & set(reference)
                         if store[k] != reference[k]}
            violations.append(
                f"DIVERGENCE: {node_id} differs from {reference_node}: "
                f"{len(only_here)} extra, {len(only_there)} missing, "
                f"{len(differing)} conflicting values "
                f"(e.g. {sorted(differing)[:3] or sorted(only_here)[:3]})")

    # ---- 2. Every acknowledged write is present and correct ------------
    # This is THE durability check. A failure here means we told a client
    # their data was safe and then lost it.
    total_acked = 0
    for writer in writers:
        for key, value in writer.acknowledged.items():
            total_acked += 1
            actual = reference.get(key)
            if actual is None:
                violations.append(
                    f"LOST ACKNOWLEDGED WRITE: {key} was confirmed committed "
                    f"but is absent from the final state")
            elif actual != value:
                violations.append(
                    f"CORRUPTED VALUE: {key} was acknowledged as {value!r} "
                    f"but reads back as {actual!r}")

    # ---- 3. No fabricated keys -----------------------------------------
    # Everything present must have been written by somebody. An unexpected
    # key would mean corrupted or invented data.
    expected_keys = {HOT_KEY}
    for writer in writers:
        expected_keys |= set(writer.acknowledged)
        expected_keys |= set(writer.unknown)
    unexpected = set(reference) - expected_keys
    if unexpected:
        violations.append(
            f"FABRICATED KEYS: {len(unexpected)} keys exist that were never "
            f"written (e.g. {sorted(unexpected)[:5]})")

    # ---- 4. The hot key holds a value someone actually wrote -----------
    # Concurrent writers overwrite each other, so we can't predict WHICH
    # value wins — but it must be one that was really submitted, not a
    # mixture, a stale resurrection, or something invented.
    hot_value = reference.get(HOT_KEY)
    all_hot = {v for w in writers for v, _ in w.hot_writes}
    if hot_value is None:
        if all_hot:
            violations.append("HOT KEY MISSING: writes were made but the key is absent")
    elif hot_value not in all_hot:
        violations.append(
            f"HOT KEY INVENTED: final value {hot_value!r} was never written")

    # ---- 5. The read path agrees with the replicated state -------------
    # Verifies linearizable reads against a sample of known-good keys —
    # the store could be right while the read path serves something else.
    sample = []
    for writer in writers:
        sample.extend(list(writer.acknowledged.items())[:5])
    for key, value in sample[:20]:
        try:
            got = client.get(key)
            if got != value:
                violations.append(
                    f"READ PATH DISAGREES: {key} is {value!r} in the store "
                    f"but a linearizable read returned {got!r}")
        except (ClusterUnavailable, WriteOutcomeUnknown) as e:
            violations.append(f"READ FAILED after recovery: {key}: {e}")

    return violations


# ======================================================================

def main() -> None:
    parser = argparse.ArgumentParser(description="Chaos-test the Raft cluster.")
    parser.add_argument("--duration", type=float, default=45.0,
                        help="seconds of chaos (longer finds more)")
    parser.add_argument("--writers", type=int, default=3)
    parser.add_argument("--seed", type=int, default=None,
                        help="RNG seed; printed each run so a failure can be replayed")
    parser.add_argument("--verbose", action="store_true",
                        help="show node output instead of suppressing it")
    args = parser.parse_args()

    # A seeded RNG makes a failing run REPRODUCIBLE. Without this, a
    # chaos test that finds a bug once may never find it again, which
    # makes the bug nearly impossible to fix. Always print the seed.
    seed = args.seed if args.seed is not None else random.randrange(1 << 30)
    rng = random.Random(seed)

    os.makedirs(DATA_DIR, exist_ok=True)
    for name in os.listdir(DATA_DIR):
        if name.endswith("_state.json") or name.endswith(".tmp"):
            os.remove(os.path.join(DATA_DIR, name))

    print("=" * 70)
    print("CHAOS TEST")
    print(f"  seed      : {seed}   (rerun with --seed {seed})")
    print(f"  duration  : {args.duration}s")
    print(f"  writers   : {args.writers}")
    print(f"  cluster   : {len(NODES)} nodes, majority = {len(NODES) // 2 + 1}")
    print("=" * 70)

    cluster = Cluster(quiet=not args.verbose)
    client = RaftClient([f"http://127.0.0.1:{p}" for p in NODES.values()])
    stop_event = threading.Event()
    event_log: list = []

    for node_id in NODES:
        cluster.start(node_id)

    print("\nwaiting for the cluster to elect a leader...")
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if any((s := node_status(n)) and s["role"] == "leader" for n in NODES):
            break
        time.sleep(0.3)
    else:
        cluster.stop_all()
        raise SystemExit("cluster never elected a leader; aborting")
    print("leader elected. starting writers and chaos.\n")

    writers = [Writer(i, RaftClient([f"http://127.0.0.1:{p}" for p in NODES.values()]),
                      stop_event)
               for i in range(args.writers)]
    monkey = ChaosMonkey(cluster, rng, stop_event, event_log)

    started = time.time()
    for writer in writers:
        writer.start()
    monkey.start()

    # Progress ticker so a long run doesn't look hung.
    try:
        while time.time() - started < args.duration:
            time.sleep(5)
            acked = sum(w.counts["ok"] for w in writers)
            elapsed = time.time() - started
            print(f"  [{elapsed:5.1f}s] {acked} writes acknowledged, "
                  f"{monkey.kills} kills, nodes up: {len(cluster.alive())}/{len(NODES)}")
    except KeyboardInterrupt:
        print("\ninterrupted - proceeding to verification")

    stop_event.set()
    for writer in writers:
        writer.join(timeout=15)
    monkey.join(timeout=15)

    print("\nchaos over. restarting every node and waiting for convergence...")
    for node_id in NODES:
        cluster.start(node_id)

    converged, statuses = wait_for_convergence()

    print("\n" + "=" * 70)
    print("CHAOS EVENTS")
    print("=" * 70)
    for when, what in event_log:
        print(f"  +{when - started:5.1f}s  {what}")

    print("\n" + "=" * 70)
    print("WORKLOAD")
    print("=" * 70)
    acked = sum(w.counts["ok"] for w in writers)
    unknown = sum(w.counts["unknown"] for w in writers)
    unavailable = sum(w.counts["unavailable"] for w in writers)
    print(f"  acknowledged writes : {acked}   (MUST all survive)")
    print(f"  unknown outcome     : {unknown}   (may or may not survive)")
    print(f"  cluster unavailable : {unavailable}   (may or may not survive)")
    print(f"  node kills          : {monkey.kills}")
    print(f"  leader kills        : {monkey.leader_kills}   (each forces an election)")
    print(f"  quorum losses       : {monkey.quorum_losses}")

    # The final term counts elections: it starts at 1 and rises by at
    # least one every time leadership changes hands. A run that ends at
    # term 1 never exercised failover at all, however many nodes it
    # killed — worth surfacing, because such a run looks like a thorough
    # pass while having tested far less than it appears to.
    final_term = max((s["term"] for s in statuses.values() if s), default=0)
    print(f"  final term          : {final_term}   "
          f"({final_term - 1} leadership change{'s' if final_term != 2 else ''})")
    if final_term <= 1:
        print("  WARNING: no election ever happened - this run did not "
              "exercise failover")

    print("\n" + "=" * 70)
    print("FINAL STATE")
    print("=" * 70)
    for node_id, status in statuses.items():
        if status is None:
            print(f"  {node_id}: DID NOT COME BACK")
        else:
            print(f"  {node_id}: {status['role'].upper():<9} term={status['term']} "
                  f"commit={status['commit_index']} keys={len(status['store'])}")

    if not converged:
        print("\nRESULT: FAILED - cluster did not converge after chaos stopped")
        cluster.stop_all()
        raise SystemExit(1)

    violations = verify(writers, statuses, client)

    print("\n" + "=" * 70)
    print("VERIFICATION")
    print("=" * 70)
    checks = [
        "all nodes hold identical data",
        f"all {acked} acknowledged writes survived with correct values",
        "no fabricated keys",
        "contended key holds a genuinely written value",
        "linearizable reads agree with replicated state",
    ]
    if violations:
        for violation in violations[:20]:
            print(f"  FAIL  {violation}")
        if len(violations) > 20:
            print(f"  ... and {len(violations) - 20} more")
        print(f"\nRESULT: FAILED ({len(violations)} violations)")
        print(f"Reproduce with: python scripts/chaos_test.py "
              f"--seed {seed} --duration {args.duration}")
        cluster.stop_all()
        raise SystemExit(1)

    for check in checks:
        print(f"  PASS  {check}")
    print(f"\nRESULT: PASSED - {acked} acknowledged writes survived "
          f"{monkey.kills} node kills")
    cluster.stop_all()


if __name__ == "__main__":
    main()
