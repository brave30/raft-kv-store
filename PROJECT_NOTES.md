# Raft-based Distributed Key-Value Store — Project Notes

## What this is
A distributed key-value store built from scratch in Python to learn
distributed systems fundamentals and to serve as a resume project
demonstrating consensus, replication, and fault tolerance.

This project is being built as a *teaching* exercise, not just a
finished deliverable — the goal is to understand every design decision,
not just get it working. When continuing this project (including in
Claude Code), explain the reasoning behind each piece as you go, the
way you'd teach someone new to systems/distributed systems — plain
English alongside any jargon, and why each mechanism exists before
just writing the code for it.

## Roadmap
1. **Data model + persistence** — DONE (see below)
2. **Leader election** — DONE (see below)
3. **Log replication** — DONE (see below)
4. **Client API** — NEXT UP. Leader forwarding so clients don't have to
   shop around for the leader, and linearizable reads (leader confirms
   it's still leader via a heartbeat round before answering, or uses a
   read index). The write path already exists and commits correctly.
5. **Chaos testing** — script that kills/restarts nodes mid-write to
   prove data survives — this becomes the demo GIF/log output for the
   GitHub README.
6. **Benchmarking + polish** — throughput, latency, leader-election
   recovery time. These numbers go in the resume bullet.

## What's built so far (Phase 1)

- `raft_kv/log.py` — `LogEntry` dataclass: term, index, command.
  This is the unit of replication — see the docstring in the file for
  why a log (not just "send the final value") is necessary: it's what
  lets a crashed/lagging node replay and catch up to an identical state.

- `raft_kv/state.py` — `PersistentState` class: durably stores
  `current_term`, `voted_for`, and the `log` to a JSON file on disk.
  Core principle established here: **write to disk BEFORE responding
  to any RPC** — never mutate in-memory only and reply, because a crash
  between those two steps can cause double-voting and split-brain
  (two leaders at once). Uses a temp-file-then-rename pattern so a
  crash mid-write never leaves a corrupted state file.

- `tests/test_state.py` — proves a node "crashes" (object destroyed)
  and "restarts" (reloaded from disk) with term/vote/log intact.

## What's built so far (Phase 2 — leader election)

- `raft_kv/rpc.py` — the network layer. JSON-over-HTTP using only the
  standard library (`http.server` + `urllib`), no Flask. Two properties
  that matter: every outbound RPC has a short timeout (0.5s), and every
  network failure returns `None` rather than raising — "peer didn't
  answer" is a normal outcome in Raft, not an error.

- `raft_kv/node.py` — the Follower/Candidate/Leader state machine. This
  is the file to reread when you want to remember how Raft works; the
  header docstring explains terms, majorities, and why election timeouts
  are randomized. Key mechanisms implemented:
  - randomized election timeout (1.5–3.0s) to prevent split votes
  - RequestVote, including the "at most one vote per term" rule and the
    log-up-to-dateness check (already built, matters in Phase 3)
  - AppendEntries used as an empty-payload heartbeat (0.5s interval)
  - step-down-on-higher-term, applied everywhere
  - a lock guarding all node state, since HTTP request threads and the
    election ticker thread touch it concurrently

- `raft_kv/server.py` — CLI entry point; one OS process per node.
  Separate processes (not threads) on purpose, so "kill a node" is a
  real crash and shared memory can't hide bugs.

- `scripts/run_cluster.py` — launches 3 nodes with merged output, and
  accepts `status`, `kill <id>`, `start <id>`, `quit` at the prompt.

- `tests/test_election.py` — 12 tests of the voting *rules* with no
  network or timers, so the dangerous cases (double-voting, stale
  leaders, a candidate with a short log) are deterministic.

**Timing invariant to preserve:** RPC timeout (0.5s) < heartbeat
interval (0.5s) << election timeout (1.5–3.0s). These are deliberately
slow so elections are watchable; production Raft uses ~150–300ms.

**Bug caught while building this, worth remembering:** votes were only
tallied when an RPC *reply* arrived, so a 1-node cluster (majority = 1)
never noticed it had already won with its own self-vote and campaigned
forever. The tally now also happens right after voting for self.

Verified end-to-end: 3 processes elect a leader, killing the leader
triggers a new election in a higher term, and the restarted old leader
rejoins as a follower without stealing leadership back.

## What's built so far (Phase 3 — log replication)

- `raft_kv/store.py` — `KeyValueStore`, the state machine the log is
  replayed into. The key rule documented there: commands must be
  deterministic *facts*, never instructions to compute something. A
  command containing `now()` would make every node compute a different
  value and diverge silently, with Raft none the wiser.

- `raft_kv/state.py` — added `entry_at`, `term_at` (isolating the 1-based
  log index vs 0-based list off-by-one in one place) and `overwrite_from`
  (truncate + append in a single disk write).

- `raft_kv/node.py` — the big one. Added:
  - `next_index` / `match_index` per follower (guess vs confirmed fact)
  - the AppendEntries consistency check via prev_log_index/prev_log_term,
    with a conflict-index hint so repairing a badly lagging follower
    takes one round trip instead of one per missing entry
  - `_merge_entries` — truncates only on a genuine term conflict, never
    blindly (see the bug note below)
  - `_advance_commit_index` — majority counting, plus the current-term
    restriction from Figure 8 of the paper
  - `_apply_committed` — feeds committed entries into the store, in order
  - `submit()` — accepts a client write and blocks until a majority has
    it, then reports success. Refuses on a non-leader and names the leader.
  - a new leader appends a NOOP from its own term, so it always has
    something committable and old stranded entries get carried to safety

- `raft_kv/server.py` — added `/write` and `/read` endpoints.

- `scripts/run_cluster.py` — added `set <k> <v>` and `get <k>`; `get`
  queries every node so replication is visible.

- `tests/test_replication.py` — 19 tests.

**Two real bugs caught while building this, both the same shape:** the
leader is itself a replica, and forgetting that strands the degenerate
case. (1) A single-node cluster never committed, because commit
advancement only ran when a follower *replied* — with no peers, no
replies. (2) Same root cause as the Phase 2 bug where a single-node
cluster never counted its own vote. Worth remembering as a category:
whenever logic is triggered by a peer response, ask what happens with
zero peers.

**Also fixed:** `_advance_commit_index` / `_apply_committed` originally
documented a "caller must hold the lock" contract, which is a footgun.
They now take the lock themselves — safe because `self.lock` is an
`RLock`, so nesting under callers that already hold it is fine.

Verified end-to-end with 3 processes: two writes replicate to all three
nodes, killing the leader preserves all committed data and elects a new
leader, the cluster keeps accepting writes with 2 of 3 up, and the
restarted node catches up to an identical store.

Run everything with:
```
cd raft-kv-store
python tests/test_state.py
python tests/test_election.py
python tests/test_replication.py
python scripts/run_cluster.py
# then: set colour blue / get colour / kill <leader> / get colour / start <id>
```

## Key design decisions worth remembering
- Language: Python (chosen for familiarity over Go, despite Go being
  more conventional for this project type — the user is comfortable in
  Python and wants to focus on distributed systems concepts rather than
  learning a new language at the same time).
- No third-party consensus libraries — implementing Raft itself is the
  point, not gluing together `hashicorp/raft` or similar.
- RPC layer: HTTP between nodes rather than gRPC, to keep the networking
  layer transparent and easy to inspect/debug while learning, rather than
  hidden behind a heavier framework. Ended up using the **standard
  library** (`http.server`) instead of Flask — zero dependencies, and for
  a learning project it's better to see the whole request path than to
  have it hidden behind routing decorators. Every RPC is curl-able.

## Recommended learning resources (for the human, not the AI)
- http://thesecretlivesofdata.com/raft/ — visual intro, do this first
- https://raft.github.io/ — interactive live cluster in the browser
- MIT 6.824 Distributed Systems, Lectures 6–7 (YouTube) — the rigorous version
- The original Raft paper (raft.github.io/raft.pdf) — read last, once
  the visualizations have built intuition

## Suggested first prompt to Claude Code when resuming
"Continue the Raft KV store project — read PROJECT_NOTES.md for
context, then let's build Phase 4 (client API): have followers forward
writes to the leader instead of making the client shop around, and make
reads linearizable so a stale follower or a just-deposed leader can't
return old data. Explain the reasoning as we go since I'm new to
distributed systems."
