# raft-kv-store

An implementation of the [Raft consensus algorithm](https://raft.github.io/)
in Python, written from scratch — no consensus libraries — as the foundation
for a distributed, fault-tolerant key-value store.

The goal is to understand consensus rather than to ship a database, so the
code is heavily commented with the *reasoning* behind each mechanism: why
votes are written to disk before they're sent, why election timeouts are
randomized, why a majority is what makes single-leadership safe.

## Status

**A working replicated key-value store with linearizable reads.** Writes
commit only once a majority has them on disk, any node accepts a request
and forwards it to the leader, and a leader that can't reach a majority
refuses to answer rather than return stale data.

| Phase | Status |
|---|---|
| 1. Persistent state (term, vote, log survive a crash) | Done |
| 2. Leader election (RequestVote, heartbeats, failover) | Done |
| 3. Log replication (AppendEntries, commit index, KV store) | Done |
| 4. Client API (forwarding, linearizable reads) | Done |
| 5. Chaos testing (kill nodes mid-write, prove durability) | Next |
| 6. Benchmarking (throughput, latency, recovery time) | Planned |

## What works today

Three nodes run as separate OS processes and talk over JSON-over-HTTP.
They elect a leader, replicate writes to a majority before acknowledging
them, and apply committed entries to a key-value store on every node.
Kill the leader and a new one takes over with all committed data intact;
restart the dead node and it catches up on everything it missed.

```
>>> writing colour=blue, size=large
node3 (term 1) accepted write #2: {'op': 'SET', 'key': 'colour', 'value': 'blue'}
node3 (term 1) committed up to #2 (2/3 replicas)
node1 (term 1) applied #2: {'op': 'SET', 'key': 'colour', 'value': 'blue'}

  node1: FOLLOWER  log=3 commit=3 store={'colour': 'blue', 'size': 'large'}
  node2: FOLLOWER  log=3 commit=3 store={'colour': 'blue', 'size': 'large'}
  node3: LEADER    log=3 commit=3 store={'colour': 'blue', 'size': 'large'}

>>> KILLING THE LEADER (node3)

node1 (term 2) election timeout -> becoming CANDIDATE, requesting votes
node1 (term 2) *** WON ELECTION with 2 votes -> now LEADER ***

  node1: LEADER    log=4 commit=4 store={'colour': 'blue', 'size': 'large'}
  node2: FOLLOWER  log=4 commit=4 store={'colour': 'blue', 'size': 'large'}
  node3: DOWN

>>> writing shape=round with only 2 of 3 nodes up   -> committed
>>> RESTARTING node3

node3 (term 2) appended entries 4..5 from leader
node3 (term 2) applied #5: {'op': 'SET', 'key': 'shape', 'value': 'round'}

  all three stores identical: {'colour': 'blue', 'size': 'large', 'shape': 'round'}
```

## Running it

Requires Python 3.10+. No dependencies — standard library only.

```bash
# Launch a 3-node cluster with merged output
python scripts/run_cluster.py
```

Then type commands at the prompt:

| Command | Effect |
|---|---|
| `status` | Ask every node its role, log length, commit index and store |
| `set <k> <v>` | Write a key (routed to the leader) |
| `get <k>` | Read the key from *every* node, so you can see replication |
| `kill <id>` | Hard-kill a node (simulates a crash) — try killing the leader |
| `start <id>` | Restart a killed node; it reloads its state from disk |
| `quit` | Shut the cluster down |

The sequence worth running:

```
set colour blue
get colour                  # all three nodes agree
kill node3                  # whichever one is leader
get colour                  # survivors still have it, new leader elected
set colour red              # still accepting writes with 2 of 3 up
start node3
get colour                  # the restarted node catches up to "red"
```

Nodes can also be run individually, and every RPC is plain HTTP, so the
cluster is inspectable with `curl`:

```bash
python -m raft_kv.server --id node1 --port 5001 \
    --peers node2=http://127.0.0.1:5002,node3=http://127.0.0.1:5003

curl http://127.0.0.1:5001/status

curl -X POST http://127.0.0.1:5001/write \
     -d '{"command": {"op": "SET", "key": "colour", "value": "blue"}}'

curl -X POST http://127.0.0.1:5001/read -d '{"key": "colour"}'
```

A `/write` returns only once a majority has the entry on disk. Any node
accepts one — non-leaders forward to the leader internally.

Or use the client CLI, which needs to know nothing about who leads:

```bash
python -m raft_kv.client set colour blue
python -m raft_kv.client get colour
python -m raft_kv.client get colour --local   # opt in to a stale read
python -m raft_kv.client delete colour
python -m raft_kv.client status
```

## Reads: two consistency levels

```python
from raft_kv.client import RaftClient

client = RaftClient(["http://127.0.0.1:5001", "http://127.0.0.1:5002"])
client.set("colour", "blue")
client.get("colour")                        # linearizable (default)
client.get("colour", consistency="local")   # fast, may be stale
```

**Linearizable** is the default: the read is served by the leader, and
only after the leader has proven it is *still* leader by getting a
majority to accept a message from it. Any value returned reflects every
write acknowledged before the read began.

That proof is not optional paranoia. A leader that gets partitioned away
has no idea — nothing tells it, and its own term never changes. It keeps
believing it leads while the majority elects a successor and moves on. A
read from that "zombie leader" would be confidently, unboundedly stale.
The confirmation round costs a network round trip per read; that is the
honest price of the guarantee. (Production systems soften it by batching
concurrent reads into one round, or by trading the proof for a
clock-drift assumption via leader leases.)

**Local** reads skip all of that and return whatever that node has
applied. Fast, no network, possibly stale — appropriate when staleness is
acceptable, and something you have to ask for explicitly.

## Tests

```bash
python tests/test_state.py        # crash recovery of persistent state
python tests/test_election.py     # the voting rules (12 tests)
python tests/test_replication.py  # replication and commit rules (19 tests)
python tests/test_client_api.py   # forwarding and read consistency (13 tests)
```

The election tests deliberately avoid the network and timers, calling the
RPC handlers directly with hand-crafted messages. The dangerous cases —
double-voting within a term, a stale leader's heartbeat, a candidate whose
log is missing entries — are timing-dependent in a live cluster and would
be nearly impossible to trigger reliably; called directly they're
deterministic and instant.

## Layout

| File | Role |
|---|---|
| `raft_kv/log.py` | `LogEntry` — the unit of replication |
| `raft_kv/state.py` | State that must survive a crash, persisted to disk |
| `raft_kv/store.py` | The key-value state machine the log is replayed into |
| `raft_kv/rpc.py` | JSON-over-HTTP transport between nodes |
| `raft_kv/node.py` | The Follower/Candidate/Leader state machine |
| `raft_kv/server.py` | CLI entry point — one OS process per node |
| `raft_kv/client.py` | Client library + CLI (retries, leader-agnostic) |
| `scripts/run_cluster.py` | Local 3-node cluster launcher |

## Design notes

**Python, not Go.** Go is the conventional choice here, but the point of
the exercise is distributed systems, and learning a language at the same
time would have split the attention.

**Standard library HTTP, not gRPC or Flask.** Every RPC is a JSON POST you
can reproduce with `curl`. A faster transport would have hidden the
network behind a framework, and the network is the thing being studied.

**Separate processes, not threads.** Threads share memory, which would
quietly mask bugs where one node reads another's state, and "kill a node"
wouldn't be a real crash. Separate processes impose the actual constraint:
the only way to share information is to send a message.

**Three nodes.** A majority of 3 is 2, so the cluster survives exactly one
failure. Kill a second and the survivor campaigns forever without winning
— that's Raft correctly refusing to proceed without a majority. Lost
availability is recoverable; lost consensus is not. This is also why
cluster sizes are odd: 4 nodes tolerate only 1 failure, same as 3.

## Three details that look wrong and aren't

These are the parts of Raft that took the longest to understand, so
they're the most heavily commented in the code.

**A leader won't commit an old entry just because a majority has it.**
`_advance_commit_index` in `node.py` only commits entries from the
leader's *own* term. This looks overly cautious — the data is demonstrably
on a majority of disks — but an entry from a previous term can still be
overwritten by a future leader, so "a majority has it" isn't yet the same
as "it's permanent." This is Figure 8 of the Raft paper; the walkthrough
in the code shows the five-step sequence where committing early would
lose acknowledged data. It's also why a new leader appends a no-op entry:
that gives it something from its own term to commit, which drags all the
older entries to safety with it.

**A follower doesn't truncate its log just because the leader sent
entries.** The obvious implementation of AppendEntries — delete from
`prev_log_index + 1`, then append — is wrong. A delayed duplicate of an
older request can arrive after newer entries have been accepted, and
blind truncation would destroy committed data because a stale packet
showed up late. `_merge_entries` truncates only at a position where the
terms genuinely differ. There's a test for exactly this.

**`next_index` and `match_index` look redundant.** One is a guess (where
the leader *thinks* a follower's log ends, adjusted by trial and error);
the other is a fact (what a follower has actually confirmed). Commit
decisions count only the facts. Committing off the optimistic guess would
mean declaring data safe that no follower necessarily has.

**A leader can't be trusted to know it's the leader.** See the read
consistency section above — this is why every linearizable read pays for
a majority confirmation round.

## Known limitations

- **A write timeout means "unknown", not "failed".** If a write times out
  waiting for commitment, it may still commit moments later — the client
  raises a distinct `WriteOutcomeUnknown` for this rather than pretending
  to know. Retrying is safe here only because every command is
  idempotent: `SET` and `DELETE` applied twice equal applied once. A
  hypothetical `INCREMENT` would need per-client sequence numbers so the
  cluster could recognise and discard a duplicate. That restriction on
  the command set is a deliberate design decision, not an oversight.
- **The log grows forever.** No snapshotting or compaction, so a
  long-running node replays its entire history on restart.
- **Fixed cluster membership.** Nodes are passed on the command line at
  startup. Changing membership on a live cluster has its own subtle
  safety rules and isn't implemented.

## Reading

- [The Secret Lives of Data](http://thesecretlivesofdata.com/raft/) — visual walkthrough
- [raft.github.io](https://raft.github.io/) — interactive cluster, and the paper
- MIT 6.824, lectures 6–7 — the rigorous treatment
