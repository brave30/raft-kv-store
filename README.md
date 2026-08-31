# raft-kv-store

An implementation of the [Raft consensus algorithm](https://raft.github.io/)
in Python, written from scratch — no consensus libraries — as the foundation
for a distributed, fault-tolerant key-value store.

The goal is to understand consensus rather than to ship a database, so the
code is heavily commented with the *reasoning* behind each mechanism: why
votes are written to disk before they're sent, why election timeouts are
randomized, why a majority is what makes single-leadership safe.

## Status

**Leader election works. Log replication is next — there is no key-value
API yet.**

| Phase | Status |
|---|---|
| 1. Persistent state (term, vote, log survive a crash) | Done |
| 2. Leader election (RequestVote, heartbeats, failover) | Done |
| 3. Log replication (AppendEntries, commit index) | Next |
| 4. Client API (SET / GET against the cluster) | Planned |
| 5. Chaos testing (kill nodes mid-write, prove durability) | Planned |
| 6. Benchmarking (throughput, latency, recovery time) | Planned |

## What works today

Three nodes run as separate OS processes, talk to each other over
JSON-over-HTTP, and elect a leader among themselves. Kill the leader and
the survivors detect the silence, hold a new election in a higher term,
and agree on a new leader. Restart the dead node and it rejoins as a
follower without disrupting the cluster.

```
[20:29:12] node1 (term 1) election timeout -> becoming CANDIDATE, requesting votes
[20:29:12] node2 (term 1) granting vote to node1
[20:29:12] node1 (term 1) received vote from node2 (2/2 needed)
[20:29:12] node1 (term 1) *** WON ELECTION with 2 votes -> now LEADER ***

>>> killing the leader (node1)...

[20:29:17] node3 (term 2) election timeout -> becoming CANDIDATE, requesting votes
[20:29:17] node2 (term 2) granting vote to node3
[20:29:17] node3 (term 2) *** WON ELECTION with 2 votes -> now LEADER ***
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
| `status` | Ask every node what role it thinks it has |
| `kill <id>` | Hard-kill a node (simulates a crash) — try killing the leader |
| `start <id>` | Restart a killed node; it reloads its state from disk |
| `quit` | Shut the cluster down |

Nodes can also be run individually, and every RPC is plain HTTP, so the
cluster is inspectable with `curl`:

```bash
python -m raft_kv.server --id node1 --port 5001 \
    --peers node2=http://127.0.0.1:5002,node3=http://127.0.0.1:5003

curl http://127.0.0.1:5001/status
```

## Tests

```bash
python tests/test_state.py      # crash recovery of persistent state
python tests/test_election.py   # the voting rules (12 tests)
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
| `raft_kv/rpc.py` | JSON-over-HTTP transport between nodes |
| `raft_kv/node.py` | The Follower/Candidate/Leader state machine |
| `raft_kv/server.py` | CLI entry point — one OS process per node |
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

## Reading

- [The Secret Lives of Data](http://thesecretlivesofdata.com/raft/) — visual walkthrough
- [raft.github.io](https://raft.github.io/) — interactive cluster, and the paper
- MIT 6.824, lectures 6–7 — the rigorous treatment
