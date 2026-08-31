"""
The Raft log: an append-only list of commands.

WHY THIS EXISTS:
Instead of a leader directly telling followers "the value of X is now 5",
Raft has the leader append an entry to a LOG, and followers copy that log.
Why go through a log instead of just sending the final value?

Because the log is what lets a crashed node catch back up. If a follower
was down for 10 writes, it can't just ask "what's the current value?" —
it needs to replay everything it missed, in order, to guarantee it ends
up in the exact same state as everyone else. The log is that replay tape.
"""

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LogEntry:
    # `term` = which leader's "reign" created this entry. Every time a new
    # leader is elected, the term number goes up. This is how a node can
    # tell "this command came from an old, possibly-outdated leader" vs
    # "this came from the current leader" — it's Raft's logical clock.
    term: int

    # `index` = this entry's position in the log (1-based). Used to check
    # "does my log match yours at this position?" during replication.
    index: int

    # `command` = the actual operation, e.g. {"op": "SET", "key": "x", "value": "5"}
    command: dict[str, Any]

    def to_dict(self) -> dict:
        return {"term": self.term, "index": self.index, "command": self.command}

    @staticmethod
    def from_dict(d: dict) -> "LogEntry":
        return LogEntry(term=d["term"], index=d["index"], command=d["command"])
