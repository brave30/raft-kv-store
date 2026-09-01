"""
The state machine: the actual key-value store.

WHY THIS IS SEPARATE FROM THE LOG:
This is the single most important idea in the whole design, so it's worth
being precise about it.

The LOG is the list of commands, in order: "SET x=1", "SET y=2", "DELETE x".
The STORE is what you get by replaying that list from the beginning.

Raft's entire job is to make every node agree on *the log*. Once the logs
agree, the stores agree for free — because applying the same commands in
the same order to the same starting state must produce the same result.
That property has a name: the state machine must be DETERMINISTIC.

This is why you must never put anything non-deterministic in a command.
A command like {"op": "SET", "key": "t", "value": "now()"} would be a
catastrophe: each node would evaluate now() at a different moment and
they'd silently diverge, with Raft none the wiser — it would have
faithfully replicated identical logs that produce different data. If you
need the current time, the LEADER resolves it to a literal value first
and replicates that. The rule: commands must be facts, never instructions
to compute something.

WHY WE ONLY APPLY *COMMITTED* ENTRIES:
An entry sitting in the log is a proposal, not a decision. It may still be
overwritten if the leader that created it dies before replicating it to a
majority. If we applied entries the moment they were appended, we could
show a client a value that later gets rolled back — and there's no way to
un-tell a client something. So entries flow through three stages:

    appended  ->  committed (majority has it)  ->  applied (visible here)

Only the last stage touches this class.
"""

from typing import Any


class KeyValueStore:
    def __init__(self) -> None:
        self.data: dict[str, str] = {}

    def apply(self, command: dict[str, Any]) -> Any:
        """
        Apply one committed command. Called ONLY by RaftNode._apply_committed,
        strictly in log order, and never for an uncommitted entry.
        """
        op = command.get("op")

        if op == "SET":
            self.data[command["key"]] = command["value"]
            return command["value"]

        if op == "DELETE":
            # .pop with a default so deleting a missing key is a no-op
            # rather than an error. Determinism matters here: this must
            # behave identically on a node that has the key and one that
            # doesn't — though if the logs are correct, that can't happen.
            return self.data.pop(command["key"], None)

        if op == "NOOP":
            # A no-op entry carries no data. A newly elected leader appends
            # one so it has an entry from its OWN term to commit — see the
            # long comment on _advance_commit_index in node.py for why that
            # is necessary rather than merely tidy.
            return None

        raise ValueError(f"unknown op: {op!r}")

    def get(self, key: str) -> str | None:
        return self.data.get(key)

    def snapshot(self) -> dict[str, str]:
        """A copy of the current data, for /status and debugging."""
        return dict(self.data)
