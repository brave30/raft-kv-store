"""
Persistent state: the three pieces of data a Raft node MUST write to disk
before it can safely respond to any RPC.

WHY THESE SPECIFIC THREE THINGS:

1. currentTerm — the highest "election term" this node has ever seen.
   If this weren't persisted, a node could restart, forget which term it
   was in, and accidentally vote twice in the same election after a crash
   — which could let two leaders exist at once. That's the exact thing
   Raft is designed to prevent.

2. votedFor — who this node voted for in currentTerm (or None).
   Same reasoning: without persisting this, a node could crash right
   after voting, restart, and vote again for a different candidate in
   the same term, because it "forgot" it already voted. One vote per
   term per node is the rule that makes majority-based election safe.

3. log — the append-only list of LogEntry objects (see log.py).
   This is the actual data. Without persisting it, a node that crashes
   and restarts would come back with an empty log and have to re-receive
   everything from the leader — which is wasteful, and if a majority of
   nodes lost their logs simultaneously, you'd lose committed data
   entirely.

Notice what's NOT here: things like "who is the current leader" or
"what's in the key-value store right now" are NOT persisted directly.
Those are called "volatile state" in the Raft paper — they get rebuilt
by replaying the persisted log on startup. We'll build that replay logic
in Phase 3.
"""

import json
import os
from dataclasses import dataclass, field

from .log import LogEntry


class PersistentState:
    def __init__(self, storage_path: str):
        self.storage_path = storage_path
        self.current_term: int = 0
        self.voted_for: str | None = None
        self.log: list[LogEntry] = []

        if os.path.exists(storage_path):
            self._load()
        else:
            # First time this node has ever started — write an initial
            # empty state file so a crash before the first real write
            # still has something valid to load.
            self._save()

    def _save(self) -> None:
        """
        Write state to disk BEFORE we act on it.

        This ordering matters a lot: if we voted for someone, then crashed
        before saving that vote, on restart we'd have no memory of voting
        — and could vote again. So the rule we'll follow everywhere in
        this codebase is: mutate in memory, save to disk, THEN respond to
        the RPC. Never respond first.
        """
        data = {
            "current_term": self.current_term,
            "voted_for": self.voted_for,
            "log": [entry.to_dict() for entry in self.log],
        }
        # Write to a temp file and rename — this avoids a corrupted file
        # if the process dies mid-write (a half-written JSON file would
        # be unreadable on the next restart).
        tmp_path = self.storage_path + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(data, f)
        os.replace(tmp_path, self.storage_path)

    def _load(self) -> None:
        with open(self.storage_path, "r") as f:
            data = json.load(f)
        self.current_term = data["current_term"]
        self.voted_for = data["voted_for"]
        self.log = [LogEntry.from_dict(e) for e in data["log"]]

    def set_term(self, term: int) -> None:
        self.current_term = term
        self._save()

    def set_voted_for(self, candidate_id: str | None) -> None:
        self.voted_for = candidate_id
        self._save()

    def append_entries(self, entries: list[LogEntry]) -> None:
        self.log.extend(entries)
        self._save()

    def truncate_from(self, index: int) -> None:
        """
        Delete all log entries from `index` onward.

        WHY THIS IS NEEDED: if a follower's log has entries a stale/old
        leader gave it, and a NEW leader shows up with different entries
        at the same positions, the follower's old entries are wrong and
        must be thrown away before the new ones are appended. This is
        called a "log conflict" and resolving it is one of the trickiest
        parts of Raft — we'll hit this for real in Phase 3.
        """
        self.log = [e for e in self.log if e.index < index]
        self._save()

    def overwrite_from(self, index: int, entries: list[LogEntry]) -> None:
        """
        Delete everything from `index` onward, then append `entries` —
        in a SINGLE disk write.

        Why one method instead of calling truncate_from() then
        append_entries()? Those would be two separate saves, and a crash
        in between would leave the node on disk with entries deleted and
        the replacements missing. Not fatal (the leader would just resend
        them) but it needlessly throws away data we already had in hand.
        One write, one atomic rename, no window.
        """
        self.log = [e for e in self.log if e.index < index]
        self.log.extend(entries)
        self._save()

    def entry_at(self, index: int) -> LogEntry | None:
        """
        The entry at a 1-based log index, or None if we don't have it.

        The log list is 0-based while Raft indices are 1-based, so entry
        `i` lives at `self.log[i - 1]`. Rather than scatter that off-by-one
        across the codebase (where it would eventually be gotten wrong),
        it's isolated right here.
        """
        if index < 1 or index > len(self.log):
            return None
        entry = self.log[index - 1]
        # Cheap internal consistency check. If this ever fires, the log
        # list and the entries' own index fields have drifted apart, which
        # would mean a bug in truncation — better to fail loudly than to
        # silently replicate a corrupted log.
        assert entry.index == index, f"log corrupted: slot {index} holds {entry.index}"
        return entry

    def term_at(self, index: int) -> int:
        """
        The term of the entry at `index`; 0 for index 0 (the empty log).

        Index 0 is a useful fiction: it's the position "before the first
        entry," and every node trivially agrees about it. That gives the
        consistency check in AppendEntries a base case, so a leader
        replicating to a completely empty follower needs no special path.
        """
        if index == 0:
            return 0
        entry = self.entry_at(index)
        return entry.term if entry else 0

    def last_log_index(self) -> int:
        return self.log[-1].index if self.log else 0

    def last_log_term(self) -> int:
        return self.log[-1].term if self.log else 0
