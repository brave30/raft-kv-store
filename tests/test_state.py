"""
This test proves the one thing Phase 1 exists to prove: if a node
"crashes" (we just stop using the Python object) and comes back up,
it remembers its term, its vote, and its log — instead of starting
from a blank slate and risking double-voting or losing data.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft_kv.log import LogEntry
from raft_kv.state import PersistentState


def test_survives_a_crash():
    with tempfile.TemporaryDirectory() as tmpdir:
        storage_path = os.path.join(tmpdir, "node1_state.json")

        # --- "Before the crash" ---
        state = PersistentState(storage_path)
        state.set_term(3)
        state.set_voted_for("node2")
        state.append_entries([
            LogEntry(term=3, index=1, command={"op": "SET", "key": "x", "value": "5"}),
            LogEntry(term=3, index=2, command={"op": "SET", "key": "y", "value": "10"}),
        ])

        # --- Simulate a crash: throw away the in-memory object entirely ---
        del state

        # --- "After the restart": load a fresh object from the same file ---
        recovered = PersistentState(storage_path)

        assert recovered.current_term == 3, "forgot its term after crash!"
        assert recovered.voted_for == "node2", "forgot who it voted for after crash!"
        assert len(recovered.log) == 2, "lost log entries after crash!"
        assert recovered.log[1].command["key"] == "y"

        print("PASS: node correctly recovered term, vote, and log after a crash")


if __name__ == "__main__":
    test_survives_a_crash()
