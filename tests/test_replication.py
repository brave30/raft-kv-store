"""
Tests for log replication: the consistency check, conflict resolution,
commit-index advancement, and applying committed entries to the store.

As with test_election.py, these drive the RPC handlers directly rather
than over a network. The cases that matter most here — a delayed
duplicate AppendEntries, a majority that must NOT commit — depend on
precise interleavings that are essentially impossible to reproduce on
demand in a live cluster.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft_kv.log import LogEntry
from raft_kv.node import FOLLOWER, LEADER, RaftNode


def make_node(tmpdir, node_id="node1", peers=("node2", "node3")):
    peer_map = {p: f"http://127.0.0.1:9{i}" for i, p in enumerate(peers)}
    return RaftNode(
        node_id=node_id,
        peers=peer_map,
        storage_path=os.path.join(tmpdir, f"{node_id}_state.json"),
    )


def entry(term, index, key="k", value="v"):
    return LogEntry(term=term, index=index,
                    command={"op": "SET", "key": key, "value": value})


def append_msg(term=1, leader="node2", prev_index=0, prev_term=0,
               entries=(), commit=0):
    return {
        "term": term,
        "leader_id": leader,
        "prev_log_index": prev_index,
        "prev_log_term": prev_term,
        "entries": [e.to_dict() for e in entries],
        "leader_commit": commit,
    }


# ---------------------------------------------------------------- the
# consistency check
# ---------------------------------------------------------------------

def test_follower_appends_to_an_empty_log():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        reply = node.handle_append_entries(
            append_msg(entries=[entry(1, 1), entry(1, 2)]))
        assert reply["success"] is True
        assert node.state.last_log_index() == 2
    print("PASS: follower appends to an empty log (index 0 base case)")


def test_follower_rejects_when_its_log_is_too_short():
    """The leader guessed we were caught up; we're not."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, 1)])
        # Leader assumes we have index 5; we only have 1.
        reply = node.handle_append_entries(
            append_msg(prev_index=5, prev_term=1, entries=[entry(1, 6)]))
        assert reply["success"] is False
        assert node.state.last_log_index() == 1, "must not create a gap"
        # The hint should point just past our real end, so the leader can
        # skip straight there instead of decrementing one at a time.
        assert reply["conflict_index"] == 2
    print("PASS: rejects an append that would leave a gap, and hints where to resume")


def test_follower_rejects_on_term_mismatch():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, 1), entry(1, 2)])
        # Leader says index 2 should be term 3; ours is term 1.
        reply = node.handle_append_entries(
            append_msg(term=3, prev_index=2, prev_term=3, entries=[entry(3, 3)]))
        assert reply["success"] is False
        # Both our entries are from the bad term 1, so the hint should
        # skip back over the whole run rather than inch back one index.
        assert reply["conflict_index"] == 1
    print("PASS: rejects on term mismatch and hints past the whole bad run")


# ---------------------------------------------------------------------
# conflict resolution
# ---------------------------------------------------------------------

def test_follower_overwrites_conflicting_entries():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        # Junk from a leader that died: indices 2-3 from term 1.
        node.state.append_entries([entry(1, 1), entry(1, 2, key="old"),
                                   entry(1, 3, key="old")])
        # New leader (term 2) agrees at index 1, disagrees from 2 on.
        reply = node.handle_append_entries(append_msg(
            term=2, prev_index=1, prev_term=1,
            entries=[entry(2, 2, key="new"), entry(2, 3, key="new")]))
        assert reply["success"] is True
        assert node.state.last_log_index() == 3
        assert node.state.entry_at(2).term == 2
        assert node.state.entry_at(2).command["key"] == "new"
        assert node.state.entry_at(3).command["key"] == "new"
    print("PASS: overwrites conflicting entries from a superseded leader")


def test_conflicting_overwrite_truncates_a_longer_log():
    """A follower ahead of the leader with junk must be cut back."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, i) for i in range(1, 6)])
        reply = node.handle_append_entries(append_msg(
            term=2, prev_index=1, prev_term=1, entries=[entry(2, 2)]))
        assert reply["success"] is True
        assert node.state.last_log_index() == 2, "stale tail was not truncated"
    print("PASS: truncates a follower's extra entries that conflict")


def test_delayed_duplicate_append_does_not_truncate():
    """
    THE SUBTLE ONE. A stale AppendEntries arrives late, carrying entries
    we already have. Naive "truncate then append" would delete the newer
    entries after it — which may already be committed elsewhere.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, i) for i in range(1, 6)])  # 1..5

        # An OLD request that only knows about entries 2-3 shows up now.
        reply = node.handle_append_entries(append_msg(
            term=1, prev_index=1, prev_term=1,
            entries=[entry(1, 2), entry(1, 3)]))

        assert reply["success"] is True
        assert node.state.last_log_index() == 5, \
            "a delayed duplicate destroyed entries 4-5!"
    print("PASS: a delayed duplicate append is a no-op, not a truncation")


def test_partially_overlapping_append_keeps_matching_prefix():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, 1), entry(1, 2)])
        # Entries 2 (already held, same term) and 3 (new).
        reply = node.handle_append_entries(append_msg(
            term=1, prev_index=1, prev_term=1,
            entries=[entry(1, 2), entry(1, 3)]))
        assert reply["success"] is True
        assert node.state.last_log_index() == 3
    print("PASS: an overlapping batch appends only the genuinely new tail")


# ---------------------------------------------------------------------
# commit index
# ---------------------------------------------------------------------

def test_leader_commits_once_a_majority_has_the_entry():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)  # 3-node cluster, majority 2
        node._become_candidate()
        node._become_leader()   # appends a NOOP at index 1
        node.state.append_entries([entry(node.state.current_term, 2)])

        # Nobody has acknowledged yet -> nothing is committable.
        node._advance_commit_index()
        assert node.commit_index == 0

        # One follower confirms index 2. With the leader itself that's
        # 2 of 3 = a majority.
        node.match_index["node2"] = 2
        node._advance_commit_index()
        assert node.commit_index == 2
    print("PASS: leader commits an entry once a majority holds it")


def test_leader_does_not_commit_without_a_majority():
    with tempfile.TemporaryDirectory() as tmp:
        # 5-node cluster: majority is 3.
        node = make_node(tmp, peers=("n2", "n3", "n4", "n5"))
        node._become_candidate()
        node._become_leader()
        node.state.append_entries([entry(node.state.current_term, 2)])

        node.match_index["n2"] = 2  # leader + 1 = 2 of 5. Not enough.
        node._advance_commit_index()
        assert node.commit_index == 0, "committed without a majority!"

        node.match_index["n3"] = 2  # now 3 of 5.
        node._advance_commit_index()
        assert node.commit_index == 2
    print("PASS: does not commit until a strict majority is reached")


def test_leader_will_not_commit_a_previous_terms_entry_by_count():
    """
    Figure 8 of the Raft paper. An old entry sitting on a majority is NOT
    yet safe — it can still be overwritten by a future leader. Committing
    it on the strength of the count alone would be reporting durability
    that doesn't exist.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        # An entry left over from term 1...
        node.state.append_entries([entry(1, 1)])
        # ...and we are now leader in a LATER term.
        node.state.set_term(3)
        node.role = LEADER
        node.next_index = {p: 2 for p in node.peers}
        node.match_index = {p: 0 for p in node.peers}

        # A majority physically holds that old entry.
        node.match_index["node2"] = 1
        node._advance_commit_index()
        assert node.commit_index == 0, \
            "committed a previous-term entry on majority count alone!"

        # Now append an entry from OUR term and get it on a majority.
        node.state.append_entries([entry(3, 2)])
        node.match_index["node2"] = 2
        node._advance_commit_index()
        # Committing our own term's entry carries the old one with it.
        assert node.commit_index == 2, "current-term entry should commit"
    print("PASS: refuses to commit an old-term entry by count, commits it "
          "indirectly via a current-term entry")


def test_new_leader_appends_a_noop_from_its_own_term():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, 1)])
        node.state.set_term(1)

        node._become_candidate()   # term -> 2
        node._become_leader()

        last = node.state.entry_at(node.state.last_log_index())
        assert last.command["op"] == "NOOP"
        assert last.term == node.state.current_term
    print("PASS: a new leader appends a no-op from its own term")


def test_new_leader_initializes_peer_tracking():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([entry(1, 1), entry(1, 2)])
        node._become_candidate()
        node._become_leader()  # NOOP lands at index 3

        for peer in node.peers:
            # Optimistic guess: assume they're caught up (before the NOOP).
            assert node.next_index[peer] == 3
            # But we have confirmed nothing.
            assert node.match_index[peer] == 0, \
                "match_index must start at 0 — it's a fact, not a guess"
    print("PASS: new leader sets next_index optimistically, match_index at 0")


# ---------------------------------------------------------------------
# applying to the store
# ---------------------------------------------------------------------

def test_committed_entries_are_applied_in_order():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([
            LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "value": "1"}),
            LogEntry(term=1, index=2, command={"op": "SET", "key": "x", "value": "2"}),
            LogEntry(term=1, index=3, command={"op": "DELETE", "key": "x"}),
        ])
        node.commit_index = 2
        node._apply_committed()
        assert node.store.get("x") == "2", "order matters: last write wins"
        assert node.last_applied == 2

        # The DELETE is in the log but not yet committed, so not applied.
        node.commit_index = 3
        node._apply_committed()
        assert node.store.get("x") is None
        assert node.last_applied == 3
    print("PASS: committed entries are applied in log order, uncommitted are not")


def test_uncommitted_entries_are_never_applied():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([
            LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "value": "1"}),
        ])
        node._apply_committed()  # commit_index is still 0
        assert node.store.get("x") is None, "applied an uncommitted entry!"
        assert node.last_applied == 0
    print("PASS: an appended but uncommitted entry is invisible to readers")


def test_follower_adopts_leader_commit_but_never_beyond_its_own_log():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        # Leader claims commit=10 while sending us only entries 1-2. We
        # must not claim to have committed entries we don't hold.
        node.handle_append_entries(append_msg(
            entries=[entry(1, 1), entry(1, 2)], commit=10))
        assert node.commit_index == 2, \
            "claimed a commit index beyond the end of our own log"
        assert node.last_applied == 2
    print("PASS: follower caps the leader's commit index at its own log end")


def test_follower_applies_replicated_entries_to_its_store():
    """Followers apply too — that's what keeps every node's data identical."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_append_entries(append_msg(
            entries=[LogEntry(term=1, index=1,
                              command={"op": "SET", "key": "colour", "value": "blue"})],
            commit=1))
        assert node.store.get("colour") == "blue"
    print("PASS: a follower applies replicated entries to its own store")


# ---------------------------------------------------------------------
# client writes
# ---------------------------------------------------------------------

def test_non_leader_refuses_writes_and_names_the_leader():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_append_entries(append_msg(term=2, leader="node2"))
        assert node.role == FOLLOWER

        result = node.submit({"op": "SET", "key": "x", "value": "1"})
        assert result["ok"] is False
        assert result["error"] == "not_leader"
        assert result["leader_id"] == "node2", "should redirect to the real leader"
    print("PASS: a follower refuses writes and names the current leader")


def test_single_node_leader_commits_its_own_write():
    """Majority of 1, so the leader's own disk write is sufficient."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp, peers=())
        node._start_election()
        assert node.role == LEADER

        result = node.submit({"op": "SET", "key": "x", "value": "42"})
        assert result["ok"] is True
        assert node.store.get("x") == "42"
    print("PASS: a single-node leader commits and applies its own write")


def test_write_is_rejected_if_command_is_malformed():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp, peers=())
        node._start_election()
        assert node.handle_client_write({"command": "not a dict"})["ok"] is False
        assert node.handle_client_write({})["ok"] is False
    print("PASS: malformed client commands are rejected")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("\nAll replication tests passed.")
