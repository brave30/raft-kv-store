"""
Tests for the voting RULES, with no network and no timers involved.

WHY TEST IT THIS WAY:
Running three real processes and eyeballing the output proves the happy
path works, but it can't reliably reproduce the dangerous cases — you'd
have to get the timing exactly right to catch a double-vote. So we call
handle_request_vote / handle_append_entries directly with hand-crafted
messages, which makes the nasty scenarios deterministic and instant.

The rules under test are the ones that keep "at most one leader per term"
true. If any of these break, the whole safety argument collapses.
"""

import os
import sys
import tempfile

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft_kv.log import LogEntry
from raft_kv.node import CANDIDATE, FOLLOWER, LEADER, RaftNode


def make_node(tmpdir, node_id="node1", peers=("node2", "node3")):
    peer_map = {p: f"http://127.0.0.1:9{i}" for i, p in enumerate(peers)}
    return RaftNode(
        node_id=node_id,
        peers=peer_map,
        storage_path=os.path.join(tmpdir, f"{node_id}_state.json"),
    )


def vote_request(term, candidate_id, last_log_index=0, last_log_term=0):
    return {
        "term": term,
        "candidate_id": candidate_id,
        "last_log_index": last_log_index,
        "last_log_term": last_log_term,
    }


def test_grants_vote_to_first_candidate():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        reply = node.handle_request_vote(vote_request(1, "node2"))
        assert reply["vote_granted"] is True
        assert node.state.current_term == 1
        assert node.state.voted_for == "node2"
    print("PASS: grants its vote to the first candidate in a new term")


def test_refuses_to_vote_twice_in_one_term():
    """THE safety rule. If this fails, two leaders can exist at once."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        assert node.handle_request_vote(vote_request(1, "node2"))["vote_granted"] is True
        second = node.handle_request_vote(vote_request(1, "node3"))
        assert second["vote_granted"] is False, "voted twice in the same term!"
        assert node.state.voted_for == "node2"
    print("PASS: refuses a second vote in the same term")


def test_repeat_request_from_same_candidate_is_idempotent():
    """A retried RPC (its reply got lost) must get the same answer."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_request_vote(vote_request(1, "node2"))
        again = node.handle_request_vote(vote_request(1, "node2"))
        assert again["vote_granted"] is True, "retry of the same request was refused"
    print("PASS: a duplicate request from the same candidate is granted again")


def test_new_term_unlocks_a_fresh_vote():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_request_vote(vote_request(1, "node2"))
        reply = node.handle_request_vote(vote_request(2, "node3"))
        assert reply["vote_granted"] is True
        assert node.state.current_term == 2
        assert node.state.voted_for == "node3"
    print("PASS: a higher term resets votedFor and allows a new vote")


def test_rejects_candidate_from_an_older_term():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.set_term(5)
        reply = node.handle_request_vote(vote_request(3, "node2"))
        assert reply["vote_granted"] is False
        assert reply["term"] == 5, "must report our term so the stale node steps down"
    print("PASS: rejects a candidate whose term is behind ours")


def test_rejects_candidate_with_a_shorter_log():
    """
    Protects committed data: a node missing entries must never win.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.append_entries([
            LogEntry(term=1, index=1, command={"op": "SET", "key": "x", "value": "1"}),
            LogEntry(term=1, index=2, command={"op": "SET", "key": "y", "value": "2"}),
        ])
        behind = node.handle_request_vote(
            vote_request(2, "node2", last_log_index=1, last_log_term=1))
        assert behind["vote_granted"] is False, "voted for a candidate missing entries!"

        caught_up = node.handle_request_vote(
            vote_request(2, "node3", last_log_index=2, last_log_term=1))
        assert caught_up["vote_granted"] is True
    print("PASS: rejects a behind candidate, accepts an up-to-date one")


def test_higher_last_log_term_beats_a_longer_log():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        # Our log is LONG but all from the stale term 1.
        node.state.append_entries([
            LogEntry(term=1, index=i, command={"op": "SET", "key": "k", "value": str(i)})
            for i in range(1, 6)
        ])
        # Candidate has a SHORTER log, but its last entry is from term 2.
        reply = node.handle_request_vote(
            vote_request(3, "node2", last_log_index=2, last_log_term=2))
        assert reply["vote_granted"] is True, "term must outrank length"
    print("PASS: a newer last-log term outranks a longer stale log")


def test_leader_steps_down_when_it_sees_a_higher_term():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node._become_candidate()
        node._become_leader()
        assert node.role == LEADER

        node.handle_append_entries({
            "term": node.state.current_term + 1,
            "leader_id": "node2",
            "prev_log_index": 0, "prev_log_term": 0,
            "entries": [], "leader_commit": 0,
        })
        assert node.role == FOLLOWER, "a leader must step down for a higher term"
        assert node.leader_id == "node2"
    print("PASS: a leader steps down when it learns of a higher term")


def test_candidate_yields_to_a_leader_in_the_same_term():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node._become_candidate()
        assert node.role == CANDIDATE

        # Someone else won this same term and is now heartbeating us.
        node.handle_append_entries({
            "term": node.state.current_term,
            "leader_id": "node2",
            "prev_log_index": 0, "prev_log_term": 0,
            "entries": [], "leader_commit": 0,
        })
        assert node.role == FOLLOWER, "candidate kept campaigning against a real leader"
    print("PASS: a losing candidate yields to the winner of its term")


def test_heartbeat_from_a_stale_leader_is_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.state.set_term(7)
        reply = node.handle_append_entries({
            "term": 4, "leader_id": "node2",
            "prev_log_index": 0, "prev_log_term": 0,
            "entries": [], "leader_commit": 0,
        })
        assert reply["success"] is False
        assert reply["term"] == 7
        assert node.leader_id is None, "accepted a deposed leader's authority"
    print("PASS: rejects a heartbeat from a leader with a stale term")


def test_single_node_cluster_elects_itself():
    """A majority of 1 is 1, so it wins with only its own vote."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp, peers=())
        assert node._majority() == 1
        node._start_election()
        assert node.role == LEADER
    print("PASS: a one-node cluster elects itself with no RPCs")


def test_majority_math():
    with tempfile.TemporaryDirectory() as tmp:
        assert make_node(tmp, "a", peers=())._majority() == 1               # 1 node
        assert make_node(tmp, "b", peers=("x", "y"))._majority() == 2       # 3 nodes
        assert make_node(tmp, "c", peers=("x", "y", "z", "w"))._majority() == 3  # 5 nodes
    print("PASS: majority is N//2 + 1")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("\nAll election tests passed.")
