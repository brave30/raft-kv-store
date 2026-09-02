"""
Tests for the client-facing API: leader forwarding and linearizable reads.

The interesting cases here are about REFUSING to answer. A read path that
returns a plausible-looking stale value is worse than one that fails
loudly, so most of these assert that the node declines to serve a read it
cannot stand behind.
"""

import os
import sys
import tempfile
import threading
import time

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from raft_kv.log import LogEntry
from raft_kv.node import FOLLOWER, LEADER, RaftNode
from raft_kv.rpc import RaftHTTPServer
from raft_kv.client import RaftClient, ClusterUnavailable


def make_node(tmpdir, node_id="node1", peers=("node2", "node3")):
    peer_map = {p: f"http://127.0.0.1:9{i}" for i, p in enumerate(peers)}
    return RaftNode(
        node_id=node_id,
        peers=peer_map,
        storage_path=os.path.join(tmpdir, f"{node_id}_state.json"),
    )


def append_msg(term=1, leader="node2", prev_index=0, prev_term=0,
               entries=(), commit=0):
    return {
        "term": term, "leader_id": leader,
        "prev_log_index": prev_index, "prev_log_term": prev_term,
        "entries": [e.to_dict() for e in entries], "leader_commit": commit,
    }


# ---------------------------------------------------------------------
# consistency levels
# ---------------------------------------------------------------------

def test_local_read_is_served_without_a_leader():
    """A local read is explicitly allowed to be stale — no leader needed."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_append_entries(append_msg(entries=[
            LogEntry(term=1, index=1,
                     command={"op": "SET", "key": "x", "value": "1"})], commit=1))
        reply = node.handle_client_read({"key": "x", "consistency": "local"})
        assert reply["ok"] is True
        assert reply["value"] == "1"
        assert reply["consistency"] == "local"
    print("PASS: a local read is served from a follower without a leader")


def test_linearizable_read_is_refused_by_a_follower_with_no_known_leader():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        reply = node.handle_client_read({"key": "x"})  # default: linearizable
        assert reply["ok"] is False
        assert reply["error"] == "no_known_leader"
    print("PASS: linearizable read refused when no leader is known")


def test_unknown_consistency_level_is_rejected():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        reply = node.handle_client_read({"key": "x", "consistency": "eventually?"})
        assert reply["ok"] is False
        assert reply["error"] == "bad_consistency_level"
    print("PASS: an unrecognised consistency level is rejected, not guessed")


def test_linearizable_is_the_default():
    """Safe by default: you must opt IN to staleness, never into safety."""
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        assert node.handle_client_read({"key": "x"})["error"] == "no_known_leader"
    print("PASS: reads default to linearizable, staleness must be requested")


# ---------------------------------------------------------------------
# the leader's own read path
# ---------------------------------------------------------------------

def test_single_node_leader_serves_a_linearizable_read():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp, peers=())
        node._start_election()
        assert node.role == LEADER
        node.submit({"op": "SET", "key": "x", "value": "42"})

        reply = node.handle_client_read({"key": "x"})
        assert reply["ok"] is True
        assert reply["value"] == "42"
        assert reply["consistency"] == "linearizable"
    print("PASS: a single-node leader serves a linearizable read")


def test_leader_refuses_a_read_before_committing_its_own_term():
    """
    A fresh leader knows its log is complete, but not how much of it the
    previous leader had committed. Until one of ITS OWN entries commits,
    it cannot say where the committed boundary is — so it must not serve.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node._become_candidate()
        node._become_leader()   # appends a NOOP, but no majority has it yet
        assert node.commit_index == 0

        reply = node.handle_client_read({"key": "x"})
        assert reply["ok"] is False
        assert reply["error"] == "leader_not_ready"
    print("PASS: a new leader refuses reads until an entry of its own term commits")


def test_leader_refuses_a_read_when_it_cannot_reach_a_majority():
    """
    THE ZOMBIE LEADER TEST. A partitioned leader still believes it leads.
    Its peers are unreachable (ports nothing is listening on), so the
    confirmation round fails and it must decline rather than serve
    possibly-stale data with full confidence.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node._become_candidate()
        node._become_leader()
        # Pretend the NOOP committed under a healthy majority, so that
        # step 1 passes and we are testing step 2 specifically.
        node.commit_index = node.state.last_log_index()
        node._apply_committed()

        started = time.monotonic()
        reply = node.handle_client_read({"key": "x"})
        elapsed = time.monotonic() - started

        assert reply["ok"] is False
        assert reply["error"] == "leadership_not_confirmed"
        assert elapsed < 5.0, "should fail fast, not hang"
    print("PASS: a partitioned leader refuses to serve reads it cannot vouch for")


# ---------------------------------------------------------------------
# forwarding
# ---------------------------------------------------------------------

def test_follower_refuses_write_when_no_leader_is_known():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        reply = node.handle_client_write({"command": {"op": "SET", "key": "x",
                                                      "value": "1"}})
        assert reply["ok"] is False
        assert reply["error"] == "no_known_leader"
    print("PASS: a write is refused when no leader is known")


def test_already_forwarded_request_is_not_forwarded_again():
    """
    Loop protection. Two nodes can briefly each think the other leads;
    without this flag the request would ping-pong between them.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        node.handle_append_entries(append_msg(term=2, leader="node2"))
        assert node.role == FOLLOWER and node.leader_id == "node2"

        reply = node.handle_client_write({
            "command": {"op": "SET", "key": "x", "value": "1"},
            "forwarded": True,
        })
        assert reply["ok"] is False
        assert reply["error"] == "not_leader", "should refuse, not forward again"
    print("PASS: an already-forwarded request is refused rather than re-forwarded")


def test_forwarding_reports_an_unreachable_leader():
    with tempfile.TemporaryDirectory() as tmp:
        node = make_node(tmp)
        # Believes node2 leads, but node2's URL points at a dead port.
        node.handle_append_entries(append_msg(term=2, leader="node2"))
        reply = node.handle_client_write({"command": {"op": "SET", "key": "x",
                                                      "value": "1"}})
        assert reply["ok"] is False
        assert reply["error"] == "leader_unreachable"
        assert reply["leader_id"] == "node2"
    print("PASS: forwarding to a dead leader reports it rather than hanging")


# ---------------------------------------------------------------------
# end-to-end over real HTTP, single node
# ---------------------------------------------------------------------

def test_client_against_a_real_single_node_server():
    """
    Exercises the whole stack: RaftClient -> HTTP -> node -> store.
    A single node is a legitimate (if trivial) cluster: majority is 1.
    """
    with tempfile.TemporaryDirectory() as tmp:
        node = RaftNode("solo", {}, os.path.join(tmp, "solo.json"))
        server = RaftHTTPServer("127.0.0.1", 5599, {
            "/request_vote": node.handle_request_vote,
            "/append_entries": node.handle_append_entries,
            "/write": node.handle_client_write,
            "/read": node.handle_client_read,
            "/status": node.handle_status,
        })
        server.start()
        node.start()
        try:
            # Wait for it to elect itself.
            deadline = time.monotonic() + 6
            while node.role != LEADER and time.monotonic() < deadline:
                time.sleep(0.05)
            assert node.role == LEADER, "solo node never became leader"

            client = RaftClient(["http://127.0.0.1:5599"])
            index = client.set("colour", "blue")
            assert index >= 1
            assert client.get("colour") == "blue"
            assert client.get("colour", consistency="local") == "blue"

            client.set("colour", "green")
            assert client.get("colour") == "green", "overwrite not visible"

            client.delete("colour")
            assert client.get("colour") is None

            # Reading a key that was never written is not an error.
            assert client.get("never-set") is None
        finally:
            node.stop()
            server.stop()
    print("PASS: end-to-end client set/get/delete over real HTTP")


def test_client_gives_up_on_a_dead_cluster():
    client = RaftClient(["http://127.0.0.1:5998"], timeout=0.3, retry_window=1.0)
    started = time.monotonic()
    try:
        client.get("x")
        raise AssertionError("should have raised")
    except ClusterUnavailable:
        pass
    elapsed = time.monotonic() - started
    assert elapsed < 6.0, "should give up within roughly the retry window"
    print("PASS: the client gives up on an unreachable cluster within its window")


def test_client_rejects_a_permanent_error_without_retrying():
    """A malformed request should fail fast, not burn the whole window."""
    with tempfile.TemporaryDirectory() as tmp:
        node = RaftNode("solo2", {}, os.path.join(tmp, "solo2.json"))
        server = RaftHTTPServer("127.0.0.1", 5597, {
            "/read": node.handle_client_read,
        })
        server.start()
        try:
            client = RaftClient(["http://127.0.0.1:5597"], retry_window=10.0)
            started = time.monotonic()
            try:
                client.get("x", consistency="nonsense")
                raise AssertionError("should have raised")
            except ClusterUnavailable as e:
                assert "bad_consistency_level" in str(e)
            elapsed = time.monotonic() - started
            assert elapsed < 2.0, "a permanent error should not be retried"
        finally:
            server.stop()
    print("PASS: a permanent error fails immediately instead of retrying")


if __name__ == "__main__":
    for name, fn in sorted(globals().items()):
        if name.startswith("test_") and callable(fn):
            fn()
    print("\nAll client API tests passed.")
