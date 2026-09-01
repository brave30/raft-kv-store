"""
Entry point: turn a RaftNode into an actual OS process with a network
identity, so we can run several of them and let them find each other.

Run one node:
    python -m raft_kv.server --id node1 --port 5001 \
        --peers node2=http://127.0.0.1:5002,node3=http://127.0.0.1:5003

Or just run scripts/run_cluster.py to launch three at once.

WHY EACH NODE IS A SEPARATE OS PROCESS:
We could have simulated a cluster with three objects and three threads in
one process, and it would be far easier to debug. But it would quietly
cheat: threads share memory, so bugs where one node accidentally reads
another's state would go unnoticed, and "kill a node" wouldn't really
test crash recovery. Separate processes give us the real constraint —
the only way to share information is to send a message — and they let us
kill a leader with Ctrl+C and watch the cluster genuinely recover.

WHY PEERS ARE A STATIC LIST ON THE COMMAND LINE:
The cluster membership is fixed and known at startup. Changing membership
while running ("dynamic reconfiguration") is a genuinely hard part of Raft
with its own subtle safety rules, and it's not needed for this project.
Hardcoding the roster is the right call.
"""

import argparse
import os
import time

from .node import RaftNode
from .rpc import RaftHTTPServer


def parse_peers(raw: str) -> dict[str, str]:
    """Parse 'node2=http://127.0.0.1:5002,node3=http://...' into a dict."""
    peers: dict[str, str] = {}
    for chunk in raw.split(","):
        chunk = chunk.strip()
        if not chunk:
            continue
        peer_id, url = chunk.split("=", 1)
        peers[peer_id.strip()] = url.strip().rstrip("/")
    return peers


def main() -> None:
    parser = argparse.ArgumentParser(description="Run one Raft node.")
    parser.add_argument("--id", required=True, help="this node's id, e.g. node1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--peers", default="", help="id=url,id=url (excluding self)")
    parser.add_argument("--data-dir", default="data", help="where state files live")
    args = parser.parse_args()

    os.makedirs(args.data_dir, exist_ok=True)
    storage_path = os.path.join(args.data_dir, f"{args.id}_state.json")

    node = RaftNode(
        node_id=args.id,
        peers=parse_peers(args.peers),
        storage_path=storage_path,
    )

    server = RaftHTTPServer(
        host=args.host,
        port=args.port,
        handlers={
            # Node-to-node consensus RPCs
            "/request_vote": node.handle_request_vote,
            "/append_entries": node.handle_append_entries,
            # Client-facing endpoints. Deliberately on the same port and
            # server as the consensus RPCs for now — Phase 4 is where the
            # client API grows a real front end (leader forwarding,
            # linearizable reads). These exist so Phase 3's replication
            # can be driven and observed.
            "/write": node.handle_client_write,
            "/read": node.handle_client_read,
            "/status": node.handle_status,
        },
    )

    # Start the HTTP server BEFORE the election ticker. If we started
    # campaigning before we could receive replies, we'd waste an election.
    server.start()
    node.log(f"listening on http://{args.host}:{args.port} "
             f"(peers: {', '.join(node.peers) or 'none'})")
    node.start()

    try:
        while True:
            time.sleep(0.5)
    except KeyboardInterrupt:
        # Ctrl+C is how we simulate a crash in the demo. Note we do NOT
        # try to hand off leadership or notify peers — a real crash gets
        # no such courtesy, and the cluster must recover without it.
        node.log("shutting down")
        node.stop()
        server.stop()


if __name__ == "__main__":
    main()
