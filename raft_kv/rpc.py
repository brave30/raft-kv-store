"""
The network layer: how one node talks to another.

WHY WE NEED THIS AT ALL:
In Phase 1 a "node" was just a Python object in one process. Raft only
becomes interesting when nodes are separate processes that can ONLY
communicate by sending messages over a network — because that's where
all the hard problems live: messages arrive late, arrive out of order,
or never arrive at all, and a node has no way to tell "my peer crashed"
apart from "my peer is slow" or "the network dropped my message."

That indistinguishability is THE core difficulty of distributed systems.
Raft is essentially a protocol for making correct decisions despite
never being able to tell those three cases apart.

DESIGN CHOICE — plain HTTP + JSON, using only the standard library:
Every RPC is an HTTP POST with a JSON body, and every reply is JSON.
That means you can debug the whole cluster with `curl`, and there's no
framework magic between you and the wire. Slower than gRPC? Absolutely.
Irrelevant here — we're optimizing for "you can see what's happening."

TWO IMPORTANT PROPERTIES OF THIS LAYER:

1. Every outbound call has a SHORT TIMEOUT. If a peer is dead, we must
   not block forever waiting for it — a leader that hangs waiting on one
   dead follower would stop sending heartbeats to the healthy ones, and
   they'd start a pointless election. In Raft, "no reply" is a normal,
   expected outcome, not an error.

2. Every failure is swallowed and returned as None. A refused connection
   (peer process not running) is completely routine. The consensus logic
   above this layer treats "None" the same as "no vote" / "no ack" and
   carries on. Crashing on a network error would defeat the entire point
   of building a fault-tolerant system.
"""

import json
import threading
import urllib.error
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any, Callable


# How long to wait for a peer to respond before giving up on this RPC.
# This must be comfortably SHORTER than the election timeout, otherwise
# a leader could still be blocked waiting on RPCs when followers have
# already given up on it and started an election.
RPC_TIMEOUT_SECONDS = 0.5


def send_rpc(url: str, payload: dict[str, Any]) -> dict[str, Any] | None:
    """
    Send one JSON-over-HTTP RPC. Returns the decoded reply, or None if
    the peer was unreachable / too slow / returned something unusable.

    Returning None instead of raising is deliberate: see note 2 above.
    """
    body = json.dumps(payload).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(request, timeout=RPC_TIMEOUT_SECONDS) as response:
            return json.loads(response.read().decode("utf-8"))
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
        # Peer is down, slow, or babbling. All three are the same to us.
        return None


class RaftHTTPServer:
    """
    The inbound half: an HTTP server exposing this node's RPC endpoints.

    Routes:
      POST /request_vote   — "vote for me, I want to be leader"
      POST /append_entries — heartbeat (and, in Phase 3, real log entries)
      GET  /status         — human-readable state, for watching the demo

    NOTE ON THREADING: this uses ThreadingHTTPServer, so each incoming
    request is handled on its own thread, concurrently with the node's
    own background election timer. That means the node's state is touched
    by several threads at once, which is exactly why RaftNode guards all
    of its state with a lock. Getting this wrong would be a subtle,
    intermittent bug — the worst kind.
    """

    def __init__(self, host: str, port: int, handlers: dict[str, Callable]):
        self._handlers = handlers
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def _reply(self, code: int, payload: dict) -> None:
                encoded = json.dumps(payload).encode("utf-8")
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(encoded)))
                self.end_headers()
                self.wfile.write(encoded)

            def do_POST(self) -> None:
                handler = outer._handlers.get(self.path)
                if handler is None:
                    self._reply(404, {"error": "no such rpc"})
                    return
                length = int(self.headers.get("Content-Length", 0))
                try:
                    payload = json.loads(self.rfile.read(length).decode("utf-8"))
                except json.JSONDecodeError:
                    self._reply(400, {"error": "bad json"})
                    return
                self._reply(200, handler(payload))

            def do_GET(self) -> None:
                handler = outer._handlers.get(self.path)
                if handler is None:
                    self._reply(404, {"error": "no such endpoint"})
                    return
                self._reply(200, handler({}))

            def log_message(self, *args) -> None:
                # Silence the default per-request access log — heartbeats
                # every 0.5s would completely bury our own election output.
                pass

        self._server = ThreadingHTTPServer((host, port), Handler)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._server.shutdown()
        self._server.server_close()
