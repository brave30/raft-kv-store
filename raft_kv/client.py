"""
A client for the cluster.

WHY A CLIENT NEEDS ANY LOGIC AT ALL:
Talking to a distributed system is not like talking to one server. Three
things are different, and each one shapes a method below.

1. ANY NODE WILL DO. The client shouldn't have to know or track who the
   leader is — leadership can change between finding out and using the
   information. Nodes forward writes to the leader themselves, so the
   client just picks a node. If that node is down, it tries another.

2. RETRIES ARE NORMAL, NOT EXCEPTIONAL. Elections take a second or two,
   during which the cluster genuinely cannot accept writes — there is no
   leader to accept them. That's not an error, it's the system correctly
   refusing to proceed without consensus. A client that gives up on the
   first failure would look broken during an event the cluster handles
   perfectly well. So we retry across nodes and across a short window.

3. A TIMEOUT IS NOT A FAILURE. This is the uncomfortable one. If a write
   times out, the entry may still commit a moment later — we simply don't
   know. The honest report is "unknown", not "failed", and the caller
   must not assume the write didn't happen.

   This is why RETRIES REQUIRE IDEMPOTENT COMMANDS. Retrying "SET x=5"
   is harmless: applying it twice gives the same result as once. Retrying
   a hypothetical "INCREMENT x" would not be — a timed-out-then-retried
   increment could apply twice and silently corrupt the value. Making
   non-idempotent operations safe needs per-client sequence numbers so
   the cluster can recognise and ignore a duplicate. Our command set is
   deliberately restricted to idempotent operations instead; that
   restriction is a design decision, not an oversight.
"""

import argparse
import json
import time
import urllib.error
import urllib.request
from typing import Any

DEFAULT_TIMEOUT = 6.0

# How long to keep retrying across the cluster before giving up. Comfortably
# longer than an election (1.5-3.0s) so a leader change is ridden out rather
# than surfaced to the caller as a failure.
DEFAULT_RETRY_WINDOW = 8.0

# Errors that mean "the cluster is mid-changeover, ask again shortly"
# rather than "this request is invalid."
TRANSIENT_ERRORS = {
    "not_leader",
    "no_known_leader",
    "leader_unreachable",
    "leadership_lost",
    "leadership_not_confirmed",
    "leader_not_ready",
    "apply_timeout",
}


class ClusterUnavailable(Exception):
    """No node could service the request within the retry window."""


class WriteOutcomeUnknown(Exception):
    """
    A write timed out waiting for commitment.

    Deliberately a DIFFERENT exception from ClusterUnavailable, because it
    demands different handling: the write may yet succeed. Retrying is safe
    only because our commands are idempotent (see the module docstring).
    """


class RaftClient:
    def __init__(self, addresses: list[str], timeout: float = DEFAULT_TIMEOUT,
                 retry_window: float = DEFAULT_RETRY_WINDOW):
        if not addresses:
            raise ValueError("need at least one node address")
        self.addresses = [a.rstrip("/") for a in addresses]
        self.timeout = timeout
        self.retry_window = retry_window
        # Remember who last served us successfully and try them first.
        # Purely an optimisation — correctness never depends on this being
        # right, which matters because it frequently won't be.
        self._preferred = 0

    # ------------------------------------------------------------------
    def _post(self, address: str, path: str, payload: dict) -> dict | None:
        request = urllib.request.Request(
            f"{address}{path}",
            data=json.dumps(payload).encode("utf-8"),
            headers={"Content-Type": "application/json"},
            method="POST",
        )
        try:
            with urllib.request.urlopen(request, timeout=self.timeout) as response:
                return json.loads(response.read().decode("utf-8"))
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            return None

    def _request(self, path: str, payload: dict) -> dict:
        """
        Try nodes in turn until one gives a definitive answer, re-sweeping
        the cluster until the retry window expires.
        """
        deadline = time.monotonic() + self.retry_window
        last_error = "no node reachable"
        attempt = 0

        while time.monotonic() < deadline:
            # Start from the node that worked last time, then fan out.
            order = [self.addresses[(self._preferred + i) % len(self.addresses)]
                     for i in range(len(self.addresses))]
            for address in order:
                reply = self._post(address, path, payload)
                if reply is None:
                    last_error = "unreachable"
                    continue
                if reply.get("ok"):
                    self._preferred = self.addresses.index(address)
                    return reply
                error = reply.get("error", "unknown")
                last_error = error
                if error == "commit_timeout":
                    raise WriteOutcomeUnknown(
                        f"write may or may not have committed at index "
                        f"{reply.get('index')} — retry is safe for idempotent "
                        f"commands")
                if error not in TRANSIENT_ERRORS:
                    # A permanent error (malformed command, bad consistency
                    # level). Retrying cannot help; fail immediately rather
                    # than hammering the cluster for the full window.
                    raise ClusterUnavailable(f"request rejected: {error}")

            # Everything we tried was transient. Back off briefly — an
            # election needs time to finish, and retrying in a tight loop
            # just adds load to a cluster that's already busy.
            attempt += 1
            time.sleep(min(0.1 * attempt, 0.5))

        raise ClusterUnavailable(
            f"no node could service the request within {self.retry_window}s "
            f"(last error: {last_error})")

    # ------------------------------------------------------------------
    def set(self, key: str, value: str) -> int:
        """Write a key. Returns the log index it committed at."""
        reply = self._request("/write", {
            "command": {"op": "SET", "key": key, "value": value}})
        return reply["index"]

    def delete(self, key: str) -> int:
        """Delete a key. Idempotent: deleting a missing key is fine."""
        reply = self._request("/write", {"command": {"op": "DELETE", "key": key}})
        return reply["index"]

    def get(self, key: str, consistency: str = "linearizable") -> Any:
        """
        Read a key.

        consistency="linearizable" (default): served by a leader that has
            proven it's still leader. Costs a round trip; never stale.
        consistency="local": whatever the node we happen to reach has
            applied. Fast, may be stale. Opt in deliberately.
        """
        reply = self._request("/read", {"key": key, "consistency": consistency})
        return reply["value"]

    def status(self) -> list[dict]:
        """Ask every node how it sees the world. For debugging and demos."""
        out = []
        for address in self.addresses:
            try:
                with urllib.request.urlopen(f"{address}/status", timeout=1.0) as r:
                    out.append(json.loads(r.read().decode("utf-8")))
            except Exception:
                out.append({"node_id": address, "role": "DOWN"})
        return out


# ----------------------------------------------------------------------
# CLI: python -m raft_kv.client set colour blue
# ----------------------------------------------------------------------

DEFAULT_ADDRESSES = [
    "http://127.0.0.1:5001",
    "http://127.0.0.1:5002",
    "http://127.0.0.1:5003",
]


def main() -> None:
    parser = argparse.ArgumentParser(description="Talk to a Raft KV cluster.")
    parser.add_argument("--nodes", default=",".join(DEFAULT_ADDRESSES),
                        help="comma-separated node URLs")
    sub = parser.add_subparsers(dest="command", required=True)

    p_set = sub.add_parser("set", help="write a key")
    p_set.add_argument("key")
    p_set.add_argument("value")

    p_get = sub.add_parser("get", help="read a key")
    p_get.add_argument("key")
    p_get.add_argument("--local", action="store_true",
                       help="allow a possibly-stale local read")

    p_del = sub.add_parser("delete", help="delete a key")
    p_del.add_argument("key")

    sub.add_parser("status", help="show every node's view of the cluster")

    args = parser.parse_args()
    client = RaftClient([a.strip() for a in args.nodes.split(",") if a.strip()])

    try:
        if args.command == "set":
            index = client.set(args.key, args.value)
            print(f"OK: {args.key}={args.value} committed at index {index}")
        elif args.command == "get":
            level = "local" if args.local else "linearizable"
            value = client.get(args.key, consistency=level)
            print(f"{args.key} = {value!r}  ({level} read)")
        elif args.command == "delete":
            index = client.delete(args.key)
            print(f"OK: deleted {args.key} at index {index}")
        elif args.command == "status":
            for s in client.status():
                if s.get("role") == "DOWN":
                    print(f"  {s['node_id']}: DOWN")
                else:
                    print(f"  {s['node_id']}: {s['role'].upper():<9} "
                          f"term={s['term']} commit={s['commit_index']} "
                          f"store={s['store']}")
    except WriteOutcomeUnknown as e:
        # Exit code 2, distinct from a clean failure, because the caller
        # genuinely does not know whether the write landed.
        print(f"UNKNOWN: {e}")
        raise SystemExit(2)
    except ClusterUnavailable as e:
        print(f"FAILED: {e}")
        raise SystemExit(1)


if __name__ == "__main__":
    main()
