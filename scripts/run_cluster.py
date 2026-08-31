"""
Launch a 3-node Raft cluster locally and watch a leader get elected.

    python scripts/run_cluster.py

All three nodes' output is merged into this one terminal, so you can see
the election as a single ordered story instead of juggling three windows.

Once it's running, type commands at the prompt:

    status        — ask every node what it thinks its role is
    kill <id>     — hard-kill a node (simulates a crash). Kill the leader!
    start <id>    — bring a killed node back (it reloads state from disk)
    quit          — shut everything down

WHY A 3-NODE CLUSTER:
A majority of 3 is 2, so the cluster tolerates exactly ONE failure and
keeps working. Kill a second node and the survivor can never reach 2
votes — it will campaign forever without winning. That's not a bug, it's
Raft refusing to proceed without a majority. Given the choice between
"stay available and risk two leaders corrupting data" and "stop until a
majority returns," Raft always chooses to stop. Losing availability is
recoverable; losing consensus is not.

Note that 2-node clusters are pointless (majority of 2 is 2, so ANY
failure halts you — strictly worse than 1 node), and even-sized clusters
in general buy nothing: 4 nodes also tolerate only 1 failure, same as 3.
This is why real clusters are almost always 3, 5, or 7.
"""

import os
import subprocess
import sys
import threading
import urllib.request
import json

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(ROOT, "data")

NODES = {
    "node1": 5001,
    "node2": 5002,
    "node3": 5003,
}

processes: dict[str, subprocess.Popen] = {}


def peers_for(node_id: str) -> str:
    return ",".join(
        f"{other}=http://127.0.0.1:{port}"
        for other, port in NODES.items()
        if other != node_id
    )


def pump_output(node_id: str, proc: subprocess.Popen) -> None:
    """Forward one node's stdout into our terminal, line by line."""
    for line in proc.stdout:
        sys.stdout.write(line)
        sys.stdout.flush()


def start_node(node_id: str) -> None:
    if node_id in processes and processes[node_id].poll() is None:
        print(f"  {node_id} is already running")
        return
    proc = subprocess.Popen(
        [
            sys.executable, "-u", "-m", "raft_kv.server",
            "--id", node_id,
            "--port", str(NODES[node_id]),
            "--peers", peers_for(node_id),
            "--data-dir", DATA_DIR,
        ],
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
    )
    processes[node_id] = proc
    threading.Thread(target=pump_output, args=(node_id, proc), daemon=True).start()


def kill_node(node_id: str) -> None:
    proc = processes.get(node_id)
    if proc is None or proc.poll() is not None:
        print(f"  {node_id} is not running")
        return
    # kill(), not terminate() — we want an abrupt crash with no cleanup,
    # because that's the failure mode Raft has to survive.
    proc.kill()
    proc.wait()
    print(f"  *** KILLED {node_id} ***")


def show_status() -> None:
    print("  ---- cluster status ----")
    for node_id, port in NODES.items():
        try:
            with urllib.request.urlopen(
                f"http://127.0.0.1:{port}/status", timeout=0.5
            ) as response:
                s = json.loads(response.read().decode())
            print(f"  {node_id}: {s['role'].upper():<9} term={s['term']} "
                  f"leader={s['leader_id']} voted_for={s['voted_for']}")
        except Exception:
            print(f"  {node_id}: DOWN (no response)")
    print("  ------------------------")


def main() -> None:
    os.makedirs(DATA_DIR, exist_ok=True)
    # Start from a clean slate each run so terms don't climb forever
    # across demos. (Real nodes obviously keep their state — this is
    # purely a convenience for repeatable demos.)
    for name in os.listdir(DATA_DIR):
        if name.endswith("_state.json"):
            os.remove(os.path.join(DATA_DIR, name))

    print(__doc__)
    print("Starting 3 nodes...\n")
    for node_id in NODES:
        start_node(node_id)

    try:
        while True:
            try:
                command = input().strip()
            except EOFError:
                break
            if not command:
                continue
            parts = command.split()
            verb = parts[0].lower()

            if verb == "quit":
                break
            elif verb == "status":
                show_status()
            elif verb in ("kill", "start") and len(parts) == 2:
                target = parts[1]
                if target not in NODES:
                    print(f"  unknown node {target}")
                elif verb == "kill":
                    kill_node(target)
                else:
                    start_node(target)
            else:
                print("  commands: status | kill <id> | start <id> | quit")
    except KeyboardInterrupt:
        pass
    finally:
        print("\nShutting down cluster...")
        for proc in processes.values():
            if proc.poll() is None:
                proc.kill()


if __name__ == "__main__":
    main()
