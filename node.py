#!/usr/bin/env python3
import argparse
import json
import random
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib import request, error

# Helper function to send HTTP POST requests with JSON payload
def http_post(url: str, payload: dict, timeout: float = 0.7) -> dict:
    data = json.dumps(payload).encode("utf-8")  # Convert dict to JSON bytes
    req = request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json"}  # Specify JSON content
    )
    with request.urlopen(req, timeout=timeout) as resp:
        return json.loads(resp.read().decode("utf-8"))  # Return JSON response as dict

# RaftNode class implements Raft Lite functionality
class RaftNode:
    def __init__(self, node_id: str, port: int, peers: list[str]):
        self.id = node_id
        self.port = port
        self.peers = peers  # list of peer URLs like ["http://10.0.1.11:8001", ...]

        # Required Raft state
        self.currentTerm = 0
        self.votedFor = None
        self.log = []          # list of log entries: {"term": int, "cmd": str}
        self.commitIndex = -1
        self.state = "Follower"  # Node role: Follower | Candidate | Leader

        # Extra state for leader tracking and timeouts
        self.leaderId = None
        self.lastHeartbeat = time.time()
        self.lock = threading.RLock()  # Thread-safe updates

        # Leader-only replication tracking
        self.nextIndex = {}    # next log index to send to each peer
        self.matchIndex = {}   # highest log index known replicated for each peer

        # KV state machine
        self.kv = {}
        self.lastApplied = -1

        # Timing configuration
        self.heartbeatInterval = 0.5
        self.electionTimeoutMin = 4.0
        self.electionTimeoutMax = 7.0
        self._curTimeout = self._new_timeout()

        self.running = True  # Main loop flag

    # Print message with node ID prefix
    def p(self, msg: str):
        print(f"[Node {self.id}] {msg}", flush=True)

    # Print message with role and term
    def role_line(self, msg: str):
        print(f"[Node {self.id} | {self.state} | term {self.currentTerm}] {msg}", flush=True)

    # Generate new random election timeout
    def _new_timeout(self) -> float:
        return random.uniform(self.electionTimeoutMin, self.electionTimeoutMax)

    # Transition to follower
    def become_follower(self, term: int, leaderId=None):
        with self.lock:
            if term > self.currentTerm:
                self.currentTerm = term
                self.votedFor = None
            self.state = "Follower"
            self.leaderId = leaderId
            self.lastHeartbeat = time.time()
            self._curTimeout = self._new_timeout()

    # Transition to candidate and start election
    def become_candidate(self):
        with self.lock:
            self.state = "Candidate"
            self.currentTerm += 1
            self.votedFor = self.id  # vote for self
            self.leaderId = None
            self.lastHeartbeat = time.time()
            self._curTimeout = self._new_timeout()
            self.role_line(f"Timeout -> Candidate (term {self.currentTerm})")

    # Transition to leader and initialize replication state
    def become_leader(self):
        with self.lock:
            self.state = "Leader"
            self.leaderId = self.id
            for p in self.peers:  # initialize leader replication tracking
                self.nextIndex[p] = len(self.log)
                self.matchIndex[p] = -1
            self.role_line("Received majority votes -> Leader")

    # Handle incoming vote requests
    def handle_request_vote(self, term: int, candidateId: str) -> dict:
        with self.lock:
            if term > self.currentTerm:
                self.become_follower(term)
            if term < self.currentTerm:
                return {"term": self.currentTerm, "voteGranted": False}
            if self.votedFor is None or self.votedFor == candidateId:
                self.votedFor = candidateId
                self.lastHeartbeat = time.time()
                return {"term": self.currentTerm, "voteGranted": True}
            return {"term": self.currentTerm, "voteGranted": False}

    # Handle AppendEntries requests from leader (log replication / heartbeat)
    def handle_append_entries(self, term: int, leaderId: str, entries: list) -> dict:
        with self.lock:
            if term > self.currentTerm:
                self.become_follower(term, leaderId=leaderId)
            if term < self.currentTerm:
                return {"term": self.currentTerm, "success": False, "logLen": len(self.log), "commitIndex": self.commitIndex}

            self.state = "Follower"
            self.leaderId = leaderId
            self.lastHeartbeat = time.time()

            appended = 0
            for e in entries:  # append missing entries
                self.log.append(e)
                appended += 1
            if appended > 0:
                self.role_line(f"Append success (+{appended})")

            return {"term": self.currentTerm, "success": True, "logLen": len(self.log), "commitIndex": self.commitIndex}

    # Election loop to monitor timeouts and start elections
    def election_loop(self):
        while self.running:
            time.sleep(0.05)
            with self.lock:
                if self.state == "Leader":
                    continue
                if (time.time() - self.lastHeartbeat) < self._curTimeout:
                    continue
            self.start_election()

    # Start an election by requesting votes
    def start_election(self):
        self.become_candidate()
        with self.lock:
            term = self.currentTerm
        votes = 1  # self vote
        needed = (len(self.peers) + 1) // 2 + 1
        votes_lock = threading.Lock()

        def ask(peer):
            nonlocal votes
            try:
                resp = http_post(peer + "/request_vote",
                                 {"term": term, "candidateId": self.id},
                                 timeout=0.6)
                with self.lock:
                    if self.state != "Candidate":
                        return
                    if resp.get("term", 0) > self.currentTerm:
                        self.become_follower(resp["term"])
                        return
                if resp.get("voteGranted"):
                    with votes_lock:
                        votes += 1
            except Exception:
                pass

        threads = []
        for p in self.peers:
            t = threading.Thread(target=ask, args=(p,), daemon=True)
            t.start()
            threads.append(t)

        # Wait briefly for votes
        deadline = time.time() + 0.9
        while time.time() < deadline:
            with self.lock:
                if self.state != "Candidate":
                    return
                if votes >= needed:
                    self.become_leader()
                    return
            time.sleep(0.02)

    # Leader sends periodic heartbeats to followers
    def heartbeat_loop(self):
        while self.running:
            time.sleep(self.heartbeatInterval)
            with self.lock:
                if self.state != "Leader":
                    continue
                term = self.currentTerm
            for p in self.peers:
                try:
                    resp = http_post(p + "/append_entries",
                                     {"term": term, "leaderId": self.id, "entries": []},
                                     timeout=0.4)
                    with self.lock:
                        if resp.get("term", 0) > self.currentTerm:
                            self.become_follower(resp["term"])
                except Exception:
                    pass

    # Replication loop: leader sends missing log entries to followers
    def replication_loop(self):
        while self.running:
            time.sleep(0.15)
            with self.lock:
                if self.state != "Leader":
                    continue
                term = self.currentTerm
                my_len = len(self.log)
            for p in self.peers:
                with self.lock:
                    ni = self.nextIndex.get(p, my_len)
                    if ni >= my_len:
                        continue
                    entries = self.log[ni:]
                try:
                    resp = http_post(p + "/append_entries",
                                     {"term": term, "leaderId": self.id, "entries": entries},
                                     timeout=0.7)
                    with self.lock:
                        if resp.get("term", 0) > self.currentTerm:
                            self.become_follower(resp["term"])
                            continue
                        if resp.get("success"):
                            self.matchIndex[p] = my_len - 1
                            self.nextIndex[p] = my_len
                            self.try_advance_commit()
                except Exception:
                    pass

    # Leader commits entries once majority replicated
    def try_advance_commit(self):
        if self.state != "Leader":
            return
        n = len(self.log)
        if n == 0:
            return
        needed = (len(self.peers) + 1) // 2 + 1
        for i in range(self.commitIndex + 1, n):
            count = 1  # leader itself
            for p in self.peers:
                if self.matchIndex.get(p, -1) >= i:
                    count += 1
            if count >= needed:
                self.commitIndex = i
                self.apply_commits()
                self.role_line(f"Entry committed (index={i})")

    # Apply committed entries to state machine
    def apply_commits(self):
        while self.lastApplied < self.commitIndex:
            self.lastApplied += 1
            cmd = self.log[self.lastApplied]["cmd"]
            self.apply_cmd(cmd)

    # Minimal KV command execution
    def apply_cmd(self, cmd: str):
        parts = cmd.strip().split()
        if len(parts) >= 4 and parts[0].upper() == "SET" and parts[2] == "=":
            key = parts[1]
            value = " ".join(parts[3:])
            self.kv[key] = value

    # Handle commands sent by clients
    def handle_client(self, cmd: str) -> dict:
        with self.lock:
            if self.state != "Leader":
                return {
                    "ok": False,
                    "error": "not_leader",
                    "leaderId": self.leaderId,
                    "term": self.currentTerm
                }
            entry = {"term": self.currentTerm, "cmd": cmd}
            self.log.append(entry)
            index = len(self.log) - 1
            self.role_line(f"Append log entry (term={entry['term']}, cmd={cmd})")
        self.push_once()  # try to replicate immediately
        with self.lock:
            return {"ok": True, "index": index, "term": self.currentTerm}

    # Single push of missing entries to followers
    def push_once(self):
        with self.lock:
            if self.state != "Leader":
                return
            term = self.currentTerm
            my_len = len(self.log)
        for p in self.peers:
            with self.lock:
                ni = self.nextIndex.get(p, my_len)
                if ni >= my_len:
                    continue
                entries = self.log[ni:]
            try:
                resp = http_post(p + "/append_entries",
                                 {"term": term, "leaderId": self.id, "entries": entries},
                                 timeout=0.7)
                with self.lock:
                    if resp.get("term", 0) > self.currentTerm:
                        self.become_follower(resp["term"])
                        continue
                    if resp.get("success"):
                        self.matchIndex[p] = my_len - 1
                        self.nextIndex[p] = my_len
            except Exception:
                pass
        with self.lock:
            if self.state == "Leader":
                self.try_advance_commit()

    # Return current node status
    def status(self) -> dict:
        with self.lock:
            return {
                "id": self.id,
                "state": self.state,
                "currentTerm": self.currentTerm,
                "votedFor": self.votedFor,
                "leaderId": self.leaderId,
                "logLen": len(self.log),
                "commitIndex": self.commitIndex,
                "kv": dict(self.kv),
            }

# HTTP request handler generator
def make_handler(node: RaftNode):
    class Handler(BaseHTTPRequestHandler):
        # Send JSON response
        def _send(self, code: int, obj: dict):
            b = json.dumps(obj).encode("utf-8")
            self.send_response(code)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(b)))
            self.end_headers()
            self.wfile.write(b)

        # Read JSON from request body
        def _read_json(self):
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length) if length > 0 else b"{}"
            return json.loads(raw.decode("utf-8"))

        # GET requests
        def do_GET(self):
            if self.path == "/status":
                self._send(200, node.status())
            else:
                self._send(404, {"error": "not_found"})

        # POST requests
        def do_POST(self):
            try:
                body = self._read_json()
            except Exception:
                self._send(400, {"error": "bad_json"})
                return

            if self.path == "/request_vote":
                resp = node.handle_request_vote(
                    term=int(body.get("term", 0)),
                    candidateId=str(body.get("candidateId", "")),
                )
                self._send(200, resp)
                return

            if self.path == "/append_entries":
                resp = node.handle_append_entries(
                    term=int(body.get("term", 0)),
                    leaderId=str(body.get("leaderId", "")),
                    entries=list(body.get("entries", [])),
                )
                self._send(200, resp)
                return

            if self.path == "/client":
                resp = node.handle_client(cmd=str(body.get("cmd", "")))
                self._send(200, resp)
                return

            self._send(404, {"error": "not_found"})

        # Suppress default HTTP logs
        def log_message(self, fmt, *args):
            return

    return Handler

# Main entry point
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--id", required=True, help="Node ID like A/B/C/D/E")
    ap.add_argument("--host", default="0.0.0.0")
    ap.add_argument("--port", type=int, required=True)
    ap.add_argument("--peers", default="", help="Comma-separated peer base URLs")
    args = ap.parse_args()

    peers = [p.strip() for p in args.peers.split(",") if p.strip()]
    node = RaftNode(args.id, args.port, peers)
    node.p(f"Listening on {args.host}:{args.port} peers={peers}")

    # Start background threads for election, heartbeats, and replication
    threading.Thread(target=node.election_loop, daemon=True).start()
    threading.Thread(target=node.heartbeat_loop, daemon=True).start()
    threading.Thread(target=node.replication_loop, daemon=True).start()

    # Start HTTP server
    server = ThreadingHTTPServer((args.host, args.port), make_handler(node))
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        node.running = False
        server.server_close()

if __name__ == "__main__":
    main()
