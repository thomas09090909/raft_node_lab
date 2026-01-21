#!/usr/bin/env python3
import argparse, json
from urllib import request

# Function to send a POST request to a given URL with JSON payload
def post(url, payload):
    # Convert Python dictionary to JSON bytes
    data = json.dumps(payload).encode("utf-8")
    
    # Prepare the HTTP POST request with appropriate headers
    req = request.Request(
        url, 
        data=data, 
        method="POST", 
        headers={"Content-Type":"application/json"}
    )
    
    # Send the request and read the JSON response
    with request.urlopen(req, timeout=2.0) as resp:
        return json.loads(resp.read().decode("utf-8"))

# Main function to parse command-line arguments and send command
def main():
    ap = argparse.ArgumentParser()
    
    # --node: specify the Raft node to send the command to (leader IP:port)
    ap.add_argument("--node", required=True, help="http://<ip>:<port>")
    
    # --cmd: the command to execute, e.g., "SET x = 5"
    ap.add_argument("--cmd", required=True, help='e.g. "SET x = 5"')
    
    args = ap.parse_args()
    
    # Send the command to the node and print the response
    print(post(args.node + "/client", {"cmd": args.cmd}))

# Standard Python entry point
if __name__ == "__main__":
    main()
