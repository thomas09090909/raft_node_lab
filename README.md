Raft Lite — Run Instructions
1. Start Nodes

Launch a Raft Lite node on each EC2 instance.

Each node must have a unique ID and port number, and know the addresses of all other nodes.

Example for Node A:

python3 node.py --id A --host 0.0.0.0 --port 8000 \
--peers http://NODE_B_IP:8001,http://NODE_C_IP:8002,http://NODE_D_IP:8003,http://NODE_E_IP:8004


Repeat this process for the remaining nodes, changing:

--id to the node’s unique identifier

--port to a distinct port for that node

--peers to list all other nodes in the cluster

2. Verify Leader

Check the status of a node to see if it is the leader or a follower:

curl http://NODE_IP:PORT/status


The response will indicate the node’s role, current term, and log information.

3. Submit Client Commands

Commands must be sent only to the leader node.

Example of sending a command to set a key-value pair:

python3 client.py --node http://LEADER_IP:LEADER_PORT --cmd "SET x = 5"


The leader appends the entry to its log and replicates it to followers.

4. Test Failure Handling

Leader crash: Stop the leader node using Ctrl+C.

Observe that a new leader is elected automatically by the remaining nodes.

Submit a new client command to the new leader and verify replication to followers.

Follower crash: Stop a follower and submit commands to the leader.

Restart the follower and check that it catches up with the latest log entries.
