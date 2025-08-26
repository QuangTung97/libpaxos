#/bin/bash
set -e

while true; do
  go test -count=1 -timeout=5s -run="TestPaxos__Normal_Three_Nodes__Insert_Many_Commands$" ./paxos
done