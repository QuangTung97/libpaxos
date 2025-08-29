#/bin/bash
set -e

while true; do
  go test -count=1 -timeout=30s ./paxos
done
