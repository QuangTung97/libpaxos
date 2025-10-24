#/bin/bash
set -e

NUM=0

while true; do
  NUM=$((NUM + 1))
  echo -n "Step $NUM: "
  go test -count=1 -timeout=30s ./paxos
done
