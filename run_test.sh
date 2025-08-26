#/bin/bash
set -e

while true; do
  go test -count=1 -timeout=5s -run="TestSyncTest_Wait_Group$" ./paxos/testutil
done