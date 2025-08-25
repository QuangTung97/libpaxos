#/bin/bash
while true; do
  go test -count=1 -timeout=5s ./...
done