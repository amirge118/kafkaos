#!/usr/bin/env bash
# Re-creates the Stage 15 ClickHouse sink connector from the saved JSON file
# in ./connectors/, against a running kafka-connect (localhost:8083).
set -euo pipefail
cd "$(dirname "$0")/connectors"

for file in *.json; do
  name="${file%.json}"
  echo "applying $name..."
  curl -s -X PUT "http://localhost:8083/connectors/$name/config" \
    -H "Content-Type: application/json" \
    -d @"$file" | python3 -m json.tool
  echo ""
done
