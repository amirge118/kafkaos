#!/usr/bin/env bash
# Re-creates all four Stage 8 connectors from the saved JSON files in
# ./connectors/, against a running kafka-connect (localhost:8083).
# This is the actual, re-runnable source of truth for "how is Kafka Connect
# wired to Postgres/files" — not just documentation.
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
