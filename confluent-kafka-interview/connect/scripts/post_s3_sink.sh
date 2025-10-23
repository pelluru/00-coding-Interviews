#!/usr/bin/env bash
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
CFG="$(dirname "$0")/../configs/s3-sink-orders-high.json"

echo "Posting S3 Sink (orders_high) ..."
curl -s -X PUT -H "Content-Type: application/json" \
  --data @"$CFG" \
  "$CONNECT_URL/connectors/s3-sink-orders-high/config" | jq .
