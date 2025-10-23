#!/usr/bin/env bash
set -euo pipefail
CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"

echo "Posting FileStreamSource (orders) ..."
curl -s -X PUT -H "Content-Type: application/json" \
  --data @"$(dirname "$0")/../configs/file-source-orders.json" \
  "$CONNECT_URL/connectors/file-source-orders/config" | jq .

echo "Posting FileStreamSink (orders_high) ..."
curl -s -X PUT -H "Content-Type: application/json" \
  --data @"$(dirname "$0")/../configs/file-sink-orders-high.json" \
  "$CONNECT_URL/connectors/file-sink-orders-high/config" | jq .

echo "Done."
