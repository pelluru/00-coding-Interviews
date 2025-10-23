#!/usr/bin/env bash
set -euo pipefail
KSQL_URL="${KSQL_URL:-http://localhost:8088}"
SQL_FILE="$(dirname "$0")/windowed.sql"

echo "Submitting windowed aggregation from $SQL_FILE"
curl -s -X POST -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
  --data @<(jq -n --arg sql "$(tr '\n' ' ' < "$SQL_FILE")" '{ksql:$sql, streamsProperties:{}}') \
  "$KSQL_URL/ksql" | jq .
