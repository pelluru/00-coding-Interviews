# ksqlDB Push vs Pull Queries

## Push Query (continuous results)
```bash
curl -s -N -X POST \
  -H "Content-Type: application/vnd.ksqlapi.delimited.v1" \
  --data '{"sql":"SELECT * FROM ORDERS_HIGH EMIT CHANGES;"}' \
  http://localhost:8088/query-stream
```

## Pull Query (point-in-time lookup on a TABLE)
> Requires a **materialized TABLE** like `USER_SPEND_MINUTE` (created by windowed aggregation).
```bash
curl -s -X POST -H "Content-Type: application/vnd.ksql.v1+json; charset=utf-8" \
  --data '{"ksql":"SELECT * FROM USER_SPEND_MINUTE WHERE user_id='\''user-1'\'' LIMIT 10;"}' \
  http://localhost:8088/query
```
