# ksqlDB Examples

```sql
-- Stream on orders (Avro/JSON/Protobuf depends on Schema Registry config)
CREATE STREAM ORDERS (id INT KEY, user_id STRING, amount DOUBLE, ts BIGINT)
  WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

-- Filter fraud-like events
CREATE STREAM ORDERS_HIGH AS
  SELECT id, user_id, amount, ts
  FROM ORDERS
  WHERE amount > 1000 EMIT CHANGES;

-- Rolling sum per user, hopping window
CREATE TABLE USER_SPEND AS
  SELECT user_id, SUM(amount) AS total
  FROM ORDERS
  WINDOW HOPPING (SIZE 30 MINUTES, ADVANCE BY 5 MINUTES)
  GROUP BY user_id EMIT CHANGES;

-- Join with a reference KTable (user profile)
CREATE TABLE USERS (user_id STRING PRIMARY KEY, tier STRING)
  WITH (KAFKA_TOPIC='users', VALUE_FORMAT='JSON');

CREATE TABLE USER_TIER_SPEND AS
  SELECT o.user_id, u.tier, SUM(o.amount) AS total
  FROM ORDERS o
  LEFT JOIN USERS u ON o.user_id = u.user_id
  GROUP BY o.user_id, u.tier EMIT CHANGES;
```

