-- ksqlDB demo: create streams and filtered output
CREATE STREAM ORDERS (id INT, user_id STRING, amount DOUBLE, ts BIGINT)
  WITH (KAFKA_TOPIC='orders', VALUE_FORMAT='JSON');

CREATE STREAM ORDERS_HIGH WITH (KAFKA_TOPIC='orders_high', VALUE_FORMAT='JSON') AS
  SELECT id, user_id, amount, ts
  FROM ORDERS
  WHERE amount > 1000
  EMIT CHANGES;
