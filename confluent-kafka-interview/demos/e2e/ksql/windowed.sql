-- Windowed aggregation: 1-minute tumbling window spend per user
CREATE TABLE USER_SPEND_MINUTE WITH (KAFKA_TOPIC='user_spend_minute', VALUE_FORMAT='JSON') AS
  SELECT user_id,
         WINDOWSTART AS window_start,
         WINDOWEND   AS window_end,
         SUM(amount) AS total
  FROM ORDERS
  WINDOW TUMBLING (SIZE 1 MINUTE)
  GROUP BY user_id
  EMIT CHANGES;
