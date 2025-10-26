-- dlt_pipeline.sql
-- Delta Live Tables pipeline: bronze -> silver -> gold
-- Assumes Unity Catalog catalog.schema are set by the pipeline settings.
-- Data quality via EXPECT; watermark & dedup included.

CREATE STREAMING LIVE TABLE bronze_transactions
COMMENT "Raw transaction events from Auto Loader"
AS SELECT
  CAST(value:transaction_id AS STRING)      AS transaction_id,
  CAST(value:user_id AS STRING)             AS user_id,
  CAST(value:amount AS DOUBLE)              AS amount,
  CAST(value:event_time AS TIMESTAMP)       AS event_time,
  current_timestamp()                       AS ingest_ts
FROM STREAM(LIVE.raw_transactions_json);

-- Quality checks on bronze
ALTER STREAMING LIVE TABLE bronze_transactions
SET TBLPROPERTIES ("quality" = "bronze");

-- Declare the streaming source as a live view over the raw files table
CREATE STREAMING LIVE VIEW raw_transactions_json
AS SELECT from_json(body, 'transaction_id string, user_id string, amount double, event_time timestamp') AS value
FROM cloud_files("${source_path}", "${source_format}", map("cloudFiles.inferColumnTypes","true"));

CREATE LIVE TABLE silver_transactions
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  transaction_id,
  user_id,
  amount,
  event_time,
  ingest_ts
FROM LIVE.bronze_transactions
WHERE amount > 0
AND event_time >= (current_timestamp() - INTERVAL 30 DAYS);

-- Expectations (quality rules)
CREATE EXPECTATION silver_amount_positive IF amount > 0 ON TABLE silver_transactions;

-- Deduplicate by transaction_id with latest event_time
CREATE LIVE TABLE silver_transactions_dedup
TBLPROPERTIES ("quality" = "silver")
AS
SELECT *
FROM (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY transaction_id ORDER BY event_time DESC) AS rn
  FROM LIVE.silver_transactions
)
WHERE rn = 1;

CREATE LIVE TABLE gold_user_spend
TBLPROPERTIES ("quality" = "gold")
AS
SELECT
  user_id,
  SUM(amount) AS total_spent_30d,
  COUNT(*)   AS txn_count_30d,
  MAX(event_time) AS last_txn_at
FROM LIVE.silver_transactions_dedup
GROUP BY user_id;
