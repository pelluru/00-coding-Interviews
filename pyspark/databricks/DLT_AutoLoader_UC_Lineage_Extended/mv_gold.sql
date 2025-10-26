-- mv_gold.sql
-- Create a materialized view on top of gold for BI acceleration (platform-dependent syntax).
-- Databricks SQL currently supports materialized views in Unity Catalog (check your runtime/privileges).

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.core.mv_gold_user_spend
AS
SELECT user_id, total_spent_30d, txn_count_30d, last_txn_at
FROM analytics.core.gold_user_spend;
