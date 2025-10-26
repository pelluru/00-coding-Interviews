-- uc_lineage.sql
-- Explore lineage using UC system tables (availability varies by workspace/runtime).

-- Example: find lineage for a target table
-- SELECT *
-- FROM system.information_schema.table_lineage
-- WHERE target_table_full_name = 'analytics.core.gold_user_spend';

-- Example: find upstream tables feeding a model
-- SELECT upstream_table_full_name, downstream_table_full_name
-- FROM system.information_schema.table_lineage
-- WHERE downstream_table_full_name = 'analytics.core.gold_user_spend';

-- Example: list grants to audit access
SHOW GRANTS ON SCHEMA analytics.core;
SHOW GRANTS ON TABLE analytics.core.gold_user_spend;
