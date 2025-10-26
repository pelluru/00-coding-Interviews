-- uc_setup.sql
-- Create catalog/schema, sample external locations, and grants.
-- Adjust names/locations per your workspace.

CREATE CATALOG IF NOT EXISTS analytics COMMENT 'Analytics catalog';
USE CATALOG analytics;

CREATE SCHEMA IF NOT EXISTS core COMMENT 'Core analytics schema';
CREATE SCHEMA IF NOT EXISTS raw COMMENT 'Raw landing';

-- Optional: create external volume or managed locations as needed

-- Create base tables if not created by pipelines
CREATE TABLE IF NOT EXISTS raw.transactions_raw (
  transaction_id STRING,
  user_id STRING,
  amount DOUBLE,
  event_time TIMESTAMP
) USING DELTA;

-- Grants (principle of least privilege)
GRANT USAGE ON CATALOG analytics TO `data_engineers`, `analysts`;
GRANT USAGE ON SCHEMA analytics.core TO `data_engineers`, `analysts`;
GRANT SELECT ON ALL TABLES IN SCHEMA analytics.core TO `analysts`;
GRANT MODIFY, SELECT ON ALL TABLES IN SCHEMA analytics.core TO `data_engineers`;

-- Future grants
ALTER SCHEMA analytics.core OWNER TO `data_engineers`;
GRANT SELECT ON FUTURE TABLES IN SCHEMA analytics.core TO `analysts`;
