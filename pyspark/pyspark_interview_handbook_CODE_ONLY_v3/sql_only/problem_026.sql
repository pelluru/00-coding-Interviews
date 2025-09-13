-- Problem 026: 026 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- clicks.createOrReplaceTempView('clicks_view');

SELECT *
FROM clicks_view
LIMIT 100;
