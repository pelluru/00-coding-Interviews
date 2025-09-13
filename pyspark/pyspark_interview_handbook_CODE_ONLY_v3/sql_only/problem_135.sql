-- Problem 135: 135 - Spark SQL: Functions in sql challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- clicks.createOrReplaceTempView('clicks_view');

SELECT *
FROM clicks_view
LIMIT 100;
