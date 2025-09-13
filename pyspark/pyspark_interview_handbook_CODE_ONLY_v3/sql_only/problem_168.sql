-- Problem 168: 168 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- logs.createOrReplaceTempView('logs_view');

SELECT *
FROM logs_view
LIMIT 100;
