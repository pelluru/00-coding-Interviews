-- Problem 187: 187 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- sessions.createOrReplaceTempView('sessions_view');

SELECT *
FROM sessions_view
LIMIT 100;
