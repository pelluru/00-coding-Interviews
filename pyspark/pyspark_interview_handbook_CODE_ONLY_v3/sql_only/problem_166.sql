-- Problem 166: 166 - Spark SQL: Create temp view challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- sessions.createOrReplaceTempView('sessions_view');

SELECT *
FROM sessions_view
LIMIT 100;
