-- Problem 032: 032 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- users.createOrReplaceTempView('users_view');

SELECT *
FROM users_view
LIMIT 100;
