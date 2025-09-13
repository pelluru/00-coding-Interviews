-- Problem 188: 188 - Spark SQL: Functions in sql challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- transactions.createOrReplaceTempView('transactions_view');

SELECT *
FROM transactions_view
LIMIT 100;
