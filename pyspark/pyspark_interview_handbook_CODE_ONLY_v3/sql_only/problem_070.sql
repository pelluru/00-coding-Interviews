-- Problem 070: 070 - Spark SQL: Sql queries challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- transactions.createOrReplaceTempView('transactions_view');

SELECT *
FROM transactions_view
LIMIT 100;
