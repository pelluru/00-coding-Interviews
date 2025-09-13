-- Problem 016: 016 - Spark SQL: Functions in sql challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- orders.createOrReplaceTempView('orders_view');

SELECT *
FROM orders_view
LIMIT 100;
