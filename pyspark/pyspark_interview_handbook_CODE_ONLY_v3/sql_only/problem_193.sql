-- Problem 193: 193 - Spark SQL: Create temp view challenge
-- Category: Spark SQL

-- Register temp view in PySpark:
-- products.createOrReplaceTempView('products_view');

SELECT *
FROM products_view
LIMIT 100;
