# Problem 016: 016 - Spark SQL: Functions in sql challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `orders`

Schema:
```
root
 |-- txn_id: string
 |-- user_id: string
 |-- amount: double
 |-- currency: string
 |-- ts: timestamp
```

## Solution (PySpark)
```python
orders.createOrReplaceTempView("orders_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM orders_view GROUP BY user_id")
```

## Variations
- SQL window row_number.
- Join two views.
- Call UDF from SQL.

---
