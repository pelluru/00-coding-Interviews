# Problem 193: 193 - Spark SQL: Create temp view challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `products`

Schema:
```
root
 |-- uid: string
 |-- name: string
 |-- email: string
 |-- country: string
 |-- signup_ts: timestamp
```

## Solution (PySpark)
```python
products.createOrReplaceTempView("products_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM products_view GROUP BY user_id")
```

## Variations
- SQL window row_number.
- Join two views.
- Call UDF from SQL.

---
