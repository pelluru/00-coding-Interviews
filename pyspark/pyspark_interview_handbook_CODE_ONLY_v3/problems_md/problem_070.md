# Problem 070: 070 - Spark SQL: Sql queries challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `transactions`

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
transactions.createOrReplaceTempView("transactions_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM transactions_view GROUP BY user_id")
```

## Variations
- SQL window row_number.
- Join two views.
- Call UDF from SQL.

---
