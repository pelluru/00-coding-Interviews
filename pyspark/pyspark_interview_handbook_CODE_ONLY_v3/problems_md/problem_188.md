# Problem 188: 188 - Spark SQL: Functions in sql challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `transactions`

Schema:
```
root
 |-- id: string
 |-- ts: timestamp
 |-- user_id: string
 |-- event_type: string
 |-- value: double
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
