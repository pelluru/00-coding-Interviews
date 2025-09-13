# Problem 168: 168 - Spark SQL: Sql queries challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `logs`

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
logs.createOrReplaceTempView("logs_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM logs_view GROUP BY user_id")
```

## Variations
- SQL window row_number.
- Join two views.
- Call UDF from SQL.

---
