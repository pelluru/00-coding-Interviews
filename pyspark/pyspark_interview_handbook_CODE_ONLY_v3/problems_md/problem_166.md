# Problem 166: 166 - Spark SQL: Create temp view challenge

**Category:** Spark SQL

## Problem
Create temp view and solve with SQL.

### Input DataFrame
Name: `sessions`

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
sessions.createOrReplaceTempView("sessions_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM sessions_view GROUP BY user_id")
```

## Variations
- SQL window row_number.
- Join two views.
- Call UDF from SQL.

---
