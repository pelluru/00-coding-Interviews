# Problem 161: 161 - File IO & Formats: Orc challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `logs`

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
# Example write (commented):
# logs.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = logs
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
