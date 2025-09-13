# Problem 123: 123 - File IO & Formats: Csv challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

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
# Example write (commented):
# logs.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = logs
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
