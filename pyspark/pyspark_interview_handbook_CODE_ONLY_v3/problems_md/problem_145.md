# Problem 145: 145 - File IO & Formats: Delta-like challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

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
# Example write (commented):
# sessions.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = sessions
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
