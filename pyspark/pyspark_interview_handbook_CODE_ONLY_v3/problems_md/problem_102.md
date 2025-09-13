# Problem 102: 102 - File IO & Formats: Delta-like challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `sessions`

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
# Example write (commented):
# sessions.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = sessions
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
