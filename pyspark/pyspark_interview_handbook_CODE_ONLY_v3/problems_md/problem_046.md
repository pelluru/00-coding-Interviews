# Problem 046: 046 - File IO & Formats: Json challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `users`

Schema:
```
root
 |-- session_id: string
 |-- user_id: string
 |-- page: string
 |-- referrer: string
 |-- ts: timestamp
 |-- attrs: map<string,string>
```

## Solution (PySpark)
```python
# Example write (commented):
# users.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = users
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
