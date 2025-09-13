# Problem 060: 060 - File IO & Formats: Delta-like challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

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
# Example write (commented):
# transactions.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = transactions
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
