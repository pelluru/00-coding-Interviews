# Problem 132: 132 - File IO & Formats: Delta-like challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `orders`

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
# orders.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = orders
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
