# Problem 045: 045 - File IO & Formats: Orc challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `orders`

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
# orders.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = orders
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
