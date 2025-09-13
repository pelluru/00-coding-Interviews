# Problem 169: 169 - File IO & Formats: Parquet challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `orders`

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
# orders.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = orders
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
