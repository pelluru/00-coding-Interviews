# Problem 118: 118 - File IO & Formats: Delta-like challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `products`

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
# products.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = products
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
