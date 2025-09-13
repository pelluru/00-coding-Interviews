# Problem 081: 081 - File IO & Formats: Json challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `sessions`

Schema:
```
root
 |-- order_id: string
 |-- product_id: string
 |-- price: double
 |-- quantity: int
 |-- order_ts: timestamp
 |-- tags: array<string>
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
