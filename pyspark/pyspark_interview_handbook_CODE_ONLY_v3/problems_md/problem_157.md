# Problem 157: 157 - File IO & Formats: Orc challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `transactions`

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
# transactions.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = transactions
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
