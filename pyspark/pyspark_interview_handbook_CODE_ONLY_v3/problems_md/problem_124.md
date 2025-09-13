# Problem 124: 124 - File IO & Formats: Csv challenge

**Category:** File IO & Formats

## Problem
Write partitioned Parquet (example commented).

### Input DataFrame
Name: `products`

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
# products.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = products
```

## Variations
- mergeSchema option.
- ORC and pushdown.
- partition pruning.

---
