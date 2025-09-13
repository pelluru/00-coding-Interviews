# Problem 038: 038 - Pivot & Crosstab: Rollup challenge

**Category:** Pivot & Crosstab

## Problem
Pivot event_type counts by user.

### Input DataFrame
Name: `users`

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
from pyspark.sql import functions as F
res = users.groupBy("user_id").pivot("event_type").agg(F.count("*")).fillna(0)
```

## Variations
- Limit pivot values list.
- cube/rollup on country.
- Purchase ratio per user.

---
