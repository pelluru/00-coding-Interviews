# Problem 015: 015 - Pivot & Crosstab: Cube challenge

**Category:** Pivot & Crosstab

## Problem
Pivot event_type counts by user.

### Input DataFrame
Name: `orders`

Schema:
```
root
 |-- uid: string
 |-- name: string
 |-- email: string
 |-- country: string
 |-- signup_ts: timestamp
```

## Solution (PySpark)
```python
from pyspark.sql import functions as F
res = orders.groupBy("user_id").pivot("event_type").agg(F.count("*")).fillna(0)
```

## Variations
- Limit pivot values list.
- cube/rollup on country.
- Purchase ratio per user.

---
