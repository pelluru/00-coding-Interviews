# Problem 010: 010 - Pivot & Crosstab: Cube challenge

**Category:** Pivot & Crosstab

## Problem
Pivot event_type counts by user.

### Input DataFrame
Name: `logs`

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
res = logs.groupBy("user_id").pivot("event_type").agg(F.count("*")).fillna(0)
```

## Variations
- Limit pivot values list.
- cube/rollup on country.
- Purchase ratio per user.

---
