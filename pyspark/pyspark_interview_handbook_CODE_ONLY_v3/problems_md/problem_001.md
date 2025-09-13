# Problem 001: 001 - Aggregations & GroupBy: Approx_count_distinct challenge

**Category:** Aggregations & GroupBy

## Problem
Per-user count/sum/avg and global distinct users.

### Input DataFrame
Name: `transactions`

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
user_stats = transactions.groupBy("user_id").agg(F.count("*").alias("cnt"), F.sum("value").alias("sum_value"), F.avg("value").alias("avg_value"))
global_distinct = transactions.select(F.countDistinct("user_id").alias("distinct_users"))
res = user_stats
```

## Variations
- Use approx_count_distinct.
- Aggregate by (user_id,event_type) then pivot.
- Add stddev and percentiles.

---
