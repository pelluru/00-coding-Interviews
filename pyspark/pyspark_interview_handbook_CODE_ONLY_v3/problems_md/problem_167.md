# Problem 167: 167 - Aggregations & GroupBy: Agg challenge

**Category:** Aggregations & GroupBy

## Problem
Per-user count/sum/avg and global distinct users.

### Input DataFrame
Name: `products`

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
from pyspark.sql import functions as F
user_stats = products.groupBy("user_id").agg(F.count("*").alias("cnt"), F.sum("value").alias("sum_value"), F.avg("value").alias("avg_value"))
global_distinct = products.select(F.countDistinct("user_id").alias("distinct_users"))
res = user_stats
```

## Variations
- Use approx_count_distinct.
- Aggregate by (user_id,event_type) then pivot.
- Add stddev and percentiles.

---
