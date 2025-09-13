# Problem 179: 179 - Aggregations & GroupBy: Groupby challenge

**Category:** Aggregations & GroupBy

## Problem
Per-user count/sum/avg and global distinct users.

### Input DataFrame
Name: `logs`

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
user_stats = logs.groupBy("user_id").agg(F.count("*").alias("cnt"), F.sum("value").alias("sum_value"), F.avg("value").alias("avg_value"))
global_distinct = logs.select(F.countDistinct("user_id").alias("distinct_users"))
res = user_stats
```

## Variations
- Use approx_count_distinct.
- Aggregate by (user_id,event_type) then pivot.
- Add stddev and percentiles.

---
