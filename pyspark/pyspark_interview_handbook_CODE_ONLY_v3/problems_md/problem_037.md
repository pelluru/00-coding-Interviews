# Problem 037: 037 - Dates & Timestamps: From_unixtime challenge

**Category:** Dates & Timestamps

## Problem
Group by day; compute counts and averages.

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
from pyspark.sql import functions as F
res = products.withColumn("day", F.to_date("ts")).groupBy("day").agg(F.count("*").alias("events"), F.avg("value").alias("avg_value"))
```

## Variations
- date_trunc hour buckets.
- timezone conversions.
- watermark-friendly ts.

---
