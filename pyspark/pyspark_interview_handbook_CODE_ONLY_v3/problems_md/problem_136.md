# Problem 136: 136 - Dates & Timestamps: Date_trunc challenge

**Category:** Dates & Timestamps

## Problem
Group by day; compute counts and averages.

### Input DataFrame
Name: `products`

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
res = products.withColumn("day", F.to_date("ts")).groupBy("day").agg(F.count("*").alias("events"), F.avg("value").alias("avg_value"))
```

## Variations
- date_trunc hour buckets.
- timezone conversions.
- watermark-friendly ts.

---
