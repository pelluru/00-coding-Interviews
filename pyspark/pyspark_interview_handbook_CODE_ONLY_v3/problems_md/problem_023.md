# Problem 023: 023 - Dates & Timestamps: Date_trunc challenge

**Category:** Dates & Timestamps

## Problem
Group by day; compute counts and averages.

### Input DataFrame
Name: `events`

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
res = events.withColumn("day", F.to_date("ts")).groupBy("day").agg(F.count("*").alias("events"), F.avg("value").alias("avg_value"))
```

## Variations
- date_trunc hour buckets.
- timezone conversions.
- watermark-friendly ts.

---
