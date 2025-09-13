# Problem 137: 137 - Dates & Timestamps: Date_trunc challenge

**Category:** Dates & Timestamps

## Problem
Group by day; compute counts and averages.

### Input DataFrame
Name: `logs`

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
res = logs.withColumn("day", F.to_date("ts")).groupBy("day").agg(F.count("*").alias("events"), F.avg("value").alias("avg_value"))
```

## Variations
- date_trunc hour buckets.
- timezone conversions.
- watermark-friendly ts.

---
