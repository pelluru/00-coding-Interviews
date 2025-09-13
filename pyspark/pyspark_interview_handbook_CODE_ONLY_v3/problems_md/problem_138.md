# Problem 138: 138 - DataFrame Basics: Withcolumn challenge

**Category:** DataFrame Basics

## Problem
Select a subset, create derived column, and filter by condition.

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
from pyspark.sql.window import Window
res = products.select("id", "user_id", "event_type", "value") \    .withColumn("value_norm", (F.col("value") - F.mean("value").over(Window.partitionBy()))/F.stddev_pop("value").over(Window.partitionBy())) \    .filter(F.col("event_type") == "purchase")
assert "value_norm" in res.columns
```

## Variations
- Compute 90th percentile flag via approxQuantile.
- Drop null user_id via na.drop.
- Use selectExpr instead of select.

---
