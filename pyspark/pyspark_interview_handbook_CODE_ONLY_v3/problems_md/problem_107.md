# Problem 107: 107 - DataFrame Basics: Select challenge

**Category:** DataFrame Basics

## Problem
Select a subset, create derived column, and filter by condition.

### Input DataFrame
Name: `clicks`

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
from pyspark.sql.window import Window
res = clicks.select("id", "user_id", "event_type", "value") \    .withColumn("value_norm", (F.col("value") - F.mean("value").over(Window.partitionBy()))/F.stddev_pop("value").over(Window.partitionBy())) \    .filter(F.col("event_type") == "purchase")
assert "value_norm" in res.columns
```

## Variations
- Compute 90th percentile flag via approxQuantile.
- Drop null user_id via na.drop.
- Use selectExpr instead of select.

---
