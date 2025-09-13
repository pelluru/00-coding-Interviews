# Problem 028: 028 - Window Functions: Rank challenge

**Category:** Window Functions

## Problem
Compute row_number, lag, rolling sum per user ordered by ts.

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
w = Window.partitionBy("user_id").orderBy("ts")
res = clicks.withColumn("rn", F.row_number().over(w)) \    .withColumn("prev_value", F.lag("value", 1).over(w)) \    .withColumn("rolling_sum_3", F.sum("value").over(w.rowsBetween(-2,0)))
```

## Variations
- Use rangeBetween time windows.
- Use rank/dense_rank for ties.
- Add moving average.

---
