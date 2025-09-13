# Problem 146: 146 - Window Functions: Row_number challenge

**Category:** Window Functions

## Problem
Compute row_number, lag, rolling sum per user ordered by ts.

### Input DataFrame
Name: `orders`

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
w = Window.partitionBy("user_id").orderBy("ts")
res = orders.withColumn("rn", F.row_number().over(w)) \    .withColumn("prev_value", F.lag("value", 1).over(w)) \    .withColumn("rolling_sum_3", F.sum("value").over(w.rowsBetween(-2,0)))
```

## Variations
- Use rangeBetween time windows.
- Use rank/dense_rank for ties.
- Add moving average.

---
