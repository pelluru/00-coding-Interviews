# Problem 019: 019 - Window Functions: Rank challenge

**Category:** Window Functions

## Problem
Compute row_number, lag, rolling sum per user ordered by ts.

### Input DataFrame
Name: `transactions`

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
from pyspark.sql.window import Window
w = Window.partitionBy("user_id").orderBy("ts")
res = transactions.withColumn("rn", F.row_number().over(w)) \    .withColumn("prev_value", F.lag("value", 1).over(w)) \    .withColumn("rolling_sum_3", F.sum("value").over(w.rowsBetween(-2,0)))
```

## Variations
- Use rangeBetween time windows.
- Use rank/dense_rank for ties.
- Add moving average.

---
