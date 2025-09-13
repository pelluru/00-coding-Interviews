# Problem 158: 158 - Joins: Right challenge

**Category:** Joins

## Problem
Join with users dim; handle duplicate key columns.

### Input DataFrame
Name: `clicks`

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
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
users = spark.createDataFrame([("u1","US"),("u2","IN")], ["user_id","country"])
res = clicks.join(F.broadcast(users), "user_id", "left")
```

## Variations
- Switch to semi join.
- Broadcast hint conditionally.
- Alias to resolve duplicates.

---
