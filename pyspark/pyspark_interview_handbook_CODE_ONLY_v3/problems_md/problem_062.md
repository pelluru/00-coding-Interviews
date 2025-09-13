# Problem 062: 062 - Joins: Right challenge

**Category:** Joins

## Problem
Join with users dim; handle duplicate key columns.

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
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
users = spark.createDataFrame([("u1","US"),("u2","IN")], ["user_id","country"])
res = events.join(F.broadcast(users), "user_id", "left")
```

## Variations
- Switch to semi join.
- Broadcast hint conditionally.
- Alias to resolve duplicates.

---
