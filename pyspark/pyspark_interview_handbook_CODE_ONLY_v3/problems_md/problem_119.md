# Problem 119: 119 - Joins: Left challenge

**Category:** Joins

## Problem
Join with users dim; handle duplicate key columns.

### Input DataFrame
Name: `users`

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
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
users = spark.createDataFrame([("u1","US"),("u2","IN")], ["user_id","country"])
res = users.join(F.broadcast(users), "user_id", "left")
```

## Variations
- Switch to semi join.
- Broadcast hint conditionally.
- Alias to resolve duplicates.

---
