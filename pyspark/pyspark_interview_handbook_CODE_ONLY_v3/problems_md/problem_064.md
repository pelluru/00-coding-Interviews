# Problem 064: 064 - Performance & Tuning: Skew challenge

**Category:** Performance & Tuning

## Problem
Repartition/cache before heavy aggregations.

### Input DataFrame
Name: `sessions`

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
res = sessions.repartition(200, "user_id").groupBy("user_id").agg(F.count("*").alias("cnt"))
```

## Variations
- coalesce after filter.
- checkpoint iterative.
- skew salting & AQE.

---
