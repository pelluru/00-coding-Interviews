# Problem 084: 084 - Performance & Tuning: Skew challenge

**Category:** Performance & Tuning

## Problem
Repartition/cache before heavy aggregations.

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
res = clicks.repartition(200, "user_id").groupBy("user_id").agg(F.count("*").alias("cnt"))
```

## Variations
- coalesce after filter.
- checkpoint iterative.
- skew salting & AQE.

---
