# Problem 153: 153 - Performance & Tuning: Coalesce challenge

**Category:** Performance & Tuning

## Problem
Repartition/cache before heavy aggregations.

### Input DataFrame
Name: `users`

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
res = users.repartition(200, "user_id").groupBy("user_id").agg(F.count("*").alias("cnt"))
```

## Variations
- coalesce after filter.
- checkpoint iterative.
- skew salting & AQE.

---
