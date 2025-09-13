# Problem 111: 111 - Performance & Tuning: Aqe challenge

**Category:** Performance & Tuning

## Problem
Repartition/cache before heavy aggregations.

### Input DataFrame
Name: `products`

Schema:
```
root
 |-- id: string
 |-- ts: timestamp
 |-- user_id: string
 |-- event_type: string
 |-- value: double
```

## Solution (PySpark)
```python
from pyspark.sql import functions as F
res = products.repartition(200, "user_id").groupBy("user_id").agg(F.count("*").alias("cnt"))
```

## Variations
- coalesce after filter.
- checkpoint iterative.
- skew salting & AQE.

---
