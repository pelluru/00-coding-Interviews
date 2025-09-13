# Problem 085: 085 - Performance & Tuning: Checkpoint challenge

**Category:** Performance & Tuning

## Problem
Repartition/cache before heavy aggregations.

### Input DataFrame
Name: `products`

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
res = products.repartition(200, "user_id").groupBy("user_id").agg(F.count("*").alias("cnt"))
```

## Variations
- coalesce after filter.
- checkpoint iterative.
- skew salting & AQE.

---
