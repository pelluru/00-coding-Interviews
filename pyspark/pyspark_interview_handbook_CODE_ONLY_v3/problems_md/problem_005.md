# Problem 005: 005 - UDFs & Pandas UDFs: Returntype challenge

**Category:** UDFs & Pandas UDFs

## Problem
Define UDF and apply.

### Input DataFrame
Name: `products`

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
from pyspark.sql.types import DoubleType
@F.udf(DoubleType())
def score(x): return float(x)*1.1 if x is not None else None
res = products.withColumn("score", score("value"))
```

## Variations
- pandas_udf variant.
- Handle nulls.
- Register UDF for SQL.

---
