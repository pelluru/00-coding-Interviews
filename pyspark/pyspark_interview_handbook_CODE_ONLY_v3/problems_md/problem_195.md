# Problem 195: 195 - UDFs & Pandas UDFs: Returntype challenge

**Category:** UDFs & Pandas UDFs

## Problem
Define UDF and apply.

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
from pyspark.sql.types import DoubleType
@F.udf(DoubleType())
def score(x): return float(x)*1.1 if x is not None else None
res = users.withColumn("score", score("value"))
```

## Variations
- pandas_udf variant.
- Handle nulls.
- Register UDF for SQL.

---
