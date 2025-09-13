# Problem 194: 194 - Complex Types: Arrays challenge

**Category:** Complex Types

## Problem
Explode arrays/maps, flatten nested structures.

### Input DataFrame
Name: `products`

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
res = products
if "tags" in res.columns:
    res = res.withColumn("tag", F.explode_outer("tags"))
```

## Variations
- posexplode for index.
- map_keys/map_values.
- struct and nest back.

---
