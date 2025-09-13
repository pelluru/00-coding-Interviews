# Problem 009: 009 - Complex Types: Structs challenge

**Category:** Complex Types

## Problem
Explode arrays/maps, flatten nested structures.

### Input DataFrame
Name: `products`

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
res = products
if "tags" in res.columns:
    res = res.withColumn("tag", F.explode_outer("tags"))
```

## Variations
- posexplode for index.
- map_keys/map_values.
- struct and nest back.

---
