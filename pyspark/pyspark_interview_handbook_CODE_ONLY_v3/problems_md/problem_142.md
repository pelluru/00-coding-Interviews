# Problem 142: 142 - Complex Types: Arrays challenge

**Category:** Complex Types

## Problem
Explode arrays/maps, flatten nested structures.

### Input DataFrame
Name: `sessions`

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
res = sessions
if "tags" in res.columns:
    res = res.withColumn("tag", F.explode_outer("tags"))
```

## Variations
- posexplode for index.
- map_keys/map_values.
- struct and nest back.

---
