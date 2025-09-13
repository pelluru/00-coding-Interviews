# Problem 199: 199 - Strings & Regex: Regexp_replace challenge

**Category:** Strings & Regex

## Problem
Extract domain from email; normalize.

### Input DataFrame
Name: `orders`

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
res = orders
if "email" in res.columns:
    res = res.withColumn("domain", F.regexp_extract("email", "@(.*)$", 1))
```

## Variations
- Stricter email validation.
- Split path and pick segment.
- lower/trim/regexp_replace.

---
