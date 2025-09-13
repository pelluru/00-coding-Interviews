# Problem 076: 076 - Strings & Regex: Concat_ws challenge

**Category:** Strings & Regex

## Problem
Extract domain from email; normalize.

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
res = users
if "email" in res.columns:
    res = res.withColumn("domain", F.regexp_extract("email", "@(.*)$", 1))
```

## Variations
- Stricter email validation.
- Split path and pick segment.
- lower/trim/regexp_replace.

---
