# Problem 175: 175 - Strings & Regex: Concat_ws challenge

**Category:** Strings & Regex

## Problem
Extract domain from email; normalize.

### Input DataFrame
Name: `users`

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
res = users
if "email" in res.columns:
    res = res.withColumn("domain", F.regexp_extract("email", "@(.*)$", 1))
```

## Variations
- Stricter email validation.
- Split path and pick segment.
- lower/trim/regexp_replace.

---
