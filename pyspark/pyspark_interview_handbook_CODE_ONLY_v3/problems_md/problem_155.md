# Problem 155: 155 - Pivot & Crosstab: Pivot challenge

**Category:** Pivot & Crosstab

## Problem
Pivot event_type counts by user.

### Input DataFrame
Name: `transactions`

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
res = transactions.groupBy("user_id").pivot("event_type").agg(F.count("*")).fillna(0)
```

## Variations
- Limit pivot values list.
- cube/rollup on country.
- Purchase ratio per user.

---
