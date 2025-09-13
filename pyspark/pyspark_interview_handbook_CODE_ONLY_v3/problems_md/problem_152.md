# Problem 152: 152 - Misc Utilities: Mappartitions challenge

**Category:** Misc Utilities

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `clicks`

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
res = clicks.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
