# Problem 088: 088 - Misc Utilities: Mappartitions challenge

**Category:** Misc Utilities

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `sessions`

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
res = sessions.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
