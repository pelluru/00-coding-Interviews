# Problem 054: 054 - Misc Utilities: Accumulators (concept) challenge

**Category:** Misc Utilities

## Problem
Use broadcast hint or mapPartitions.

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
res = sessions.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
