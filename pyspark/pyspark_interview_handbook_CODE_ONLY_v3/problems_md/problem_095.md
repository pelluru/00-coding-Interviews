# Problem 095: 095 - Misc Utilities: Broadcast joins challenge

**Category:** Misc Utilities

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `products`

Schema:
```
root
 |-- uid: string
 |-- name: string
 |-- email: string
 |-- country: string
 |-- signup_ts: timestamp
```

## Solution (PySpark)
```python
res = products.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
