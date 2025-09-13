# Problem 139: 139 - Graph-ish / Hierarchical: Self-join paths challenge

**Category:** Graph-ish / Hierarchical

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `orders`

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
res = orders.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
