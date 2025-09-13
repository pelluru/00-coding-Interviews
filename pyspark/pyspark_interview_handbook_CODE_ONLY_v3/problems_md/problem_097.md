# Problem 097: 097 - Graph-ish / Hierarchical: Recursive-like with joins challenge

**Category:** Graph-ish / Hierarchical

## Problem
Use broadcast hint or mapPartitions.

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
res = users.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
