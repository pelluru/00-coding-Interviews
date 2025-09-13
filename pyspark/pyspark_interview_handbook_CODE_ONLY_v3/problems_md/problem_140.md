# Problem 140: 140 - Graph-ish / Hierarchical: Recursive-like with joins challenge

**Category:** Graph-ish / Hierarchical

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `transactions`

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
res = transactions.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
