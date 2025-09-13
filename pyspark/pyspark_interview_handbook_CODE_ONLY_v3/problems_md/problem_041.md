# Problem 041: 041 - Misc Utilities: Broadcast joins challenge

**Category:** Misc Utilities

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
