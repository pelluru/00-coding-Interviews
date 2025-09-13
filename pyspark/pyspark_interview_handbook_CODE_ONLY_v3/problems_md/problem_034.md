# Problem 034: 034 - Graph-ish / Hierarchical: Recursive-like with joins challenge

**Category:** Graph-ish / Hierarchical

## Problem
Use broadcast hint or mapPartitions.

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
res = transactions.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
