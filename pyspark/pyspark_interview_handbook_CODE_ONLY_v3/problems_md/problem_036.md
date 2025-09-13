# Problem 036: 036 - Graph-ish / Hierarchical: Recursive-like with joins challenge

**Category:** Graph-ish / Hierarchical

## Problem
Use broadcast hint or mapPartitions.

### Input DataFrame
Name: `events`

Schema:
```
root
 |-- order_id: string
 |-- product_id: string
 |-- price: double
 |-- quantity: int
 |-- order_ts: timestamp
 |-- tags: array<string>
```

## Solution (PySpark)
```python
res = events.hint("broadcast")
```

## Variations
- accumulator concept.
- mapPartitions for expensive init.
- benchmark broadcast effect.

---
