# Problem 092: 092 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `transactions`

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
# streaming example would use readStream; here batch placeholder
res = transactions
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
