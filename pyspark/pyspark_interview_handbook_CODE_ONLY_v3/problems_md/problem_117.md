# Problem 117: 117 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `users`

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
res = users
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
