# Problem 100: 100 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `clicks`

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
# streaming example would use readStream; here batch placeholder
res = clicks
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
