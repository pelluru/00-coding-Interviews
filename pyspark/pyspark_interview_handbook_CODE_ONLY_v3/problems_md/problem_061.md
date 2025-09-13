# Problem 061: 061 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `events`

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
# streaming example would use readStream; here batch placeholder
res = events
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
