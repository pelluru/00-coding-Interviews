# Problem 027: 027 - Streaming (Structured): Trigger challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `clicks`

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
res = clicks
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
