# Problem 058: 058 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `events`

Schema:
```
root
 |-- id: string
 |-- ts: timestamp
 |-- user_id: string
 |-- event_type: string
 |-- value: double
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
