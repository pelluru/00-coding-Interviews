# Problem 197: 197 - Streaming (Structured): Trigger challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `events`

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
# streaming example would use readStream; here batch placeholder
res = events
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
