# Problem 150: 150 - Streaming (Structured): Readstream challenge

**Category:** Streaming (Structured)

## Problem
Windowed aggregation with watermark (illustrative).

### Input DataFrame
Name: `logs`

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
res = logs
```

## Variations
- trigger once.
- checkpoint/Sinks.
- append vs update vs complete.

---
