# Apache Flink Window Types — Event Time + Watermarks

This bundle demonstrates **Tumbling**, **Hopping/Sliding**, and **Session** windows using **event time** with **watermarks** in both **Java** and **PyFlink**.

## Why event time + watermarks?
- **Event time** uses the timestamp embedded in events (deterministic, replay-safe).
- **Watermarks** are progress markers telling Flink it has likely seen all events up to time *T*, allowing windows to **close**.
- Handles **out-of-order** events while keeping low latency.

## Window Types

### 1) Tumbling Window
- **Fixed-size, non-overlapping** time buckets (e.g., exactly every 1 minute).
- Each event belongs to exactly **one** window.
- Great for periodic metrics: per-minute counts, per-hour sums.

```
Time  ──|----1m----|----1m----|----1m----|──▶
Events   *** **   **  *  **   *   *   *
Buckets  [  0-1m  ][  1-2m  ][  2-3m  ]
```

### 2) Hopping / Sliding Window
- **Fixed-size** windows but the **start advances** by a hop < size, so windows **overlap**.
- Each event can contribute to **multiple** windows.
- Use for **rolling** stats (e.g., 5-minute window updated every minute).

```
Size = 5m, Hop = 1m
|------5m------|
     |------5m------|
          |------5m------|
```

### 3) Session Window
- Groups events into **sessions** per key.
- A session **closes** after **inactivity gap** (e.g., 30s without events).
- Perfect for user activity bursts with idle periods.

```
key=A   [active----][gap][--active----]
gap=30s → windows split across the gap
```

## Late Data & Grace
- Configure bounded out-of-orderness in **WatermarkStrategy** (e.g., 5s).
- For late events, you can:
  - Extend **allowed lateness** (Java DataStream API).
  - Use **side output** for very late records.

## Files
- **/java**: `TumblingWindowExample.java`, `HoppingWindowExample.java`, `SessionWindowExample.java`
- **/python**: `pyflink_tumbling_window.py`, `pyflink_hopping_window.py`, `pyflink_session_window.py`

Each file:
- Assigns **event-time** timestamps.
- Uses **watermarks** with bounded out-of-orderness (5s).
- Aggregates per user and prints windowed results.
