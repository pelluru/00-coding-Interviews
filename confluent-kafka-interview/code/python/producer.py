# Python Producer (kafka-python)

```python

```

```python
from kafka import KafkaProducer
import json, time, os

"""
Algorithm (Producer):
1) Create a KafkaProducer with idempotence enabled for safety.
2) Serialize payloads to bytes (JSON here).
3) Use a key to preserve per-key ordering (same key -> same partition).
4) Batch via linger_ms/batch_size for throughput; retries for resiliency.
5) Close producer cleanly to flush remaining records.
"""

def create_producer(bootstrap, client_id="py-producer"):
    return KafkaProducer(
        bootstrap_servers=bootstrap.split(","),
        acks="all",
        enable_idempotence=True,
        linger_ms=20,
        retries=5,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8")
    )

def main():
    topic = os.getenv("TOPIC", "orders")
    bootstrap = os.getenv("BOOTSTRAP", "localhost:9092")
    p = create_producer(bootstrap)

    for i in range(10):
        key = f"user-{i%3}"
        payload = {"event":"order_created","id":i,"ts":int(time.time())}
        fut = p.send(topic, key=key, value=payload)
        md = fut.get(timeout=10)
        print(f"Produced to {md.topic}[{md.partition}]@{md.offset} key={key}")

    p.flush(10)
    p.close()

if __name__ == "__main__":
    main()
```

