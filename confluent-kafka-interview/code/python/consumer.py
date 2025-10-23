# Python Consumer (kafka-python)

```python
from kafka import KafkaConsumer, TopicPartition
import json, os

"""
Algorithm (Consumer):
1) Join a consumer group; subscribe to topic(s).
2) Poll records and process safely.
3) Commit offsets after processing (sync or async).
4) Tune `max_poll_records`, `max_poll_interval_ms` for backpressure.
"""

def main():
    topic = os.getenv("TOPIC", "orders")
    bootstrap = os.getenv("BOOTSTRAP", "localhost:9092")
    group = os.getenv("GROUP", "py-consumers")

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=bootstrap.split(","),
        group_id=group,
        auto_offset_reset="earliest",
        enable_auto_commit=False,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        key_deserializer=lambda b: b.decode("utf-8") if b else None,
        max_poll_records=200
    )

    try:
        while True:
            records = consumer.poll(timeout_ms=1000)
            for tp, msgs in records.items():
                for m in msgs:
                    print(f"Consumed {m.topic}[{m.partition}]@{m.offset} key={m.key} value={m.value}")
                # commit after batch
                consumer.commit()
    finally:
        consumer.close()

if __name__ == "__main__":
    main()
```

