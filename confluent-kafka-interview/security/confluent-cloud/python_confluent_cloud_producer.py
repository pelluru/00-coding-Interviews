# python_confluent_cloud_producer.py (uses confluent-kafka)
from confluent_kafka import Producer
import json, time, os

"""
Algorithm:
1) Build secure config with SASL_SSL + PLAIN using API Key/Secret.
2) Produce keyed messages to preserve per-key ordering.
3) Flush on shutdown.
"""

def make_conf():
    return {
        "bootstrap.servers": os.getenv("BOOTSTRAP", "<BOOTSTRAP_HOST:PORT>"),
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": os.getenv("API_KEY", "<API_KEY>"),
        "sasl.password": os.getenv("API_SECRET", "<API_SECRET>"),
        "compression.type": "zstd",
        "linger.ms": 20,
        "enable.idempotence": True,
        "acks": "all",
    }

def delivery(err, msg):
    if err: print("Delivery failed:", err)
    else: print(f"Produced {msg.topic()}[{msg.partition()}]@{msg.offset()} key={msg.key()}")

def main():
    topic = os.getenv("TOPIC", "orders")
    p = Producer(make_conf())
    for i in range(10):
        key = f"user-{i%3}".encode()
        val = json.dumps({"id": i, "ts": int(time.time())}).encode()
        p.produce(topic, key=key, value=val, callback=delivery)
        p.poll(0)
    p.flush()

if __name__ == "__main__":
    main()
