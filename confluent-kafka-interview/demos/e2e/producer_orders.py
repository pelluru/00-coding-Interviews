from kafka import KafkaProducer
import json, time, os, random

"""
Demo Producer:
- Sends random 'orders' with amount 100..2000
- Keyed by user-N to illustrate partition-key ordering
"""

def main():
    bootstrap = os.getenv("BOOTSTRAP", "localhost:29092")
    topic = os.getenv("TOPIC", "orders")
    p = KafkaProducer(
        bootstrap_servers=[bootstrap],
        acks="all",
        value_serializer=lambda v: json.dumps(v).encode(),
        key_serializer=lambda k: k.encode()
    )
    now = int(time.time())
    for i in range(50):
        uid = f"user-{i%5}"
        amt = random.randint(100, 2000)
        evt = {"id": i, "user_id": uid, "amount": amt, "ts": now + i}
        md = p.send(topic, key=uid, value=evt).get(timeout=10)
        print(f"Produced {md.topic}[{md.partition}]@{md.offset} amount={amt}")
    p.flush(); p.close()

if __name__ == "__main__":
    main()
