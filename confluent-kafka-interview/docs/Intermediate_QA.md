# Confluent Kafka — Intermediate Interview Q&A


### 1) Leader/Follower, ISR
Each partition elects a **leader**; followers replicate. **ISR** (In-Sync Replicas) are replicas fully caught up. Only ISR can become leaders (under clean election).

### 2) Producer Acks & Idempotence
- `acks=0/1/all` impact latency & durability.
- **Idempotent producers** (`enable.idempotence=true`) prevent duplicates on retries.

### 3) Rebalancing
Group membership changes trigger **rebalances**. **Cooperative rebalancing (KIP-429)** reduces stop-the-world disruptions.

### 4) Backpressure
Use proper `max.poll.interval.ms`, `max.poll.records`, batching (`linger.ms`, `batch.size`), and consumer concurrency. Consider DLQ/retry topics for poison messages.

### 5) Exactly-Once Semantics (EOS)
Use **Idempotent Producer** + **Transactions** (`transactional.id`) + **read_committed** isolation in consumers or Kafka Streams EOS.
