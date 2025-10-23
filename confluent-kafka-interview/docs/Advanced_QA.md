# Confluent Kafka — Advanced Interview Q&A


### 1) EOS End-to-End
- Producer uses transactions to atomically write to multiple topics and commit consumer offsets.
- Streams uses **processing.guarantee=exactly_once_v2**.
- Sinks (e.g., BigQuery, S3) need idempotent upserts/merge semantics to avoid duplicates.

### 2) Multi-Region DR
Use **Cluster Linking** (Confluent Cloud) or **MirrorMaker 2**; prefer **active/active** for regional latency or **active/passive** for simpler failover. Address offset translation, schema sync, and security.

### 3) Security
TLS everywhere, SASL (OAUTHBEARER/PLAIN/SCRAM), RBAC in Confluent, scoped API Keys/Service Accounts, private networking, and secrets management. Apply ACLs by **principal** against resources (Topic, Group, TransactionalId).

### 4) Performance Tuning
- Partitions: increase for throughput but avoid tiny messages & excessive partitions per broker.
- Producer: `compression.type` (lz4/zstd), `linger.ms`, `batch.size`.
- Consumer: concurrency via groups; tune fetch sizes; isolate heavy operators.
- Broker: page cache, I/O, network, GC, and proper retention settings.
