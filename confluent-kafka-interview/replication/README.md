# Cross-Cluster Replication

## MirrorMaker 2 (MM2)
- Good for **Kafka OSS** and hybrid setups.
- Replicates topics/consumer group offsets between clusters.
- Configure `clusters` (source/target), topics/regex, and replication policy.

## Cluster Linking (Confluent Cloud)
- Native replication between Confluent clusters (no external workers).
- Lower latency and more operationally simple.
- Great for **active/active** or **active/passive** DR.

Pick one based on your platform and control requirements.
