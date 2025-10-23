# Python Transactional Producer (Conceptual)

```python
"""
Note: kafka-python has limited transaction support; EOS is typically done with Java client or Kafka Streams.
This file outlines the algorithm for EOS with transactions:

Algorithm:
1) Configure producer with transactional.id and enable.idempotence=true.
2) initTransactions(); beginTransaction().
3) Send records to output topic(s).
4) sendOffsetsToTransaction() to atomically commit offsets.
5) commitTransaction() or abortTransaction() on failure.
"""
```

