# Structured Streaming Word Count (Databricks)

This mini-bundle adds **two streaming notebooks**:

- `notebooks/08_stream_wordcount_file.py` — watches a DBFS folder for new text files.
- `notebooks/09_stream_wordcount_kafka.py` — consumes messages from a Kafka topic.

## Quick Start

### File Source
1. Import `08_stream_wordcount_file.py` into Databricks.
2. Run the notebook to start the stream.
3. In another notebook/cell, add files into DBFS path `/FileStore/wordcount_stream/input`:
   ```python
   dbutils.fs.put("/FileStore/wordcount_stream/input/batch1.txt", "Hello Spark streaming streaming!", True)
   ```
4. Watch the console sink for counts, and look at Parquet under `/FileStore/wordcount_stream/output/file_parquet`.

### Kafka Source
1. Ensure a Kafka cluster is reachable; edit `KAFKA_BROKERS` and `TOPIC` in the notebook.
2. Import and run `09_stream_wordcount_kafka.py`.
3. Produce messages to the topic (one message per line):
   ```
   kafka-console-producer --broker-list localhost:9092 --topic wordcount
   >Hello Spark streaming from Kafka!
   ```
4. Observe console counts and Parquet at `/FileStore/wordcount_stream/output/kafka_parquet`.

## Notes
- Both notebooks use **`outputMode='complete'`** for running totals.
- Each sink has its own **checkpoint** directory for fault tolerance.
- For production, adjust starting offsets, checkpoint locations, security settings, and cluster configs.
