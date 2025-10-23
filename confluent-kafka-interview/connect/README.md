# Kafka Connect Templates


Fill placeholders for your environment (bootstrap servers, credentials, topics, connector class).

- `source-postgres.json`: CDC from Postgres (Debezium) to Kafka.
- `sink-s3.json`: Sink topic to S3 in Parquet/JSON with DLQ and error tolerance.
