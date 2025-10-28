# Databricks notebook source
%md
# 09 — Structured Streaming Word Count (Kafka Source)
Consumes text messages from Kafka topic, tokenizes, and counts words.

# COMMAND ----------
from pyspark.sql.functions import split, explode, lower, trim, regexp_replace, col

# Kafka configs
KAFKA_BROKERS = "localhost:9092"    # adjust for your cluster
TOPIC = "wordcount"

CHECKPOINT_DIR = "/FileStore/wordcount_stream/chkpts/kafka"
OUTPUT_DIR = "/FileStore/wordcount_stream/output/kafka_parquet"

dbutils.fs.mkdirs(CHECKPOINT_DIR)
dbutils.fs.mkdirs(OUTPUT_DIR)

# Read from Kafka (value is binary). Convert to string.
raw = (spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", KAFKA_BROKERS)
            .option("subscribe", TOPIC)
            .option("startingOffsets", "earliest")   # change to 'latest' in prod
            .load())

lines = raw.selectExpr("CAST(value AS STRING) AS value")

cleaned = lines.select(
    regexp_replace(lower(trim(col("value"))), "[^\w\s]", " ").alias("line")
)
words = cleaned.select(explode(split(col("line"), "\\s+")).alias("word")).filter(col("word") != "")

counts = words.groupBy("word").count()

q_console = (counts.writeStream
                    .format("console")
                    .outputMode("complete")
                    .option("truncate", False)
                    .option("checkpointLocation", CHECKPOINT_DIR + "/console")
                    .start())

q_parquet = (counts.writeStream
                    .format("parquet")
                    .option("path", OUTPUT_DIR)
                    .option("checkpointLocation", CHECKPOINT_DIR + "/parquet")
                    .outputMode("complete")
                    .start())

# To test locally, produce messages to Kafka topic 'wordcount'
# kafka-console-producer --broker-list localhost:9092 --topic wordcount
# >Hello Spark Streaming from Kafka!

q_console.awaitTermination()
q_parquet.awaitTermination()

