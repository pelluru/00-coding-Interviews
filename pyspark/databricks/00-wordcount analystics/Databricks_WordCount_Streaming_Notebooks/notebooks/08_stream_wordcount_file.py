# Databricks notebook source
%md
# 08 — Structured Streaming Word Count (File Source)
Watches a directory for new text files, tokenizes, and maintains running counts.

# COMMAND ----------
from pyspark.sql.functions import split, explode, lower, trim, regexp_replace, col

# Directory to watch (DBFS). Upload or stream files into this path.
DATA_DIR = "/FileStore/wordcount_stream/input"
CHECKPOINT_DIR = "/FileStore/wordcount_stream/chkpts/file"
OUTPUT_DIR = "/FileStore/wordcount_stream/output/file_parquet"

# Ensure folders exist (idempotent)
dbutils.fs.mkdirs(DATA_DIR)
dbutils.fs.mkdirs(CHECKPOINT_DIR)
dbutils.fs.mkdirs(OUTPUT_DIR)

# Read stream from files. Each new file's lines appear as rows in 'value' column.
lines = (spark.readStream
              .format("text")
              .load(DATA_DIR))

# Clean and tokenize
cleaned = lines.select(
    regexp_replace(lower(trim(col("value"))), "[^\w\s]", " ").alias("line")
)
words = cleaned.select(explode(split(col("line"), "\\s+")).alias("word")).filter(col("word") != "")

counts = words.groupBy("word").count()

# Write to console for demo
q_console = (counts.writeStream
                    .format("console")
                    .outputMode("complete")   # complete mode for running totals
                    .option("truncate", False)
                    .option("checkpointLocation", CHECKPOINT_DIR + "/console")
                    .start())

# Optionally persist to Parquet
q_parquet = (counts.writeStream
                    .format("parquet")
                    .option("path", OUTPUT_DIR)
                    .option("checkpointLocation", CHECKPOINT_DIR + "/parquet")
                    .outputMode("complete")
                    .start())

# To test quickly, in another notebook/terminal, put a file under DATA_DIR
# e.g., dbutils.fs.put(f"{DATA_DIR}/batch1.txt", "Hello Spark streaming streaming!", True)

q_console.awaitTermination()
q_parquet.awaitTermination()

