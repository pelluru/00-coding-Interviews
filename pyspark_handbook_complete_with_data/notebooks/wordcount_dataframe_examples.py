# Databricks notebook source
# MAGIC %md
# MAGIC # Word Count with DataFrames Only
# MAGIC This notebook demonstrates Word Count using **Spark DataFrames / SQL only** (no RDD API).
# MAGIC It includes static word count, streaming word count, and windowed streaming counts.
# COMMAND ----------

from pyspark.sql import SparkSession, functions as F, types as T
spark = SparkSession.builder.getOrCreate()

# Widgets (Databricks). If not present (local run), define fallbacks.
try:
    dbutils.widgets.text("INPUT_DIR", "/dbfs/mnt/data/wordcount_df_texts", "Input Directory")
    dbutils.widgets.text("STOPWORDS", "the,and,or,to,of,is,are,a,an", "Stopwords (comma-separated)")
    dbutils.widgets.dropdown("LOWERCASE", "true", ["true","false"], "Lowercase tokens")
    dbutils.widgets.text("STREAM_INPUT", "/dbfs/mnt/data/wordcount_df_stream_in", "Streaming Input Dir")
    dbutils.widgets.text("STREAM_CHECKPOINT", "/dbfs/mnt/data/wordcount_df_stream_chk", "Streaming Checkpoint")
except NameError:
    class Dummy:
        def get(self, *a, **k): return ""
    dbutils = type("dbutils", (), {"widgets": Dummy()})()

INPUT_DIR = (dbutils.widgets.get("INPUT_DIR") or "/dbfs/mnt/data/wordcount_df_texts")
STOPWORDS = (dbutils.widgets.get("STOPWORDS") or "the,and,or,to,of,is,are,a,an")
LOWERCASE = (dbutils.widgets.get("LOWERCASE") or "true").lower() == "true"
STREAM_INPUT = (dbutils.widgets.get("STREAM_INPUT") or "/dbfs/mnt/data/wordcount_df_stream_in")
STREAM_CHECKPOINT = (dbutils.widgets.get("STREAM_CHECKPOINT") or "/dbfs/mnt/data/wordcount_df_stream_chk")

stop_set = [s.strip() for s in STOPWORDS.split(",") if s.strip()]

# COMMAND ----------
# MAGIC %md
# MAGIC ## (Optional) Seed sample text files to DBFS

import os, shutil
local_dir = "/mnt/data/wordcount_df_only_notebook/sample_texts"
dbfs_target = INPUT_DIR

if dbfs_target.startswith("/dbfs/") and os.path.isdir(local_dir):
    os.makedirs(dbfs_target, exist_ok=True)
    for fn in os.listdir(local_dir):
        if fn.endswith(".txt"):
            shutil.copy(os.path.join(local_dir, fn), os.path.join(dbfs_target, fn))
    print("Seeded sample files to:", dbfs_target)
else:
    print("Skipping seeding: not a /dbfs path or local samples missing.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## 1) Static DataFrame Word Count

# Read text files into a DataFrame (single string column 'value')
text_df = spark.read.text(INPUT_DIR.rstrip("/") + "/*")

clean = text_df
if LOWERCASE:
    clean = clean.withColumn("value", F.lower(F.col("value")))

# Replace non-alphanumeric with space, split, explode
tokens = (clean
          .withColumn("value", F.regexp_replace("value", r"[^a-zA-Z0-9]+", " "))
          .withColumn("token", F.explode(F.split(F.col("value"), r"\s+")))
          .filter(F.col("token") != ""))

# Remove stopwords
if stop_set:
    tokens = tokens.filter(~F.col("token").isin(stop_set))

counts_df = (tokens.groupBy("token")
             .agg(F.count("*").alias("cnt"))
             .orderBy(F.desc("cnt"), F.asc("token")))

display(counts_df.limit(50)) if "display" in globals() else counts_df.show(50, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 2) Structured Streaming Word Count

stream_src = (spark.readStream
              .format("text")
              .load(STREAM_INPUT))

stream_clean = stream_src
if LOWERCASE:
    stream_clean = stream_clean.withColumn("value", F.lower(F.col("value")))

stream_tokens = (stream_clean
                 .withColumn("value", F.regexp_replace("value", r"[^a-zA-Z0-9]+", " "))
                 .withColumn("token", F.explode(F.split(F.col("value"), r"\s+")))
                 .filter(F.col("token") != ""))

if stop_set:
    stream_tokens = stream_tokens.filter(~F.col("token").isin(stop_set))

stream_counts = (stream_tokens
                 .groupBy("token")
                 .agg(F.count("*").alias("cnt"))
                 .orderBy(F.desc("cnt")))

q = (stream_counts.writeStream
     .format("memory")            # demo sink for quick querying
     .queryName("wc_df_only")
     .outputMode("complete")
     .option("checkpointLocation", STREAM_CHECKPOINT)
     .trigger(once=True)          # set to processingTime trigger for continuous demo
     .start())

q.awaitTermination()

try:
    display(spark.sql("SELECT * FROM wc_df_only ORDER BY cnt DESC LIMIT 50"))
except:
    spark.sql("SELECT * FROM wc_df_only ORDER BY cnt DESC LIMIT 50").show(truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 3) Windowed Streaming Counts

from pyspark.sql.functions import current_timestamp, window

stream_src2 = (spark.readStream
               .format("text")
               .load(STREAM_INPUT))

with_time = stream_src2.withColumn("ts", current_timestamp())

prep = with_time
if LOWERCASE:
    prep = prep.withColumn("value", F.lower(F.col("value")))

tokens2 = (prep
           .withColumn("value", F.regexp_replace("value", r"[^a-zA-Z0-9]+", " "))
           .withColumn("token", F.explode(F.split(F.col("value"), r"\s+")))
           .filter(F.col("token") != ""))

if stop_set:
    tokens2 = tokens2.filter(~F.col("token").isin(stop_set))

windowed = (tokens2
            .withWatermark("ts", "5 minutes")
            .groupBy(window("ts", "2 minutes"), F.col("token"))
            .agg(F.count("*").alias("cnt"))
            .orderBy(F.desc("cnt")))

q2 = (windowed.writeStream
      .format("memory")
      .queryName("wc_df_windowed")
      .outputMode("complete")
      .option("checkpointLocation", STREAM_CHECKPOINT + "_w")
      .trigger(once=True)
      .start())

q2.awaitTermination()

try:
    display(spark.sql("SELECT * FROM wc_df_windowed ORDER BY cnt DESC LIMIT 50"))
except:
    spark.sql("SELECT * FROM wc_df_windowed ORDER BY cnt DESC LIMIT 50").show(truncate=False)
