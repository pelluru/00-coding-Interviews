# Databricks notebook source
%md
# 07 — Weighted Word Count (TF-style)

# COMMAND ----------
from pyspark.sql.functions import split, explode, col, lower, trim, regexp_replace, sum as _sum

DATA_BASE = "/FileStore/wordcount"
SAMPLE_PATH = f"{DATA_BASE}/sample_text.txt"

lines = spark.read.text(SAMPLE_PATH)
cleaned = lines.select(regexp_replace(lower(trim(col("value"))), "[^\w\s]", " ").alias("line"))
words = cleaned.select(explode(split(col("line"), "\s+")).alias("word")).filter(col("word") != "")

word_count = words.groupBy("word").count()
weighted = word_count.withColumn("weighted_score", col("count") * 0.5)

totals = weighted.agg(_sum("weighted_score").alias("total_weight"))
display(weighted.orderBy(col("weighted_score").desc()))
display(totals)

