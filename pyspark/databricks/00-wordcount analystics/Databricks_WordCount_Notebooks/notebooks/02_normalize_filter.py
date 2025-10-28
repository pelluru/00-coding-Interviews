# Databricks notebook source
%md
# 02 — Normalize Case & Filter Blanks
Lowercase, trim, and remove empty tokens before counting.

# COMMAND ----------
from pyspark.sql.functions import split, explode, col, lower, trim, regexp_replace

DATA_BASE = "/FileStore/wordcount"
SAMPLE_PATH = f"{DATA_BASE}/sample_text.txt"

lines = spark.read.text(SAMPLE_PATH)

cleaned = lines.select(
    lower(trim(col("value"))).alias("line")
)
# Optional: strip punctuation with regex replace
cleaned = cleaned.select(regexp_replace(col("line"), "[^\w\s]", " ").alias("line"))

words = cleaned.select(explode(split(col("line"), "\s+")).alias("word")).filter(col("word") != "")
word_count = words.groupBy("word").count().orderBy(col("count").desc())

display(word_count)

