# Databricks notebook source
%md
# 01 — Basic Word Count (DataFrames)
This notebook reads a text file and performs a basic word count using `split` + `explode`.

# COMMAND ----------
from pyspark.sql.functions import split, explode, col

# Base path in DBFS where you uploaded the data files
DATA_BASE = "/FileStore/wordcount"
SAMPLE_PATH = f"{DATA_BASE}/sample_text.txt"

# Read as one column 'value' using text source
lines = spark.read.text(SAMPLE_PATH)

words = lines.select(explode(split(col("value"), "\s+")).alias("word"))
word_count = words.groupBy("word").count().orderBy(col("count").desc())

display(word_count)

