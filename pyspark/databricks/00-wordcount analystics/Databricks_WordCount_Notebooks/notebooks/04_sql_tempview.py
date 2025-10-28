# Databricks notebook source
%md
# 04 — DataFrames → Temp View → Spark SQL

# COMMAND ----------
from pyspark.sql.functions import lower, split, explode, col

DATA_BASE = "/FileStore/wordcount"
SAMPLE_PATH = f"{DATA_BASE}/sample_text.txt"

df = spark.read.text(SAMPLE_PATH).toDF("text")
df.createOrReplaceTempView("lines")

# COMMAND ----------
%sql
-- Split and count with SQL
SELECT word, COUNT(*) AS count
FROM (
  SELECT explode(split(lower(text), '\\s+')) AS word FROM lines
)
WHERE word <> ''
GROUP BY word
ORDER BY count DESC

