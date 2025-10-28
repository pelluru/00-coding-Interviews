# Databricks notebook source
%md
# 06 — Pivot Word Count (Wide Feature Matrix)

# COMMAND ----------
from pyspark.sql.functions import split, explode, col

DATA_BASE = "/FileStore/wordcount"
CATEGORY_PATH = f"{DATA_BASE}/category_text.csv"

df = spark.read.option("header", True).csv(CATEGORY_PATH)
words = df.withColumn("word", explode(split(col("text"), "\s+"))).drop("text")
word_count = words.groupBy("category", "word").count()

pivot_df = (word_count.groupBy("category")
                      .pivot("word")
                      .sum("count")
                      .fillna(0))
display(pivot_df.orderBy(col("category")))

