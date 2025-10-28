# Databricks notebook source
%md
# 05 — Word Count by Category (Group Context Example)
Counts words within each category.

# COMMAND ----------
from pyspark.sql.functions import split, explode, col

DATA_BASE = "/FileStore/wordcount"
CATEGORY_PATH = f"{DATA_BASE}/category_text.csv"

df = (spark.read.option("header", True).csv(CATEGORY_PATH))
# df: category, text
words = df.withColumn("word", explode(split(col("text"), "\s+"))).drop("text")
word_count = (words.groupBy("category", "word")
                    .count()
                    .orderBy(col("category"), col("count").desc()))
display(word_count)

