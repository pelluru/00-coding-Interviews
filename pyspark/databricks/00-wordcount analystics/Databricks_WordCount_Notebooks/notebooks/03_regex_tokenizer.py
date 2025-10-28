# Databricks notebook source
%md
# 03 — RegexTokenizer (ML) for Punctuation-Resilient Tokenization

# COMMAND ----------
from pyspark.ml.feature import RegexTokenizer
from pyspark.sql.functions import explode, col

DATA_BASE = "/FileStore/wordcount"
SAMPLE_PATH = f"{DATA_BASE}/sample_text.txt"

df = spark.read.text(SAMPLE_PATH).toDF("text")

tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\W+")
tokenized = tokenizer.transform(df)

words = tokenized.select(explode(col("words")).alias("word")).filter(col("word") != "")
word_count = words.groupBy("word").count().orderBy(col("count").desc())

display(word_count)

