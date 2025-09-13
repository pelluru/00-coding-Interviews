# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 118: 118 - File IO & Formats: Delta-like challenge
# MAGIC **Category:** File IO & Formats

# COMMAND ----------
# MAGIC %python
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.window import Window
spark = SparkSession.builder.getOrCreate()

# COMMAND ----------
# MAGIC %md
# MAGIC ## Solution

# COMMAND ----------
# MAGIC %python
# Example write (commented):
# products.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = products

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
