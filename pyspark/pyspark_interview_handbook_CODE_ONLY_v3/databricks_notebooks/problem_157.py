# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 157: 157 - File IO & Formats: Orc challenge
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
# transactions.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = transactions

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
