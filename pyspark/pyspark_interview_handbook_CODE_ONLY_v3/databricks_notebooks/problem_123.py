# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 123: 123 - File IO & Formats: Csv challenge
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
# logs.write.mode("overwrite").partitionBy("country").parquet("/path/out") 
res = logs

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
