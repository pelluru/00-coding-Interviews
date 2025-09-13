# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 126: 126 - Pivot & Crosstab: Rollup challenge
# MAGIC **Category:** Pivot & Crosstab

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
from pyspark.sql import functions as F
res = events.groupBy("user_id").pivot("event_type").agg(F.count("*")).fillna(0)

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
