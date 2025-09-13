# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 149: 149 - Aggregations & GroupBy: Approx_count_distinct challenge
# MAGIC **Category:** Aggregations & GroupBy

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
user_stats = clicks.groupBy("user_id").agg(F.count("*").alias("cnt"), F.sum("value").alias("sum_value"), F.avg("value").alias("avg_value"))
global_distinct = clicks.select(F.countDistinct("user_id").alias("distinct_users"))
res = user_stats

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
