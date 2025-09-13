# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 165: 165 - DataFrame Basics: Select challenge
# MAGIC **Category:** DataFrame Basics

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
from pyspark.sql.window import Window
res = sessions.select("id", "user_id", "event_type", "value") \    .withColumn("value_norm", (F.col("value") - F.mean("value").over(Window.partitionBy()))/F.stddev_pop("value").over(Window.partitionBy())) \    .filter(F.col("event_type") == "purchase")
assert "value_norm" in res.columns

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
