# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 087: 087 - Complex Types: Maps challenge
# MAGIC **Category:** Complex Types

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
res = transactions
if "tags" in res.columns:
    res = res.withColumn("tag", F.explode_outer("tags"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
