# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 091: 091 - Misc Utilities: Broadcast joins challenge
# MAGIC **Category:** Misc Utilities

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
res = products.hint("broadcast")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
