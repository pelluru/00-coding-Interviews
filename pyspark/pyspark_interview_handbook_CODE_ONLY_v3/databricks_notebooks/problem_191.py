# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 191: 191 - Graph-ish / Hierarchical: Recursive-like with joins challenge
# MAGIC **Category:** Graph-ish / Hierarchical

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
res = orders.hint("broadcast")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
