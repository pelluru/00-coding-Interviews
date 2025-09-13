# Databricks notebook source
# MAGIC %md
# MAGIC # Problem 032: 032 - Spark SQL: Sql queries challenge
# MAGIC **Category:** Spark SQL

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
users.createOrReplaceTempView("users_view")
res = spark.sql("SELECT user_id, COUNT(*) AS cnt FROM users_view GROUP BY user_id")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Tests
# MAGIC - `res.count()` should run; add chispa assertions if available.
