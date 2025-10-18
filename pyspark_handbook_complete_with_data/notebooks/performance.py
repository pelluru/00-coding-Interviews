# Databricks notebook source
# MAGIC %md
# MAGIC # Advanced PySpark — Performance
# MAGIC These cells were generated from the 250-question PySpark handbook.
# MAGIC Each question has a Markdown overview and a code cell.
# COMMAND ----------
# MAGIC %md
# MAGIC ## 2. Complex Joins & Skew Handling: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `customer_id`, `event_time`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a complex joins & skew handling problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Complex Joins & Skew Handling** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC **Skew joins** cause a few keys to dominate shuffles. We first profile key frequency, then **salt**
# MAGIC hot keys and **broadcast** small dimension tables where possible. Enabling **AQE** can also coalesce
# MAGIC skewed partitions. We demonstrate a salting approach.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

key_freq = df.groupBy("customer_id").count()
hot = key_freq.filter("count > 1000000").select("customer_id").withColumn("is_hot", F.lit(1))
df2 = df.join(hot, on="customer_id", how="left").fillna({"is_hot":0})

salt_mod = 16
df_salted = df2.withColumn("salt", F.when(F.col("is_hot")==1, F.rand()*salt_mod).otherwise(F.lit(0)).cast("int"))

dim_salted = dim.crossJoin(
    spark.range(salt_mod).withColumnRenamed("id","salt")
)
joined = df_salted.join(dim_salted, on=[ "customer_id", "salt" ], how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 11. Bucketing, Partitioning & Writer Jobs: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `account_id`, `created_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a bucketing, partitioning & writer jobs problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Bucketing, Partitioning & Writer Jobs** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 12. Adaptive Query Execution (AQE) and Shuffle Partitions: Advanced Task on `impressions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `impressions` dataset with columns like `session_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a adaptive query execution (aqe) and shuffle partitions problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Adaptive Query Execution (AQE) and Shuffle Partitions** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Enable AQE and tune shuffle partitions for better task balance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

dfj = fact.join(F.broadcast(dim), on="session_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 13. Broadcast Joins and Hints: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `session_id`, `created_at`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a broadcast joins and hints problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Broadcast Joins and Hints** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Broadcast small side tables to avoid shuffles.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
joined = fact.hint("broadcast").join(dim, on="session_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 14. Skew Join Salting Techniques: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `session_id`, `ts`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a skew join salting techniques problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Skew Join Salting Techniques** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 25. Dynamic File Pruning: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `customer_id`, `updated_at`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a dynamic file pruning problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Dynamic File Pruning** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Partition by time and filter by partition columns for pruning.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pruned = spark.read.parquet("/out/logs").filter(F.col("updated_at") >= "2025-01-01")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 28. Performance Debugging with UI & Query Plans: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `session_id`, `updated_at`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a performance debugging with ui & query plans problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Performance Debugging with UI & Query Plans** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Inspect query plans and the Spark UI; avoid Python UDFs and skew.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
df_explain = df.select("session_id", "score").groupBy("session_id").agg(F.sum("score"))
print(df_explain._jdf.queryExecution().toString())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 29. Caching vs Checkpointing vs Persist: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `customer_id`, `updated_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a caching vs checkpointing vs persist problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Caching vs Checkpointing vs Persist** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Checkpoint offsets/state to recover after failures; use idempotent sinks.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
q = (streaming_df
     .writeStream
     .format("parquet")
     .option("checkpointLocation", "/chk/orders")
     .start("/out/orders"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 42. File Compaction Job: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `account_id`, `ts`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a file compaction job problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **File Compaction Job** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/logs")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/logs_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 43. Small-file Problem Mitigation: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `session_id`, `event_time`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a small-file problem mitigation problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Small-file Problem Mitigation** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/orders")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/orders_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 52. Complex Joins & Skew Handling: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `order_id`, `event_time`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a complex joins & skew handling problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Complex Joins & Skew Handling** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC **Skew joins** cause a few keys to dominate shuffles. We first profile key frequency, then **salt**
# MAGIC hot keys and **broadcast** small dimension tables where possible. Enabling **AQE** can also coalesce
# MAGIC skewed partitions. We demonstrate a salting approach.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

key_freq = df.groupBy("order_id").count()
hot = key_freq.filter("count > 1000000").select("order_id").withColumn("is_hot", F.lit(1))
df2 = df.join(hot, on="order_id", how="left").fillna({"is_hot":0})

salt_mod = 16
df_salted = df2.withColumn("salt", F.when(F.col("is_hot")==1, F.rand()*salt_mod).otherwise(F.lit(0)).cast("int"))

dim_salted = dim.crossJoin(
    spark.range(salt_mod).withColumnRenamed("id","salt")
)
joined = df_salted.join(dim_salted, on=[ "order_id", "salt" ], how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 61. Bucketing, Partitioning & Writer Jobs: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `user_id`, `ts`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a bucketing, partitioning & writer jobs problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Bucketing, Partitioning & Writer Jobs** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 62. Adaptive Query Execution (AQE) and Shuffle Partitions: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `session_id`, `updated_at`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a adaptive query execution (aqe) and shuffle partitions problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Adaptive Query Execution (AQE) and Shuffle Partitions** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Enable AQE and tune shuffle partitions for better task balance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

dfj = fact.join(F.broadcast(dim), on="session_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 63. Broadcast Joins and Hints: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `customer_id`, `created_at`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a broadcast joins and hints problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Broadcast Joins and Hints** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Broadcast small side tables to avoid shuffles.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
joined = fact.hint("broadcast").join(dim, on="customer_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 64. Skew Join Salting Techniques: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `user_id`, `updated_at`, and `latency_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a skew join salting techniques problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Skew Join Salting Techniques** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 75. Dynamic File Pruning: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `customer_id`, `event_time`, and `latency_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a dynamic file pruning problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Dynamic File Pruning** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Partition by time and filter by partition columns for pruning.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pruned = spark.read.parquet("/out/transactions").filter(F.col("event_time") >= "2025-01-01")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 78. Performance Debugging with UI & Query Plans: Advanced Task on `events`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `events` dataset with columns like `session_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a performance debugging with ui & query plans problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Performance Debugging with UI & Query Plans** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Inspect query plans and the Spark UI; avoid Python UDFs and skew.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
df_explain = df.select("session_id", "duration_ms").groupBy("session_id").agg(F.sum("duration_ms"))
print(df_explain._jdf.queryExecution().toString())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 79. Caching vs Checkpointing vs Persist: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `account_id`, `created_at`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a caching vs checkpointing vs persist problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Caching vs Checkpointing vs Persist** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Checkpoint offsets/state to recover after failures; use idempotent sinks.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
q = (streaming_df
     .writeStream
     .format("parquet")
     .option("checkpointLocation", "/chk/metrics")
     .start("/out/metrics"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 92. File Compaction Job: Advanced Task on `impressions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `impressions` dataset with columns like `account_id`, `event_time`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a file compaction job problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **File Compaction Job** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/impressions")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/impressions_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 93. Small-file Problem Mitigation: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `account_id`, `event_time`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a small-file problem mitigation problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Small-file Problem Mitigation** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/payments")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/payments_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 102. Complex Joins & Skew Handling: Advanced Task on `clicks`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `clicks` dataset with columns like `customer_id`, `ts`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a complex joins & skew handling problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Complex Joins & Skew Handling** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC **Skew joins** cause a few keys to dominate shuffles. We first profile key frequency, then **salt**
# MAGIC hot keys and **broadcast** small dimension tables where possible. Enabling **AQE** can also coalesce
# MAGIC skewed partitions. We demonstrate a salting approach.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

key_freq = df.groupBy("customer_id").count()
hot = key_freq.filter("count > 1000000").select("customer_id").withColumn("is_hot", F.lit(1))
df2 = df.join(hot, on="customer_id", how="left").fillna({"is_hot":0})

salt_mod = 16
df_salted = df2.withColumn("salt", F.when(F.col("is_hot")==1, F.rand()*salt_mod).otherwise(F.lit(0)).cast("int"))

dim_salted = dim.crossJoin(
    spark.range(salt_mod).withColumnRenamed("id","salt")
)
joined = df_salted.join(dim_salted, on=[ "customer_id", "salt" ], how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 111. Bucketing, Partitioning & Writer Jobs: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `account_id`, `event_time`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a bucketing, partitioning & writer jobs problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Bucketing, Partitioning & Writer Jobs** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 112. Adaptive Query Execution (AQE) and Shuffle Partitions: Advanced Task on `sessions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `sessions` dataset with columns like `customer_id`, `created_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a adaptive query execution (aqe) and shuffle partitions problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Adaptive Query Execution (AQE) and Shuffle Partitions** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Enable AQE and tune shuffle partitions for better task balance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

dfj = fact.join(F.broadcast(dim), on="customer_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 113. Broadcast Joins and Hints: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `session_id`, `updated_at`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a broadcast joins and hints problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Broadcast Joins and Hints** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Broadcast small side tables to avoid shuffles.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
joined = fact.hint("broadcast").join(dim, on="session_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 114. Skew Join Salting Techniques: Advanced Task on `logs`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `logs` dataset with columns like `device_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a skew join salting techniques problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Skew Join Salting Techniques** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 125. Dynamic File Pruning: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `device_id`, `created_at`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a dynamic file pruning problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Dynamic File Pruning** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Partition by time and filter by partition columns for pruning.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pruned = spark.read.parquet("/out/orders").filter(F.col("created_at") >= "2025-01-01")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 128. Performance Debugging with UI & Query Plans: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `account_id`, `created_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a performance debugging with ui & query plans problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Performance Debugging with UI & Query Plans** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Inspect query plans and the Spark UI; avoid Python UDFs and skew.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
df_explain = df.select("account_id", "amount").groupBy("account_id").agg(F.sum("amount"))
print(df_explain._jdf.queryExecution().toString())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 129. Caching vs Checkpointing vs Persist: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `order_id`, `created_at`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a caching vs checkpointing vs persist problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Caching vs Checkpointing vs Persist** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Checkpoint offsets/state to recover after failures; use idempotent sinks.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
q = (streaming_df
     .writeStream
     .format("parquet")
     .option("checkpointLocation", "/chk/orders")
     .start("/out/orders"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 142. File Compaction Job: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `account_id`, `updated_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a file compaction job problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **File Compaction Job** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/orders")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/orders_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 143. Small-file Problem Mitigation: Advanced Task on `sessions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `sessions` dataset with columns like `customer_id`, `created_at`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a small-file problem mitigation problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Small-file Problem Mitigation** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/sessions")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/sessions_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 152. Complex Joins & Skew Handling: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `session_id`, `ts`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a complex joins & skew handling problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Complex Joins & Skew Handling** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC **Skew joins** cause a few keys to dominate shuffles. We first profile key frequency, then **salt**
# MAGIC hot keys and **broadcast** small dimension tables where possible. Enabling **AQE** can also coalesce
# MAGIC skewed partitions. We demonstrate a salting approach.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

key_freq = df.groupBy("session_id").count()
hot = key_freq.filter("count > 1000000").select("session_id").withColumn("is_hot", F.lit(1))
df2 = df.join(hot, on="session_id", how="left").fillna({"is_hot":0})

salt_mod = 16
df_salted = df2.withColumn("salt", F.when(F.col("is_hot")==1, F.rand()*salt_mod).otherwise(F.lit(0)).cast("int"))

dim_salted = dim.crossJoin(
    spark.range(salt_mod).withColumnRenamed("id","salt")
)
joined = df_salted.join(dim_salted, on=[ "session_id", "salt" ], how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 161. Bucketing, Partitioning & Writer Jobs: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `account_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a bucketing, partitioning & writer jobs problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Bucketing, Partitioning & Writer Jobs** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 162. Adaptive Query Execution (AQE) and Shuffle Partitions: Advanced Task on `events`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `events` dataset with columns like `order_id`, `event_time`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a adaptive query execution (aqe) and shuffle partitions problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Adaptive Query Execution (AQE) and Shuffle Partitions** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Enable AQE and tune shuffle partitions for better task balance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

dfj = fact.join(F.broadcast(dim), on="order_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 163. Broadcast Joins and Hints: Advanced Task on `orders`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `orders` dataset with columns like `order_id`, `event_time`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a broadcast joins and hints problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Broadcast Joins and Hints** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Broadcast small side tables to avoid shuffles.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
joined = fact.hint("broadcast").join(dim, on="order_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 164. Skew Join Salting Techniques: Advanced Task on `events`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `events` dataset with columns like `user_id`, `ts`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a skew join salting techniques problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Skew Join Salting Techniques** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 175. Dynamic File Pruning: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `device_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a dynamic file pruning problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Dynamic File Pruning** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Partition by time and filter by partition columns for pruning.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pruned = spark.read.parquet("/out/payments").filter(F.col("event_time") >= "2025-01-01")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 178. Performance Debugging with UI & Query Plans: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `order_id`, `created_at`, and `latency_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a performance debugging with ui & query plans problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Performance Debugging with UI & Query Plans** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Inspect query plans and the Spark UI; avoid Python UDFs and skew.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
df_explain = df.select("order_id", "latency_ms").groupBy("order_id").agg(F.sum("latency_ms"))
print(df_explain._jdf.queryExecution().toString())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 179. Caching vs Checkpointing vs Persist: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `device_id`, `updated_at`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a caching vs checkpointing vs persist problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Caching vs Checkpointing vs Persist** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Checkpoint offsets/state to recover after failures; use idempotent sinks.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
q = (streaming_df
     .writeStream
     .format("parquet")
     .option("checkpointLocation", "/chk/metrics")
     .start("/out/metrics"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 192. File Compaction Job: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `account_id`, `created_at`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a file compaction job problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **File Compaction Job** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/metrics")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/metrics_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 193. Small-file Problem Mitigation: Advanced Task on `impressions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `impressions` dataset with columns like `account_id`, `created_at`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a small-file problem mitigation problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Small-file Problem Mitigation** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/impressions")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/impressions_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 202. Complex Joins & Skew Handling: Advanced Task on `clicks`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `clicks` dataset with columns like `order_id`, `updated_at`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a complex joins & skew handling problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Complex Joins & Skew Handling** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC **Skew joins** cause a few keys to dominate shuffles. We first profile key frequency, then **salt**
# MAGIC hot keys and **broadcast** small dimension tables where possible. Enabling **AQE** can also coalesce
# MAGIC skewed partitions. We demonstrate a salting approach.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
from pyspark.sql import types as T

key_freq = df.groupBy("order_id").count()
hot = key_freq.filter("count > 1000000").select("order_id").withColumn("is_hot", F.lit(1))
df2 = df.join(hot, on="order_id", how="left").fillna({"is_hot":0})

salt_mod = 16
df_salted = df2.withColumn("salt", F.when(F.col("is_hot")==1, F.rand()*salt_mod).otherwise(F.lit(0)).cast("int"))

dim_salted = dim.crossJoin(
    spark.range(salt_mod).withColumnRenamed("id","salt")
)
joined = df_salted.join(dim_salted, on=[ "order_id", "salt" ], how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 211. Bucketing, Partitioning & Writer Jobs: Advanced Task on `payments`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `payments` dataset with columns like `account_id`, `event_time`, and `latency_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a bucketing, partitioning & writer jobs problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Bucketing, Partitioning & Writer Jobs** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 212. Adaptive Query Execution (AQE) and Shuffle Partitions: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `customer_id`, `ts`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a adaptive query execution (aqe) and shuffle partitions problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Adaptive Query Execution (AQE) and Shuffle Partitions** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Enable AQE and tune shuffle partitions for better task balance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.shuffle.partitions", "200")

dfj = fact.join(F.broadcast(dim), on="customer_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 213. Broadcast Joins and Hints: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `customer_id`, `ts`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a broadcast joins and hints problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Broadcast Joins and Hints** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Broadcast small side tables to avoid shuffles.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
from pyspark.sql import functions as F
joined = fact.hint("broadcast").join(dim, on="customer_id", how="left")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 214. Skew Join Salting Techniques: Advanced Task on `metrics`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `metrics` dataset with columns like `user_id`, `ts`, and `amount`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a skew join salting techniques problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Skew Join Salting Techniques** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC General advanced PySpark pattern.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pass
# COMMAND ----------
# MAGIC %md
# MAGIC ## 225. Dynamic File Pruning: Advanced Task on `transactions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `transactions` dataset with columns like `session_id`, `event_time`, and `duration_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a dynamic file pruning problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Dynamic File Pruning** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Partition by time and filter by partition columns for pruning.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
pruned = spark.read.parquet("/out/transactions").filter(F.col("event_time") >= "2025-01-01")
# COMMAND ----------
# MAGIC %md
# MAGIC ## 228. Performance Debugging with UI & Query Plans: Advanced Task on `sessions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `sessions` dataset with columns like `session_id`, `event_time`, and `value`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a performance debugging with ui & query plans problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Performance Debugging with UI & Query Plans** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Inspect query plans and the Spark UI; avoid Python UDFs and skew.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
df_explain = df.select("session_id", "value").groupBy("session_id").agg(F.sum("value"))
print(df_explain._jdf.queryExecution().toString())
# COMMAND ----------
# MAGIC %md
# MAGIC ## 229. Caching vs Checkpointing vs Persist: Advanced Task on `impressions`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `impressions` dataset with columns like `session_id`, `updated_at`, and `score`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a caching vs checkpointing vs persist problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Caching vs Checkpointing vs Persist** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Checkpoint offsets/state to recover after failures; use idempotent sinks.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
q = (streaming_df
     .writeStream
     .format("parquet")
     .option("checkpointLocation", "/chk/impressions")
     .start("/out/impressions"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 242. File Compaction Job: Advanced Task on `events`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `events` dataset with columns like `order_id`, `ts`, and `quantity`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a file compaction job problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **File Compaction Job** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/events")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/events_compacted"))
# COMMAND ----------
# MAGIC %md
# MAGIC ## 243. Small-file Problem Mitigation: Advanced Task on `clicks`
# MAGIC # MAGIC **Question**
# MAGIC 
# MAGIC **Scenario.** You have a large `clicks` dataset with columns like `account_id`, `event_time`, and `latency_ms`.
# MAGIC The data arrives from multiple sources as Parquet/JSON with evolving schemas.
# MAGIC 
# MAGIC **Task.** Using PySpark, implement a robust solution to solve a small-file problem mitigation problem:
# MAGIC - Ingest data with proper schema handling.
# MAGIC - Apply necessary transformations (null-safety, casting, deduplication).
# MAGIC - Implement the core logic related to **Small-file Problem Mitigation** (detailed below).
# MAGIC - Produce an optimized output suitable for downstream consumption (partitioning/bucketing where applicable).
# MAGIC 
# MAGIC **Why this is hard**
# MAGIC 
# MAGIC - Large scale, evolving schemas, and skewed keys.
# MAGIC - Requires balancing correctness, latency, and cost.
# MAGIC - Involves optimizer behavior, partitions, and state (for streaming).
# MAGIC 
# MAGIC **Solution Outline & Explanation**
# MAGIC 
# MAGIC Coalesce many small files into fewer large ones to improve read performance.
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC **Validation**
# MAGIC 
# MAGIC - Unit tests over representative edge cases (nulls, duplicates, late/out-of-order events).
# MAGIC - Profile partitions and task skew in Spark UI.
# MAGIC - Compare aggregates vs. source-of-truth; implement data quality gates.
# MAGIC 
# MAGIC
# COMMAND ----------
(spark.read.parquet("/bronze/clicks")
      .repartition(64)
      .write.mode("overwrite").parquet("/silver/clicks_compacted"))
# COMMAND ----------