# Databricks notebook source
# MAGIC %md
# MAGIC # Customer I/O Formats Pack
# MAGIC This notebook collects JSON, CSV, Parquet, and XML customer examples.
# COMMAND ----------
# MAGIC %md
# MAGIC ## 01_json_customers_explicit_schema.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Explicit schema is safer/faster than inference on large datasets
customer_schema = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("name", T.StringType(), True),
    T.StructField("email", T.StringType(), True),
    T.StructField("signup_ts", T.TimestampType(), True),
    T.StructField("age", T.IntegerType(), True),
    T.StructField("is_premium", T.BooleanType(), True)
])

df = (spark.read
      .option("mode", "PERMISSIVE")              # keep corrupt rows rather than fail
      .option("columnNameOfCorruptRecord", "_corrupt_record")
      .schema(customer_schema)
      .json("/path/to/customers/*.json"))

# Basic quality checks
dq = (df
      .withColumn("rule_id_not_null", F.col("customer_id").isNotNull())
      .withColumn("rule_email_like", F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
      .withColumn("rule_age_range", (F.col("age") >= 18) & (F.col("age") <= 120)))

bad = dq.filter(~(F.col("rule_id_not_null") & F.col("rule_email_like") & F.col("rule_age_range")))
good = dq.filter(F.col("rule_id_not_null") & F.col("rule_email_like") & F.col("rule_age_range"))          .drop("rule_id_not_null","rule_email_like","rule_age_range")

good.show(5, truncate=False)
bad.select("_corrupt_record").where("_corrupt_record is not null").show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 02_json_nested_multiline_explode.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Example nested JSON with orders per customer (multiline JSON objects)
schema = T.StructType([
    T.StructField("customer_id", T.StringType(), False),
    T.StructField("profile", T.StructType([
        T.StructField("name", T.StringType(), True),
        T.StructField("age", T.IntegerType(), True)
    ]), True),
    T.StructField("orders", T.ArrayType(T.StructType([
        T.StructField("order_id", T.StringType(), True),
        T.StructField("amount", T.DoubleType(), True),
        T.StructField("ordered_at", T.TimestampType(), True)
    ])), True)
])

raw = (spark.read
       .option("multiLine", True)
       .schema(schema)
       .json("/path/to/nested/customers_orders/*.json"))

orders = (raw
          .select("customer_id", "profile.*", F.explode_outer("orders").alias("ord"))
          .select("customer_id", "name", "age",
                  F.col("ord.order_id").alias("order_id"),
                  F.col("ord.amount").alias("amount"),
                  F.col("ord.ordered_at").alias("ordered_at")))

# Top-3 orders per customer by amount
w = W.partitionBy("customer_id").orderBy(F.desc("amount"))
top3 = (orders
        .withColumn("r", F.row_number().over(w))
        .filter(F.col("r") <= 3).drop("r"))
top3.show(10, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 03_csv_customers_options.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

schema = "customer_id string, name string, email string, signup_date date, country string, spend double"

df = (spark.read
      .option("header", True)
      .option("mode", "DROPMALFORMED")
      .option("timestampFormat", "yyyy-MM-dd'T'HH:mm:ss")
      .schema(schema)
      .csv("/path/to/customers/*.csv"))

# Normalize and validate
df2 = (df
       .withColumn("email", F.lower(F.trim("email")))
       .withColumn("country", F.upper("country"))
       .withColumn("valid_country", F.col("country").isin("US","CA","UK","IN","AU"))
       .withColumn("yr", F.year("signup_date"))
       .withColumn("mo", F.month("signup_date")))

# Write partitioned CSV (often better to write Parquet; this is illustrative)
(df2.write
    .mode("overwrite")
    .partitionBy("yr","mo")
    .option("header", True)
    .csv("/path/to/out/customers_csv_partitioned"))

# COMMAND ----------
# MAGIC %md
# MAGIC ## 04_parquet_partition_pruning.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Read partitioned Parquet and demonstrate partition pruning
df = spark.read.parquet("/path/to/customers_parquet/")  # e.g., .../country=US/yr=2025/mo=10/

# Filter on partition columns to allow pruning
subset = df.filter((F.col("country") == "US") & (F.col("yr") == 2025) & (F.col("mo") == 10))

subset.explain(True)
subset.groupBy("country","yr","mo").agg(F.count("*").alias("cnt")).show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 05_parquet_schema_evolution.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Write two versions with different columns and read back with mergeSchema
base = spark.createDataFrame([("c1","Alice","US")], ["customer_id","name","country"])
extra = spark.createDataFrame([("c1","Alice","US", True)], ["customer_id","name","country","is_premium"])

base.write.mode("overwrite").parquet("/tmp/customers_evo")
extra.write.mode("append").parquet("/tmp/customers_evo")  # different schema

merged = (spark.read
          .option("mergeSchema","true")
          .parquet("/tmp/customers_evo"))
merged.printSchema()
merged.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 06_upsert_foreachBatch.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# This example assumes a streaming source or batched micro-ingests.
# With Delta, replace the MERGE block with spark.sql('MERGE INTO ...') on a Delta table.
def upsert_to_target(batch_df, batch_id: int):
    batch_df.createOrReplaceTempView("batch_upserts")
    # Example MERGE for Delta (uncomment when Delta is available):
    # spark.sql(\"""
    # MERGE INTO customer_dim t
    # USING batch_upserts s
    #   ON t.customer_id = s.customer_id
    # WHEN MATCHED THEN UPDATE SET *
    # WHEN NOT MATCHED THEN INSERT *
    # \""")

# batch_df example
incoming = spark.createDataFrame([
    ("c1","Alice","US", 100.0),
    ("c2","Bob","UK",  50.0),
], ["customer_id","name","country","lifetime_spend"])

# Simulate a foreachBatch call
upsert_to_target(incoming, 1)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 07_xml_customers_basic.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Requires: com.databricks:spark-xml_2.12:<version>
# Example XML:
# <customers>
#   <customer><id>c1</id><name>Alice</name><email>a@example.com</email></customer>
#   <customer><id>c2</id><name>Bob</name><email>b@example.com</email></customer>
# </customers>

df = (spark.read
      .format("xml")
      .option("rowTag", "customer")
      .load("/path/to/customers.xml"))

flat = (df
        .withColumnRenamed("id", "customer_id")
        .select("customer_id", "name", "email"))
flat.show()

# COMMAND ----------
# MAGIC %md
# MAGIC ## 08_xml_customers_nested.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# Example XML with nested elements/arrays:
# <customers>
#   <customer id="c1">
#     <profile><name>Alice</name><age>29</age></profile>
#     <orders><order><order_id>o1</order_id><amount>12.5</amount></order></orders>
#   </customer>
# </customers>
df = (spark.read.format("xml")
      .option("rowTag","customer")
      .load("/path/to/customers_nested.xml"))

# If attributes appear as _attr, you may need to rename/select
# Explode arrays if needed
cols = df.columns
df.printSchema()
df.show(5, truncate=False)

# COMMAND ----------
# MAGIC %md
# MAGIC ## 09_csv_vs_parquet_perf.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

# WARNING: This is a sketch; measuring performance reliably requires stable input sizes and cluster.
customers = spark.range(0, 1_000_00).select(
    F.col("id").cast("string").alias("customer_id"),
    F.concat(F.lit("name_"), F.col("id")).alias("name"),
    F.lit("US").alias("country"),
    (F.rand()*1000).cast("double").alias("spend")
)

(customers.write.mode("overwrite").option("header", True).csv("/tmp/customers_csv_bench"))
(customers.write.mode("overwrite").parquet("/tmp/customers_parquet_bench"))

csv_df = spark.read.option("header", True).csv("/tmp/customers_csv_bench")
parq_df = spark.read.parquet("/tmp/customers_parquet_bench")

print("CSV count:", csv_df.count())
print("Parquet count:", parq_df.count())

# COMMAND ----------
# MAGIC %md
# MAGIC ## 10_jsonlines_customers.py
# COMMAND ----------
"""
Customer I/O Formats Pack — Pure PySpark Examples

Each script is standalone and focuses on a specific file format or pattern
(JSON, CSV, Parquet, XML). Replace input paths with your own.
For XML, add the package `com.databricks:spark-xml_2.12:<version>` to your cluster.
"""
from pyspark.sql import SparkSession, functions as F, types as T, Window as W

spark = (SparkSession.builder
         .appName("CustomerFormatsPack")
         # For XML support, ensure the spark-xml package is attached in your cluster/library config
         # .config("spark.jars.packages", "com.databricks:spark-xml_2.12:0.20.0")
         .getOrCreate())

df = spark.read.json("/path/to/customers/*.json")  # json-per-line is default
df.select("customer_id","name","email").show(10, truncate=False)

# COMMAND ----------