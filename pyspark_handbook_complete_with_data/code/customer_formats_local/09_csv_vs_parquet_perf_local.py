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

(customers.write.mode("overwrite").option("header", True).csv("/mnt/data/customer_demo_data/customers_csv_bench"))
(customers.write.mode("overwrite").parquet("/mnt/data/customer_demo_data/customers_parquet_bench"))

csv_df = spark.read.option("header", True).csv("/mnt/data/customer_demo_data/customers_csv_bench")
parq_df = spark.read.parquet("/mnt/data/customer_demo_data/customers_parquet_bench")

print("CSV count:", csv_df.count())
print("Parquet count:", parq_df.count())
