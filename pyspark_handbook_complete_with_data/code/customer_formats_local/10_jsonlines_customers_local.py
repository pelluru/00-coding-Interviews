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

df = spark.read.json("/mnt/data/customer_demo_data/customers.jsonl")  # json-per-line is default
df.select("customer_id","name","email").show(10, truncate=False)
