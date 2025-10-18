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
