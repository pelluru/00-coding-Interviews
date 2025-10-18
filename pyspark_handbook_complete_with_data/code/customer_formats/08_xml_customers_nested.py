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
