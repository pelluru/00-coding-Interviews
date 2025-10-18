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
