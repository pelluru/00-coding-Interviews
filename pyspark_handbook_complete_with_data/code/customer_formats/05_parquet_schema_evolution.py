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
