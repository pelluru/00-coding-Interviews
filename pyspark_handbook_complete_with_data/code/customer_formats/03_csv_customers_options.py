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
