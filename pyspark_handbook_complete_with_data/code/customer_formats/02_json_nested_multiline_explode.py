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
