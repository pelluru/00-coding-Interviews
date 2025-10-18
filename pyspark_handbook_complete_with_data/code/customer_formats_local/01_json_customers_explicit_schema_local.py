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
      .json("/mnt/data/customer_demo_data/customers.jsonl"))

# Basic quality checks
dq = (df
      .withColumn("rule_id_not_null", F.col("customer_id").isNotNull())
      .withColumn("rule_email_like", F.col("email").rlike(r"^[^@]+@[^@]+\.[^@]+$"))
      .withColumn("rule_age_range", (F.col("age") >= 18) & (F.col("age") <= 120)))

bad = dq.filter(~(F.col("rule_id_not_null") & F.col("rule_email_like") & F.col("rule_age_range")))
good = dq.filter(F.col("rule_id_not_null") & F.col("rule_email_like") & F.col("rule_age_range"))          .drop("rule_id_not_null","rule_email_like","rule_age_range")

good.show(5, truncate=False)
bad.select("_corrupt_record").where("_corrupt_record is not null").show(5, truncate=False)
