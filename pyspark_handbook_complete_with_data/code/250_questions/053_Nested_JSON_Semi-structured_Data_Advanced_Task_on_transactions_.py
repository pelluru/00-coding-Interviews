"""
Question 53: Nested JSON & Semi-structured Data: Advanced Task on `transactions`

Pure PySpark solution scaffold with detailed comments.
Auto-extracted from the handbook. Replace placeholders and adjust inputs.

Best Practices Notes:
- Prefer built-in SQL functions over Python UDFs for performance.
- Use AQE (`spark.sql.adaptive.enabled=true`) to mitigate skew and auto-optimize joins.
- For streaming, always define watermarks and deterministic sinks (foreachBatch + MERGE).
- Validate with pytest + chispa; profile tasks/skew in the Spark UI.
"""
from pyspark.sql import SparkSession, functions as F, Window as W

spark = (SparkSession.builder
         .appName("PySpark_Question_53")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, types as T

schema = T.StructType([
    T.StructField("user_id", T.StringType()),
    T.StructField("created_at", T.TimestampType()),
    T.StructField("payload", T.StructType([
        T.StructField("items", T.ArrayType(T.StructType([
            T.StructField("sku", T.StringType()),
            T.StructField("amount", T.DoubleType())
        ])))
    ]))
])

raw = (spark.read
       .option("multiLine", True)
       .option("badRecordsPath", "/tmp/bad_records")
       .json("/data/transactions/*.json"))

dfj = raw.select(F.from_json(F.col("value").cast("string"), schema).alias("r")).select("r.*")
items = dfj.select("user_id", "created_at", F.explode_outer("payload.items").alias("it"))
result = items.select("user_id", "created_at", F.col("it.sku").alias("sku"), F.col(f"it.amount").alias("amount"))
