"""
Question 51: Window Functions & Analytics: Advanced Task on `metrics`

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
         .appName("PySpark_Question_51")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, Window as W

w = W.partitionBy("customer_id").orderBy(F.col("ts").cast("timestamp"))

df_clean = (
    df
    .withColumn("ts", F.to_timestamp("ts"))
    .withColumn("amount", F.col("amount").cast("double"))
    .dropna(subset=["customer_id", "ts"])
)

result = (
    df_clean
    .withColumn("prev_amount", F.lag("amount").over(w))
    .withColumn("rolling_sum_3", F.sum("amount").over(w.rowsBetween(-2, 0)))
    .withColumn("rank_desc", F.row_number().over(w.orderBy(F.desc("amount"))))
)
