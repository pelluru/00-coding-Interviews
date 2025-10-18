"""
Question 1: Window Functions & Analytics: Advanced Task on `transactions`

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
         .appName("PySpark_Question_1")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, Window as W

w = W.partitionBy("user_id").orderBy(F.col("created_at").cast("timestamp"))

df_clean = (
    df
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("value", F.col("value").cast("double"))
    .dropna(subset=["user_id", "created_at"])
)

result = (
    df_clean
    .withColumn("prev_value", F.lag("value").over(w))
    .withColumn("rolling_sum_3", F.sum("value").over(w.rowsBetween(-2, 0)))
    .withColumn("rank_desc", F.row_number().over(w.orderBy(F.desc("value"))))
)
