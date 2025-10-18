"""
Question 151: Window Functions & Analytics: Advanced Task on `sessions`

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
         .appName("PySpark_Question_151")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, Window as W

w = W.partitionBy("device_id").orderBy(F.col("updated_at").cast("timestamp"))

df_clean = (
    df
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("latency_ms", F.col("latency_ms").cast("double"))
    .dropna(subset=["device_id", "updated_at"])
)

result = (
    df_clean
    .withColumn("prev_latency_ms", F.lag("latency_ms").over(w))
    .withColumn("rolling_sum_3", F.sum("latency_ms").over(w.rowsBetween(-2, 0)))
    .withColumn("rank_desc", F.row_number().over(w.orderBy(F.desc("latency_ms"))))
)
