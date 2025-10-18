"""
Question 67: Sessionization (clickstreams): Advanced Task on `impressions`

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
         .appName("PySpark_Question_67")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, Window as W

w = W.partitionBy("customer_id").orderBy("updated_at")
df2 = (df
       .withColumn("prev_ts", F.lag("updated_at").over(w))
       .withColumn("gap_sec", F.coalesce(F.col("updated_at").cast("long") - F.col("prev_ts").cast("long"), F.lit(0)))
       .withColumn("new_sess", (F.col("gap_sec") > 1800).cast("int"))
       .withColumn("session_seq", F.sum("new_sess").over(w.rowsBetween(Window.unboundedPreceding, 0)))
      )
