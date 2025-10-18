"""
Question 5: Stateful Structured Streaming: Advanced Task on `metrics`

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
         .appName("PySpark_Question_5")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, types as T
from pyspark.sql.streaming import GroupState, GroupStateTimeout

schema = " session_id string, event_time timestamp, latency_ms double "

stream = (spark.readStream.format("json")
          .schema(schema)
          .option("maxFilesPerTrigger", 1)
          .load("/data/metrics"))

def update_state(key_value, rows_iter, state: GroupState):
    total = state.get("total") if state.exists else 0.0
    for r in rows_iter:
        total += r["latency_ms"] or 0.0
    state.update({"total": total})
    state.setTimeoutDuration("1 hour")
    return [(key_value, total)]

agg = (stream
       .withWatermark("event_time", "30 minutes")
       .groupByKey(lambda r: r["session_id"])
       .flatMapGroupsWithState(
            outputMode="update",
            stateTimeout=GroupStateTimeout.ProcessingTimeTimeout(),
            func=update_state
       ))

q = (agg.toDF("session_id", "running_total")
     .writeStream
     .format("delta")
     .outputMode("update")
     .option("checkpointLocation", "/chk/metrics")
     .start("/out/metrics"))
