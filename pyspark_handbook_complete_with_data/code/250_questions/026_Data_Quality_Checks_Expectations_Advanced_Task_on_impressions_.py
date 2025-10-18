"""
Question 26: Data Quality Checks & Expectations: Advanced Task on `impressions`

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
         .appName("PySpark_Question_26")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F

rules = [
    ("not_null_key", F.col("customer_id").isNotNull()),
    ("val_non_negative", F.col("value") >= 0)
]

def apply_rules(df):
    for rule_name, cond in rules:
        df = df.withColumn("rule_" + rule_name, cond)
    return df

scored = apply_rules(df)
bad = scored.filter("NOT (rule_not_null_key AND rule_val_non_negative)").withColumn("reason", F.lit("dq_failed"))
good = scored.filter("rule_not_null_key AND rule_val_non_negative").drop("rule_not_null_key","rule_val_non_negative")
