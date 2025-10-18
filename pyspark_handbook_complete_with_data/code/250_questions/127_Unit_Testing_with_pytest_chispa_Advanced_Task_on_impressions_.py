"""
Question 127: Unit Testing with pytest & chispa: Advanced Task on `impressions`

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
         .appName("PySpark_Question_127")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

# pip install chispa
from chispa import assert_df_equality

def transform(df):
    return df.filter("amount > 0")

def test_transform(spark):
    input_df = spark.createDataFrame([(1, -1.0), (2, 3.0)], ["id","amount"])
    exp_df = spark.createDataFrame([(2, 3.0)], ["id","amount"])
    assert_df_equality(transform(input_df), exp_df, ignore_column_order=True)
