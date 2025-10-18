"""
Question 154: UDFs vs Pandas UDFs & Vectorization: Advanced Task on `events`

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
         .appName("PySpark_Question_154")
         .getOrCreate())

# Example placeholder inputs:
# df = spark.read.parquet("/path/to/fact")
# dim = spark.read.parquet("/path/to/dim")
# streaming_df = (spark.readStream.format("json").schema("...").load("/path/to/stream"))

# ---- Solution code from handbook ----

from pyspark.sql import functions as F, types as T
import pandas as pd

@F.pandas_udf("double")
def zscore(col: pd.Series) -> pd.Series:
    mu = col.mean()
    sig = col.std(ddof=0) or 1.0
    return (col - mu) / sig

df2 = df.withColumn("quantity", F.col("quantity").cast("double"))
out = df2.withColumn("z_quantity", zscore(F.col("quantity")))
