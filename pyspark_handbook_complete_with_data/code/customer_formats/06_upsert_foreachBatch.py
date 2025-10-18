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

# This example assumes a streaming source or batched micro-ingests.
# With Delta, replace the MERGE block with spark.sql('MERGE INTO ...') on a Delta table.
def upsert_to_target(batch_df, batch_id: int):
    batch_df.createOrReplaceTempView("batch_upserts")
    # Example MERGE for Delta (uncomment when Delta is available):
    # spark.sql(\"""
    # MERGE INTO customer_dim t
    # USING batch_upserts s
    #   ON t.customer_id = s.customer_id
    # WHEN MATCHED THEN UPDATE SET *
    # WHEN NOT MATCHED THEN INSERT *
    # \""")

# batch_df example
incoming = spark.createDataFrame([
    ("c1","Alice","US", 100.0),
    ("c2","Bob","UK",  50.0),
], ["customer_id","name","country","lifetime_spend"])

# Simulate a foreachBatch call
upsert_to_target(incoming, 1)
