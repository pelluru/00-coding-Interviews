# autoloader_ingest.py
# Databricks Auto Loader to ingest raw JSON to a Delta table in Unity Catalog.

from pyspark.sql import functions as F

SOURCE_PATH = "/mnt/raw/transactions"
CHECKPOINT = "/mnt/chk/autoloader/transactions"
TARGET_TABLE = "raw.transactions_raw"  # UC: <catalog>.<schema>.<table>

df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.inferColumnTypes", "true")
         .option("cloudFiles.schemaLocation", "/mnt/autoloader_schemas/transactions")
         .load(SOURCE_PATH)
)

# Basic cleanup + watermark for late data handling
clean = (
    df.withColumn("event_time", F.col("event_time").cast("timestamp"))
      .withColumn("amount", F.col("amount").cast("double"))
      .filter(F.col("amount") > 0)
      .withWatermark("event_time", "2 hours")
)

(
    clean.writeStream
         .format("delta")
         .option("checkpointLocation", CHECKPOINT)
         .outputMode("append")
         .toTable(TARGET_TABLE)
)
