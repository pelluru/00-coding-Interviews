"""
Delta Upsert Example (Pure PySpark + Delta Lake)
- Requires Delta Lake on your Spark cluster.
- Edit `TARGET_PATH` as needed (DBFS, S3, ADLS, GCS).
"""
from pyspark.sql import SparkSession, functions as F

spark = (SparkSession.builder
         .appName("DeltaUpsertExample")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

TARGET_PATH = "/tmp/delta/customers_dim"   # change for your environment
spark.sql(f"CREATE TABLE IF NOT EXISTS customers_dim USING DELTA LOCATION '{TARGET_PATH}' AS SELECT 'seed' as customer_id")

incoming = spark.createDataFrame([
    ("c1","Alice","US", 100.0, "2025-10-01"),
    ("c2","Bob","UK",  50.0, "2025-10-01"),
    ("c3","Chen","US", 230.0, "2025-10-02"),
], ["customer_id","name","country","lifetime_spend","as_of_date"])

incoming.createOrReplaceTempView("incoming")

merge_sql = f"""
MERGE INTO customers_dim t
USING incoming s
ON t.customer_id = s.customer_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
"""

spark.sql(merge_sql)
print("Delta MERGE completed into:", TARGET_PATH)
