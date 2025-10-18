"""
Delta Optimize + Z-Ordering Sketch
- Requires Delta Lake and a cluster that supports OPTIMIZE/ZORDER.
"""
from pyspark.sql import SparkSession

spark = (SparkSession.builder
         .appName("DeltaOptimizeZOrderExample")
         .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
         .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
         .getOrCreate())

TABLE = "customers_dim"   # use the table created by the upsert example
try:
    spark.sql(f"OPTIMIZE {TABLE}")
    spark.sql(f"OPTIMIZE {TABLE} ZORDER BY (customer_id)")
    print("OPTIMIZE/ZORDER executed.")
except Exception as e:
    print("OPTIMIZE/ZORDER may not be supported on this cluster:", e)
