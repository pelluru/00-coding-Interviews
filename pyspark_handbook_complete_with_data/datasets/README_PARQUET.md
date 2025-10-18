# Parquet sample data

To generate partitioned Parquet under this folder with Spark:

```python
from pyspark.sql import SparkSession, functions as F
spark = SparkSession.builder.getOrCreate()

rows = [("c1","Alice","alice@example.com","US",120.5,"2024-06-15"),
        ("c2","Bob","bob@example.co.uk","UK",89.0,"2024-07-01")]
df = spark.createDataFrame(rows, ["customer_id","name","email","country","spend","signup_date"])              .withColumn("yr", F.year(F.to_date("signup_date")))              .withColumn("mo", F.month(F.to_date("signup_date")))

(df.write.mode("overwrite")
   .partitionBy("country","yr","mo")
   .parquet("customers_parquet"))
```