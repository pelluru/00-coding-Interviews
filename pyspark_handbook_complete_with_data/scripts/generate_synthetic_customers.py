"""
Synthetic Customer Data Generator (PySpark)

Usage:
  spark-submit generate_synthetic_customers.py --rows 100000 --out /mnt/data/synth --formats csv jsonl parquet
"""
import argparse, random
from datetime import datetime, timedelta
from pyspark.sql import SparkSession, functions as F

def rand_name():
    first = ["Alice","Bob","Chen","Dia","Ed","Fatima","Gita","Hiro","Ivan","Jae","Kai","Liu","Mei","Noah"]
    last = ["Smith","Chen","Kim","Khan","Garcia","Patel","Singh","Ivanov","Nguyen","Brown","Lee"]
    return f"{random.choice(first)} {random.choice(last)}"

def rand_email(name):
    user = name.lower().replace(" ", ".")
    dom = random.choice(["example.com","mail.com","corp.local","test.org"])
    return f"{user}@{dom}"

def rand_country():
    return random.choice(["US","UK","IN","CA","AU","DE","FR"])

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=100000)
    ap.add_argument("--out", type=str, required=True)
    ap.add_argument("--formats", nargs="+", default=["csv","jsonl","parquet"])
    args = ap.parse_args()

    spark = SparkSession.builder.appName("SyntheticCustomers").getOrCreate()

    base = []
    now = datetime(2025, 10, 1)
    for i in range(args.rows):
        cid = f"c{i:07d}"
        nm = rand_name()
        em = rand_email(nm)
        dt = now - timedelta(days=random.randint(0, 365*2))
        country = rand_country()
        spend = round(random.random()*1000, 2)
        base.append((cid, nm, em, dt.strftime("%Y-%m-%dT%H:%M:%S"), country, spend))

    df = spark.createDataFrame(base, ["customer_id","name","email","signup_ts","country","spend"])              .withColumn("yr", F.year(F.to_timestamp("signup_ts")))              .withColumn("mo", F.month(F.to_timestamp("signup_ts")))

    if "csv" in args.formats:
        (df.write.mode("overwrite").option("header", True)
           .partitionBy("yr","mo","country").csv(args.out + "/csv"))

    if "jsonl" in args.formats:
        (df.drop("yr","mo")
           .write.mode("overwrite").json(args.out + "/jsonl"))

    if "parquet" in args.formats:
        (df.write.mode("overwrite").partitionBy("country","yr","mo")
           .parquet(args.out + "/parquet"))

    print("Done. Wrote:", args.out)

if __name__ == "__main__":
    main()
