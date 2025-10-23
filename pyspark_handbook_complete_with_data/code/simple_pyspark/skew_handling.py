# skew_handling_examples.py
# =========================
# Handling Data Skew in PySpark:
#   1) Baseline skewed join (shows the problem shape)
#   2) Broadcast join for small dimension tables
#   3) Salting technique to spread hot keys
#   4) Repartitioning by key (control partition count)
#   5) AQE Skew Join (enable adaptive execution)
#   6) Pre-aggregation before join (reduce dup rows per key)
#
# Each example prints an explanation + a small result so you can see it working.

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, rand, floor, when, explode, sequence, expr,
    broadcast, sum as Fsum, count as Fcount
)
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType


# ------------------------------------------------------------
# Synthetic skewed data generator
# ------------------------------------------------------------
def make_skewed_data(spark, big_rows=100000, hot_ratio=0.9, num_cold_keys=50):
    """
    Create two DataFrames:
      fact_big(id, k, v)   : large, skewed on key k (one 'hot' key with many rows)
      dim_small(k, attr)   : small dimension table with distinct keys (including hot)

    - hot_ratio% of big_rows belong to key 'hot', rest spread across cold keys c0..c{num_cold_keys-1}
    - dim_small contains 'hot' + all cold keys (so joins match)

    Returns: (fact_big, dim_small)
    """
    # Build big fact with skew on 'hot' key
    n_hot = int(big_rows * hot_ratio)
    n_cold = big_rows - n_hot

    # hot rows
    hot = spark.range(n_hot).withColumn("k", lit("hot")).withColumn("v", (col("id") % 100).cast("int"))
    # cold rows spread across num_cold_keys
    cold = (spark.range(n_hot, n_hot + n_cold)
            .withColumn("k", expr(f"'c' || (cast((id - {n_hot}) % {num_cold_keys} as string))"))
            .withColumn("v", (col("id") % 100).cast("int")))

    fact_big = hot.unionByName(cold).select(col("id").cast("long"), "k", "v")

    # small dimension with one row per key (hot + cold keys)
    dim_hot = spark.createDataFrame([("hot", 1)], ["k", "attr"])
    dim_cold = spark.createDataFrame([(f"c{i}", i + 2) for i in range(num_cold_keys)], ["k", "attr"])
    dim_small = dim_hot.unionByName(dim_cold)

    return fact_big, dim_small


# ------------------------------------------------------------
# 1) Baseline skewed join (illustration)
# ------------------------------------------------------------
def ex1_baseline_skewed_join(spark):
    print("\n[Ex1] Baseline: skewed inner join (this shape often causes a single huge task).")
    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)

    # Normal shuffle join
    joined = fact.join(dim, "k", "inner")
    print("Rows (fact):", fact.count(), "Rows (dim):", dim.count(), "Joined rows:", joined.count())

    # Show rough distribution of the skewed key in the big table
    skew_dist = fact.groupBy("k").agg(Fcount("*").alias("cnt")).orderBy(col("cnt").desc())
    skew_dist.show(5, truncate=False)


# ------------------------------------------------------------
# 2) Broadcast join (when the dimension is small)
# ------------------------------------------------------------
def ex2_broadcast_join(spark):
    print("\n[Ex2] Broadcast join: send the small table to all executors to avoid a big shuffle.")
    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)

    joined = fact.join(broadcast(dim), "k", "inner")
    print("Joined (broadcast) row count:", joined.count())

    # Note:
    # - Broadcasting avoids shuffling the big table on 'k'. Good when 'dim' is small (< few hundred MB).
    # - It also helps with skew because we don't hash-partition the hot key; each partition can probe locally.


# ------------------------------------------------------------
# 3) Salting technique (distribute the hot key across buckets)
# ------------------------------------------------------------
def ex3_salting(spark):
    print("\n[Ex3] Salting: split a hot key into S buckets to distribute processing across partitions.")
    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)

    S = 10  # number of salt buckets

    # SALT the big (skewed) side:
    # Create a deterministic salt for each row based on id (or any stable hash) mod S.
    fact_salted = (fact
                   .withColumn("salt", (col("id") % S).cast("int"))
                   .withColumn("k_salted",
                               when(col("k") == "hot", expr("concat(k, '#', salt)"))
                               .otherwise(col("k"))))

    # SALT the small side for hot key only:
    # replicate the 'hot' row S times with salts 0..S-1 -> explode(sequence(0, S-1))
    dim_hot = dim.filter(col("k") == "hot") \
                 .withColumn("salt", explode(sequence(lit(0), lit(S - 1)))) \
                 .withColumn("k_salted", expr("concat(k, '#', salt)")) \
                 .select("k_salted", "attr")

    # keep cold keys as-is (no salt)
    dim_cold = dim.filter(col("k") != "hot").withColumnRenamed("k", "k_salted")

    dim_salted = dim_hot.unionByName(dim_cold)

    # Join on salted key (k_salted)
    joined = fact_salted.join(dim_salted, "k_salted", "inner")
    print("Joined (salting) row count:", joined.count())

    # Validate distribution — hot is now spread across 10 salted keys
    dist = fact_salted.groupBy("k_salted").agg(Fcount("*").alias("cnt")).orderBy(col("cnt").desc())
    print("Top 5 most frequent salted keys after salting:")
    dist.show(5, truncate=False)

    # Notes:
    # - You can choose S based on observed skew (e.g., bytes or rows for the hot key).
    # - For multiple hot keys, maintain a list and salt only those keys.


# ------------------------------------------------------------
# 4) Repartitioning by key (control partitions to reduce skewed partition size)
# ------------------------------------------------------------
def ex4_repartition_by_key(spark):
    print("\n[Ex4] Repartition by join key: increase partitions to reduce per-partition size of hot key.")
    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)

    # Before join, force a higher number of partitions on the big side by key
    fact_rep = fact.repartition(200, col("k"))   # (tune this for your cluster/data)
    dim_rep = dim.repartition(200, col("k"))

    joined = fact_rep.join(dim_rep, "k", "inner")
    print("Joined (repartition by key) row count:", joined.count())

    # Note:
    # - This doesn't remove skew, but can help by spreading data across more buckets.
    # - Combine with salting for the hot key for best effect.


# ------------------------------------------------------------
# 5) AQE Skew Join (Adaptive Query Execution)
# ------------------------------------------------------------
def ex5_aqe_skew_join(spark):
    print("\n[Ex5] AQE Skew Join: let Spark split skewed partitions at runtime.")
    # Enable AQE + skew handling
    spark.conf.set("spark.sql.adaptive.enabled", "true")
    spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
    # Optional: thresholds (defaults are often fine; tune for your data)
    # spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", 64 * 1024 * 1024)
    # spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", 2)

    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)
    joined = fact.join(dim, "k", "inner")

    print("Joined (AQE skew join) row count:", joined.count())
    print("AQE enabled?:", spark.conf.get("spark.sql.adaptive.enabled"))
    print("AQE skew join enabled?:", spark.conf.get("spark.sql.adaptive.skewJoin.enabled"))

    # Note:
    # - With AQE on, Spark can detect an overly large post-shuffle partition for the hot key
    #   and split it into smaller sub-partitions dynamically, reducing straggler tasks.


# ------------------------------------------------------------
# 6) Pre-aggregation before join (reduce duplicates per key)
# ------------------------------------------------------------
def ex6_preaggregate_before_join(spark):
    print("\n[Ex6] Pre-aggregation: reduce duplicate rows per key before joining.")
    fact, dim = make_skewed_data(spark, big_rows=100_000, hot_ratio=0.9, num_cold_keys=20)

    # Imagine fact has many duplicates per (k). Pre-aggregate to reduce rows shuffled for 'hot'.
    fact_pre = fact.groupBy("k").agg(Fsum("v").alias("total_v"))  # <- fewer rows, one per key
    joined = fact_pre.join(dim, "k", "inner")

    print("Rows before pre-agg (fact):", fact.count(), "After pre-agg:", fact_pre.count())
    print("Joined (pre-agg) row count:", joined.count())
    joined.orderBy("k").show(10, truncate=False)

    # Notes:
    # - This reduces data volume for the hot key dramatically if there are many duplicates.
    # - You can also pre-aggregate on both sides (e.g., groupBy key on both tables) before the final join.


# ------------------------------------------------------------
# Main driver
# ------------------------------------------------------------
def main():
    spark = (SparkSession.builder
             .appName("SkewHandlingExamples")
             # For local runs; adjust shuffle partitions to keep demos quick/visible
             .config("spark.sql.shuffle.partitions", "200")
             .getOrCreate())

    # Run all examples
    ex1_baseline_skewed_join(spark)
    ex2_broadcast_join(spark)
    ex3_salting(spark)
    ex4_repartition_by_key(spark)
    ex5_aqe_skew_join(spark)
    ex6_preaggregate_before_join(spark)

    spark.stop()


if __name__ == "__main__":
    main()
