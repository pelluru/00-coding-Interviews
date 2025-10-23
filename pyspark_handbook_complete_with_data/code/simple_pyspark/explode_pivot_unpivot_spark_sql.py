# pivot_unpivot_explode_sql_examples.py
# =====================================
# 10 examples that implement pivot, unpivot, and explode using *Spark SQL*
# with temporary views (temp tables). Each example:
#   - Builds a small DataFrame
#   - Registers it as a temp view via createOrReplaceTempView
#   - Runs spark.sql(...) showing the SQL-only approach
#
# Notes:
# - Spark SQL does not have a native PIVOT keyword in most versions.
#   We implement "pivot" via conditional aggregation:
#       SUM(CASE WHEN pivot_col = 'value' THEN metric ELSE 0 END) AS value_col
# - UNPIVOT can be done using SQL's stack() to reshape wide→long.
# - EXPLODE is done using LATERAL VIEW EXPLODE(...) (or posexplode / explode_outer).

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *


# ------------------------------------------------------------------------------
# Example 1: Basic pivot via conditional aggregation (doc-term matrix)
# ------------------------------------------------------------------------------
def ex1_pivot_sql_basic(spark):
    # Algorithm: group by doc_id, turn each word into a column using SUM(CASE ...)
    data = [(1, "spark"), (1, "spark"), (1, "aws"),
            (2, "python"), (2, "aws"), (2, "spark")]
    df = spark.createDataFrame(data, ["doc_id", "word"])
    df.createOrReplaceTempView("tokens")

    # discover small vocab (demo only; in practice, pass known list to avoid collecting big vocab)
    vocab = [r["word"] for r in df.select("word").distinct().collect()]

    # Build SQL with conditional sums dynamically
    exprs = ",\n       ".join(
        [f"SUM(CASE WHEN word = '{w}' THEN 1 ELSE 0 END) AS `{w}`" for w in sorted(vocab)]
    )
    sql = f"""
        SELECT doc_id,
               {exprs}
        FROM tokens
        GROUP BY doc_id
        ORDER BY doc_id
    """
    print("\n[Ex1] Pivot via SQL (conditional aggregation)")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 2: Pivot with multiple aggregations (sum + avg) using conditional agg
# ------------------------------------------------------------------------------
def ex2_pivot_sql_multi_aggs(spark):
    # Algorithm: group by dept; for each month produce sum and avg columns
    data = [("HR", "Jan", 1000), ("HR", "Feb", 1500), ("IT", "Jan", 2000),
            ("IT", "Feb", 1800), ("IT", "Mar", 2100)]
    df = spark.createDataFrame(data, ["dept", "month", "sales"])
    df.createOrReplaceTempView("sales")

    months = [r["month"] for r in df.select("month").distinct().collect()]
    parts = []
    for m in sorted(months):
        parts.append(f"SUM(CASE WHEN month = '{m}' THEN sales ELSE 0 END) AS `{m}_sum`")
        parts.append(f"AVG(CASE WHEN month = '{m}' THEN sales END)      AS `{m}_avg`")
    sql = f"""
        SELECT dept,
               {", ".join(parts)}
        FROM sales
        GROUP BY dept
        ORDER BY dept
    """
    print("\n[Ex2] Pivot (sum + avg) via SQL conditionals")
    spark.sql(sql).show(truncate=False)


# ------------------------------------------------------------------------------
# Example 3: Multi-key pivot (city, dept) → months as columns (sum sales)
# ------------------------------------------------------------------------------
def ex3_pivot_sql_multi_keys(spark):
    data = [("NY", "HR", "Jan", 10), ("NY", "HR", "Feb", 15),
            ("NY", "IT", "Jan", 20), ("SF", "IT", "Jan", 25),
            ("SF", "IT", "Feb", 18)]
    df = spark.createDataFrame(data, ["city", "dept", "month", "sales"])
    df.createOrReplaceTempView("city_sales")

    months = [r["month"] for r in df.select("month").distinct().collect()]
    sel = ",\n       ".join(
        [f"SUM(CASE WHEN month = '{m}' THEN sales ELSE 0 END) AS `{m}`" for m in sorted(months)]
    )
    sql = f"""
        SELECT city, dept,
               {sel}
        FROM city_sales
        GROUP BY city, dept
        ORDER BY city, dept
    """
    print("\n[Ex3] Pivot with multiple grouping keys via SQL")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 4: Pivot then derive new metric (post-processing in SQL)
# ------------------------------------------------------------------------------
def ex4_pivot_sql_with_derived(spark):
    data = [("A", "Jan", 5), ("A", "Feb", 7), ("A", "Mar", 1),
            ("B", "Jan", 3), ("B", "Mar", 4)]
    df = spark.createDataFrame(data, ["product", "month", "units"])
    df.createOrReplaceTempView("prod")

    sql = """
        WITH base AS (
          SELECT product,
                 SUM(CASE WHEN month='Jan' THEN units ELSE 0 END) AS Jan_units,
                 SUM(CASE WHEN month='Feb' THEN units ELSE 0 END) AS Feb_units,
                 SUM(CASE WHEN month='Mar' THEN units ELSE 0 END) AS Mar_units
          FROM prod
          GROUP BY product
        )
        SELECT *,
               (Jan_units + Feb_units + Mar_units) AS Q1_total
        FROM base
        ORDER BY product
    """
    print("\n[Ex4] Pivot + derived metric in SQL")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 5: Filter before pivot (reduce cardinality / cost)
# ------------------------------------------------------------------------------
def ex5_pivot_sql_with_prefilter(spark):
    data = [(1, "w1", 2), (1, "w2", 1), (1, "w3", 1),
            (2, "w1", 1), (2, "w4", 10), (2, "w5", 1)]
    df = spark.createDataFrame(data, ["doc_id", "word", "cnt"])
    df.createOrReplaceTempView("wc")

    # Keep only frequent words (cnt >= 2) before pivoting
    sql = """
        WITH filtered AS (
          SELECT * FROM wc WHERE cnt >= 2
        ),
        agg AS (
          SELECT doc_id, word, SUM(cnt) AS cnt
          FROM filtered
          GROUP BY doc_id, word
        )
        SELECT
          doc_id,
          SUM(CASE WHEN word = 'w1' THEN cnt ELSE 0 END) AS w1,
          SUM(CASE WHEN word = 'w4' THEN cnt ELSE 0 END) AS w4
        FROM agg
        GROUP BY doc_id
        ORDER BY doc_id
    """
    print("\n[Ex5] Prefilter + pivot via SQL")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 6: UNPIVOT with stack() — known columns (wide → long)
# ------------------------------------------------------------------------------
def ex6_unpivot_sql_stack(spark):
    data = [("HR", 1000, 1500, 0), ("IT", 2000, 1800, 2100)]
    df = spark.createDataFrame(data, ["dept", "Jan", "Feb", "Mar"])
    df.createOrReplaceTempView("dept_months")

    # Algorithm: stack(N, 'colName1', col1, 'colName2', col2, ...) → rows (name, value)
    sql = """
        SELECT dept, m AS month, v AS sales
        FROM (
          SELECT dept, Jan, Feb, Mar FROM dept_months
        )
        LATERAL VIEW STACK(3,
            'Jan', Jan,
            'Feb', Feb,
            'Mar', Mar
        ) s AS m, v
        ORDER BY dept, month
    """
    print("\n[Ex6] UNPIVOT with stack() in SQL")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 7: Generic UNPIVOT (unknown columns) via arrays + explode in SQL
# ------------------------------------------------------------------------------
def ex7_unpivot_sql_generic(spark):
    data = [("HR", 1000, 1500, None), ("IT", 2000, 1800, 2100)]
    df = spark.createDataFrame(data, ["dept", "Jan", "Feb", "Mar"])
    df.createOrReplaceTempView("dept_months2")

    # Algorithm:
    # - Build two arrays: (names), (values)
    # - Use arrays_zip(names, values) → array<struct<col_name: string, col_val: ...>>
    # - LATERAL VIEW EXPLODE to rows
    sql = """
        WITH base AS (
          SELECT
            dept,
            array('Jan','Feb','Mar') AS name_arr,
            array(Jan,  Feb,  Mar )  AS val_arr
          FROM dept_months2
        )
        SELECT dept,
               kv.col_name AS month,
               kv.col_val  AS sales
        FROM base
        LATERAL VIEW EXPLODE(arrays_zip(name_arr, val_arr)) s AS kv
        ORDER BY dept, month
    """
    print("\n[Ex7] Generic UNPIVOT with arrays_zip + explode (SQL)")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 8: EXPLODE words from text using LATERAL VIEW EXPLODE + COUNT
# ------------------------------------------------------------------------------
def ex8_explode_sql_wordcount(spark):
    data = [("doc1", "spark is fast"),
            ("doc2", "spark is powerful"),
            ("doc3", "spark runs on aws")]
    df = spark.createDataFrame(data, ["doc_id", "text"])
    df.createOrReplaceTempView("docs")

    sql = r"""
        WITH tok AS (
          SELECT doc_id, SPLIT(text, '\s+') AS tokens
          FROM docs
        ),
        words AS (
          SELECT doc_id, w AS word
          FROM tok
          LATERAL VIEW EXPLODE(tokens) t AS w
        )
        SELECT word, COUNT(*) AS cnt
        FROM words
        GROUP BY word
        ORDER BY cnt DESC, word
    """
    print("\n[Ex8] EXPLODE tokens + COUNT (SQL)")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 9: POSEXPLODE to keep positions, then filter & aggregate
# ------------------------------------------------------------------------------
def ex9_posexplode_sql_positions(spark):
    data = [("doc1", "a b c d e f"), ("doc2", "x y z")]
    df = spark.createDataFrame(data, ["doc_id", "text"])
    df.createOrReplaceTempView("docs2")

    sql = r"""
        WITH tok AS (
          SELECT doc_id, SPLIT(text, '\s+') AS tokens
          FROM docs2
        ),
        pos AS (
          SELECT doc_id, pos, word
          FROM tok
          LATERAL VIEW POSEXPLODE(tokens) t AS pos, word
        ),
        even_only AS (
          SELECT word FROM pos WHERE (pos % 2) = 0
        )
        SELECT word, COUNT(*) AS cnt
        FROM even_only
        GROUP BY word
        ORDER BY cnt DESC, word
    """
    print("\n[Ex9] POSEXPLODE (keep positions) → filter → aggregate (SQL)")
    spark.sql(sql).show()


# ------------------------------------------------------------------------------
# Example 10: Map explode → pivot (round trip) fully in SQL
# ------------------------------------------------------------------------------
def ex10_map_explode_sql_and_pivot(spark):
    data = [("HR", ["Jan","Feb","Mar"], [1000,1500,0]),
            ("IT", ["Jan","Mar","Apr"], [2000,2100,50])]
    df = spark.createDataFrame(data, ["dept", "months", "values"])
    df.createOrReplaceTempView("mv")

    # Build a map from arrays in SQL using map_from_arrays, then explode to (k,v) rows
    # and pivot back with conditional sums.
    sql = """
        WITH kv AS (
          SELECT dept, k AS month, v AS sales
          FROM (
            SELECT dept, map_from_arrays(months, values) AS m
            FROM mv
          )
          LATERAL VIEW EXPLODE(m) e AS k, v
        )
        SELECT dept,
               SUM(CASE WHEN month='Apr' THEN sales ELSE 0 END) AS Apr,
               SUM(CASE WHEN month='Jan' THEN sales ELSE 0 END) AS Jan,
               SUM(CASE WHEN month='Feb' THEN sales ELSE 0 END) AS Feb,
               SUM(CASE WHEN month='Mar' THEN sales ELSE 0 END) AS Mar
        FROM kv
        GROUP BY dept
        ORDER BY dept
    """
    print("\n[Ex10] Map explode → conditional-agg pivot (SQL)")
    spark.sql(sql).show()


def main():
    spark = (SparkSession.builder
             .appName("PivotUnpivotExplodeSQLTempTables")
             .getOrCreate())

    ex1_pivot_sql_basic(spark)
    ex2_pivot_sql_multi_aggs(spark)
    ex3_pivot_sql_multi_keys(spark)
    ex4_pivot_sql_with_derived(spark)
    ex5_pivot_sql_with_prefilter(spark)
    ex6_unpivot_sql_stack(spark)
    ex7_unpivot_sql_generic(spark)
    ex8_explode_sql_wordcount(spark)
    ex9_posexplode_sql_positions(spark)
    ex10_map_explode_sql_and_pivot(spark)

    spark.stop()


if __name__ == "__main__":
    main()
