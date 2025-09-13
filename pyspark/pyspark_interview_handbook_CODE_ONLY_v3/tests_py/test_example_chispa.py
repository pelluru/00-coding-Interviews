import pytest
from chispa import assert_df_equality
from pyspark.sql import SparkSession, functions as F

@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.appName("tests").getOrCreate()

def test_example(spark):
    actual = spark.createDataFrame([(1,),(2,)], ["id"]).withColumn("x", F.lit(1))
    expected = spark.createDataFrame([(1,1),(2,1)], ["id","x"])
    assert_df_equality(actual, expected, ignore_row_order=True, ignore_nullable=True)
