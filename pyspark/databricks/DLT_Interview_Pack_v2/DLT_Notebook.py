
# Databricks notebook source
import dlt
from pyspark.sql.functions import *

@dlt.table(comment="Clean customer stream data")
def customers_cleaned():
    return spark.readStream.table("customers_raw").filter("email IS NOT NULL")

@dlt.table(comment="Aggregate orders by customer")
def orders_aggregated():
    return (
        spark.readStream.table("orders_raw")
        .groupBy("customer_id")
        .agg(sum("amount").alias("total_amount"))
    )
