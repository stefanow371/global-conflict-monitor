# Databricks notebook source
# DBTITLE 1,Tests
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def run_data_quality_suite(table_name: str):
    df = spark.table(table_name)

    results = {
        "null_event_id": df.filter(F.col("event_id").isNull()).count(),
        "out_of_range_goldstein": df.filter(
            (F.col("goldstein_scale") < -10) | (F.col("goldstein_scale") > 10)
        ).count(),
    }

    if results["null_event_id"] > 0:
        raise ValueError(
            f"Data Quality Check Failed: Found NULLs in event_id in {table_name}"
        )

    return results


run_data_quality_suite("silver_gdelt_refined")
