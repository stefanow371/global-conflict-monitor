# Databricks notebook source
# DBTITLE 1,Tests
from pyspark.sql import functions as F
from datetime import datetime


def run_data_quality_suite(table_name: str):
    df = spark.table(table_name)
    row_count = df.count()

    if row_count == 0:
        raise ValueError(f"Table {table_name} is empty")

    checks = {
        "null_event_id": df.filter(F.col("event_id").isNull()).count(),
        "null_keys": df.filter(
            F.col("location").isNull() | F.col("event_category").isNull()
        ).count(),
        "invalid_lat": df.filter(
            (F.col("lat") < -90) | (F.col("lat") > 90) | F.col("lat").isNull()
        ).count(),
        "invalid_long": df.filter(
            (F.col("long") < -180) | (F.col("long") > 180) | F.col("long").isNull()
        ).count(),
        "out_of_range_goldstein": df.filter(
            (F.col("goldstein_scale") < -10) | (F.col("goldstein_scale") > 10)
        ).count(),
        "negative_intensity": df.filter(F.col("conflict_intensity") < 0).count(),
        "future_events": df.filter(
            F.col("event_date") > F.lit(datetime.now().strftime("%Y-%m-%d"))
        ).count(),
    }

    critical_errors = [f"{k}: {v}" for k, v in checks.items() if v > 0]

    if critical_errors:
        raise ValueError(f"DQ Fail: {critical_errors}")

    return checks


if __name__ == "__main__":
    run_data_quality_suite("silver_gdelt_refined")
