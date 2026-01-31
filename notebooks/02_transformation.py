# Databricks notebook source
# DBTITLE 1,Transform Ingested Data
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F


class GdeltSilverTransformer:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def transform(self, source_table: str, target_table: str):
        raw_df = self.spark.table(source_table)

        refined_df = raw_df.select(
            F.col("event_id").cast("string"),
            F.to_timestamp(F.col("date_int").cast("string"), "yyyyMMdd").alias(
                "event_date"
            ),
            F.col("quad_class").cast("int"),
            F.expr("try_cast(goldstein_scale as double)").alias("raw_goldstein"),
            F.expr("try_cast(num_mentions as int)").alias("num_mentions"),
            F.col("geo_fullname").cast("string").alias("location"),
            F.expr("try_cast(lat as double)").alias("lat"),
            F.expr("try_cast(long as double)").alias("long"),
            F.col("ingested_at"),
        )

        enriched_df = (
            refined_df.withColumn(
                "goldstein_scale",
                F.greatest(
                    F.lit(-10.0),
                    F.least(
                        F.lit(10.0), F.coalesce(F.col("raw_goldstein"), F.lit(0.0))
                    ),
                ),
            )
            .withColumn(
                "event_category",
                F.when(F.col("quad_class") == 1, "Verbal Cooperation")
                .when(F.col("quad_class") == 2, "Material Cooperation")
                .when(F.col("quad_class") == 3, "Verbal Conflict")
                .when(F.col("quad_class") == 4, "Material Conflict")
                .otherwise("Unknown"),
            )
            .withColumn(
                "conflict_intensity",
                (F.col("num_mentions") * (10.5 - F.col("goldstein_scale"))) / 100,
            )
            .drop("raw_goldstein")
        )

        enriched_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable(target_table)


transformer = GdeltSilverTransformer(spark)
transformer.transform("bronze_gdelt_events", "silver_gdelt_refined")
