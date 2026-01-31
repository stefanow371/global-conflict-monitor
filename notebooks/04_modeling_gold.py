# Databricks notebook source
# DBTITLE 1,Model Gold Version
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


class GdeltGoldModeler:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def run(self):
        silver_df = self.spark.table("silver_gdelt_refined")

        location_dim = silver_df.select(
            F.md5(F.col("location").cast("string")).alias("location_key"),
            F.col("location").alias("location_name"),
            F.col("lat"),
            F.col("long"),
        ).distinct()
        location_dim.write.format("delta").mode("overwrite").saveAsTable(
            "gold_dim_location"
        )

        event_dim = silver_df.select(
            F.md5(F.col("event_category").cast("string")).alias("event_type_key"),
            F.col("event_category"),
        ).distinct()
        event_dim.write.format("delta").mode("overwrite").saveAsTable(
            "gold_dim_event_types"
        )

        fact_df = silver_df.select(
            F.md5(F.col("event_id").cast("string")).alias("event_key"),
            F.md5(F.col("location").cast("string")).alias("location_key"),
            F.md5(F.col("event_category").cast("string")).alias("event_type_key"),
            F.col("event_date"),
            F.col("conflict_intensity"),
            F.col("num_mentions"),
            F.col("ingested_at"),
        )
        fact_df.write.format("delta").mode("overwrite").option(
            "overwriteSchema", "true"
        ).saveAsTable("gold_fact_conflicts")

        self.spark.sql(
            "OPTIMIZE gold_fact_conflicts ZORDER BY (event_date, location_key)"
        )
        self._deploy_views()

    def _deploy_views(self):
        self.spark.sql(
            """
            CREATE OR REPLACE VIEW gold_vw_alert_hotspots AS
            WITH SignalAnalytics AS (
                SELECT 
                    f.*, 
                    l.location_name,
                    l.lat,
                    l.long,
                    AVG(f.conflict_intensity) OVER (
                        PARTITION BY f.location_key 
                        ORDER BY f.event_date 
                        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                    ) as baseline_avg,
                    STDDEV(f.conflict_intensity) OVER (
                        PARTITION BY f.location_key 
                        ORDER BY f.event_date 
                        ROWS BETWEEN 7 PRECEDING AND 1 PRECEDING
                    ) as baseline_std
                FROM gold_fact_conflicts f
                JOIN gold_dim_location l ON f.location_key = l.location_key
            )
            SELECT *,
                CASE 
                    WHEN conflict_intensity > (baseline_avg + (2 * baseline_std)) THEN 1 
                    ELSE 0 
                END as is_anomaly
            FROM SignalAnalytics
            WHERE conflict_intensity > 0
        """
        )


if __name__ == "__main__":
    modeler = GdeltGoldModeler(spark)
    modeler.run()
