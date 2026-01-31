# Databricks notebook source
# DBTITLE 1,Ingest GDELT telemetry
import requests
import zipfile
import io
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit


class GdeltBronzeIngestor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.master_list_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.column_indices = [0, 1, 26, 27, 30, 31, 34, 39, 40, 52, 53]
        self.column_names = [
            "event_id",
            "date_int",
            "event_code",
            "event_base_code",
            "quad_class",
            "goldstein_scale",
            "num_mentions",
            "geo_type",
            "geo_fullname",
            "lat",
            "long",
        ]

    def _get_latest_url(self) -> str:
        response = requests.get(self.master_list_url)
        response.raise_for_status()
        return response.text.split("\n")[0].split(" ")[2]

    def run(self, target_table: str = "bronze_gdelt_events"):
        url = self._get_latest_url()
        file_name = url.split("/")[-1]

        response = requests.get(url)
        with zipfile.ZipFile(io.BytesIO(response.content)) as z:
            with z.open(z.namelist()[0]) as f:
                pdf = pd.read_csv(
                    f,
                    sep="\t",
                    header=None,
                    usecols=self.column_indices,
                    names=self.column_names,
                )

        df = (
            self.spark.createDataFrame(pdf)
            .withColumn("ingested_at", current_timestamp())
            .withColumn("source_file", lit(file_name))
        )

        df.write.mode("append").format("delta").option(
            "mergeSchema", "true"
        ).saveAsTable(target_table)


ingestor = GdeltBronzeIngestor(spark)
ingestor.run()
