# Databricks notebook source
# DBTITLE 1,Ingest GDELT telemetry
import requests
import zipfile
import io
import pandas as pd
from pyspark.sql.functions import current_timestamp, lit, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
)


class GdeltBronzeIngestor:
    def __init__(self, spark):
        self.spark = spark
        self.master_list_url = "http://data.gdeltproject.org/gdeltv2/lastupdate.txt"
        self.config = {
            "indices": [0, 1, 26, 27, 29, 30, 31, 51, 52, 56, 57],
            "names": [
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
            ],
        }

    def get_latest_url(self):
        response = requests.get(self.master_list_url)
        return response.text.split("\n")[0].split(" ")[2]

    def run(self, target_table="bronze_gdelt_events"):
        url = self.get_latest_url()
        file_name = url.split("/")[-1]

        resp = requests.get(url)
        with zipfile.ZipFile(io.BytesIO(resp.content)) as z:
            with z.open(z.namelist()[0]) as f:
                pdf = pd.read_csv(
                    f,
                    sep="\t",
                    header=None,
                    usecols=self.config["indices"],
                    names=self.config["names"],
                    dtype=str,
                )

        if pdf["lat"].isnull().all():
            raise ValueError("Data Load Error: Geographic coordinates missing")

        df_bronze = (
            self.spark.createDataFrame(pdf)
            .withColumn("ingested_at", current_timestamp())
            .withColumn("source_file", lit(file_name))
        )

        df_bronze.write.format("delta").mode("append").option(
            "mergeSchema", "true"
        ).saveAsTable(target_table)
        return file_name


if __name__ == "__main__":
    GdeltBronzeIngestor(spark).run()
