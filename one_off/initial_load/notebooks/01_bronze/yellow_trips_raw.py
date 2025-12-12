# Databricks notebook source
from pyspark.sql.functions import current_timestamp


# COMMAND ----------

df=spark.read.format("parquet").load("/Volumes/nyctaxi/00_landing/data_sources/nctaxi_yellow/*")


# COMMAND ----------

df=df.withColumn("processed_timestamp",current_timestamp())
df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")