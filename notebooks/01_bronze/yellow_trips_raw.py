# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date



# COMMAND ----------

two_month_ago=date.today()-relativedelta(months=2)
formated_date=two_month_ago.strftime("%Y-%m")
df=spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nctaxi_yellow/{formated_date}")


# COMMAND ----------

df=df.withColumn("processed_timestamp",current_timestamp())
df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")