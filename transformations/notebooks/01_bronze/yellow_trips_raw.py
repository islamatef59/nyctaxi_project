# Databricks notebook source
from pyspark.sql.functions import current_timestamp
from dateutil.relativedelta import relativedelta
from datetime import date
import os
import sys
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)
from modules.utils.date_utils import get_target_yyyymm
from modules.utils.date_utils import get_target_yyyymm
from modules.transformations.metadata import add_processed_timestamp



# COMMAND ----------

formatted_date=get_target_yyyymm(2)


df=spark.read.format("parquet").load(f"/Volumes/nyctaxi/00_landing/data_sources/nctaxi_yellow/{formated_date}")


# COMMAND ----------

df=add_processed_timestamp(df)
df.write.mode("overwrite").saveAsTable("nyctaxi.01_bronze.yellow_trips_raw")
