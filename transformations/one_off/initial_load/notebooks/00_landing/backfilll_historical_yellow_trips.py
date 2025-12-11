# Databricks notebook source
import urllib.request
import shutil
import os

# COMMAND ----------

date_to_process=['2025-01','2025-02','2025-03','2025-04','2025-05','2025-06','2025-07','2025-08','2025-09','2025-10']

for date in date_to_process:
    url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{date}.parquet"
    response=urllib.request.urlopen(url)

    dir_path=f"/Volumes/nyctaxi/00_landing/data_sources/nctaxi_yellow/{date}"
    os.makedirs(dir_path,exist_ok=True)
    local_path=f"{dir_path}/yellow_tripdata_{date}.parquet"
    with open(local_path, 'wb') as f:
      shutil.copyfileobj(response, f)