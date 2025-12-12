# Databricks notebook source
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)
import urllib.request

import os
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta
from modules.utils.date_utils import get_target_yyyymm
from modules.data_loader.file_downloader import download_file

# COMMAND ----------
formatted_date=get_target_yyyymm(2)

#obtain local path of date's data
dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/nyctaxi_yellow/{formatted_date}"
local_path=f"{dir_path}/yellow_tripdata_{formatted_date}.parquet"

try: 
    # Check if the file already exists
    dbutils.fs.ls(local_path)
    # If the file already exists then set continue_downstream to no
    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File already downloaded, aborting downstream tasks")
except:
    try:
        url=f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{formatted_date}.parquet"
        # Open a connection and stream the remote file
        download_file(url,dir_path,local_path)
        # Set continue_downstream to yes
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File downloaded successfully")
    except Exception as e:
        # If there is an error, set continue_downstream to no dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"Error downloading file: {e}")

