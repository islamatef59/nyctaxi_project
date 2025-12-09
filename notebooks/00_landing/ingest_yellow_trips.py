# Databricks notebook source
import urllib.request
import os
import shutil
from datetime import datetime
from datetime import date, datetime, timezone
from dateutil.relativedelta import relativedelta

# COMMAND ----------

#get the year and month for the two month ago date
two_motn_ago=date.today()-relativedelta(months=2)
formatted_date=two_motn_ago.strftime("%Y-%m")
#obtain local bathr of date's data
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
        response = urllib.request.urlopen(url)
        os.makedirs(dir_path,exist_ok=True)
        # Open a local file with a binary writer
        with open(local_path, 'wb') as local_file:
            shutil.copyfileobj(response, local_file)
        # Set continue_downstream to yes
        dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
        print("File downloaded successfully")
    except Exception as e:
        # If there is an error, set continue_downstream to no dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
        print(f"Error downloading file: {e}")

