# Databricks notebook source

import urllib.request
import os
import shutil
import sys
project_root = os.path.abspath(os.path.join(os.getcwd(), "../.."))

if project_root not in sys.path:
    sys.path.append(project_root)
from modules.data_loader.file_downloader import download_file



# COMMAND ----------

try:
    #construct the csv file
    url = "https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"

    # Define and create the local directory for this date's data
    dir_path = f"/Volumes/nyctaxi/00_landing/data_sources/lookup"
    
    # Define the full path for the downloaded file
    local_path=f"{dir_path}/taxi_zone_lookup.csv"

    # Download the file
    download_file(url,dir_path,local_path)


    dbutils.jobs.taskValues.set(key="continue_downstream", value="yes")
    print("File succesfully uploaded")

except Exception as e:
    dbutils.jobs.taskValues.set(key="continue_downstream", value="no")
    print(f"File download failed: {str(e)}")
