# Databricks notebook source
import urllib.request
import shutil
import os

url="https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv"
response=urllib.request.urlopen(url)
dir_path="/Volumes/nyctaxi/00_landing/data_sources/lookup"
os.makedirs(dir_path, exist_ok=True)

with open(dir_path+"/taxi_zone_lookup.csv", 'wb') as out_file:
    shutil.copyfileobj(response, out_file)


# COMMAND ----------

