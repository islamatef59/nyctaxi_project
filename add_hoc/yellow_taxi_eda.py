# Databricks notebook source
df=spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

from pyspark.sql.functions import *

# COMMAND ----------

df.groupBy("vendor").agg(round(sum(df.total_amount),2).alias("total_revenue")).orderBy("total_revenue",acsend=True).display()

# COMMAND ----------

df.groupBy("pu_borough").agg(count("*").alias("number_of_trips")).orderBy("number_of_trips", ascending=False).display()


# COMMAND ----------

df.groupBy(concat("pu_borough",lit("->"),"do_borough").alias("Journy")).agg(count("*").alias("most_common_trips")).orderBy("most_common_trips", ascending=False).display()

# COMMAND ----------

spark.read.table("nyctaxi.03_gold.daily_trip_summary").display()