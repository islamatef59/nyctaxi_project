# Databricks notebook source
from pyspark.sql.functions import count, avg, max, min, sum, round


# COMMAND ----------

df=spark.read.table("nyctaxi.02_silver.yellow_trips_enriched")

# COMMAND ----------

df.display()

# COMMAND ----------

df=df.groupBy(df.tpep_pickup_datetime.cast("date").alias("pickup_date")).\
    agg(count("*").alias("total_trips"),
        round(avg(df.trip_distance)).alias("average_distance"),
        round(avg(df.fare_amount)).alias("average_fare"),
        round(avg(df.tip_amount)).alias("average_tip"),
        round(avg(df.passenger_count)).alias("avg_passenger"),
        max(df.fare_amount).alias("max_fare_per_trip"),
        min(df.fare_amount).alias("min_fare_per_trip"),
        round(sum("total_amount")).alias("total_amount")
    )
        

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.03_gold.daily_trip_summary")