# Databricks notebook source
df=spark.read.table("nyctaxi.01_bronze.yellow_trips_raw")


# COMMAND ----------

df=df.filter((df.tpep_pickup_datetime >= "2025-01-01") & (df.tpep_pickup_datetime <= "2025-10-31"))

df=df.filter((df.tpep_dropoff_datetime >= "2025-01-01") & (df.tpep_dropoff_datetime <= "2025-10-31"))


# COMMAND ----------

from pyspark.sql.functions import when, col,timestamp_diff

df = df.select(
    when(col("vendorID") == 1, "creative mobile technologies, LLc")
    .when(col("vendorID") == 2, "curb mobility,LLc")
    .when(col("vendorID") == 6, "mile Technologies, Inc")
    .when(col("vendorID") == 7, "Helix, Inc")
    .otherwise("unknown")
    .alias("vendor"),

"tpep_pickup_datetime",
"tpep_dropoff_datetime",
timestamp_diff('MINUTE',df.tpep_pickup_datetime ,df.tpep_dropoff_datetime).alias("trip_duration"),
"passenger_count",
"trip_distance",
when(col("RatecodeID") == 1, "Standard Rate")
.when(col("RatecodeID") == 2, "jfk")
    .when(col("RatecodeID") == 3, "newark")
    .when(col("RatecodeID") == 4, "nassau or westchester")
    .when(col("RatecodeID") == 5, "negotiated fare")
    .when(col("RatecodeID") == 6, "group ride")
    .otherwise("unknown")
    .alias("rate_type"),
"store_and_fwd_flag",    
col("PULocationID").alias("pu_location_id"),
col("DOLocationID").alias("do_location_id"),
when(col("payment_type") == 1, "credit card")
.when(col("payment_type") == 2, "cash")
.when(col("payment_type") == 3, "no charge")
.when(col("payment_type") == 4, "dispute")
.when(col("payment_type") == 5, "unknown")
.when(col("payment_type") == 6, "voided trip")
.otherwise("unknown")
.alias("payment_type"),
"fare_amount",
"extra",
"mta_tax",
"tip_amount",
"tolls_amount",
"improvement_surcharge",    
"total_amount",
"congestion_surcharge",
col("Airport_fee").alias("airport_fee"),
"cbd_congestion_fee",
"processed_timestamp"
)   


# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.yellow_trips_cleansed")