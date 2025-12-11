# Databricks notebook source
from pyspark.sql.functions import lit,col,current_timestamp
from pyspark.sql.types import TimestampType,IntegerType,StringType
from datetime import datetime
from delta.tables import DeltaTable


# COMMAND ----------

df=spark.read.format("csv").options(header="true").load("/Volumes/nyctaxi/00_landing/data_sources/lookup/taxi_zone_lookup.csv")



# COMMAND ----------

df=df.select(
    col("LocationID").cast(IntegerType()).alias("location_id"),
    col("Borough").alias("borough"),
    col("Zone").alias("zone"),
    col("service_zone").alias("service_zone"),
    current_timestamp().alias("effective_date"),
    lit(None).cast(TimestampType()).alias("end_date")

    )
    

# COMMAND ----------

from datetime import datetime


end_timestamp = datetime.now()

# Load the SCD2 Delta table
dt = DeltaTable.forName(spark, "nyctaxi.02_silver.taxi_zone_lookup")

#Identify active records that changed
# Compares old and new records
# Finds active records that have changed
# Closes those records by updating end_date
# Performs one atomic Delta Lake transaction
dt.alias("t").\
    merge(
        source    = df.alias("s"),
        condition = "t.location_id = s.location_id AND t.end_date IS NULL AND (t.borough != s.borough OR t.zone != s.zone OR t.service_zone != s.service_zone)"
    ).\
    whenMatchedUpdate(
        set = { "t.end_date": lit(end_timestamp).cast(TimestampType()) }
    ).\
    execute()

insert_id_list = [row.location_id for row in dt.toDF().filter(f"end_date = '{end_timestamp}' ").select("location_id").collect()]



# COMMAND ----------

# If the list is empty, don't try to insert anything
if len(insert_id_list) == 0:
    print("No updated records to insert")
else:
    dt.alias("t").\
        merge(
            source    = df.alias("s"),
            condition = f"s.location_id not in ({', '.join(map(str, insert_id_list))})"
        ).\
        whenNotMatchedInsert(
            values = { "t.location_id": "s.location_id",
                    "t.borough": "s.borough",
                    "t.zone": "s.zone",
                    "t.service_zone": "s.service_zone",
                    "t.effective_date": current_timestamp(),
                    "t.end_date": lit(None).cast(TimestampType()) }
        ).\
        execute()



# COMMAND ----------

dt.alias("t").\
    merge(
        source    = df.alias("s"),
        condition = "t.location_id = s.location_id"
    ).\
    whenNotMatchedInsert(
        values = { "t.location_id": "s.location_id",
                "t.borough": "s.borough",
                "t.zone": "s.zone",
                "t.service_zone": "s.service_zone",
                "t.effective_date": current_timestamp(),
                "t.end_date": lit(None).cast(TimestampType()) }
    ).\
    execute()  

# COMMAND ----------

df.write.mode("overwrite").saveAsTable("nyctaxi.02_silver.taxi_zone_lookup")