# silver layer - clean, deduplicate, and standardize nyc taxi zones and trips
import os
import sys
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd())
from occ_utils import upsert_delta_table

spark = SparkSession.builder.getOrCreate()
CATALOG = "workspace"

# 1. taxi zones lookup
zones_df = (
    spark.table(f"{CATALOG}.bronze.raw_taxi_zones")
    .select(
        F.col("LocationID").cast("bigint").alias("location_id"),
        F.trim(F.col("Borough")).alias("borough"),
        F.trim(F.col("Zone")).alias("zone_name"),
        F.trim(F.col("service_zone")).alias("service_zone"),
    )
    .filter(F.col("location_id").isNotNull())
    .dropDuplicates(["location_id"])
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_delta_table(
    spark,
    zones_df,
    f"{CATALOG}.silver.taxi_zones",
    merge_condition="t.location_id = s.location_id",
)

# 2. nyc yellow taxi trips - filter bad meter dates/outliers and enrich time fields
trips_df = (
    spark.table(f"{CATALOG}.bronze.raw_taxi_trips")
    .withColumn("pickup_ts", F.col("tpep_pickup_datetime").cast("timestamp"))
    .withColumn("dropoff_ts", F.col("tpep_dropoff_datetime").cast("timestamp"))
    .withColumn("pickup_year_month", F.date_format("pickup_ts", "yyyy-MM"))
    .withColumn(
        "trip_duration_min",
        (F.unix_timestamp("dropoff_ts") - F.unix_timestamp("pickup_ts")) / 60.0,
    )
    .filter(F.col("pickup_ts").isNotNull() & F.col("dropoff_ts").isNotNull())
    .filter(F.col("pickup_year_month") == F.col("_source_month"))
    .filter((F.col("trip_duration_min") > 0) & (F.col("trip_duration_min") <= 360))
    .filter((F.col("trip_distance") > 0) & (F.col("trip_distance") <= 200))
    .filter((F.col("fare_amount") > 0) & (F.col("total_amount") > 0))
    .filter(F.col("pulocationid").isNotNull())
    .select(
        F.sha2(
            F.concat_ws(
                "|",
                F.col("vendorid").cast("string"),
                F.col("pickup_ts").cast("string"),
                F.col("dropoff_ts").cast("string"),
                F.col("pulocationid").cast("string"),
                F.col("dolocationid").cast("string"),
                F.col("total_amount").cast("string"),
            ),
            256,
        ).alias("trip_id"),
        F.col("vendorid").alias("vendor_id"),
        F.col("pickup_ts"),
        F.col("dropoff_ts"),
        F.to_date("pickup_ts").alias("pickup_date"),
        F.hour("pickup_ts").alias("pickup_hour"),
        F.col("pickup_year_month"),
        F.col("pulocationid").alias("pu_location_id"),
        F.col("dolocationid").alias("do_location_id"),
        F.coalesce(F.col("passenger_count"), F.lit(1.0)).alias("passenger_count"),
        F.col("trip_distance"),
        F.col("fare_amount"),
        F.col("tip_amount"),
        F.col("total_amount"),
        F.round("trip_duration_min", 2).alias("trip_duration_min"),
        F.current_timestamp().alias("_updated_at"),
    )
    .dropDuplicates(["trip_id"])
)

# partition match enables partition pruning and reduces occ file conflicts
upsert_delta_table(
    spark,
    trips_df,
    f"{CATALOG}.silver.taxi_trips",
    merge_condition="t.pickup_year_month = s.pickup_year_month AND t.trip_id = s.trip_id",
    partition_col="pickup_year_month",
)

print(
    f"silver zones: {spark.table(f'{CATALOG}.silver.taxi_zones').count()}, "
    f"silver trips: {spark.table(f'{CATALOG}.silver.taxi_trips').count()}"
)
