# silver layer - clean openaq nyc daily and hourly measurements and map boroughs
import os
import sys
from pyspark.sql import SparkSession, functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd())
from occ_utils import upsert_delta_table

spark = SparkSession.builder.getOrCreate()
CATALOG = "workspace"

# map openaq nyc monitoring stations to boroughs
loc_df = (
    spark.table(f"{CATALOG}.bronze.raw_openaq_locations")
    .withColumn(
        "borough",
        F.when(F.lower("location_name").rlike("manhattan|ccny"), F.lit("Manhattan"))
        .when(F.lower("location_name").rlike("bronx|pfizer"), F.lit("Bronx"))
        .when(F.lower("location_name").rlike("queens|maspeth"), F.lit("Queens"))
        .when(F.lower("location_name").rlike("bklyn|brooklyn"), F.lit("Brooklyn"))
        .when(F.lower("location_name").rlike("staten"), F.lit("Staten Island"))
        .otherwise(F.lit("NYC Metro (NJ)")),
    )
    .select("sensor_id", "location_id", "location_name", "borough", "latitude", "longitude")
    .dropDuplicates(["sensor_id"])
)

# 1. daily air quality measurements (use local nyc date from iso string)
daily_df = (
    spark.table(f"{CATALOG}.bronze.raw_openaq_daily")
    .withColumn("measurement_date", F.to_date(F.substring("period_start_local", 1, 10)))
    .withColumn("measurement_year_month", F.date_format("measurement_date", "yyyy-MM"))
    .filter(F.col("measurement_date").isNotNull() & F.col("value").isNotNull() & (F.col("value") >= 0))
    .join(loc_df, on=["sensor_id", "location_id"], how="left")
    .select(
        "sensor_id",
        "location_id",
        "location_name",
        F.coalesce("borough", F.lit("Unknown")).alias("borough"),
        "latitude",
        "longitude",
        F.lower(F.trim("parameter")).alias("parameter"),
        "units",
        "measurement_date",
        "measurement_year_month",
        F.col("value").alias("daily_value"),
        "min_value",
        "max_value",
        "avg_value",
        "observed_count",
        F.current_timestamp().alias("_updated_at"),
    )
    .dropDuplicates(["sensor_id", "measurement_date", "parameter"])
)

upsert_delta_table(
    spark,
    daily_df,
    f"{CATALOG}.silver.air_quality_daily",
    merge_condition=(
        "t.measurement_year_month = s.measurement_year_month "
        "AND t.sensor_id = s.sensor_id "
        "AND t.measurement_date = s.measurement_date "
        "AND t.parameter = s.parameter"
    ),
    partition_col="measurement_year_month",
)

# 2. hourly air quality sample for time-of-day correlation
hourly_df = (
    spark.table(f"{CATALOG}.bronze.raw_openaq_hourly")
    .withColumn("measurement_date", F.to_date(F.substring("period_start_local", 1, 10)))
    .withColumn("measurement_hour", F.substring("period_start_local", 12, 2).cast("int"))
    .filter(
        F.col("measurement_date").isNotNull()
        & F.col("measurement_hour").between(0, 23)
        & F.col("value").isNotNull()
        & (F.col("value") >= 0)
    )
    .join(loc_df, on=["sensor_id", "location_id"], how="left")
    .select(
        "sensor_id",
        "location_id",
        "location_name",
        F.coalesce("borough", F.lit("Unknown")).alias("borough"),
        F.lower(F.trim("parameter")).alias("parameter"),
        "units",
        "measurement_date",
        "measurement_hour",
        F.col("value").alias("hourly_value"),
        F.current_timestamp().alias("_updated_at"),
    )
    .dropDuplicates(["sensor_id", "measurement_date", "measurement_hour", "parameter"])
)

upsert_delta_table(
    spark,
    hourly_df,
    f"{CATALOG}.silver.air_quality_hourly",
    merge_condition=(
        "t.sensor_id = s.sensor_id "
        "AND t.measurement_date = s.measurement_date "
        "AND t.measurement_hour = s.measurement_hour "
        "AND t.parameter = s.parameter"
    ),
)

print(
    f"silver aq daily: {spark.table(f'{CATALOG}.silver.air_quality_daily').count()}, "
    f"hourly: {spark.table(f'{CATALOG}.silver.air_quality_hourly').count()}"
)
