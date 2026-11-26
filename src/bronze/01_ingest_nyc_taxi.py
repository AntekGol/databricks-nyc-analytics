# bronze layer - download nyc yellow taxi parquet files and zone lookup csv
import os
import urllib.request
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "workspace"
MONTHS = ["2023-06", "2023-12", "2024-06", "2024-12", "2025-06", "2025-12"]
LANDING_DIR = f"/Volumes/{CATALOG}/bronze/raw_landing/nyc_taxi"
os.makedirs(LANDING_DIR, exist_ok=True)


def download(url: str, path: str) -> None:
    if os.path.exists(path):
        print(f"exists: {path}")
        return
    urllib.request.urlretrieve(url, path)
    print(f"downloaded: {path}")


# fetch zone lookup and monthly trip files into uc volume
zones_path = f"{LANDING_DIR}/taxi_zone_lookup.csv"
download("https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv", zones_path)

for m in MONTHS:
    url = f"https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_{m}.parquet"
    download(url, f"{LANDING_DIR}/yellow_tripdata_{m}.parquet")

# save taxi zones to bronze delta table
zones_df = spark.read.option("header", True).option("inferSchema", True).csv(zones_path)
zones_df.withColumn("_ingested_at", F.current_timestamp()).write.mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable(f"{CATALOG}.bronze.raw_taxi_zones")

# read monthly parquets separately so minor 2023-2025 schema drifts merge cleanly
dfs = []
for m in MONTHS:
    df = spark.read.parquet(f"{LANDING_DIR}/yellow_tripdata_{m}.parquet")
    df = df.toDF(*[c.lower() for c in df.columns])
    for c in ["vendorid", "pulocationid", "dolocationid", "payment_type"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("bigint"))
    for c in ["passenger_count", "ratecodeid", "airport_fee", "cbd_congestion_fee"]:
        if c in df.columns:
            df = df.withColumn(c, F.col(c).cast("double"))
    dfs.append(df.withColumn("_source_month", F.lit(m)).withColumn("_ingested_at", F.current_timestamp()))

trips_df = dfs[0]
for next_df in dfs[1:]:
    trips_df = trips_df.unionByName(next_df, allowMissingColumns=True)

trips_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_taxi_trips"
)
print(f"loaded zones: {zones_df.count()}, trips: {spark.table(f'{CATALOG}.bronze.raw_taxi_trips').count()}")
