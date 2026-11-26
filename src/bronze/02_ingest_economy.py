# bronze layer - fetch world bank usa gdp (json) and ecb daily usd/eur fx (csv)
import json
import os
import requests
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "workspace"
LANDING_DIR = f"/Volumes/{CATALOG}/bronze/raw_landing/economy"
os.makedirs(LANDING_DIR, exist_ok=True)

# 1. world bank usa gdp (yearly, usd)
wb_url = "https://api.worldbank.org/v2/country/USA/indicator/NY.GDP.MKTP.CD?format=json&per_page=100"
wb_payload = requests.get(wb_url, timeout=30).json()

with open(f"{LANDING_DIR}/worldbank_gdp_usa.json", "w") as f:
    json.dump(wb_payload, f)

gdp_rows = [
    {
        "country_iso3": row.get("countryiso3code"),
        "country_name": (row.get("country") or {}).get("value"),
        "indicator_id": (row.get("indicator") or {}).get("id"),
        "year": int(row["date"]),
        "gdp_usd": float(row["value"]) if row.get("value") is not None else None,
    }
    for row in wb_payload[1]
    if row.get("date")
]

gdp_df = spark.createDataFrame(gdp_rows).withColumn("_ingested_at", F.current_timestamp())
gdp_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_worldbank_gdp"
)

# 2. ecb daily usd/eur exchange rate (csv)
ecb_url = "https://data-api.ecb.europa.eu/service/data/EXR/D.USD.EUR.SP00.A?format=csvdata&startPeriod=2022-12-01"
ecb_path = f"{LANDING_DIR}/ecb_fx_usd_eur.csv"
ecb_resp = requests.get(ecb_url, timeout=30)
ecb_resp.raise_for_status()
with open(ecb_path, "wb") as f:
    f.write(ecb_resp.content)

fx_df = (
    spark.read.option("header", True)
    .csv(ecb_path)
    .select(
        F.col("TIME_PERIOD").alias("rate_date"),
        F.col("OBS_VALUE").cast("double").alias("usd_per_eur"),
        F.col("CURRENCY").alias("currency"),
        F.col("CURRENCY_DENOM").alias("currency_denom"),
    )
    .withColumn("_ingested_at", F.current_timestamp())
)

fx_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_ecb_fx"
)

print(f"loaded gdp rows: {gdp_df.count()}, fx rows: {fx_df.count()}")
