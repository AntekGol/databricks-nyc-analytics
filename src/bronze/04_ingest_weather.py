# bronze layer - fetch daily nyc historical weather from open-meteo archive api
import json
import os
import requests
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "workspace"
LANDING_DIR = f"/Volumes/{CATALOG}/bronze/raw_landing/weather"
os.makedirs(LANDING_DIR, exist_ok=True)

url = "https://archive-api.open-meteo.com/v1/archive"
params = {
    "latitude": 40.7128,
    "longitude": -74.0060,
    "start_date": "2023-06-01",
    "end_date": "2025-12-31",
    "daily": ",".join([
        "temperature_2m_mean",
        "temperature_2m_max",
        "temperature_2m_min",
        "precipitation_sum",
        "rain_sum",
        "snowfall_sum",
        "wind_speed_10m_max",
    ]),
    "timezone": "America/New_York",
}

resp = requests.get(url, params=params, timeout=30)
resp.raise_for_status()
payload = resp.json()

with open(f"{LANDING_DIR}/nyc_weather_daily.json", "w") as f:
    json.dump(payload, f)

daily = payload["daily"]
rows = []
for i, day in enumerate(daily["time"]):
    rows.append({
        "weather_date": day,
        "temp_mean_c": float(daily["temperature_2m_mean"][i]) if daily["temperature_2m_mean"][i] is not None else None,
        "temp_max_c": float(daily["temperature_2m_max"][i]) if daily["temperature_2m_max"][i] is not None else None,
        "temp_min_c": float(daily["temperature_2m_min"][i]) if daily["temperature_2m_min"][i] is not None else None,
        "precipitation_mm": float(daily["precipitation_sum"][i]) if daily["precipitation_sum"][i] is not None else None,
        "rain_mm": float(daily["rain_sum"][i]) if daily["rain_sum"][i] is not None else None,
        "snowfall_cm": float(daily["snowfall_sum"][i]) if daily["snowfall_sum"][i] is not None else None,
        "wind_max_kmh": float(daily["wind_speed_10m_max"][i]) if daily["wind_speed_10m_max"][i] is not None else None,
    })

weather_df = spark.createDataFrame(rows).withColumn("_ingested_at", F.current_timestamp())
weather_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_weather_daily"
)

print(f"loaded weather rows: {weather_df.count()}")
