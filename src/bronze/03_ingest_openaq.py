# bronze layer - fetch openaq v3 nyc locations, daily measurements, and hourly sample
import json
import os
import time
import requests
from pyspark.sql import SparkSession, functions as F

spark = SparkSession.builder.getOrCreate()

CATALOG = "workspace"
LANDING_DIR = f"/Volumes/{CATALOG}/bronze/raw_landing/openaq"
NYC_BBOX = "-74.2591,40.4774,-73.7004,40.9176"
DATE_FROM = "2023-06-01"
DATE_TO = "2025-12-31"
HOURLY_WINDOWS = [
    ("2025-06-01T00:00:00Z", "2025-07-01T00:00:00Z"),
    ("2025-12-01T00:00:00Z", "2026-01-01T00:00:00Z"),
]
os.makedirs(LANDING_DIR, exist_ok=True)


def get_api_key() -> str:
    key = os.getenv("OPENAQ_API_KEY")
    if key:
        return key.strip()
    key_file = f"/Volumes/{CATALOG}/bronze/raw_landing/.openaq_key"
    if os.path.exists(key_file):
        return open(key_file).read().strip()
    raise ValueError(f"missing openaq api key: set OPENAQ_API_KEY or create {key_file}")


HEADERS = {"X-API-Key": get_api_key(), "User-Agent": "nyc-analytics-project"}


# paginated getter using openaq v3 page and limit (max 1000 per page)
def fetch_all_pages(url: str, params: dict, max_pages: int = 5) -> list:
    rows = []
    for page in range(1, max_pages + 1):
        query = {**params, "limit": 1000, "page": page}
        for attempt in range(3):
            resp = requests.get(url, headers=HEADERS, params=query, timeout=30)
            if resp.status_code == 429:
                time.sleep(2 * (attempt + 1))
                continue
            resp.raise_for_status()
            break
        batch = resp.json().get("results", [])
        rows.extend(batch)
        if len(batch) < 1000:
            break
        time.sleep(0.25)
    return rows


# 1. discover active nyc sensors spanning 2023-2025 for pm25, no2, o3
loc_resp = requests.get(
    "https://api.openaq.org/v3/locations",
    headers=HEADERS,
    params={"bbox": NYC_BBOX, "limit": 100},
    timeout=30,
)
loc_resp.raise_for_status()
locations_raw = loc_resp.json().get("results", [])

caps = {"pm25": 6, "no2": 4, "o3": 4}
picked = {"pm25": 0, "no2": 0, "o3": 0}
sensors = []

for loc in locations_raw:
    first_dt = (loc.get("datetimeFirst") or {}).get("utc", "9999")
    last_dt = (loc.get("datetimeLast") or {}).get("utc", "0000")
    coords = loc.get("coordinates") or {}
    if first_dt <= "2023-06-01" and last_dt >= "2025-12-01":
        for s in loc.get("sensors", []):
            param = (s.get("parameter") or {}).get("name")
            if param in caps and picked[param] < caps[param]:
                sensors.append({
                    "location_id": int(loc["id"]),
                    "location_name": str(loc.get("name", "")),
                    "latitude": float(coords["latitude"]) if coords.get("latitude") is not None else None,
                    "longitude": float(coords["longitude"]) if coords.get("longitude") is not None else None,
                    "sensor_id": int(s["id"]),
                    "parameter": str(param),
                    "units": str((s.get("parameter") or {}).get("units", "")),
                })
                picked[param] += 1

with open(f"{LANDING_DIR}/nyc_sensors.json", "w") as f:
    json.dump(sensors, f)

sensors_df = spark.createDataFrame(sensors).withColumn("_ingested_at", F.current_timestamp())
sensors_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_openaq_locations"
)


def to_float(val):
    return float(val) if val is not None else None


# 2. fetch daily measurements for all selected nyc sensors
daily_rows = []
for s in sensors:
    url = f"https://api.openaq.org/v3/sensors/{s['sensor_id']}/days"
    items = fetch_all_pages(url, {"date_from": DATE_FROM, "date_to": DATE_TO})
    for item in items:
        period = item.get("period") or {}
        summary = item.get("summary") or {}
        coverage = item.get("coverage") or {}
        daily_rows.append({
            "sensor_id": s["sensor_id"],
            "location_id": s["location_id"],
            "parameter": s["parameter"],
            "units": s["units"],
            "period_start_local": (period.get("datetimeFrom") or {}).get("local"),
            "period_start_utc": (period.get("datetimeFrom") or {}).get("utc"),
            "value": to_float(item.get("value")),
            "min_value": to_float(summary.get("min")),
            "max_value": to_float(summary.get("max")),
            "avg_value": to_float(summary.get("avg")),
            "observed_count": int(coverage.get("observedCount") or 0),
        })
    time.sleep(0.25)

with open(f"{LANDING_DIR}/nyc_daily_measurements.json", "w") as f:
    json.dump(daily_rows, f)

daily_df = spark.createDataFrame(daily_rows).withColumn("_ingested_at", F.current_timestamp())
daily_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_openaq_daily"
)

# 3. fetch hourly measurements for a subset of pm25 and no2 sensors (for time-of-day analysis)
hourly_sensors = [s for s in sensors if s["parameter"] == "pm25"][:4] + [
    s for s in sensors if s["parameter"] == "no2"
][:2]

hourly_rows = []
for s in hourly_sensors:
    url = f"https://api.openaq.org/v3/sensors/{s['sensor_id']}/hours"
    for dt_from, dt_to in HOURLY_WINDOWS:
        items = fetch_all_pages(url, {"datetime_from": dt_from, "datetime_to": dt_to})
        for item in items:
            period = item.get("period") or {}
            hourly_rows.append({
                "sensor_id": s["sensor_id"],
                "location_id": s["location_id"],
                "parameter": s["parameter"],
                "units": s["units"],
                "period_start_local": (period.get("datetimeFrom") or {}).get("local"),
                "period_start_utc": (period.get("datetimeFrom") or {}).get("utc"),
                "value": to_float(item.get("value")),
            })
        time.sleep(0.25)

hourly_df = spark.createDataFrame(hourly_rows).withColumn("_ingested_at", F.current_timestamp())
hourly_df.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(
    f"{CATALOG}.bronze.raw_openaq_hourly"
)

print(f"loaded sensors: {len(sensors)}, daily rows: {len(daily_rows)}, hourly rows: {len(hourly_rows)}")
