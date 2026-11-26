# external scheduled job syncing enriched nyc weather from databricks gold into timescaledb
import argparse
from datetime import datetime, timezone
import os
from apscheduler.schedulers.blocking import BlockingScheduler
from databricks import sql as dbsql
from dotenv import load_dotenv
import psycopg

load_dotenv()

UPSERT_SQL = """
INSERT INTO enriched_weather_daily (
    weather_date, temp_mean_c, temp_max_c, temp_min_c,
    precipitation_mm, rain_mm, snowfall_cm, wind_max_kmh,
    weather_condition, total_trips, total_revenue_usd, total_revenue_eur,
    avg_revenue_per_trip_usd, pm25_avg, no2_avg, o3_avg, synced_at
)
VALUES (
    %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW()
)
ON CONFLICT (weather_date) DO UPDATE SET
    temp_mean_c = EXCLUDED.temp_mean_c,
    temp_max_c = EXCLUDED.temp_max_c,
    temp_min_c = EXCLUDED.temp_min_c,
    precipitation_mm = EXCLUDED.precipitation_mm,
    rain_mm = EXCLUDED.rain_mm,
    snowfall_cm = EXCLUDED.snowfall_cm,
    wind_max_kmh = EXCLUDED.wind_max_kmh,
    weather_condition = EXCLUDED.weather_condition,
    total_trips = EXCLUDED.total_trips,
    total_revenue_usd = EXCLUDED.total_revenue_usd,
    total_revenue_eur = EXCLUDED.total_revenue_eur,
    avg_revenue_per_trip_usd = EXCLUDED.avg_revenue_per_trip_usd,
    pm25_avg = EXCLUDED.pm25_avg,
    no2_avg = EXCLUDED.no2_avg,
    o3_avg = EXCLUDED.o3_avg,
    synced_at = NOW();
"""


def fetch_enriched_weather() -> list[tuple]:
    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    query = f"""
        SELECT
            weather_date, temp_mean_c, temp_max_c, temp_min_c,
            precipitation_mm, rain_mm, snowfall_cm, wind_max_kmh,
            weather_condition, total_trips, total_revenue_usd, total_revenue_eur,
            avg_revenue_per_trip_usd, pm25_avg, no2_avg, o3_avg
        FROM {catalog}.gold.fact_weather_enriched_daily
        ORDER BY weather_date
    """
    host = os.environ["DATABRICKS_SERVER_HOSTNAME"].removeprefix("https://").removeprefix("http://").rstrip("/")
    with dbsql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(query)
            return [tuple(r) for r in cur.fetchall()]


def upsert_to_timescaledb(rows: list[tuple]) -> int:
    conninfo = (
        f"host={os.getenv('TSDB_HOST', 'localhost')} "
        f"port={os.getenv('TSDB_PORT', '5432')} "
        f"dbname={os.getenv('TSDB_DB', 'weather_tsdb')} "
        f"user={os.getenv('TSDB_USER', 'postgres')} "
        f"password={os.getenv('TSDB_PASSWORD', 'postgres')}"
    )
    with psycopg.connect(conninfo) as conn:
        with conn.cursor() as cur:
            cur.executemany(UPSERT_SQL, rows)
        conn.commit()
    return len(rows)


def run_sync() -> None:
    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    rows = fetch_enriched_weather()
    count = upsert_to_timescaledb(rows)
    print(f"[{started}] synced {count} enriched weather rows into timescaledb")


def main() -> None:
    parser = argparse.ArgumentParser(description="sync enriched nyc weather to timescaledb")
    parser.add_argument("--once", action="store_true", help="run once and exit")
    args = parser.parse_args()

    run_sync()
    if args.once:
        return

    interval_min = int(os.getenv("WEATHER_SYNC_INTERVAL_MINUTES", "60"))
    scheduler = BlockingScheduler()
    scheduler.add_job(run_sync, "interval", minutes=interval_min)
    print(f"scheduler active: syncing every {interval_min} minute(s)")
    scheduler.start()


if __name__ == "__main__":
    main()
