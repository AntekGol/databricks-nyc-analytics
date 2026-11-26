# Data Dictionary

Catalog: `workspace` (Unity Catalog on Databricks Free Edition).

## Bronze Layer (`workspace.bronze`)

| Table / Volume | Source | Description | Key Columns |
| :--- | :--- | :--- | :--- |
| `/Volumes/workspace/bronze/raw_landing` | All external sources | Managed Unity Catalog Volume storing raw Parquet, CSV, and JSON files | n/a |
| `raw_taxi_trips` | NYC TLC Parquet (`2023-06`..`2025-12`) | Raw monthly Yellow Taxi trip records with `_source_month` and `_ingested_at` | `vendorid`, `tpep_pickup_datetime`, `pulocationid`, `fare_amount`, `total_amount` |
| `raw_taxi_zones` | NYC TLC `taxi_zone_lookup.csv` | Official TLC pickup/dropoff zone mapping | `LocationID`, `Borough`, `Zone`, `service_zone` |
| `raw_openaq_locations` | OpenAQ API v3 `/v3/locations` | Selected active NYC sensors (PM2.5, NO2, O3) | `sensor_id`, `location_id`, `location_name`, `latitude`, `longitude`, `parameter` |
| `raw_openaq_daily` | OpenAQ API v3 `/v3/sensors/{id}/days` | Paginated daily pollutant readings (`2023-06`..`2025-12`) | `sensor_id`, `period_start_local`, `value`, `min_value`, `max_value`, `avg_value` |
| `raw_openaq_hourly` | OpenAQ API v3 `/v3/sensors/{id}/hours` | Hourly PM2.5 and NO2 sample (`2025-06`, `2025-12`) | `sensor_id`, `period_start_local`, `value` |
| `raw_worldbank_gdp` | World Bank API `NY.GDP.MKTP.CD` | Annual USA GDP in current USD | `country_iso3`, `year`, `gdp_usd` |
| `raw_ecb_fx` | ECB Data API `EXR/D.USD.EUR.SP00.A` | Daily reference FX rate (`1 EUR = X USD`) | `rate_date`, `usd_per_eur` |

## Silver Layer (`workspace.silver`)

| Table | Partition / Key | Cleaning & Enrichment Rules |
| :--- | :--- | :--- |
| `taxi_zones` | Key: `location_id` | Trimmed strings, deduplicated, upserted via Delta `MERGE` |
| `taxi_trips` | Partition: `pickup_year_month`; Key: `trip_id` (SHA-256) | Drops corrupt meter dates outside `_source_month`, non-positive fares/distances, trips > 6h or > 200 mi; adds `pickup_date`, `pickup_hour`, `trip_duration_min` |
| `air_quality_daily` | Partition: `measurement_year_month`; Key: `(sensor_id, measurement_date, parameter)` | Uses NYC local calendar date, filters negative values, maps stations to NYC boroughs |
| `air_quality_hourly` | Key: `(sensor_id, measurement_date, measurement_hour, parameter)` | Uses NYC local hour (0-23), filters negative readings, joins borough |
| `gdp_yearly` | Key: `(country_iso3, year)` | Removes nulls, computes `yoy_growth_pct` via window `LAG` |
| `fx_daily` | Key: `rate_date` | Continuous daily calendar with forward-fill for weekends/holidays (`is_imputed`), computes `eur_per_usd = 1 / usd_per_eur` |
| `etl_watermarks` | Isolation: `Serializable`; Key: `dataset_name` | Tracks `last_loaded_value`, `row_count`, and `row_version` for optimistic locking |

## Gold Layer (`workspace.gold` - Star Schema)

| Object | Type | Grain | Key Columns |
| :--- | :--- | :--- | :--- |
| `dim_date` | Table | 1 row per day (`2023-2026`) | `date_key` (`YYYYMMDD`), `full_date`, `year`, `quarter`, `month`, `day_name`, `is_weekend` |
| `dim_zone` | Table | 1 row per TLC zone | `zone_key`, `borough`, `zone_name`, `service_zone` |
| `dim_fx` | Table | 1 row per calendar day | `fx_date_key`, `rate_date`, `usd_per_eur`, `eur_per_usd`, `is_imputed` |
| `dim_gdp` | Table | 1 row per year | `year`, `country_iso3`, `gdp_usd`, `yoy_growth_pct` |
| `fact_taxi_daily` | Table | `(date_key, zone_key)` | `trip_count`, `total_passengers`, `avg_fare_usd`, `total_revenue_usd`, `total_revenue_eur`, `avg_revenue_per_trip_usd`, `avg_revenue_per_trip_eur` |
| `fact_air_quality_daily` | Table | `(date_key, borough, parameter)` | `sensor_count`, `avg_value`, `min_value`, `max_value`, `measurement_count` |
| `vw_mobility_air_quality_daily` | View | 1 row per taxi day (city-wide) | Daily taxi totals + city-wide `pm25_avg`, `no2_avg`, `o3_avg` |
| `vw_hourly_mobility_air_quality` | View | 1 row per hour of day (`0-23`) | Hourly taxi trips vs hourly `pm25_avg` and `no2_avg` |
| `vw_economic_impact` | View | 1 row per `year_month` | Monthly USD/EUR taxi revenues, avg FX rate, USA `gdp_usd` and `yoy_growth_pct` |
