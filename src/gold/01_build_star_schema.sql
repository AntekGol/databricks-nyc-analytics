-- gold layer - build star schema dimensions, daily fact tables, and analytical views

-- 1. date dimension covering 2023-2026
CREATE OR REPLACE TABLE workspace.gold.dim_date
USING DELTA AS
SELECT
    CAST(date_format(d, 'yyyyMMdd') AS INT) AS date_key,
    d AS full_date,
    YEAR(d) AS year,
    QUARTER(d) AS quarter,
    MONTH(d) AS month,
    date_format(d, 'MMMM') AS month_name,
    dayofweek(d) AS day_of_week,
    date_format(d, 'EEEE') AS day_name,
    dayofweek(d) IN (1, 7) AS is_weekend
FROM (
    SELECT explode(sequence(DATE '2023-01-01', DATE '2026-12-31', INTERVAL 1 DAY)) AS d
);

-- 2. nyc taxi zone dimension
CREATE OR REPLACE TABLE workspace.gold.dim_zone
USING DELTA AS
SELECT
    location_id AS zone_key,
    borough,
    zone_name,
    service_zone
FROM workspace.silver.taxi_zones;

-- 3. daily usd/eur exchange rate dimension
CREATE OR REPLACE TABLE workspace.gold.dim_fx
USING DELTA AS
SELECT
    CAST(date_format(rate_date, 'yyyyMMdd') AS INT) AS fx_date_key,
    rate_date,
    usd_per_eur,
    eur_per_usd,
    is_imputed
FROM workspace.silver.fx_daily;

-- 4. yearly usa gdp dimension
CREATE OR REPLACE TABLE workspace.gold.dim_gdp
USING DELTA AS
SELECT
    year,
    country_iso3,
    country_name,
    gdp_usd,
    yoy_growth_pct
FROM workspace.silver.gdp_yearly;

-- 5. fact table - daily taxi trips and revenues per zone (usd and eur)
CREATE OR REPLACE TABLE workspace.gold.fact_taxi_daily
USING DELTA AS
SELECT
    CAST(date_format(t.pickup_date, 'yyyyMMdd') AS INT) AS date_key,
    t.pickup_date AS trip_date,
    YEAR(t.pickup_date) AS year,
    t.pu_location_id AS zone_key,
    COALESCE(z.borough, 'Unknown') AS borough,
    COUNT(*) AS trip_count,
    CAST(SUM(t.passenger_count) AS BIGINT) AS total_passengers,
    ROUND(SUM(t.trip_distance), 2) AS total_distance_miles,
    ROUND(AVG(t.trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(t.trip_duration_min), 2) AS avg_trip_duration_min,
    ROUND(SUM(t.fare_amount), 2) AS total_fare_usd,
    ROUND(AVG(t.fare_amount), 2) AS avg_fare_usd,
    ROUND(SUM(t.total_amount), 2) AS total_revenue_usd,
    ROUND(SUM(t.total_amount / fx.usd_per_eur), 2) AS total_revenue_eur,
    ROUND(AVG(t.total_amount), 2) AS avg_revenue_per_trip_usd,
    ROUND(AVG(t.total_amount / fx.usd_per_eur), 2) AS avg_revenue_per_trip_eur,
    fx.usd_per_eur
FROM workspace.silver.taxi_trips t
LEFT JOIN workspace.gold.dim_zone z
    ON t.pu_location_id = z.zone_key
INNER JOIN workspace.gold.dim_fx fx
    ON t.pickup_date = fx.rate_date
GROUP BY
    t.pickup_date,
    t.pu_location_id,
    COALESCE(z.borough, 'Unknown'),
    fx.usd_per_eur;

-- 6. fact table - daily air quality measurements per borough and pollutant
CREATE OR REPLACE TABLE workspace.gold.fact_air_quality_daily
USING DELTA AS
SELECT
    CAST(date_format(measurement_date, 'yyyyMMdd') AS INT) AS date_key,
    measurement_date,
    YEAR(measurement_date) AS year,
    borough,
    parameter,
    units,
    COUNT(DISTINCT sensor_id) AS sensor_count,
    ROUND(AVG(daily_value), 4) AS avg_value,
    ROUND(MIN(COALESCE(min_value, daily_value)), 4) AS min_value,
    ROUND(MAX(COALESCE(max_value, daily_value)), 4) AS max_value,
    CAST(SUM(observed_count) AS BIGINT) AS measurement_count
FROM workspace.silver.air_quality_daily
GROUP BY
    measurement_date,
    borough,
    parameter,
    units;

-- analytical views answering the four project questions

-- city-wide daily taxi intensity vs pm2.5, no2, o3
CREATE OR REPLACE VIEW workspace.gold.vw_mobility_air_quality_daily AS
WITH taxi_day AS (
    SELECT
        date_key,
        trip_date,
        year,
        SUM(trip_count) AS total_trips,
        ROUND(SUM(total_revenue_usd), 2) AS total_revenue_usd,
        ROUND(SUM(total_revenue_eur), 2) AS total_revenue_eur,
        ROUND(SUM(total_revenue_usd) / SUM(trip_count), 2) AS avg_revenue_per_trip_usd,
        ROUND(SUM(total_revenue_eur) / SUM(trip_count), 2) AS avg_revenue_per_trip_eur,
        AVG(usd_per_eur) AS usd_per_eur
    FROM workspace.gold.fact_taxi_daily
    GROUP BY date_key, trip_date, year
),
aq_day AS (
    SELECT
        date_key,
        ROUND(AVG(CASE WHEN parameter = 'pm25' THEN avg_value END), 3) AS pm25_avg,
        ROUND(AVG(CASE WHEN parameter = 'no2' THEN avg_value END), 4) AS no2_avg,
        ROUND(AVG(CASE WHEN parameter = 'o3' THEN avg_value END), 4) AS o3_avg
    FROM workspace.gold.fact_air_quality_daily
    GROUP BY date_key
)
SELECT
    t.*,
    a.pm25_avg,
    a.no2_avg,
    a.o3_avg
FROM taxi_day t
LEFT JOIN aq_day a ON t.date_key = a.date_key;

-- hourly taxi volume vs hourly pm2.5 and no2 (2025-06 and 2025-12 sample)
CREATE OR REPLACE VIEW workspace.gold.vw_hourly_mobility_air_quality AS
WITH taxi_hr AS (
    SELECT
        pickup_hour AS hour_of_day,
        COUNT(*) AS total_trips,
        ROUND(AVG(total_amount), 2) AS avg_revenue_usd
    FROM workspace.silver.taxi_trips
    WHERE pickup_year_month IN ('2025-06', '2025-12')
    GROUP BY pickup_hour
),
aq_hr AS (
    SELECT
        measurement_hour AS hour_of_day,
        ROUND(AVG(CASE WHEN parameter = 'pm25' THEN hourly_value END), 3) AS pm25_avg,
        ROUND(AVG(CASE WHEN parameter = 'no2' THEN hourly_value END), 4) AS no2_avg
    FROM workspace.silver.air_quality_hourly
    GROUP BY measurement_hour
)
SELECT
    t.hour_of_day,
    t.total_trips,
    t.avg_revenue_usd,
    a.pm25_avg,
    a.no2_avg
FROM taxi_hr t
LEFT JOIN aq_hr a ON t.hour_of_day = a.hour_of_day;

-- monthly and yearly taxi revenues (usd vs eur) joined with usa gdp context
CREATE OR REPLACE VIEW workspace.gold.vw_economic_impact AS
SELECT
    d.year,
    d.month,
    date_format(f.trip_date, 'yyyy-MM') AS year_month,
    SUM(f.trip_count) AS total_trips,
    ROUND(SUM(f.total_revenue_usd), 2) AS total_revenue_usd,
    ROUND(SUM(f.total_revenue_eur), 2) AS total_revenue_eur,
    ROUND(SUM(f.total_revenue_usd) / SUM(f.trip_count), 2) AS avg_revenue_per_trip_usd,
    ROUND(SUM(f.total_revenue_eur) / SUM(f.trip_count), 2) AS avg_revenue_per_trip_eur,
    ROUND(AVG(f.usd_per_eur), 4) AS avg_usd_per_eur,
    g.gdp_usd,
    g.yoy_growth_pct
FROM workspace.gold.fact_taxi_daily f
INNER JOIN workspace.gold.dim_date d ON f.date_key = d.date_key
LEFT JOIN workspace.gold.dim_gdp g ON d.year = g.year
GROUP BY d.year, d.month, date_format(f.trip_date, 'yyyy-MM'), g.gdp_usd, g.yoy_growth_pct;

-- quick verification counts
SELECT 'dim_date' AS table_name, COUNT(*) AS rows FROM workspace.gold.dim_date
UNION ALL SELECT 'dim_zone', COUNT(*) FROM workspace.gold.dim_zone
UNION ALL SELECT 'dim_fx', COUNT(*) FROM workspace.gold.dim_fx
UNION ALL SELECT 'dim_gdp', COUNT(*) FROM workspace.gold.dim_gdp
UNION ALL SELECT 'fact_taxi_daily', COUNT(*) FROM workspace.gold.fact_taxi_daily
UNION ALL SELECT 'fact_air_quality_daily', COUNT(*) FROM workspace.gold.fact_air_quality_daily;
