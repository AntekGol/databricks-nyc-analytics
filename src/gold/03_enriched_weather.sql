-- silver and gold weather enrichment joining open-meteo nyc weather with taxi and air quality

CREATE OR REPLACE TABLE workspace.silver.weather_daily
USING DELTA AS
SELECT
    CAST(weather_date AS DATE) AS weather_date,
    ROUND(temp_mean_c, 2) AS temp_mean_c,
    ROUND(temp_max_c, 2) AS temp_max_c,
    ROUND(temp_min_c, 2) AS temp_min_c,
    ROUND(COALESCE(precipitation_mm, 0.0), 2) AS precipitation_mm,
    ROUND(COALESCE(rain_mm, 0.0), 2) AS rain_mm,
    ROUND(COALESCE(snowfall_cm, 0.0), 2) AS snowfall_cm,
    ROUND(wind_max_kmh, 2) AS wind_max_kmh,
    CASE
        WHEN COALESCE(snowfall_cm, 0.0) > 0 THEN 'Snow'
        WHEN COALESCE(precipitation_mm, 0.0) >= 10 THEN 'Heavy Rain'
        WHEN COALESCE(precipitation_mm, 0.0) > 0 THEN 'Rain'
        ELSE 'Dry'
    END AS weather_condition,
    current_timestamp() AS _updated_at
FROM workspace.bronze.raw_weather_daily
WHERE weather_date IS NOT NULL;

CREATE OR REPLACE TABLE workspace.gold.fact_weather_enriched_daily
USING DELTA
COMMENT 'daily nyc weather enriched with taxi demand, revenues, and air quality for tsdb sync'
AS
SELECT
    w.weather_date,
    w.temp_mean_c,
    w.temp_max_c,
    w.temp_min_c,
    w.precipitation_mm,
    w.rain_mm,
    w.snowfall_cm,
    w.wind_max_kmh,
    w.weather_condition,
    m.total_trips,
    m.total_revenue_usd,
    m.total_revenue_eur,
    m.avg_revenue_per_trip_usd,
    COALESCE(m.pm25_avg, aq.pm25_avg) AS pm25_avg,
    COALESCE(m.no2_avg, aq.no2_avg) AS no2_avg,
    COALESCE(m.o3_avg, aq.o3_avg) AS o3_avg,
    current_timestamp() AS enriched_at
FROM workspace.silver.weather_daily w
LEFT JOIN workspace.gold.vw_mobility_air_quality_daily m
    ON w.weather_date = m.trip_date
LEFT JOIN (
    SELECT
        measurement_date,
        ROUND(AVG(CASE WHEN parameter = 'pm25' THEN avg_value END), 3) AS pm25_avg,
        ROUND(AVG(CASE WHEN parameter = 'no2' THEN avg_value END), 4) AS no2_avg,
        ROUND(AVG(CASE WHEN parameter = 'o3' THEN avg_value END), 4) AS o3_avg
    FROM workspace.gold.fact_air_quality_daily
    GROUP BY measurement_date
) aq
    ON w.weather_date = aq.measurement_date;

SELECT COUNT(*) AS enriched_weather_rows,
       COUNT(total_trips) AS days_with_taxi_data,
       COUNT(pm25_avg) AS days_with_pm25_data
FROM workspace.gold.fact_weather_enriched_daily;
