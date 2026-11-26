-- initialize timescaledb hypertable for enriched nyc weather metrics
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE IF NOT EXISTS enriched_weather_daily (
    weather_date TIMESTAMPTZ NOT NULL PRIMARY KEY,
    temp_mean_c DOUBLE PRECISION,
    temp_max_c DOUBLE PRECISION,
    temp_min_c DOUBLE PRECISION,
    precipitation_mm DOUBLE PRECISION,
    rain_mm DOUBLE PRECISION,
    snowfall_cm DOUBLE PRECISION,
    wind_max_kmh DOUBLE PRECISION,
    weather_condition TEXT,
    total_trips BIGINT,
    total_revenue_usd DOUBLE PRECISION,
    total_revenue_eur DOUBLE PRECISION,
    avg_revenue_per_trip_usd DOUBLE PRECISION,
    pm25_avg DOUBLE PRECISION,
    no2_avg DOUBLE PRECISION,
    o3_avg DOUBLE PRECISION,
    synced_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

SELECT create_hypertable('enriched_weather_daily', 'weather_date', if_not_exists => TRUE);
