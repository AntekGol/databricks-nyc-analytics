-- unity catalog governance - table/column comments (data dictionary) and row-level security filter

COMMENT ON TABLE workspace.gold.dim_date IS 'calendar dimension from 2023 to 2026';
COMMENT ON TABLE workspace.gold.dim_zone IS 'nyc tlc taxi pickup and dropoff zones';
COMMENT ON TABLE workspace.gold.dim_fx IS 'daily ecb usd/eur exchange rates with weekend forward-fill';
COMMENT ON TABLE workspace.gold.dim_gdp IS 'annual usa gdp in current usd from world bank';
COMMENT ON TABLE workspace.gold.fact_taxi_daily IS 'daily nyc yellow taxi trips, fares, and usd/eur revenues per pickup zone';
COMMENT ON TABLE workspace.gold.fact_air_quality_daily IS 'daily nyc openaq pollutant stats (pm25, no2, o3) per borough';

ALTER TABLE workspace.gold.fact_taxi_daily
ALTER COLUMN total_revenue_eur COMMENT 'daily taxi revenue converted to eur using ecb usd_per_eur rate';

ALTER TABLE workspace.gold.fact_taxi_daily
ALTER COLUMN avg_fare_usd COMMENT 'average base metered fare per trip in usd';

ALTER TABLE workspace.gold.fact_air_quality_daily
ALTER COLUMN avg_value COMMENT 'daily mean pollutant concentration across borough monitors';

-- row-level security (rls) via unity catalog row filter
-- members of manhattan_analysts see only manhattan rows; all other authorized roles see full nyc
CREATE OR REPLACE FUNCTION workspace.gold.rls_borough_filter(borough STRING)
RETURN CASE
    WHEN is_account_group_member('manhattan_analysts') THEN borough = 'Manhattan'
    ELSE TRUE
END;

ALTER TABLE workspace.gold.fact_taxi_daily
SET ROW FILTER workspace.gold.rls_borough_filter ON (borough);

DESCRIBE TABLE EXTENDED workspace.gold.fact_taxi_daily;
