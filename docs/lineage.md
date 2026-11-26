# Data Lineage

## End-to-End Medallion Flow

```mermaid
flowchart LR
    subgraph External["External Open Data"]
        S1["NYC TLC Parquet + Zones CSV"]
        S2["OpenAQ API v3 (NYC bbox)"]
        S3["World Bank GDP API"]
        S4["ECB Daily FX API"]
    end

    subgraph Bronze["workspace.bronze"]
        V["UC Volume: raw_landing"]
        B1["raw_taxi_trips / raw_taxi_zones"]
        B2["raw_openaq_locations / daily / hourly"]
        B3["raw_worldbank_gdp / raw_ecb_fx"]
    end

    subgraph Silver["workspace.silver"]
        L1["taxi_trips / taxi_zones"]
        L2["air_quality_daily / hourly"]
        L3["gdp_yearly / fx_daily"]
        WM["etl_watermarks (OCC Serializable)"]
    end

    subgraph Gold["workspace.gold"]
        D["dim_date / dim_zone / dim_fx / dim_gdp"]
        F1["fact_taxi_daily (RLS Row Filter)"]
        F2["fact_air_quality_daily"]
        VW["vw_mobility_air_quality_daily / vw_hourly / vw_economic_impact"]
    end

    subgraph Consumers["Analytics & External Integrations"]
        DASH["Databricks AI/BI Dashboard"]
        TS["Weather Sync -> TimescaleDB -> Grafana"]
        DQ["Great Expectations -> Discord Bot"]
    end

    S1 --> V --> B1 --> L1
    S2 --> V --> B2 --> L2
    S3 --> V --> B3 --> L3
    S4 --> V --> B3

    L1 --> WM
    L2 --> WM
    L3 --> WM

    L1 --> D
    L3 --> D
    L1 --> F1
    D --> F1
    L2 --> F2
    F1 --> VW
    F2 --> VW
    D --> VW
    L1 --> VW
    L2 --> VW

    VW --> DASH
    F1 --> DASH
    F2 --> DASH
    VW --> TS
    F1 --> DQ
    F2 --> DQ
```

## Automatic Unity Catalog Lineage
Unity Catalog captures table-level and column-level lineage automatically for every Spark DataFrame and SQL transformation executed on Serverless compute:
1. Open **Catalog** in Databricks UI -> `workspace` -> `gold` -> `fact_taxi_daily`.
2. Switch to the **Lineage** tab and click **See lineage graph** to inspect upstream dependencies (`raw_taxi_trips` -> `taxi_trips` -> `fact_taxi_daily`) and downstream views/dashboards.
