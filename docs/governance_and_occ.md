# Governance and Optimistic Concurrency Control (OCC)

## 1. Optimistic Locking Architecture

Delta Lake enforces **Optimistic Concurrency Control** through its atomic JSON transaction log (`_delta_log`) instead of pessimistic table locks:
1. **Read snapshot:** A writer reads the latest committed table version.
2. **Stage files:** New immutable Parquet files are written without mutating existing files.
3. **Validate and commit:** Before committing the new log entry, Delta Lake checks whether any concurrent transaction modified overlapping files. If a conflict occurs, the losing writer aborts instead of corrupting data.

We apply OCC across three layers in this project:

- **Partitioned Delta `MERGE` + Retry (`src/silver/occ_utils.py`):**
  Silver tables (`taxi_trips`, `air_quality_daily`) use default `WriteSerializable` isolation with date-month partitioning (`pickup_year_month`, `measurement_year_month`) and partition-pruned `MERGE` predicates (`t.pickup_year_month = s.pickup_year_month AND t.trip_id = s.trip_id`). Disjoint monthly loads do not touch overlapping Parquet files; if concurrent writers collide on the same partition, `run_with_occ_retry` catches Delta concurrency exceptions (`ConcurrentAppendException`, `ConcurrentModificationException`, `DELTA_CONCURRENT`) and retries with exponential backoff and jitter.
- **Strict `Serializable` Isolation + `row_version` CAS (`src/silver/04_occ_watermarks.py`):**
  The control table `workspace.silver.etl_watermarks` sets `TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')` and maintains a monotonically increasing `row_version` column. Updates use an atomic compare-and-swap predicate (`WHERE dataset_name = :name AND row_version = :expected_ver`) and assert `num_affected_rows == 1`. Stale writers raise `OptimisticLockError`, re-read the latest version, and retry safely without lost updates.
- **Orchestration Retries (`resources/nyc_analytics.job.yml`):**
  Lakeflow Job tasks configure `max_retries: 2` as an outer safety net for transient API or concurrency errors.

## 2. Unity Catalog Governance

- **Three-Level Namespace:** Strict separation of raw (`workspace.bronze`), cleaned (`workspace.silver`), and curated (`workspace.gold`) assets.
- **Metadata & Data Dictionary (`src/gold/02_uc_governance.sql`):** All Gold tables and key metrics carry SQL `COMMENT` descriptions indexed in Unity Catalog Explorer.
- **Row-Level Security (RLS):** Implemented natively via Unity Catalog Row Filter `workspace.gold.rls_borough_filter` attached directly to `workspace.gold.fact_taxi_daily` (`ALTER TABLE ... SET ROW FILTER ... ON (borough)`). Members of `manhattan_analysts` automatically see only Manhattan rows across SQL, notebooks, and dashboards.
- **Secret Management:** API keys (`OPENAQ_API_KEY`), Databricks PATs, and bot tokens are never committed to Git (`.env` is git-ignored locally; Databricks scripts read keys from environment variables or private Unity Catalog Volume storage).
- **Automated Scheduling:** `resources/nyc_analytics.job.yml` defines a daily serverless Lakeflow Job (`0 0 6 * * ?`, `Europe/Warsaw`, `performance_target: STANDARD`).
