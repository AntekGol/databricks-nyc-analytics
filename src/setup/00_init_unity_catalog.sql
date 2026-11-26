-- setup unity catalog schemas and raw landing volume for medallion layers

CREATE SCHEMA IF NOT EXISTS workspace.bronze
COMMENT 'raw landing layer for ingested files and api payloads';

CREATE SCHEMA IF NOT EXISTS workspace.silver
COMMENT 'cleaned, deduplicated, and standardized delta tables';

CREATE SCHEMA IF NOT EXISTS workspace.gold
COMMENT 'star schema tables and analytical views for reporting';

CREATE VOLUME IF NOT EXISTS workspace.bronze.raw_landing
COMMENT 'raw parquet, csv, and json files landed from external sources';

SHOW SCHEMAS IN workspace;
SHOW VOLUMES IN workspace.bronze;
