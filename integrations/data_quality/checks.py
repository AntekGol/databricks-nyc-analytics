# great expectations data quality checks against databricks gold tables
import argparse
import os
from databricks import sql as dbsql
from dotenv import load_dotenv
import great_expectations as gx
from great_expectations import expectations as gxe
import pandas as pd

load_dotenv()

def get_expectations(table_name: str) -> list:
    suites = {
        "fact_taxi_daily": [
            gxe.ExpectTableRowCountToBeBetween(min_value=1000),
            gxe.ExpectColumnValuesToNotBeNull(column="date_key"),
            gxe.ExpectColumnValuesToNotBeNull(column="zone_key"),
            gxe.ExpectColumnValuesToNotBeNull(column="total_revenue_eur"),
            gxe.ExpectColumnValuesToBeBetween(column="trip_count", min_value=1),
            gxe.ExpectColumnValuesToBeBetween(column="avg_fare_usd", min_value=1.0, max_value=500.0),
            gxe.ExpectColumnMeanToBeBetween(column="avg_revenue_per_trip_usd", min_value=10.0, max_value=80.0),
            gxe.ExpectColumnStdevToBeBetween(column="avg_fare_usd", min_value=1.0, max_value=100.0),
        ],
        "fact_air_quality_daily": [
            gxe.ExpectTableRowCountToBeBetween(min_value=100),
            gxe.ExpectColumnValuesToNotBeNull(column="measurement_date"),
            gxe.ExpectColumnValuesToBeInSet(column="parameter", value_set=["pm25", "no2", "o3"]),
            gxe.ExpectColumnValuesToBeBetween(column="avg_value", min_value=0.0, max_value=500.0),
            gxe.ExpectColumnMedianToBeBetween(column="avg_value", min_value=0.0, max_value=50.0),
        ],
        "dim_fx": [
            gxe.ExpectColumnValuesToBeUnique(column="rate_date"),
            gxe.ExpectColumnValuesToNotBeNull(column="usd_per_eur"),
            gxe.ExpectColumnValuesToBeBetween(column="usd_per_eur", min_value=0.8, max_value=1.5),
            gxe.ExpectColumnMeanToBeBetween(column="usd_per_eur", min_value=0.95, max_value=1.25),
        ],
        "fact_weather_enriched_daily": [
            gxe.ExpectColumnValuesToBeUnique(column="weather_date"),
            gxe.ExpectColumnValuesToNotBeNull(column="temp_mean_c"),
            gxe.ExpectColumnValuesToBeBetween(column="temp_mean_c", min_value=-30.0, max_value=45.0),
            gxe.ExpectColumnValuesToBeBetween(column="precipitation_mm", min_value=0.0, max_value=300.0),
        ],
    }
    if table_name not in suites:
        raise ValueError(f"unknown table '{table_name}', choose from {list(suites)}")
    return suites[table_name]


SUPPORTED_TABLES = (
    "fact_taxi_daily",
    "fact_air_quality_daily",
    "dim_fx",
    "fact_weather_enriched_daily",
)


def fetch_gold_table(table_name: str) -> pd.DataFrame:
    if table_name not in SUPPORTED_TABLES:
        raise ValueError(f"unknown table '{table_name}', choose from {SUPPORTED_TABLES}")

    catalog = os.getenv("DATABRICKS_CATALOG", "workspace")
    host = os.environ["DATABRICKS_SERVER_HOSTNAME"].removeprefix("https://").removeprefix("http://").rstrip("/")
    with dbsql.connect(
        server_hostname=host,
        http_path=os.environ["DATABRICKS_HTTP_PATH"],
        access_token=os.environ["DATABRICKS_TOKEN"],
    ) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT * FROM {catalog}.gold.{table_name}")
            cols = [desc[0] for desc in cur.description]
            rows = cur.fetchall()
    return pd.DataFrame(rows, columns=cols)


def validate_dataframe(table_name: str, df: pd.DataFrame) -> dict:
    context = gx.get_context(mode="ephemeral")
    source = context.data_sources.add_pandas(name=f"pd_{table_name}")
    asset = source.add_dataframe_asset(name=table_name)
    batch_def = asset.add_batch_definition_whole_dataframe("full_batch")

    suite = context.suites.add(gx.ExpectationSuite(name=f"suite_{table_name}"))
    for exp in get_expectations(table_name):
        suite.add_expectation(exp)

    validation_def = gx.ValidationDefinition(name=f"val_{table_name}", data=batch_def, suite=suite)
    result = validation_def.run(batch_parameters={"dataframe": df})

    failures = []
    for item in result.results:
        if not item.success:
            cfg = item.expectation_config
            res = item.result
            if "unexpected_count" in res:
                pct = res.get("unexpected_percent", 0.0)
                detail = f"{res['unexpected_count']} unexpected rows ({pct:.2f}%)"
            else:
                detail = f"observed={res.get('observed_value')}"
            failures.append({
                "expectation": cfg.type,
                "column": cfg.kwargs.get("column", "table"),
                "detail": detail,
            })

    total = len(result.results)
    passed = total - len(failures)
    return {
        "table": table_name,
        "row_count": len(df),
        "success": bool(result.success),
        "passed": passed,
        "total": total,
        "failures": failures,
    }


def run_quality_checks(tables: list[str] | None = None) -> list[dict]:
    selected = tables or list(SUPPORTED_TABLES)
    reports = []
    for tbl in selected:
        df = fetch_gold_table(tbl)
        reports.append(validate_dataframe(tbl, df))
    return reports


def format_report_text(reports: list[dict]) -> str:
    lines = []
    for r in reports:
        status = "PASS" if r["success"] else "FAIL"
        lines.append(f"[{status}] {r['table']} ({r['row_count']} rows): {r['passed']}/{r['total']} checks passed")
        for f in r["failures"]:
            lines.append(f"  - {f['expectation']} on {f['column']} ({f['detail']})")
    return "\n".join(lines)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="run great expectations checks on databricks gold tables")
    parser.add_argument("--table", help="optional single gold table name")
    args = parser.parse_args()

    target = [args.table] if args.table else None
    print(format_report_text(run_quality_checks(target)))
