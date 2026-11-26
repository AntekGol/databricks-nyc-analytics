# silver layer - optimistic locking control table (serializable + row_version) and conflict test
from concurrent.futures import ThreadPoolExecutor
import os
import sys
from pyspark.sql import SparkSession

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd())
from occ_utils import run_with_occ_retry

spark = SparkSession.builder.getOrCreate()
CATALOG = "workspace"
WATERMARK_TABLE = f"{CATALOG}.silver.etl_watermarks"


class OptimisticLockError(RuntimeError):
    pass


# create watermark table with strictest delta isolation level (Serializable)
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
        dataset_name STRING,
        last_loaded_value STRING,
        row_count BIGINT,
        row_version BIGINT,
        updated_at TIMESTAMP
    )
    USING DELTA
    TBLPROPERTIES ('delta.isolationLevel' = 'Serializable')
    COMMENT 'etl watermark control table protected by row_version optimistic locking'
""")


def get_watermark(dataset_name: str):
    row = spark.sql(
        f"SELECT last_loaded_value, row_count, row_version FROM {WATERMARK_TABLE} WHERE dataset_name = :name",
        args={"name": dataset_name},
    ).first()
    return row


def update_watermark_optimistic(
    dataset_name: str, new_value: str, row_count: int, expected_version: int | None
) -> None:
    if expected_version is None:
        spark.sql(
            f"""
            INSERT INTO {WATERMARK_TABLE}
            VALUES (:name, :val, :cnt, 1, current_timestamp())
            """,
            args={"name": dataset_name, "val": str(new_value), "cnt": int(row_count)},
        )
        return

    res = spark.sql(
        f"""
        UPDATE {WATERMARK_TABLE}
        SET last_loaded_value = :val,
            row_count = :cnt,
            row_version = row_version + 1,
            updated_at = current_timestamp()
        WHERE dataset_name = :name AND row_version = :expected_ver
        """,
        args={
            "name": dataset_name,
            "val": str(new_value),
            "cnt": int(row_count),
            "expected_ver": int(expected_version),
        },
    ).first()

    if int(res[0]) != 1:
        raise OptimisticLockError(
            f"stale row_version={expected_version} for '{dataset_name}' (modified concurrently)"
        )


def sync_watermark(dataset_name: str, new_value: str, row_count: int, max_retries: int = 5) -> int:
    for attempt in range(max_retries):
        current = get_watermark(dataset_name)
        expected_ver = int(current["row_version"]) if current else None
        try:
            run_with_occ_retry(
                lambda: update_watermark_optimistic(dataset_name, new_value, row_count, expected_ver)
            )
            return (expected_ver or 0) + 1
        except OptimisticLockError as err:
            if attempt == max_retries - 1:
                raise
            print(f"optimistic lock retry ({attempt + 1}): {err}")
    raise OptimisticLockError(f"failed to update watermark for {dataset_name}")


# 1. sync real watermarks from current silver tables
stats_queries = {
    "nyc_taxi": f"SELECT CAST(MAX(pickup_date) AS STRING), COUNT(*) FROM {CATALOG}.silver.taxi_trips",
    "openaq_air_quality": f"SELECT CAST(MAX(measurement_date) AS STRING), COUNT(*) FROM {CATALOG}.silver.air_quality_daily",
    "economy_fx_gdp": f"SELECT CAST(MAX(rate_date) AS STRING), COUNT(*) FROM {CATALOG}.silver.fx_daily",
}

for name, query in stats_queries.items():
    max_val, cnt = spark.sql(query).first()
    new_ver = sync_watermark(name, max_val, cnt)
    print(f"watermark synced: {name} -> {max_val} ({cnt} rows, v{new_ver})")


# 2. verify optimistic locking behavior (stale version rejection + concurrent writers)
spark.sql(f"DELETE FROM {WATERMARK_TABLE} WHERE dataset_name = 'occ_test'")
update_watermark_optimistic("occ_test", "batch_1", 10, expected_version=None)

stale_ver = int(get_watermark("occ_test")["row_version"])
update_watermark_optimistic("occ_test", "batch_2", 20, expected_version=stale_ver)

try:
    update_watermark_optimistic("occ_test", "stale_writer", 99, expected_version=stale_ver)
    raise AssertionError("stale update should have been rejected")
except OptimisticLockError as err:
    print(f"occ stale check passed: {err}")

with ThreadPoolExecutor(max_workers=2) as pool:
    f1 = pool.submit(sync_watermark, "occ_test", "parallel_a", 30)
    f2 = pool.submit(sync_watermark, "occ_test", "parallel_b", 40)
    v1, v2 = f1.result(), f2.result()

final_ver = int(get_watermark("occ_test")["row_version"])
assert final_ver == 4, f"expected final version 4, got {final_ver}"
print(f"occ parallel check passed: writer versions={sorted([v1, v2])}, final_version={final_ver}")

spark.sql(f"DELETE FROM {WATERMARK_TABLE} WHERE dataset_name = 'occ_test'")
spark.table(WATERMARK_TABLE).show(truncate=False)
