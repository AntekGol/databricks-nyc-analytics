# shared delta lake optimistic concurrency helpers for silver layer
import random
import time
from delta.tables import DeltaTable
from pyspark.sql import DataFrame, SparkSession

OCC_ERRORS = (
    "ConcurrentAppendException",
    "ConcurrentModificationException",
    "ConcurrentTransactionException",
    "DELTA_CONCURRENT",
)


def run_with_occ_retry(fn, max_retries: int = 4, base_delay: float = 1.0):
    for attempt in range(max_retries + 1):
        try:
            return fn()
        except Exception as err:
            is_occ = any(code in str(err) for code in OCC_ERRORS)
            if not is_occ or attempt == max_retries:
                raise
            wait = base_delay * (2 ** attempt) + random.uniform(0, 0.5)
            print(f"occ conflict detected (attempt {attempt + 1}), retrying in {wait:.2f}s")
            time.sleep(wait)


def upsert_delta_table(
    spark: SparkSession,
    df: DataFrame,
    table_name: str,
    merge_condition: str,
    partition_col: str | None = None,
) -> None:
    if not spark.catalog.tableExists(table_name):
        writer = df.write.format("delta").mode("overwrite")
        if partition_col:
            writer = writer.partitionBy(partition_col)
        writer.saveAsTable(table_name)
        return

    def _merge():
        target = DeltaTable.forName(spark, table_name)
        (
            target.alias("t")
            .merge(df.alias("s"), merge_condition)
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    run_with_occ_retry(_merge)
