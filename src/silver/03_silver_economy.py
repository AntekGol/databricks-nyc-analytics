# silver layer - clean gdp with yoy growth and forward-fill daily ecb fx rates for weekends
import os
import sys
from pyspark.sql import SparkSession, Window, functions as F

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)) if "__file__" in globals() else os.getcwd())
from occ_utils import upsert_delta_table

spark = SparkSession.builder.getOrCreate()
CATALOG = "workspace"

# 1. yearly usa gdp with year-over-year growth percentage
gdp_window = Window.partitionBy("country_iso3").orderBy("year")

gdp_df = (
    spark.table(f"{CATALOG}.bronze.raw_worldbank_gdp")
    .filter(F.col("gdp_usd").isNotNull() & F.col("year").isNotNull())
    .dropDuplicates(["country_iso3", "year"])
    .withColumn("prev_gdp_usd", F.lag("gdp_usd").over(gdp_window))
    .withColumn(
        "yoy_growth_pct",
        F.round((F.col("gdp_usd") - F.col("prev_gdp_usd")) / F.col("prev_gdp_usd") * 100.0, 2),
    )
    .select(
        "country_iso3",
        "country_name",
        F.col("year").cast("int").alias("year"),
        "gdp_usd",
        "yoy_growth_pct",
        F.current_timestamp().alias("_updated_at"),
    )
)

upsert_delta_table(
    spark,
    gdp_df,
    f"{CATALOG}.silver.gdp_yearly",
    merge_condition="t.country_iso3 = s.country_iso3 AND t.year = s.year",
)

# 2. daily usd/eur fx with weekend/holiday forward-fill so every taxi day has a rate
raw_fx = (
    spark.table(f"{CATALOG}.bronze.raw_ecb_fx")
    .withColumn("rate_date", F.to_date("rate_date"))
    .filter(F.col("rate_date").isNotNull() & F.col("usd_per_eur").isNotNull() & (F.col("usd_per_eur") > 0))
    .select("rate_date", "usd_per_eur")
    .dropDuplicates(["rate_date"])
)

bounds = raw_fx.agg(F.min("rate_date").alias("min_d"), F.max("rate_date").alias("max_d")).first()
calendar_df = spark.sql(
    f"SELECT explode(sequence(DATE '{bounds.min_d}', DATE '{bounds.max_d}', INTERVAL 1 DAY)) AS rate_date"
)

ffill_window = Window.orderBy("rate_date").rowsBetween(Window.unboundedPreceding, 0)

fx_df = (
    calendar_df.join(raw_fx, on="rate_date", how="left")
    .withColumn("is_imputed", F.col("usd_per_eur").isNull())
    .withColumn("usd_per_eur", F.last("usd_per_eur", ignorenulls=True).over(ffill_window))
    .filter(F.col("usd_per_eur").isNotNull())
    .withColumn("eur_per_usd", F.round(F.lit(1.0) / F.col("usd_per_eur"), 6))
    .withColumn("_updated_at", F.current_timestamp())
)

upsert_delta_table(
    spark,
    fx_df,
    f"{CATALOG}.silver.fx_daily",
    merge_condition="t.rate_date = s.rate_date",
)

print(
    f"silver gdp: {spark.table(f'{CATALOG}.silver.gdp_yearly').count()}, "
    f"silver fx: {spark.table(f'{CATALOG}.silver.fx_daily').count()}"
)
