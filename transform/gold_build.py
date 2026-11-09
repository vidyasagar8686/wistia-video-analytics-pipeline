"""
gold_build_iceberg.py  -  Silver -> Gold (star schema) as Apache Iceberg tables
Wistia Video Analytics Pipeline (PySpark on AWS Glue 5.0)

Same business logic as gold_build.py. What is different:
  1. All 4 Gold tables are BUILT first and kept in memory (nothing is written yet).
  2. The reconciliation checks run BEFORE any write. If a check fails, the job stops
     and the Gold tables stay exactly as they were (last good version).
  3. Each table is written as an Iceberg table in the Glue Data Catalog with
     createOrReplace(): an atomic commit, so Athena readers see either the old
     version or the new one, never a half-written table. Older versions stay
     available for rollback (Iceberg time travel).
  4. No crawler is needed: Iceberg registers and updates the tables in the catalog itself.

  Gold table               Grain (one row per)
  dim_media      (SCD1)    video
  dim_visitor    (SCD1)    visitor who played our videos
  fact_media_daily         video x day
  fact_media_engagement    video x visitor x day

Glue job parameters:
  --base_path            s3://wistia-data-222238779166
  --gold_db              wistia_gold_test   (test)   or   wistia_gold   (production)
  --iceberg_warehouse    s3://wistia-data-222238779166/gold_iceberg_test/   (used only if the
                         database has no location of its own)
  --datalake-formats     iceberg
"""

import sys

from pyspark.sql import SparkSession, Window, functions as F


# ---------------------------------------------------------------------
# Settings (from Glue job parameters)
# ---------------------------------------------------------------------
def get_arg(name, default):
    flag = f"--{name}"
    if flag in sys.argv:
        return sys.argv[sys.argv.index(flag) + 1]
    return default


BASE_PATH = get_arg("base_path", "s3://wistia-data-222238779166")
GOLD_DB = get_arg("gold_db", "wistia_gold_test")                 # safe default = test
WAREHOUSE = get_arg("iceberg_warehouse", f"{BASE_PATH}/gold_iceberg_test/")
SILVER = f"{BASE_PATH}/silver"
CATALOG = "glue_catalog"

# ---------------------------------------------------------------------
# Spark setup with the Iceberg catalog (Glue Data Catalog)
# ---------------------------------------------------------------------
spark = (SparkSession.builder
    .appName("wistia_gold_build_iceberg")
    .config("spark.sql.extensions",
            "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config(f"spark.sql.catalog.{CATALOG}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config(f"spark.sql.catalog.{CATALOG}.warehouse", WAREHOUSE)
    .getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")
print(f"gold database: {GOLD_DB}   silver: {SILVER}")


def read_silver(table):
    """Parquet keeps the data types, so no conversions are needed when reading Silver."""
    return spark.read.parquet(f"{SILVER}/{table}")


# =====================================================================
# PART 1 - BUILD all 4 tables (business logic unchanged; nothing written yet)
# =====================================================================

# 1. dim_media  (SCD1: current values only)
#    ASSUMPTION for SME approval: vertical 8hunphufxp = Facebook, horizontal 9k4tbcdfg0 = YouTube.
dim_media = read_silver("media").withColumn(
    "channel",
    F.when(F.col("media_id") == "8hunphufxp", "Facebook")
     .when(F.col("media_id") == "9k4tbcdfg0", "YouTube")
     .otherwise("Unknown"),
)

# 2. fact_media_daily  (one row per video per day)
#    play_rate = plays / loads (0 when there were no loads)
fact_media_daily = read_silver("daily_stats").withColumn(
    "play_rate",
    F.round(
        F.when(F.col("load_count") > 0, F.col("play_count") / F.col("load_count")).otherwise(0),
        4,
    ),
)

# 3. dim_visitor  (SCD1, one row per visitor who played our videos)
#    location = location of the visitor's MOST RECENT play
silver_events = read_silver("events")

w_loc = Window.partitionBy("visitor_key").orderBy(F.col("event_at").desc())
latest_location = (silver_events
    .withColumn("rn", F.row_number().over(w_loc))
    .filter(F.col("rn") == 1)
    .select("visitor_key", "ip_masked", "country", "region", "city")
)
dim_visitor = (latest_location
    .join(read_silver("visitors"), on="visitor_key", how="left")
    .withColumnRenamed("visitor_key", "visitor_id")
)

# 4. fact_media_engagement  (one row per video x visitor x day)
plays = (silver_events
    .withColumn("date", F.to_date("event_at"))
    .join(dim_media.select("media_id", "duration_seconds"), on="media_id", how="left")
)
fact_media_engagement = (plays
    .groupBy("media_id", "visitor_key", "date")
    .agg(
        F.count("*").alias("play_count"),
        F.avg("percent_viewed").alias("watched_percent"),
        F.sum(F.col("percent_viewed") * F.col("duration_seconds")).alias("total_watch_time"),
    )
    .withColumnRenamed("visitor_key", "visitor_id")
)

# Keep the built tables in memory, so the checks and the writes use the same data
gold_tables = {
    "dim_media": dim_media.cache(),
    "fact_media_daily": fact_media_daily.cache(),
    "dim_visitor": dim_visitor.cache(),
    "fact_media_engagement": fact_media_engagement.cache(),
}
for name, df in gold_tables.items():
    print(f"built {name:<22} rows: {df.count():>6}")


# =====================================================================
# PART 2 - CHECK before writing (any failure stops the job; Gold is not touched)
# =====================================================================
total_plays = gold_tables["fact_media_engagement"].agg(F.sum("play_count")).collect()[0][0]
silver_plays = silver_events.count()
print(f"check: plays in fact = {total_plays}, events in silver = {silver_plays}")
if total_plays != silver_plays:
    raise RuntimeError(f"Reconciliation failed: {total_plays} plays in fact vs {silver_plays} events. "
                       f"Gold was NOT changed.")

missing_visitors = (gold_tables["fact_media_engagement"]
    .join(gold_tables["dim_visitor"], on="visitor_id", how="left_anti").count())
print(f"check: fact visitors missing from dim_visitor = {missing_visitors}")
if missing_visitors > 0:
    raise RuntimeError(f"Referential integrity failed: {missing_visitors} visitors missing from "
                       f"dim_visitor. Gold was NOT changed.")

print("all checks passed - writing Gold")


# =====================================================================
# PART 3 - WRITE each table as Iceberg (atomic commit per table)
# =====================================================================
for name, df in gold_tables.items():
    target = f"{CATALOG}.{GOLD_DB}.{name}"
    (df.writeTo(target)
       .using("iceberg")
       .tableProperty("format-version", "2")
       .createOrReplace())
    print(f"gold {target:<45} committed")

print("gold build finished successfully")

spark.stop()
