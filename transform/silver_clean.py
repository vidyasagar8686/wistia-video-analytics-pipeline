"""
silver_clean.py  -  Bronze -> Silver
Wistia Video Analytics Pipeline (PySpark)

Reads the raw Wistia JSON in Bronze, cleans each source, and saves it to Silver as Parquet.
Silver = one clean table per source: correct types, no duplicates, personal data removed/masked,
clear column names. No business rules here (those are in gold_build.py).

  Bronze source         ->  Silver table      Rows (2026-10-07)
  bronze/media          ->  silver/media             2
  bronze/events         ->  silver/events        1,233
  bronze/visitors       ->  silver/visitors    101,108
  bronze/daily_stats    ->  silver/daily_stats   1,490

Run on the laptop:   python transform\\silver_clean.py
In AWS Glue later:   change BASE_PATH to "s3://wistia-data-222238779166"
"""

import os
import sys

if os.name == "nt":                                  # Windows laptop only; Glue runs on Linux
    os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, Window, functions as F

# ---------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------
# Laptop: "data". Glue passes --base_path s3://wistia-data-222238779166 as a job parameter.
BASE_PATH = "data"
if "--base_path" in sys.argv:
    BASE_PATH = sys.argv[sys.argv.index("--base_path") + 1]
BRONZE = f"{BASE_PATH}/bronze"
SILVER = f"{BASE_PATH}/silver"
QUARANTINE = f"{BASE_PATH}/quarantine"

# ---------------------------------------------------------------------
# Spark setup
# ---------------------------------------------------------------------
# spark = SparkSession.builder.master("local[*]").appName("wistia_silver_clean").getOrCreate()
spark = SparkSession.builder.appName("wistia_silver_clean").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")   # all timestamps in UTC (same as ingestion)


def read_bronze(endpoint):
    """Read every JSON file of one endpoint (all load_date folders).
    Each file is one JSON document, so multiLine is needed.
    Spark adds a load_date column from the folder name load_date=YYYY-MM-DD."""
    return spark.read.option("multiLine", True).json(f"{BRONZE}/{endpoint}")


def keep_latest(df, keys, order_col):
    """Remove duplicates: keep one row per key, the one with the newest order_col.
    SQL: ROW_NUMBER() OVER (PARTITION BY keys ORDER BY order_col DESC) ... WHERE rn = 1"""
    w = Window.partitionBy(*keys).orderBy(F.col(order_col).desc())
    return df.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1).drop("rn")


def split_quarantine(df, table, rules):
    """Data quality rules. rules = [(condition_that_means_BAD, reason), ...].
    Bad rows go to quarantine/<table> with a dq_reason column (first rule that failed);
    only good rows are returned for Silver. Quarantine is rebuilt every run (overwrite),
    so it always shows the bad rows that are in Bronze right now."""
    reason = F.lit(None).cast("string")
    for condition, text in reversed(rules):
        reason = F.when(F.coalesce(condition, F.lit(False)), F.lit(text)).otherwise(reason)
    df = df.withColumn("dq_reason", reason)
    bad = df.filter(F.col("dq_reason").isNotNull())
    bad.write.mode("overwrite").parquet(f"{QUARANTINE}/{table}")
    print(f"quarantine {table:<8} rows: {bad.count():>7}")
    return df.filter(F.col("dq_reason").isNull()).drop("dq_reason")


def save_silver(df, table):
    """Save a cleaned table to Silver (full rebuild each run) and print its row count."""
    df.write.mode("overwrite").parquet(f"{SILVER}/{table}")
    print(f"silver {table:<12} saved: {df.count():>7}")


# =====================================================================
# 1. MEDIA  ->  silver/media
#    Bronze keeps one snapshot per video per day -> keep the latest one.
# =====================================================================
media = read_bronze("media")
media = keep_latest(media, ["hashed_id"], "load_date")

silver_media = media.select(
    F.col("hashed_id").alias("media_id"),                 # brief's name
    F.col("name").alias("title"),
    F.to_timestamp("created").alias("created_at"),        # JSON text -> timestamp
    F.col("share_link.url").alias("url"),                 # field inside the share_link group
    F.col("duration").alias("duration_seconds"),
    F.to_timestamp("updated").alias("updated_at"),
)
save_silver(silver_media, "media")


# =====================================================================
# 2. EVENTS  ->  silver/events   (one row per play)
#    Duplicates come from test runs + the full backfill saving the same events twice.
# =====================================================================
events = read_bronze("events")

events = events.withColumn("received_at", F.to_timestamp("received_at"))   # text -> timestamp
events = keep_latest(events, ["event_key"], "load_date")                    # one row per event

# Privacy: mask the last part of the IP  (97.100.12.233 -> 97.100.12.XXX)
events = events.withColumn("ip", F.regexp_replace(F.col("ip"), r"\.\d+$", ".XXX"))

# Keep only the columns the Gold model needs (lat/lon, email, org etc. dropped: data minimization)
silver_events = events.select(
    F.col("event_key"),
    F.col("media_id"),
    F.col("visitor_key"),
    F.col("received_at").alias("event_at"),
    F.col("percent_viewed"),                              # 0-1, how much of the video was watched
    F.col("ip").alias("ip_masked"),
    F.col("country"),
    F.col("region"),
    F.col("city"),
    F.col("user_agent_details.browser").alias("browser"),     # flattened out of the group
    F.col("user_agent_details.platform").alias("platform"),
    F.col("user_agent_details.mobile").alias("is_mobile"),
).replace("", None)                                      # empty text -> NULL

silver_events = split_quarantine(silver_events, "events", [
    (F.col("event_key").isNull(), "event_key missing"),
    (F.col("media_id").isNull(), "media_id missing"),
    ((F.col("percent_viewed") < 0) | (F.col("percent_viewed") > 1), "percent_viewed out of range"),
])
save_silver(silver_events, "events")


# =====================================================================
# 3. VISITORS  ->  silver/visitors   (account-wide list of viewers)
# =====================================================================
visitors = read_bronze("visitors")

visitors = (visitors
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("last_active_at", F.to_timestamp("last_active_at"))
)
visitors = keep_latest(visitors, ["visitor_key"], "last_active_at")   # most up-to-date version

# visitor_identity (name / email / org) is personal data -> not kept
silver_visitors = visitors.select(
    F.col("visitor_key"),
    F.col("created_at"),
    F.col("last_active_at"),
    F.col("load_count"),
    F.col("play_count"),
    F.col("user_agent_details.browser").alias("browser"),
    F.col("user_agent_details.platform").alias("platform"),
    F.col("user_agent_details.mobile").alias("is_mobile"),
).replace("", None)

silver_visitors = split_quarantine(silver_visitors, "visitors", [
    (F.col("visitor_key").isNull(), "visitor_key missing"),
])
save_silver(silver_visitors, "visitors")


# =====================================================================
# 4. DAILY STATS  ->  silver/daily_stats   (one row per video per day)
#    The by_date rows have NO media_id -> take it from the Bronze file name.
# =====================================================================
daily = read_bronze("daily_stats")

daily = daily.withColumn("date", F.to_date("date"))      # text -> date

# .../8hunphufxp_182738.json -> 8hunphufxp   (run_id part is optional for older files)
daily = daily.withColumn(
    "media_id", F.regexp_extract(F.input_file_name(), r"([a-z0-9]{10})(_\d+)?\.json$", 1)
)
# One row per video per day. The ingestion lookback loads recent days again, so keep the
# NEWEST copy: order by load_date, then by file name (it ends with the run time HHMMSS),
# so a later run on the same day wins.
daily = daily.withColumn("load_order", F.concat_ws("|", F.col("load_date").cast("string"), F.input_file_name()))
daily = keep_latest(daily, ["media_id", "date"], "load_order")

silver_daily = daily.select("media_id", "date", "load_count", "play_count", "hours_watched")
silver_daily = split_quarantine(silver_daily, "daily_stats", [
    ((F.col("media_id").isNull()) | (F.col("media_id") == ""), "media_id not found in file name"),
    ((F.col("play_count") < 0) | (F.col("load_count") < 0), "negative play or load count"),
])
save_silver(silver_daily, "daily_stats")


spark.stop()
