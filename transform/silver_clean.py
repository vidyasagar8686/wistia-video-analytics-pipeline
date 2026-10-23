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
import re
import json
from pathlib import Path
from urllib.parse import urlparse
from pipeline_support import (mask_ip as mask_ip_value, suppression_keys,
                              validate_business_keys, deduplicate_records)

if os.name == "nt":                                  # Windows laptop only; Glue runs on Linux
    os.environ["PYSPARK_PYTHON"] = sys.executable

from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import StringType

# ---------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------
# Laptop: "data". Glue passes --base_path s3://wistia-data-222238779166 as a job parameter.
BASE_PATH = "data"
if "--base_path" in sys.argv:
    BASE_PATH = sys.argv[sys.argv.index("--base_path") + 1]
BRONZE = f"{BASE_PATH}/bronze"
RUN_ID = sys.argv[sys.argv.index("--run_id") + 1] if "--run_id" in sys.argv else "local"
if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", RUN_ID):
    raise ValueError("run_id must contain only letters, numbers, underscores or hyphens")
if BASE_PATH.startswith("s3://") and RUN_ID == "local":
    raise ValueError("AWS runs require an explicit unique --run_id")
SILVER = f"{BASE_PATH}/silver_versions/{RUN_ID}"
MEDIA_IDS = {"8hunphufxp", "9k4tbcdfg0"}

# ---------------------------------------------------------------------
# Spark setup
# ---------------------------------------------------------------------
# spark = SparkSession.builder.master("local[*]").appName("wistia_silver_clean").getOrCreate()
spark = SparkSession.builder.appName("wistia_silver_clean").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")   # all timestamps in UTC (same as ingestion)
support_module = Path(__file__).with_name("pipeline_support.py")
if support_module.exists():
    spark.sparkContext.addPyFile(str(support_module))

# A missing registry in AWS is a setup error, not permission to restore erased keys.
if BASE_PATH.startswith("s3://"):
    import boto3
    parsed = urlparse(BASE_PATH)
    key = parsed.path.strip("/")
    key = (key + "/" if key else "") + "privacy/suppressed_visitors.json"
    payload = boto3.client("s3").get_object(Bucket=parsed.netloc, Key=key)["Body"].read()
    suppressed = suppression_keys(json.loads(payload))
else:
    registry = Path(BASE_PATH) / "privacy/suppressed_visitors.json"
    suppressed = suppression_keys(json.loads(registry.read_text())) if registry.exists() else set()


def remove_suppressed(df):
    if suppressed:
        denied = spark.createDataFrame([(key,) for key in suppressed], "visitor_key string")
        return df.join(denied, on="visitor_key", how="left_anti")
    return df


def read_bronze(endpoint):
    """Read every JSON file of one endpoint (all load_date folders).
    Each file is one JSON document, so multiLine is needed.
    Spark adds a load_date column from the folder name load_date=YYYY-MM-DD."""
    return spark.read.option("multiLine", True).json(f"{BRONZE}/{endpoint}")


def save_silver(df, table):
    """Save a cleaned table into this run's isolated Silver version."""
    df.write.mode("errorifexists").parquet(f"{SILVER}/{table}")
    print(f"silver {table:<12} saved: {df.count():>7}")


# =====================================================================
# 1. MEDIA  ->  silver/media
#    Bronze keeps one snapshot per video per day -> keep the latest one.
# =====================================================================
def require_columns(df, columns, source):
    missing = set(columns) - set(df.columns)
    if missing:
        raise ValueError(f"{source}: missing required columns {sorted(missing)}")


def reject_rows(df, condition, reason):
    if df.filter(condition).limit(1).count():
        raise ValueError(reason)


@F.udf(returnType=StringType())
def mask_ip(value):
    return mask_ip_value(value)


media = read_bronze("media")
require_columns(media, ["hashed_id", "name", "created", "share_link", "duration", "updated"], "media")
validate_business_keys(media, ["hashed_id"], "media")

silver_media = media.select(
    F.col("hashed_id").alias("media_id"),                 # brief's name
    F.col("name").alias("title"),
    F.to_timestamp("created").alias("created_at"),        # JSON text -> timestamp
    F.col("share_link.url").alias("url"),                 # field inside the share_link group
    F.col("duration").alias("duration_seconds"),
    F.to_timestamp("updated").alias("updated_at"),
)
silver_media = deduplicate_records(silver_media, ["media_id"], "media", order_col="updated_at")
save_silver(silver_media, "media")


# =====================================================================
# 2. EVENTS  ->  silver/events   (one row per play)
#    Duplicates come from test runs + the full backfill saving the same events twice.
# =====================================================================
events = read_bronze("events")
require_columns(events, ["event_key", "media_id", "visitor_key", "received_at", "percent_viewed", "ip",
                        "country", "region", "city", "user_agent_details"], "events")
validate_business_keys(events, ["event_key", "visitor_key", "media_id"], "events")

events = events.withColumn("received_at", F.to_timestamp("received_at"))   # text -> timestamp
events = remove_suppressed(events)

# Privacy: coarse /24 IPv4 or /48 IPv6 network; malformed inputs become null.
events = events.withColumn("ip", mask_ip(F.col("ip")))
reject_rows(events, F.col("received_at").isNull() | F.col("event_key").isNull()
            | F.col("visitor_key").isNull() | ~F.col("media_id").isin(*MEDIA_IDS)
            | F.col("media_id").isNull() | F.col("percent_viewed").isNull()
            | (F.col("percent_viewed") < 0) | (F.col("percent_viewed") > 1),
            "events: invalid key, timestamp, video or watched fraction")

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
silver_events = deduplicate_records(silver_events, ["event_key"], "events")

save_silver(silver_events, "events")


# =====================================================================
# 3. VISITORS  ->  silver/visitors   (account-wide list of viewers)
# =====================================================================
visitors = read_bronze("visitors")
require_columns(visitors, ["visitor_key", "created_at", "last_active_at", "load_count", "play_count",
                          "user_agent_details"], "visitors")
validate_business_keys(visitors, ["visitor_key"], "visitors")

visitors = (visitors
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("last_active_at", F.to_timestamp("last_active_at"))
)
visitors = remove_suppressed(visitors)

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
silver_visitors = deduplicate_records(silver_visitors, ["visitor_key"], "visitors",
                                      order_col="last_active_at")

save_silver(silver_visitors, "visitors")


# =====================================================================
# 4. DAILY STATS  ->  silver/daily_stats   (one row per video per day)
#    New files carry request metadata; older files use validated filename attribution.
# =====================================================================
daily = read_bronze("daily_stats")
daily = daily.withColumn("_source_file", F.input_file_name())
# New files preserve the exact response in data and explicit request context in ingestion.
# Legacy files remain readable through a validated filename fallback.
if "data" in daily.columns and "ingestion" in daily.columns:
    reject_rows(daily.filter(F.col("data").isNotNull()),
                F.col("ingestion.completed").isNull() | (F.col("ingestion.completed") != True),
                "daily_stats: extraction is not marked complete")
    collected_at = (F.col("ingestion.collected_at") if "collected_at" in daily.schema["ingestion"].dataType.names
                    else F.lit(None).cast("string"))
    daily = daily.withColumn("_collected_at", collected_at)
    wrapped = (daily.filter(F.col("data").isNotNull())
               .select("_source_file", "load_date", F.col("ingestion.media_id").alias("_request_id"),
                       "_collected_at", F.explode("data").alias("row"))
               .select("row.*", "_request_id", "_source_file", "load_date", "_collected_at"))
    reject_rows(wrapped, F.col("_request_id").isNull(), "daily_stats: missing request metadata")
    legacy = daily.filter(F.col("data").isNull()).drop("data", "ingestion")
    daily = wrapped.unionByName(legacy, allowMissingColumns=True)
if "_request_id" not in daily.columns:
    daily = daily.withColumn("_request_id", F.lit(None).cast("string"))
require_columns(daily, ["date", "load_count", "play_count", "hours_watched"], "daily_stats")
legacy_id = F.regexp_extract("_source_file", r"/([a-z0-9]{10})(?:_[a-zA-Z0-9-]+)?\.json$", 1)
daily = daily.withColumn("_attribution_id", F.coalesce("_request_id", legacy_id))
if "media_id" in daily.columns:
    reject_rows(daily, F.col("media_id").isNotNull()
                & (F.col("media_id") != F.col("_attribution_id")),
                "daily_stats: API video ID conflicts with ingestion metadata")
daily = daily.withColumn("media_id", F.col("_attribution_id"))
daily = daily.withColumn("date", F.to_date("date"))
validate_business_keys(daily, ["media_id", "date"], "daily_stats", string_keys=["media_id"])
reject_rows(daily, F.col("media_id").isNull() | ~F.col("media_id").isin(*MEDIA_IDS)
            | F.col("date").isNull() | F.col("load_count").isNull()
            | F.col("play_count").isNull() | F.col("hours_watched").isNull()
            | (F.col("load_count") < 0) | (F.col("play_count") < 0)
            | (F.col("hours_watched") < 0), "daily_stats: invalid attribution, date or metrics")
if "_collected_at" not in daily.columns:
    daily = daily.withColumn("_collected_at", F.lit(None).cast("string"))
daily = daily.withColumn("_observed_at", F.to_timestamp("_collected_at"))
reject_rows(daily, F.col("_collected_at").isNotNull() & F.col("_observed_at").isNull(),
            "daily_stats: invalid collection timestamp")
daily = daily.withColumn("_snapshot_at", F.coalesce("_observed_at", F.col("load_date").cast("timestamp")))
silver_daily = daily.select("media_id", "date", "load_count", "play_count", "hours_watched", "_snapshot_at")
silver_daily = deduplicate_records(silver_daily, ["media_id", "date"], "daily_stats",
                                   order_col="_snapshot_at", string_keys=["media_id"]).drop("_snapshot_at")
save_silver(silver_daily, "daily_stats")


spark.stop()

