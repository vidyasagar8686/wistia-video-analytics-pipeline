"""
gold_build.py  -  Silver -> Gold (star schema)
Wistia Video Analytics Pipeline (PySpark)

Reads the Silver tables, applies the business rules, and saves the star schema to Gold as Parquet.
Reads an isolated Silver version. Validates before writing an immutable Gold release.

  Gold table               Grain (one row per)          Built from                         Rows (2026-10-07)
  dim_media      (SCD1)    video                        silver/media + channel rule                2
  dim_visitor    (SCD1)    visitor who played our videos silver/events (latest location)
                                                         + silver/visitors (browser info)     1,158
  fact_media_daily         video x day                  silver/daily_stats + play_rate         1,490
  fact_media_engagement    video x visitor x day        silver/events aggregated
                                                         + dim_media duration                  1,170

Run on the laptop:   python transform\\gold_build.py   (after silver_clean.py)
In AWS Glue later:   change BASE_PATH to "s3://wistia-data-222238779166"
"""

import os
import sys
import re
from urllib.parse import urlparse
from pipeline_support import read_pointer, publish_catalog, publish_local

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
RUN_ID = sys.argv[sys.argv.index("--run_id") + 1] if "--run_id" in sys.argv else "local"
STARTED_AT = sys.argv[sys.argv.index("--release_started_at") + 1] if "--release_started_at" in sys.argv else None
if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", RUN_ID):
    raise ValueError("invalid run_id")
if BASE_PATH.startswith("s3://") and RUN_ID == "local":
    raise ValueError("AWS runs require an explicit unique --run_id")
if BASE_PATH.startswith("s3://") and not STARTED_AT:
    raise ValueError("AWS publication requires --release_started_at from Step Functions")
SILVER = f"{BASE_PATH}/silver_versions/{RUN_ID}"
GOLD = f"{BASE_PATH}/gold_versions/{RUN_ID}"
if BASE_PATH.startswith("s3://"):
    import boto3
    s3 = boto3.client("s3")
    glue = boto3.client("glue")
    parsed = urlparse(BASE_PATH)
    prefix = parsed.path.strip("/")
    prefix = prefix + "/" if prefix else ""
    expected_etag = read_pointer(s3, parsed.netloc, prefix + "releases/current.json", STARTED_AT)

# ---------------------------------------------------------------------
# Spark setup
# ---------------------------------------------------------------------
# spark = SparkSession.builder.master("local[*]").appName("wistia_gold_build").getOrCreate()
spark = SparkSession.builder.appName("wistia_gold_build").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")


def read_silver(table):
    """Parquet keeps the data types, so no conversions are needed when reading Silver."""
    return spark.read.parquet(f"{SILVER}/{table}")


def save_gold(df, table):
    """Stage a Gold table without overwriting an existing version."""
    df.write.mode("errorifexists").parquet(f"{GOLD}/{table}")
    print(f"gold {table:<22} saved: {df.count():>6}")


# =====================================================================
# 1. dim_media  (SCD1: current values only)
#    Business rule: channel per video (Wistia has no channel field).
#    ASSUMPTION for SME approval: vertical 8hunphufxp = Facebook, horizontal 9k4tbcdfg0 = YouTube.
#    SQL: CASE WHEN media_id = '8hunphufxp' THEN 'Facebook' WHEN ... ELSE 'Unknown' END
# =====================================================================
dim_media = read_silver("media").withColumn(
    "channel",
    F.when(F.col("media_id") == "8hunphufxp", "Facebook")
     .when(F.col("media_id") == "9k4tbcdfg0", "YouTube")
     .otherwise("Unknown"),
)


# =====================================================================
# 2. fact_media_daily  (one row per video per day)
#    Business rule: play_rate = plays / loads (0 when there were no loads).
#    Loads exist only per video per day, so play_rate lives in this fact.
# =====================================================================
fact_media_daily = read_silver("daily_stats").withColumn(
    "play_rate",
    F.round(
        F.when(F.col("load_count") > 0, F.col("play_count") / F.col("load_count")).otherwise(0),
        4,
    ),
)


# =====================================================================
# 3. dim_visitor  (SCD1, one row per visitor who played our videos)
#    Business rule: a visitor's location = the location of their MOST RECENT play.
#    Built from events (not from the account-wide visitors list), so every visitor
#    in fact_media_engagement is guaranteed to exist here (referential integrity).
# =====================================================================
silver_events = read_silver("events")

# Latest play per visitor  (SQL: ROW_NUMBER() OVER (PARTITION BY visitor_key ORDER BY event_at DESC) = 1)
w_loc = Window.partitionBy("visitor_key").orderBy(F.col("event_at").desc())
latest_location = (silver_events
    .withColumn("rn", F.row_number().over(w_loc))
    .filter(F.col("rn") == 1)
    .select("visitor_key", "ip_masked", "country", "region", "city")
)

# Add browser info from the visitors list  (SQL: LEFT JOIN silver_visitors ON visitor_key)
dim_visitor = (latest_location
    .join(read_silver("visitors"), on="visitor_key", how="left")
    .withColumnRenamed("visitor_key", "visitor_id")          # brief's name
)


# =====================================================================
# 4. fact_media_engagement  (aggregated: one row per video x visitor x day)
#    play_count       = COUNT(*)                               number of plays
#    watched_percent  = AVG(percent_viewed)                    average share of the video watched
#    total_watch_time = SUM(percent_viewed * duration_seconds) seconds watched
# =====================================================================
plays = (silver_events
    .withColumn("date", F.to_date("event_at"))                              # day of the play
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


# =====================================================================
# 5. Reconciliation checks (the run fails if any check fails)
# =====================================================================
total_plays = fact_media_engagement.agg(F.sum("play_count")).collect()[0][0]
silver_plays = silver_events.count()
print(f"check: plays in fact = {total_plays}, events in silver = {silver_plays}")
if total_plays != silver_plays:
    raise RuntimeError(f"Reconciliation failed: {total_plays} plays in fact vs {silver_plays} events")

missing_visitors = fact_media_engagement.join(dim_visitor, on="visitor_id", how="left_anti").count()
print(f"check: fact visitors missing from dim_visitor = {missing_visitors}")
if missing_visitors > 0:
    raise RuntimeError(f"Referential integrity failed: {missing_visitors} visitors missing from dim_visitor")

missing_media = (fact_media_daily.select("media_id").union(plays.select("media_id"))
                 .join(dim_media.select("media_id"), on="media_id", how="left_anti").count())
if missing_media:
    raise RuntimeError("Fact media missing from dim_media")
if plays.filter(F.col("duration_seconds").isNull() | (F.col("duration_seconds") <= 0)).limit(1).count():
    raise RuntimeError("Missing or invalid video duration")
if silver_plays == 0 or dim_media.count() != 2:
    raise RuntimeError("Unexpected empty events or missing configured media")

# Only validated data is written, and all output is versioned. Existing release
# locations are never overwritten. Consumers pin releases/current.json once.
frames = {"dim_media": dim_media, "fact_media_daily": fact_media_daily,
          "dim_visitor": dim_visitor, "fact_media_engagement": fact_media_engagement}
definitions = {}
for name, frame in frames.items():
    save_gold(frame, name)
    staged = spark.read.parquet(f"{GOLD}/{name}")
    if staged.count() != frame.count():
        raise RuntimeError(f"Staged row count mismatch for {name}")
    definitions[name] = {"location": f"{GOLD}/{name}/",
                         "columns": [{"Name": f.name, "Type": f.dataType.simpleString()}
                                     for f in staged.schema.fields]}
if BASE_PATH.startswith("s3://"):
    manifest = publish_catalog(glue, s3, parsed.netloc, prefix, RUN_ID, definitions, expected_etag, STARTED_AT)
else:
    manifest = publish_local(BASE_PATH, RUN_ID, definitions)
print(f"Published validated release: {manifest['run_id']}")

print("gold build finished successfully")

spark.stop()

