"""
gold_build.py  -  Silver -> Gold (star schema)
Wistia Video Analytics Pipeline (PySpark)

Reads the Silver tables, applies the business rules, and saves the star schema to Gold as Parquet.
Always reads from Silver (never from Bronze). Full rebuild each run (overwrite).

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
SILVER = f"{BASE_PATH}/silver"
GOLD = f"{BASE_PATH}/gold"

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
    """Save a Gold table (full rebuild each run) and print its row count."""
    df.write.mode("overwrite").parquet(f"{GOLD}/{table}")
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
save_gold(dim_media, "dim_media")


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
save_gold(fact_media_daily, "fact_media_daily")


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
save_gold(dim_visitor, "dim_visitor")


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
save_gold(fact_media_engagement, "fact_media_engagement")


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

print("gold build finished successfully")

spark.stop()
