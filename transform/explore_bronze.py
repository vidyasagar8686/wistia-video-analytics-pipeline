import os
import sys
from pyspark.sql import Window, functions as F

os.environ["PYSPARK_PYTHON"] = sys.executable   # use this venv's Python (Windows)

from pyspark.sql import SparkSession

# ---------- Spark setup ----------
spark = SparkSession.builder.master("local[*]").appName("explore_bronze").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark.conf.set("spark.sql.session.timeZone", "UTC")   # show/process timestamps in UTC


# # =====================================================================
# # MEDIA  →  Silver media  →  Gold dim_media
# # =====================================================================

# # Read all raw media files (every load_date folder)
# media = spark.read.option("multiLine", True).json("data/bronze/media")

# # Keep only the latest snapshot per video
# # (SQL: ROW_NUMBER() OVER (PARTITION BY hashed_id ORDER BY load_date DESC) ... WHERE rn = 1)
# w = Window.partitionBy("hashed_id").orderBy(F.col("load_date").desc())
# latest = media.withColumn("rn", F.row_number().over(w)).filter(F.col("rn") == 1)

# # Rename columns to the brief's names
# dim_media = latest.select(
#     F.col("hashed_id").alias("media_id"),
#     F.col("name").alias("title"),
#     F.col("created").alias("created_at"),
#     F.col("share_link.url").alias("url"),
#     F.col("duration").alias("duration_seconds"),
#     F.col("updated").alias("updated_at")
# )

# # Convert text dates to real timestamps (SQL: CAST(... AS TIMESTAMP))
# dim_media = (dim_media
#     .withColumn("created_at", F.to_timestamp(F.col("created_at")))
#     .withColumn("updated_at", F.to_timestamp(F.col("updated_at")))
# )

# # Save the cleaned table to Silver
# dim_media.write.mode("overwrite").parquet("data/silver/media")
# print("silver media saved")

# # Add channel (business rule / SME assumption) (SQL: CASE WHEN ... END)
# dim_media = dim_media.withColumn(
#     "channel",
#     F.when(F.col("media_id") == "8hunphufxp", "Facebook")
#      .when(F.col("media_id") == "9k4tbcdfg0", "YouTube")
#      .otherwise("Unknown")
# )
# dim_media.show(truncate=False)

# # Save the finished table to Gold
# dim_media.write.mode("overwrite").parquet("data/gold/dim_media")
# print("gold dim_media saved")


# # =====================================================================
# # EVENTS  →  look at the raw data first
# # =====================================================================

# events = spark.read.option("multiLine", True).json("data/bronze/events")
# events.createOrReplaceTempView("events")

# # Columns and their types (like DESCRIBE TABLE)
# spark.sql("DESCRIBE events").show(50, truncate=False)

# # convert the received_at string to timestamp
# events_clean = events.withColumn("received_at", F.to_timestamp("received_at"))

# events_clean.select("event_key","media_id","received_at").printSchema()
# events_clean.select("event_key", "media_id", "received_at").show(5, truncate=False)


# # find the duplicates on event key
# events_clean.createOrReplaceTempView("events_clean")
# spark.sql("""
#     SELECT 
#         media_id,
#         count(*) AS total_counts,
#         count(DISTINCT event_key) AS unique_events
#         FROM events_clean
#         GROUP BY media_id
# """).show()

# # find the latest records 
# w_ev = Window.partitionBy("event_key").orderBy(F.col("load_date").desc())
# events_clean = events_clean.withColumn("rn", F.row_number().over(w_ev)).filter(F.col("rn")==1).drop("rn")
# print("events after removing duplicates:", events_clean.count())

# # mask the IP address
# events_clean = events_clean.withColumn(
#     "ip", F.regexp_replace(F.col("ip"), r"\.\d+$", ".XXX")
# )
# events_clean.select("event_key","ip", "country","city").show(5,truncate=False)

# # pick and rename the columns for Silver
# events_clean = events_clean.select(
#     F.col("event_key"),
#     F.col("media_id"),
#     F.col("visitor_key"),
#     F.col("received_at").alias("event_at"),
#     F.col("percent_viewed"),
#     F.col("ip").alias("ip_masked"),
#     F.col("country"),
#     F.col("region"),
#     F.col("city"),
#     F.col("user_agent_details.browser").alias("browser"),
#     F.col("user_agent_details.platform").alias("platform"),
#     F.col("user_agent_details.mobile").alias("is_mobile"),
# )
# events_clean.show(5, truncate=False)

# # empty text handle to Null
# events_clean = events_clean.replace("", None)

# # copy from bronze to silver
# events_clean.write.mode("overwrite").parquet("data/silver/events")
# print("silver event saved", events_clean.count())

# # ===========================================================
# # VISITORS --> look at the raw data first
# # ==========================================================
# visitors = spark.read.option("multiline", True).json("data/bronze/visitors")
# visitors.createOrReplaceTempView("visitors")
# spark.sql("DESCRIBE visitors").show(50,truncate=False)

# visitors_clean = (visitors
#     .withColumn("created_at", F.to_timestamp("created_at"))
#     .withColumn("last_active_at", F.to_timestamp("last_active_at"))
# )

# visitors_clean.select("visitor_key","created_at","last_active_at").printSchema()

# # duplicate check
# visitors_clean.createOrReplaceTempView

# spark.sql("""
#     SELECT
#         count(*) AS total_rows,
#         count(DISTINCT visitor_key) AS unique_rows
#         FROM visitors
# """).show()

# # remove the dups and get the latest records

# w_vs = Window.partitionBy("visitor_key").orderBy(F.col("last_active_at").desc())
# visitors_clean = visitors_clean.withColumn("rn", F.row_number().over(w_vs)).filter(F.col("rn")==1).drop("rn")
# print("visitors after removing duplicates:", visitors_clean.count())

# # rename the clean columns

# visitors_clean = visitors_clean.select(
#     F.col("visitor_key"),
#     F.col("created_at"),
#     F.col("last_active_at"),
#     F.col("load_count"),
#     F.col("play_count"),
#     F.col("user_agent_details.browser").alias("browser"),
#     F.col("user_agent_details.platform").alias("platform"),
#     F.col("user_agent_details.mobile").alias("is_mobile"),
# )

# # empty text null 
# visitors_clean = visitors_clean.replace("",None)


# # copy into silver layer

# visitors_clean.write.mode("overwrite").parquet("data/silver/visitors")
# print("silver visitors saved:", visitors_clean.count())


# # =================================================================
# #  DAILY STATS --> look at the raw data first
# # =================================================================

# daily = spark.read.option("multiline", True).json("data/bronze/daily_stats")
# daily.createOrReplaceTempView("daily")

# spark.sql("DESCRIBE daily").show(50, truncate=False)
# daily.show(5,truncate=False)

# # convert date string to date 
# daily_clean = daily.withColumn("date", F.to_date("date"))
# daily_clean.printSchema()

# # add medium_id columns 
# daily_clean = daily_clean.withColumn(
#     "media_id", F.regexp_extract(F.input_file_name(), r"([a-z0-9]{10})(_\d+)?\.json$", 1)
# )
# daily_clean.select("media_id","date","load_count","play_count").show(5)

# # duplicate check

# daily_clean.createOrReplaceTempView("daily_clean")

# spark.sql("""
#     SELECT
#         media_id,
#         count(*) AS total_rows,
#         count(DISTINCT date) AS unique_rows
#         FROM daily_clean
#         GROUP BY media_id
# """).show()

# # remove dups and keep latest records
# w_da = Window.partitionBy("media_id", "date").orderBy(F.col("load_date").desc())
# daily_clean = daily_clean.withColumn("rn", F.row_number().over(w_da)).filter(F.col("rn")==1).drop("rn")
# print("daily rows after removing duplicates:", daily_clean.count())

# # Keep only the final columns (drop file/helper columns)
# daily_clean = daily_clean.select(
#     F.col("media_id"),
#     F.col("date"),
#     F.col("load_count"),
#     F.col("play_count"),
#     F.col("hours_watched"),
# )

# # Save to Silver
# daily_clean.write.mode("overwrite").parquet("data/silver/daily_stats")
# print("silver daily_stats saved:", daily_clean.count())


# =========================================================
# Gold fact_daily_media (from silver daily_stats)

silver_daily = spark.read.parquet("data/silver/daily_stats")
silver_daily.printSchema()
silver_daily.show(5)


# calculate the play rate
fact_media_daily = silver_daily.withColumn(
    "play_rate",
    F.round(
        F.when(F.col("load_count")>0, F.col("play_count")/ F.col("load_count")).otherwise(0),
        4,
    ),
)

fact_media_daily.show(5)

# save to gold 
fact_media_daily.write.mode("overwrite").parquet("data/gold/fact_media_daily")
print("gold fact media daily saved", fact_media_daily.count())

# =====================================================================
# GOLD: dim_visitor  (Silver visitors + latest location from Silver events)
# =====================================================================

silver_events = spark.read.parquet("data/silver/events")


w_loc = Window.partitionBy("visitor_key").orderBy(F.col("event_at").desc())
latest_locations = (silver_events
    .withColumn("rn", F.row_number().over(w_loc))
    .filter(F.col("rn") ==1)
    .select("visitor_key","ip_masked","country", "region", "city")
) 

print("visitors city:",latest_locations.count())
latest_locations.show(5, truncate=False)


# all visitords browser infor
silver_visitors = spark.read.parquet("data/silver/visitors")

dim_visitor = latest_locations.join(silver_visitors, on="visitor_key", how="left")
print("dim_visitor:", dim_visitor.count())
dim_visitor.show(5, truncate=False)

# rename the columns
dim_visitor = dim_visitor.withColumnRenamed("visitor_key", "visitor_id")

dim_visitor.write.mode("overwrite").parquet("data/gold/dim_visitor")
print("gold dim visitor saved:", dim_visitor.count())


# ==========================================================================
# GOLD: fact_media_engagement (aggregate silver events per video * visitor * day)
# ==========================================================================
events_f = spark.read.parquet("data/silver/events")
dim_media_g = spark.read.parquet("data/gold/dim_media")

events_f = events_f.withColumn("date", F.to_date("event_at"))

events_f = events_f.join(dim_media_g.select("media_id", "duration_seconds"), on="media_id", how="left")

events_f.select("media_id","visitor_key", "event_at", "date","percent_viewed","duration_seconds").show(5,truncate=False)


# one row per video * visitor * day (Group by media_id, visitor_key, date)
fact_media_engagment = (
    events_f
    .groupBy("media_id","visitor_key","date")
    .agg(
        F.count("*").alias("play_count"),
        F.avg("percent_viewed").alias("watched_percent"),
        F.sum(F.col("percent_viewed") * F.col("duration_seconds")).alias("total_watch_time"),
    )
    .withColumnRenamed("visitor_key", "visitor_id")
)

print("fact_media_engagement rows:", fact_media_engagment.count())
fact_media_engagment.show(5, truncate=False)

fact_media_engagment.write.mode("overwrite").parquet("data/gold/fact_media_engagement")
print("gold fact_media_mangment saved:", fact_media_engagment.count())

spark.read.parquet("data/gold/fact_media_engagement").agg(F.sum("play_count")).show()

spark.stop()