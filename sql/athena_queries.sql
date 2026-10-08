-- =====================================================================
-- Wistia Video Analytics Pipeline - Athena queries
-- Workgroup: wistia-workgroup   Database: wistia_gold
-- =====================================================================

-- 1. Row counts (reconciliation with the Glue run)
SELECT 'dim_media' AS table_name, COUNT(*) AS row_count FROM dim_media
UNION ALL SELECT 'dim_visitor', COUNT(*) FROM dim_visitor
UNION ALL SELECT 'fact_media_daily', COUNT(*) FROM fact_media_daily
UNION ALL SELECT 'fact_media_engagement', COUNT(*) FROM fact_media_engagement;

-- 2. Play rate per channel (fact_media_daily + dim_media)
--    CAST to DOUBLE: integer / integer is an integer in Athena
SELECT m.channel,
       SUM(f.load_count)                                               AS loads,
       SUM(f.play_count)                                               AS plays,
       ROUND(CAST(SUM(f.play_count) AS DOUBLE) / SUM(f.load_count), 4) AS play_rate
FROM fact_media_daily f
JOIN dim_media m ON f.media_id = m.media_id
GROUP BY m.channel;

-- 3. Engagement per channel (fact_media_engagement + dim_media)
SELECT m.channel,
       COUNT(DISTINCT e.visitor_id)             AS viewers,
       SUM(e.play_count)                        AS plays,
       ROUND(AVG(e.watched_percent) * 100, 1)   AS avg_watched_pct,
       ROUND(SUM(e.total_watch_time) / 3600, 1) AS hours_watched
FROM fact_media_engagement e
JOIN dim_media m ON e.media_id = m.media_id
GROUP BY m.channel;

-- 4. Monthly trend per channel (loads, plays, play rate)
SELECT m.channel,
       date_trunc('month', f."date")                                   AS month,
       SUM(f.load_count)                                               AS loads,
       SUM(f.play_count)                                               AS plays,
       ROUND(CAST(SUM(f.play_count) AS DOUBLE) / NULLIF(SUM(f.load_count), 0), 4) AS play_rate
FROM fact_media_daily f
JOIN dim_media m ON f.media_id = m.media_id
GROUP BY m.channel, date_trunc('month', f."date")
ORDER BY m.channel, month;

-- 5. Top 10 regions by plays (fact + dim_visitor)
SELECT v.country, v.region,
       COUNT(DISTINCT e.visitor_id) AS viewers,
       SUM(e.play_count)            AS plays
FROM fact_media_engagement e
JOIN dim_visitor v ON e.visitor_id = v.visitor_id
GROUP BY v.country, v.region
ORDER BY plays DESC
LIMIT 10;

-- 6. Viewing platform mix (in-app browsers: Facebook, Instagram, TikTok ...)
SELECT v.browser, v.platform, v.is_mobile,
       COUNT(DISTINCT e.visitor_id) AS viewers,
       SUM(e.play_count)            AS plays
FROM fact_media_engagement e
JOIN dim_visitor v ON e.visitor_id = v.visitor_id
GROUP BY v.browser, v.platform, v.is_mobile
ORDER BY plays DESC
LIMIT 15;

-- 7. Data quality: every fact visitor exists in dim_visitor (expect 0)
SELECT COUNT(*) AS missing_visitors
FROM fact_media_engagement e
LEFT JOIN dim_visitor v ON e.visitor_id = v.visitor_id
WHERE v.visitor_id IS NULL;

-- 8. Data quality: one row per video per day (expect no rows)
SELECT media_id, "date", COUNT(*) AS cnt
FROM fact_media_daily
GROUP BY media_id, "date"
HAVING COUNT(*) > 1;

-- 9. Freshness: latest day loaded per video (should be yesterday, UTC)
SELECT media_id, MAX("date") AS latest_day
FROM fact_media_daily
GROUP BY media_id;
