-- Resolve releases/current.json and select its database once per report.
-- Project definition v1; UTC window [2026-09-01, 2026-10-01).
-- Replace dates with the identical window used in the Wistia comparison.
SELECT media_id,
       SUM(load_count) AS loads,
       SUM(play_count) AS plays,
       CASE WHEN SUM(load_count) = 0 THEN 0.0
            ELSE CAST(SUM(play_count) AS DOUBLE) / SUM(load_count) END AS project_play_rate,
       SUM(hours_watched) AS daily_hours_watched
FROM fact_media_daily
WHERE date >= DATE '2026-09-01' AND date < DATE '2026-10-01'
GROUP BY media_id;

-- Weight aggregated watched percentages by play_count, not by visitor-day rows.
SELECT media_id,
       SUM(play_count) AS event_plays,
       SUM(watched_percent * play_count) / NULLIF(SUM(play_count), 0) AS event_avg_watched_fraction,
       SUM(total_watch_time) / 3600.0 AS event_estimated_hours_watched
FROM fact_media_engagement
WHERE date >= DATE '2026-09-01' AND date < DATE '2026-10-01'
GROUP BY media_id;
