-- Rebuild curated.daily_city_stats for the given cities from staging.
-- Full per-city recompute: rolling windows depend on history, and the data is small
-- (one row per city per day), so correctness beats incremental cleverness here.
-- RANGE frames (not ROWS) so a missing day shrinks the window instead of silently
-- pulling in an older day; days_in_*_window exposes how complete each window is.
WITH base AS (
    SELECT
        city_id,
        date,
        temperature_max_c,
        temperature_min_c,
        temperature_mean_c,
        precipitation_mm,
        wind_speed_max_kmh
    FROM staging.daily_weather
    WHERE city_id = ANY(%(city_ids)s)
),

windowed AS (
    SELECT
        b.*,
        b.temperature_max_c - b.temperature_min_c                   AS temperature_range_c,
        AVG(b.temperature_mean_c) OVER w7                           AS temperature_mean_7d_avg_c,
        AVG(b.temperature_mean_c) OVER w30                          AS temperature_mean_30d_avg_c,
        SUM(b.precipitation_mm) OVER w7                             AS precipitation_7d_sum_mm,
        SUM(b.precipitation_mm) OVER w30                            AS precipitation_30d_sum_mm,
        COUNT(*) OVER w7                                            AS days_in_7d_window,
        COUNT(*) OVER w30                                           AS days_in_30d_window,
        LAG(b.date) OVER by_city                                    AS prev_date,
        LAG(b.temperature_mean_c) OVER by_city                      AS prev_temperature_mean_c
    FROM base AS b
    WINDOW
        by_city AS (PARTITION BY b.city_id ORDER BY b.date),
        w7      AS (PARTITION BY b.city_id ORDER BY b.date
                    RANGE BETWEEN INTERVAL '6 days' PRECEDING AND CURRENT ROW),
        w30     AS (PARTITION BY b.city_id ORDER BY b.date
                    RANGE BETWEEN INTERVAL '29 days' PRECEDING AND CURRENT ROW)
)

INSERT INTO curated.daily_city_stats AS t (
    city_id, date,
    temperature_max_c, temperature_min_c, temperature_mean_c, temperature_range_c,
    temperature_mean_7d_avg_c, temperature_mean_30d_avg_c, temperature_anomaly_30d_c,
    temperature_mean_change_c,
    precipitation_mm, precipitation_7d_sum_mm, precipitation_30d_sum_mm,
    wind_speed_max_kmh,
    days_in_7d_window, days_in_30d_window,
    refreshed_at
)
SELECT
    city_id,
    date,
    temperature_max_c,
    temperature_min_c,
    temperature_mean_c,
    temperature_range_c,
    ROUND(temperature_mean_7d_avg_c, 2),
    ROUND(temperature_mean_30d_avg_c, 2),
    ROUND(temperature_mean_c - temperature_mean_30d_avg_c, 2),
    CASE WHEN prev_date = date - 1 THEN temperature_mean_c - prev_temperature_mean_c END,
    precipitation_mm,
    precipitation_7d_sum_mm,
    precipitation_30d_sum_mm,
    wind_speed_max_kmh,
    days_in_7d_window,
    days_in_30d_window,
    now()
FROM windowed
ON CONFLICT (city_id, date) DO UPDATE SET
    temperature_max_c          = EXCLUDED.temperature_max_c,
    temperature_min_c          = EXCLUDED.temperature_min_c,
    temperature_mean_c         = EXCLUDED.temperature_mean_c,
    temperature_range_c        = EXCLUDED.temperature_range_c,
    temperature_mean_7d_avg_c  = EXCLUDED.temperature_mean_7d_avg_c,
    temperature_mean_30d_avg_c = EXCLUDED.temperature_mean_30d_avg_c,
    temperature_anomaly_30d_c  = EXCLUDED.temperature_anomaly_30d_c,
    temperature_mean_change_c  = EXCLUDED.temperature_mean_change_c,
    precipitation_mm           = EXCLUDED.precipitation_mm,
    precipitation_7d_sum_mm    = EXCLUDED.precipitation_7d_sum_mm,
    precipitation_30d_sum_mm   = EXCLUDED.precipitation_30d_sum_mm,
    wind_speed_max_kmh         = EXCLUDED.wind_speed_max_kmh,
    days_in_7d_window          = EXCLUDED.days_in_7d_window,
    days_in_30d_window         = EXCLUDED.days_in_30d_window,
    refreshed_at               = EXCLUDED.refreshed_at
-- Skip no-op writes so refreshed_at means "values last changed".
WHERE (
    t.temperature_max_c, t.temperature_min_c, t.temperature_mean_c, t.temperature_range_c,
    t.temperature_mean_7d_avg_c, t.temperature_mean_30d_avg_c, t.temperature_anomaly_30d_c,
    t.temperature_mean_change_c, t.precipitation_mm, t.precipitation_7d_sum_mm,
    t.precipitation_30d_sum_mm, t.wind_speed_max_kmh, t.days_in_7d_window, t.days_in_30d_window
) IS DISTINCT FROM (
    EXCLUDED.temperature_max_c, EXCLUDED.temperature_min_c, EXCLUDED.temperature_mean_c,
    EXCLUDED.temperature_range_c, EXCLUDED.temperature_mean_7d_avg_c,
    EXCLUDED.temperature_mean_30d_avg_c, EXCLUDED.temperature_anomaly_30d_c,
    EXCLUDED.temperature_mean_change_c, EXCLUDED.precipitation_mm,
    EXCLUDED.precipitation_7d_sum_mm, EXCLUDED.precipitation_30d_sum_mm,
    EXCLUDED.wind_speed_max_kmh, EXCLUDED.days_in_7d_window, EXCLUDED.days_in_30d_window
);
