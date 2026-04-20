-- dbt/tests/assert_reddit_no_roas.sql
-- Reddit rows must never have a ROAS value (not natively available).
-- Any non-zero ROAS on Reddit rows indicates a transformer bug.

SELECT *
FROM {{ ref('stg_paid_media') }}
WHERE platform = 'reddit'
  AND roas > 0


-- dbt/tests/assert_attribution_windows.sql
-- Each platform must use its correct attribution window — no cross-contamination.
-- Reddit:         always 30d_click
-- TikTok / Meta:  always 7d_click_1d_view

SELECT platform, attribution_window, COUNT(*) AS rows
FROM {{ ref('stg_paid_media') }}
WHERE (platform = 'reddit'          AND attribution_window != '30d_click')
   OR (platform IN ('tiktok','instagram_meta') AND attribution_window != '7d_click_1d_view')
GROUP BY 1, 2
HAVING COUNT(*) > 0


-- dbt/tests/assert_no_future_dates.sql
-- No data should have dates in the future — indicates clock/timezone issues.

SELECT 'paid_media' AS source, date FROM {{ ref('stg_paid_media') }} WHERE date > CURRENT_DATE()
UNION ALL
SELECT 'installs',  install_date FROM {{ ref('stg_installs') }}   WHERE install_date > CURRENT_DATE()
UNION ALL
SELECT 'organic',   date         FROM {{ ref('stg_organic') }}    WHERE date > CURRENT_DATE()


-- dbt/tests/assert_clicks_le_impressions.sql
-- Clicks cannot exceed impressions — data integrity check.

SELECT *
FROM {{ ref('stg_paid_media') }}
WHERE clicks > impressions
  AND impressions > 0


-- dbt/tests/assert_instagram_watch_time_reasonable.sql
-- Instagram watch time should be between 1s and 120s after ms→s conversion.
-- Values outside this range suggest the conversion was not applied.

SELECT *
FROM {{ ref('stg_organic') }}
WHERE platform = 'instagram'
  AND avg_watch_time_s IS NOT NULL
  AND (avg_watch_time_s < 1 OR avg_watch_time_s > 120)


-- dbt/tests/assert_tiktok_watch_time_reasonable.sql
-- TikTok watch time in seconds. Should be between 1s and 600s.
-- Values > 1000 suggest the raw milliseconds were not converted.

SELECT *
FROM {{ ref('stg_organic') }}
WHERE platform = 'tiktok'
  AND avg_watch_time_s IS NOT NULL
  AND (avg_watch_time_s < 1 OR avg_watch_time_s > 600)


-- dbt/tests/assert_funnel_ordering.sql
-- Funnel stage counts must be monotonically non-increasing:
--   installs >= registrations >= first_activities >= paywall_views >= trials >= subscriptions

SELECT *
FROM {{ ref('mart_funnel_by_channel') }}
WHERE registrations    > installs
   OR first_activities > registrations
   OR paywall_views    > first_activities
   OR trials           > paywall_views
   OR subscriptions    > trials


-- dbt/tests/assert_ltv_not_negative.sql
-- LTV per install must be >= 0 for all rows.

SELECT *
FROM {{ ref('mart_ltv_cohort') }}
WHERE ltv_per_install < 0


-- dbt/tests/assert_organic_engagement_rate_bounded.sql
-- Engagement rate must be between 0 and 1 (0% to 100%).
-- Values > 1 indicate calculation error (wrong denominator).

SELECT *
FROM {{ ref('stg_organic') }}
WHERE blended_engagement_rate > 1
   OR blended_engagement_rate < 0


-- dbt/tests/assert_skan_no_user_data.sql
-- SKAN install rows (iOS ATT-denied) must not have a customer_user_id.
-- If they do, the SKAN attribution flag was not set correctly.

SELECT *
FROM {{ ref('stg_installs') }}
WHERE is_skan = TRUE
  AND customer_user_id IS NOT NULL
  AND TRIM(customer_user_id) != ''
