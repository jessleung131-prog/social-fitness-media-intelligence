-- ============================================================
-- dbt/models/staging/stg_paid_media.sql
-- Light cleaning on cdm_paid_media_daily.
-- Materialized as VIEW — no storage cost, always fresh.
-- ============================================================

{{ config(materialized='view') }}

SELECT
  CAST(date AS DATE)                          AS date,
  LOWER(TRIM(platform))                       AS platform,
  channel_type,
  NULLIF(TRIM(campaign_id), '')               AS campaign_id,
  NULLIF(TRIM(campaign_name), '')             AS campaign_name,
  LOWER(TRIM(objective))                      AS objective,
  NULLIF(TRIM(ad_group_id), '')               AS ad_group_id,
  NULLIF(TRIM(ad_group_name), '')             AS ad_group_name,
  NULLIF(TRIM(ad_id), '')                     AS ad_id,
  NULLIF(TRIM(ad_name), '')                   AS ad_name,
  UPPER(TRIM(country))                        AS country,

  -- Volume
  CAST(COALESCE(impressions, 0) AS INT64)     AS impressions,
  CAST(COALESCE(clicks, 0) AS INT64)          AS clicks,
  ROUND(CAST(COALESCE(cost_usd, 0) AS FLOAT64), 2) AS cost_usd,
  CAST(COALESCE(installs, 0) AS INT64)        AS installs,
  CAST(COALESCE(conversions, 0) AS INT64)     AS conversions,
  ROUND(CAST(COALESCE(conversion_value_usd, 0) AS FLOAT64), 2) AS conversion_value_usd,
  CAST(COALESCE(video_views, 0) AS INT64)     AS video_views,
  CAST(COALESCE(video_completions, 0) AS INT64) AS video_completions,
  CAST(COALESCE(engagements, 0) AS INT64)     AS engagements,
  CAST(COALESCE(likes, 0) AS INT64)           AS likes,
  CAST(COALESCE(comments, 0) AS INT64)        AS comments,
  CAST(COALESCE(shares, 0) AS INT64)          AS shares,

  -- Efficiency (recomputed from actuals — more reliable than source values)
  SAFE_DIVIDE(CAST(COALESCE(cost_usd, 0) AS FLOAT64),
              NULLIF(CAST(impressions AS INT64), 0)) * 1000 AS cpm,
  SAFE_DIVIDE(CAST(COALESCE(cost_usd, 0) AS FLOAT64),
              NULLIF(CAST(clicks AS INT64), 0))             AS cpc,
  SAFE_DIVIDE(CAST(clicks AS INT64),
              NULLIF(CAST(impressions AS INT64), 0))        AS ctr,
  SAFE_DIVIDE(CAST(COALESCE(cost_usd, 0) AS FLOAT64),
              NULLIF(CAST(installs AS INT64), 0))           AS cpi,
  SAFE_DIVIDE(CAST(COALESCE(cost_usd, 0) AS FLOAT64),
              NULLIF(CAST(conversions AS INT64), 0))        AS cps,
  SAFE_DIVIDE(CAST(COALESCE(conversion_value_usd, 0) AS FLOAT64),
              NULLIF(CAST(COALESCE(cost_usd, 0) AS FLOAT64), 0)) AS roas,

  attribution_window,
  raw_source,
  _extracted_at,
  _pipeline_version

FROM {{ source('social_fitness_staging', 'cdm_paid_media_daily') }}
WHERE date IS NOT NULL
  AND CAST(COALESCE(cost_usd, 0) AS FLOAT64) >= 0


-- ============================================================
-- dbt/models/staging/stg_installs.sql
-- ============================================================

{{ config(materialized='view') }}

SELECT
  CAST(install_date AS DATE)              AS install_date,
  TRIM(appsflyer_id)                      AS appsflyer_id,
  NULLIF(TRIM(customer_user_id), '')      AS customer_user_id,
  LOWER(TRIM(media_source))               AS media_source,
  TRIM(channel)                           AS channel,
  NULLIF(TRIM(campaign_id), '')           AS campaign_id,
  LOWER(TRIM(platform))                   AS platform,
  UPPER(TRIM(country))                    AS country,
  TRIM(device_type)                       AS device_type,
  TRIM(att_status)                        AS att_status,
  CAST(COALESCE(is_retargeting, FALSE) AS BOOL)          AS is_retargeting,
  CAST(COALESCE(is_primary_attribution, TRUE) AS BOOL)   AS is_primary_attribution,
  CAST(COALESCE(is_skan, FALSE) AS BOOL)                 AS is_skan,
  CAST(COALESCE(registered, FALSE) AS BOOL)              AS registered,
  CAST(COALESCE(first_activity, FALSE) AS BOOL)          AS first_activity,
  CAST(COALESCE(paywall_viewed, FALSE) AS BOOL)          AS paywall_viewed,
  CAST(COALESCE(trial_started, FALSE) AS BOOL)           AS trial_started,
  CAST(COALESCE(subscribed, FALSE) AS BOOL)              AS subscribed,
  NULLIF(TRIM(subscription_type), '')                    AS subscription_type,
  ROUND(CAST(COALESCE(subscription_value_usd, 0) AS FLOAT64), 2) AS subscription_value_usd,
  CAST(days_to_subscribe AS INT64)                       AS days_to_subscribe,
  raw_source,
  _extracted_at

FROM {{ source('social_fitness_staging', 'cdm_installs') }}
WHERE is_primary_attribution = TRUE   -- deduplicate retargeting + acquisition
  AND install_date IS NOT NULL


-- ============================================================
-- dbt/models/staging/stg_organic.sql
-- ============================================================

{{ config(materialized='view') }}

SELECT
  CAST(date AS DATE)                      AS date,
  LOWER(TRIM(platform))                   AS platform,
  channel_type,
  TRIM(post_id)                           AS post_id,
  TRIM(content_type)                      AS content_type,
  TRIM(topic)                             AS topic,
  CAST(COALESCE(is_viral, FALSE) AS BOOL) AS is_viral,

  CAST(COALESCE(reach, 0) AS INT64)       AS reach,
  CAST(COALESCE(impressions, 0) AS INT64) AS impressions,
  CAST(COALESCE(likes, 0) AS INT64)       AS likes,
  CAST(COALESCE(comments, 0) AS INT64)    AS comments,
  CAST(COALESCE(shares, 0) AS INT64)      AS shares,
  CAST(COALESCE(saves, 0) AS INT64)       AS saves,
  CAST(COALESCE(engagements, 0) AS INT64) AS engagements,
  CAST(COALESCE(video_views, 0) AS INT64) AS video_views,

  CAST(avg_watch_time_s AS FLOAT64)       AS avg_watch_time_s,
  CAST(video_completion_rate AS FLOAT64)  AS video_completion_rate,
  CAST(COALESCE(follower_delta, 0) AS INT64) AS follower_delta,
  CAST(COALESCE(outbound_clicks, 0) AS INT64) AS outbound_clicks,

  -- Recompute blended engagement rate consistently
  SAFE_DIVIDE(
    CAST(COALESCE(likes, 0) + COALESCE(comments, 0) +
         COALESCE(shares, 0) + COALESCE(saves, 0) AS FLOAT64),
    NULLIF(CAST(reach AS INT64), 0)
  )                                       AS blended_engagement_rate,

  platform_engagement_rate,
  raw_source,
  _extracted_at

FROM {{ source('social_fitness_staging', 'cdm_organic_daily') }}
WHERE date IS NOT NULL
  AND CAST(COALESCE(reach, 0) AS INT64) >= 0
