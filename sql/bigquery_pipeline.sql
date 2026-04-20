-- ============================================================
-- SQL Pipeline: Social Fitness App Analytics
-- Target: Google BigQuery
-- Schema: social_fitness
-- Run order: staging → marts → reports
-- ============================================================


-- ────────────────────────────────────────────────────────────
-- STAGING LAYER
-- Minimal cleaning; stays close to CDM structure
-- ────────────────────────────────────────────────────────────

-- stg_paid_media.sql
CREATE OR REPLACE VIEW social_fitness.stg_paid_media AS
SELECT
  PARSE_DATE('%Y-%m-%d', CAST(date AS STRING))   AS date,
  platform,
  channel_type,
  campaign_id,
  campaign_name,
  objective,
  ad_group_id,
  ad_group_name,
  ad_id,
  ad_name,
  country,
  CAST(impressions AS INT64)          AS impressions,
  CAST(clicks AS INT64)               AS clicks,
  ROUND(CAST(cost_usd AS FLOAT64), 2) AS cost_usd,
  CAST(installs AS INT64)             AS installs,
  CAST(conversions AS INT64)          AS conversions,
  ROUND(CAST(conversion_value_usd AS FLOAT64), 2) AS conversion_value_usd,
  CAST(video_views AS INT64)          AS video_views,
  CAST(video_completions AS INT64)    AS video_completions,
  CAST(engagements AS INT64)          AS engagements,
  attribution_window,
  raw_source
FROM `social_fitness.cdm_paid_media_daily`
WHERE date IS NOT NULL
  AND cost_usd > 0;


-- stg_organic.sql
CREATE OR REPLACE VIEW social_fitness.stg_organic AS
SELECT
  PARSE_DATE('%Y-%m-%d', CAST(date AS STRING)) AS date,
  platform,
  channel_type,
  post_id,
  content_type,
  topic,
  CAST(is_viral AS BOOL)              AS is_viral,
  CAST(reach AS INT64)                AS reach,
  CAST(impressions AS INT64)          AS impressions,
  ROUND(CAST(engagement_rate AS FLOAT64), 5) AS engagement_rate,
  CAST(likes AS INT64)                AS likes,
  CAST(comments AS INT64)             AS comments,
  CAST(shares AS INT64)               AS shares,
  CAST(saves AS INT64)                AS saves,
  CAST(video_views AS INT64)          AS video_views,
  CAST(avg_watch_time_s AS FLOAT64)   AS avg_watch_time_s,
  CAST(video_completion_rate AS FLOAT64) AS video_completion_rate,
  CAST(engagements AS INT64)          AS engagements,
  CAST(follower_delta AS INT64)       AS follower_delta,
  raw_source
FROM `social_fitness.cdm_organic_daily`;


-- stg_installs.sql
CREATE OR REPLACE VIEW social_fitness.stg_installs AS
SELECT
  PARSE_DATE('%Y-%m-%d', CAST(install_date AS STRING)) AS install_date,
  appsflyer_id,
  customer_user_id,
  media_source,
  channel,
  campaign_id,
  platform,
  country,
  device_type,
  CAST(is_retargeting AS BOOL)    AS is_retargeting,
  att_status,
  CAST(registered AS BOOL)        AS registered,
  CAST(first_activity AS BOOL)    AS first_activity,
  CAST(paywall_viewed AS BOOL)    AS paywall_viewed,
  CAST(trial_started AS BOOL)     AS trial_started,
  CAST(subscribed AS BOOL)        AS subscribed,
  subscription_type,
  ROUND(CAST(subscription_value_usd AS FLOAT64), 2) AS subscription_value_usd,
  CAST(days_to_subscribe AS INT64) AS days_to_subscribe
FROM `social_fitness.cdm_installs`;


-- ────────────────────────────────────────────────────────────
-- MART LAYER
-- Business-level aggregations
-- ────────────────────────────────────────────────────────────

-- mart_channel_performance_daily.sql
-- Cross-channel paid performance: one row per platform × campaign × day
CREATE OR REPLACE TABLE social_fitness.mart_channel_performance_daily AS
SELECT
  date,
  platform,
  campaign_id,
  campaign_name,
  objective,
  country,

  -- Volume
  SUM(impressions)   AS impressions,
  SUM(clicks)        AS clicks,
  SUM(cost_usd)      AS cost_usd,
  SUM(installs)      AS installs,
  SUM(conversions)   AS conversions,
  SUM(conversion_value_usd) AS revenue_usd,
  SUM(video_views)   AS video_views,
  SUM(engagements)   AS engagements,

  -- Efficiency
  SAFE_DIVIDE(SUM(cost_usd), SUM(impressions)) * 1000 AS cpm,
  SAFE_DIVIDE(SUM(cost_usd), SUM(clicks))             AS cpc,
  SAFE_DIVIDE(SUM(clicks),   SUM(impressions))         AS ctr,
  SAFE_DIVIDE(SUM(cost_usd), SUM(installs))            AS cpi,
  SAFE_DIVIDE(SUM(cost_usd), SUM(conversions))         AS cps,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd))         AS roas,

  -- Content efficiency
  SAFE_DIVIDE(SUM(video_views), SUM(impressions))      AS video_view_rate,
  SAFE_DIVIDE(SUM(engagements), SUM(impressions))      AS engagement_rate,

  MAX(attribution_window) AS attribution_window,
  CURRENT_TIMESTAMP()     AS _loaded_at

FROM social_fitness.stg_paid_media
GROUP BY 1,2,3,4,5,6;


-- mart_funnel_by_channel.sql
-- Full funnel conversion metrics, by acquisition channel and month
CREATE OR REPLACE TABLE social_fitness.mart_funnel_by_channel AS
WITH base AS (
  SELECT
    DATE_TRUNC(install_date, MONTH) AS cohort_month,
    channel,
    platform,
    country,
    COUNT(*)                                    AS installs,
    COUNTIF(registered)                         AS registrations,
    COUNTIF(first_activity)                     AS first_activities,
    COUNTIF(paywall_viewed)                     AS paywall_views,
    COUNTIF(trial_started)                      AS trials,
    COUNTIF(subscribed)                         AS subscriptions,
    SUM(subscription_value_usd)                 AS revenue_usd,
    AVG(CASE WHEN subscribed THEN days_to_subscribe END) AS avg_days_to_subscribe
  FROM social_fitness.stg_installs
  GROUP BY 1,2,3,4
)
SELECT
  *,
  SAFE_DIVIDE(registrations,   installs)       AS install_to_reg_rate,
  SAFE_DIVIDE(first_activities, registrations) AS reg_to_activation_rate,
  SAFE_DIVIDE(paywall_views,   first_activities) AS activation_to_paywall_rate,
  SAFE_DIVIDE(trials,          paywall_views)  AS paywall_to_trial_rate,
  SAFE_DIVIDE(subscriptions,   trials)         AS trial_to_sub_rate,
  SAFE_DIVIDE(subscriptions,   installs)       AS overall_cvr,
  SAFE_DIVIDE(revenue_usd,     installs)       AS ltv_per_install,
  CURRENT_TIMESTAMP()                          AS _loaded_at
FROM base;


-- mart_ltv_cohort.sql
-- Monthly cohort LTV: tracks cumulative subscriber revenue over time
CREATE OR REPLACE TABLE social_fitness.mart_ltv_cohort AS
SELECT
  DATE_TRUNC(install_date, MONTH)   AS cohort_month,
  channel,
  subscription_type,
  COUNT(*)                          AS installs,
  COUNTIF(subscribed)               AS subscribers,
  SUM(subscription_value_usd)       AS cohort_revenue_usd,
  SAFE_DIVIDE(
    SUM(subscription_value_usd),
    COUNTIF(subscribed)
  )                                 AS arpu_subscribers,
  SAFE_DIVIDE(
    SUM(subscription_value_usd),
    COUNT(*)
  )                                 AS ltv_per_install,
  AVG(CASE WHEN subscribed THEN days_to_subscribe END) AS avg_days_to_convert,
  CURRENT_TIMESTAMP()               AS _loaded_at
FROM social_fitness.stg_installs
GROUP BY 1,2,3;


-- mart_organic_performance.sql
-- Organic content summary by platform and content type
CREATE OR REPLACE TABLE social_fitness.mart_organic_performance AS
SELECT
  DATE_TRUNC(date, MONTH)   AS month,
  platform,
  content_type,
  topic,

  COUNT(post_id)             AS posts,
  COUNTIF(is_viral)          AS viral_posts,
  SUM(reach)                 AS total_reach,
  SUM(impressions)           AS total_impressions,
  SUM(engagements)           AS total_engagements,
  SUM(video_views)           AS total_video_views,
  SUM(follower_delta)        AS net_follower_growth,

  AVG(engagement_rate)       AS avg_engagement_rate,
  AVG(avg_watch_time_s)      AS avg_watch_time_s,
  AVG(video_completion_rate) AS avg_completion_rate,

  SAFE_DIVIDE(SUM(engagements), SUM(reach)) AS blended_engagement_rate,
  CURRENT_TIMESTAMP()        AS _loaded_at

FROM social_fitness.stg_organic
GROUP BY 1,2,3,4;


-- ────────────────────────────────────────────────────────────
-- REPORT LAYER
-- Dashboard-ready views
-- ────────────────────────────────────────────────────────────

-- rpt_executive_summary.sql
-- Weekly executive KPI view: spend, installs, subscriptions, revenue
CREATE OR REPLACE VIEW social_fitness.rpt_executive_summary AS
SELECT
  DATE_TRUNC(date, WEEK(MONDAY))  AS week,
  SUM(cost_usd)                   AS total_spend,
  SUM(impressions)                AS total_impressions,
  SUM(installs)                   AS total_installs,
  SUM(conversions)                AS total_subscriptions,
  SUM(revenue_usd)                AS total_revenue,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd)) AS blended_roas,
  SAFE_DIVIDE(SUM(cost_usd), SUM(installs))     AS blended_cpi,
  SAFE_DIVIDE(SUM(cost_usd), SUM(conversions))  AS blended_cps
FROM social_fitness.mart_channel_performance_daily
GROUP BY 1
ORDER BY 1;


-- rpt_platform_comparison.sql
-- Side-by-side platform efficiency for the reporting period
CREATE OR REPLACE VIEW social_fitness.rpt_platform_comparison AS
SELECT
  platform,
  SUM(cost_usd)     AS total_spend,
  SUM(installs)     AS total_installs,
  SUM(conversions)  AS total_conversions,
  SUM(revenue_usd)  AS total_revenue,
  AVG(cpm)          AS avg_cpm,
  AVG(ctr)          AS avg_ctr,
  AVG(cpi)          AS avg_cpi,
  AVG(roas)         AS avg_roas,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd)) AS overall_roas
FROM social_fitness.mart_channel_performance_daily
GROUP BY 1
ORDER BY total_spend DESC;


-- rpt_funnel_health.sql
-- Latest month funnel rates by channel for dashboard alerting
CREATE OR REPLACE VIEW social_fitness.rpt_funnel_health AS
SELECT
  cohort_month,
  channel,
  installs,
  subscriptions,
  overall_cvr,
  ltv_per_install,
  revenue_usd,
  CASE
    WHEN overall_cvr >= 0.05 THEN 'healthy'
    WHEN overall_cvr >= 0.03 THEN 'watch'
    ELSE 'at_risk'
  END AS funnel_status
FROM social_fitness.mart_funnel_by_channel
WHERE cohort_month = DATE_TRUNC(CURRENT_DATE(), MONTH)
ORDER BY installs DESC;
