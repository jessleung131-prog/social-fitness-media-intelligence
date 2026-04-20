-- ============================================================
-- dbt/models/marts/mart_channel_performance_daily.sql
-- Cross-channel paid performance: one row per platform × campaign × day.
-- Materialized as TABLE with date partitioning.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'date', 'data_type': 'date'},
    cluster_by=['platform', 'campaign_id'],
) }}

SELECT
  date,
  platform,
  campaign_id,
  campaign_name,
  objective,
  country,

  -- Aggregated volume
  SUM(impressions)         AS impressions,
  SUM(clicks)              AS clicks,
  SUM(cost_usd)            AS cost_usd,
  SUM(installs)            AS installs,
  SUM(conversions)         AS conversions,
  SUM(conversion_value_usd) AS revenue_usd,
  SUM(video_views)         AS video_views,
  SUM(video_completions)   AS video_completions,
  SUM(engagements)         AS engagements,

  -- Efficiency (computed from aggregated actuals)
  SAFE_DIVIDE(SUM(cost_usd), SUM(impressions)) * 1000    AS cpm,
  SAFE_DIVIDE(SUM(cost_usd), SUM(clicks))                AS cpc,
  SAFE_DIVIDE(SUM(clicks),   SUM(impressions))           AS ctr,
  SAFE_DIVIDE(SUM(cost_usd), SUM(installs))              AS cpi,
  SAFE_DIVIDE(SUM(cost_usd), SUM(conversions))           AS cps,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd))           AS roas,
  SAFE_DIVIDE(SUM(video_views),     SUM(impressions))    AS video_view_rate,
  SAFE_DIVIDE(SUM(video_completions), SUM(video_views))  AS video_completion_rate,
  SAFE_DIVIDE(SUM(engagements),  SUM(impressions))       AS engagement_rate,

  MAX(attribution_window)   AS attribution_window,
  CURRENT_TIMESTAMP()       AS _loaded_at

FROM {{ ref('stg_paid_media') }}
GROUP BY 1,2,3,4,5,6


-- ============================================================
-- dbt/models/marts/mart_funnel_by_channel.sql
-- Monthly cohort funnel: install → register → activate → subscribe
-- One row per cohort_month × channel × country.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'cohort_month', 'data_type': 'date'},
    cluster_by=['channel'],
) }}

WITH base AS (
  SELECT
    DATE_TRUNC(install_date, MONTH)       AS cohort_month,
    channel,
    platform,
    country,
    COUNT(*)                              AS installs,
    COUNTIF(registered)                   AS registrations,
    COUNTIF(first_activity)               AS first_activities,
    COUNTIF(paywall_viewed)               AS paywall_views,
    COUNTIF(trial_started)                AS trials,
    COUNTIF(subscribed)                   AS subscriptions,
    SUM(subscription_value_usd)           AS revenue_usd,
    AVG(CASE WHEN subscribed
             THEN CAST(days_to_subscribe AS FLOAT64) END)
                                          AS avg_days_to_subscribe,
    -- iOS attribution quality
    COUNTIF(att_status = 'authorized')    AS ios_att_authorized,
    COUNTIF(att_status = 'denied')        AS ios_att_denied,
    COUNTIF(is_skan)                      AS skan_installs
  FROM {{ ref('stg_installs') }}
  GROUP BY 1,2,3,4
)

SELECT
  *,

  -- Funnel conversion rates
  SAFE_DIVIDE(registrations,    installs)       AS install_to_reg_rate,
  SAFE_DIVIDE(first_activities, registrations)  AS reg_to_activation_rate,
  SAFE_DIVIDE(paywall_views,    first_activities) AS activation_to_paywall_rate,
  SAFE_DIVIDE(trials,           paywall_views)  AS paywall_to_trial_rate,
  SAFE_DIVIDE(subscriptions,    trials)         AS trial_to_sub_rate,
  SAFE_DIVIDE(subscriptions,    installs)       AS overall_cvr,

  -- Value metrics
  SAFE_DIVIDE(revenue_usd,      installs)       AS ltv_per_install,
  SAFE_DIVIDE(revenue_usd,      subscriptions)  AS arpu,

  -- Attribution quality
  SAFE_DIVIDE(ios_att_authorized,
    NULLIF(ios_att_authorized + ios_att_denied, 0)) AS ios_att_rate,
  SAFE_DIVIDE(skan_installs, installs)          AS skan_pct,

  -- Funnel health flag for dashboard alerting
  CASE
    WHEN SAFE_DIVIDE(subscriptions, installs) >= 0.05 THEN 'healthy'
    WHEN SAFE_DIVIDE(subscriptions, installs) >= 0.03 THEN 'watch'
    ELSE 'at_risk'
  END                                           AS funnel_status,

  CURRENT_TIMESTAMP()                           AS _loaded_at

FROM base


-- ============================================================
-- dbt/models/marts/mart_ltv_cohort.sql
-- Monthly subscriber LTV by channel and plan type.
-- Join to mart_channel_performance_daily on cohort_month + channel
-- to compute LTV/CAC ratio.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'cohort_month', 'data_type': 'date'},
) }}

SELECT
  DATE_TRUNC(install_date, MONTH)         AS cohort_month,
  channel,
  platform,
  COALESCE(subscription_type, 'unknown')  AS subscription_type,

  COUNT(*)                                AS installs,
  COUNTIF(subscribed)                     AS subscribers,
  SUM(subscription_value_usd)             AS cohort_revenue_usd,

  SAFE_DIVIDE(COUNTIF(subscribed), COUNT(*))          AS sub_rate,
  SAFE_DIVIDE(SUM(subscription_value_usd),
              NULLIF(COUNTIF(subscribed), 0))          AS arpu,
  SAFE_DIVIDE(SUM(subscription_value_usd), COUNT(*))  AS ltv_per_install,

  AVG(CASE WHEN subscribed
           THEN CAST(days_to_subscribe AS FLOAT64) END) AS avg_days_to_convert,

  -- Plan mix signals
  SAFE_DIVIDE(COUNTIF(subscription_type = 'annual'),
              NULLIF(COUNTIF(subscribed), 0))          AS annual_mix,
  SAFE_DIVIDE(COUNTIF(subscription_type = 'monthly'),
              NULLIF(COUNTIF(subscribed), 0))          AS monthly_mix,
  SAFE_DIVIDE(COUNTIF(subscription_type = 'family'),
              NULLIF(COUNTIF(subscribed), 0))          AS family_mix,

  CURRENT_TIMESTAMP()                                  AS _loaded_at

FROM {{ ref('stg_installs') }}
GROUP BY 1,2,3,4


-- ============================================================
-- dbt/models/marts/mart_organic_performance.sql
-- Monthly organic content summary by platform and content type.
-- ============================================================

{{ config(
    materialized='table',
    partition_by={'field': 'month', 'data_type': 'date'},
    cluster_by=['platform'],
) }}

SELECT
  DATE_TRUNC(date, MONTH)     AS month,
  platform,
  content_type,
  topic,

  COUNT(post_id)              AS posts,
  COUNTIF(is_viral)           AS viral_posts,
  SUM(reach)                  AS total_reach,
  SUM(impressions)            AS total_impressions,
  SUM(engagements)            AS total_engagements,
  SUM(video_views)            AS total_video_views,
  SUM(follower_delta)         AS net_follower_growth,
  SUM(outbound_clicks)        AS total_outbound_clicks,

  AVG(blended_engagement_rate)  AS avg_engagement_rate,
  AVG(avg_watch_time_s)         AS avg_watch_time_s,
  AVG(video_completion_rate)    AS avg_completion_rate,

  -- Viral post uplift ratio
  SAFE_DIVIDE(
    AVG(CASE WHEN is_viral THEN CAST(reach AS FLOAT64) END),
    AVG(CASE WHEN NOT is_viral THEN CAST(reach AS FLOAT64) END)
  )                           AS viral_reach_multiplier,

  CURRENT_TIMESTAMP()         AS _loaded_at

FROM {{ ref('stg_organic') }}
GROUP BY 1,2,3,4


-- ============================================================
-- dbt/models/reports/rpt_executive_summary.sql
-- Weekly executive KPI rollup — paid + organic combined.
-- ============================================================

{{ config(materialized='view') }}

WITH paid_weekly AS (
  SELECT
    DATE_TRUNC(date, WEEK(MONDAY))   AS week,
    SUM(cost_usd)                    AS total_spend,
    SUM(impressions)                 AS paid_impressions,
    SUM(installs)                    AS total_installs,
    SUM(conversions)                 AS paid_subscriptions,
    SUM(revenue_usd)                 AS paid_revenue,
    SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd))    AS blended_roas,
    SAFE_DIVIDE(SUM(cost_usd), SUM(installs))       AS blended_cpi
  FROM {{ ref('mart_channel_performance_daily') }}
  GROUP BY 1
),

organic_weekly AS (
  SELECT
    DATE_TRUNC(date, WEEK(MONDAY))   AS week,
    SUM(reach)                       AS organic_reach,
    SUM(engagements)                 AS organic_engagements,
    AVG(blended_engagement_rate)     AS avg_organic_eng_rate,
    SUM(follower_delta)              AS follower_growth
  FROM {{ ref('stg_organic') }}
  GROUP BY 1
)

SELECT
  COALESCE(p.week, o.week)      AS week,
  p.total_spend,
  p.paid_impressions,
  p.total_installs,
  p.paid_subscriptions,
  p.paid_revenue,
  p.blended_roas,
  p.blended_cpi,
  o.organic_reach,
  o.organic_engagements,
  o.avg_organic_eng_rate,
  o.follower_growth,
  -- Blended media efficiency (paid + organic combined)
  SAFE_DIVIDE(p.paid_revenue,
    NULLIF(p.total_spend, 0))     AS paid_roas,
  SAFE_DIVIDE(p.total_spend,
    NULLIF(p.paid_impressions + COALESCE(o.organic_reach, 0), 0)) * 1000
                                  AS blended_cpm_all_media
FROM paid_weekly p
FULL OUTER JOIN organic_weekly o USING (week)
ORDER BY week DESC


-- ============================================================
-- dbt/models/reports/rpt_platform_comparison.sql
-- Side-by-side platform efficiency for the reporting period.
-- ============================================================

{{ config(materialized='view') }}

SELECT
  platform,
  COUNT(DISTINCT campaign_id)   AS campaigns,
  COUNT(DISTINCT date)          AS active_days,
  SUM(cost_usd)                 AS total_spend,
  SUM(impressions)              AS total_impressions,
  SUM(installs)                 AS total_installs,
  SUM(conversions)              AS total_conversions,
  SUM(revenue_usd)              AS total_revenue,
  AVG(cpm)                      AS avg_cpm,
  AVG(ctr)                      AS avg_ctr,
  AVG(cpi)                      AS avg_cpi,
  AVG(roas)                     AS avg_roas,
  SAFE_DIVIDE(SUM(revenue_usd), SUM(cost_usd))    AS overall_roas,
  SAFE_DIVIDE(SUM(installs), SUM(impressions))    AS install_rate,
  MAX(attribution_window)       AS attribution_window
FROM {{ ref('mart_channel_performance_daily') }}
GROUP BY 1
ORDER BY total_spend DESC


-- ============================================================
-- dbt/models/reports/rpt_funnel_health.sql
-- Current funnel health status by channel — for Slack alerting.
-- ============================================================

{{ config(materialized='view') }}

SELECT
  cohort_month,
  channel,
  installs,
  subscriptions,
  overall_cvr,
  ltv_per_install,
  revenue_usd,
  funnel_status,
  -- Flag channels needing attention
  CASE
    WHEN funnel_status = 'at_risk'
     AND installs >= 100  THEN 'ALERT: High volume, low CVR'
    WHEN funnel_status = 'watch'   THEN 'Monitor'
    ELSE 'OK'
  END                               AS action_required,
  avg_days_to_subscribe,
  ios_att_rate,
  skan_pct
FROM {{ ref('mart_funnel_by_channel') }}
WHERE cohort_month >= DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 2 MONTH), MONTH)
ORDER BY cohort_month DESC, installs DESC
