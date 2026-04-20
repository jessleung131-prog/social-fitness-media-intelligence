# Data Dictionary — Social Fitness App Media Intelligence Platform

> Complete field-level documentation for all CDM tables. Read this before writing
> any cross-platform queries. Several fields require special handling.

---

## Critical cross-platform gotchas

These are the most common sources of incorrect analysis when combining data across platforms.

### 1. Attribution windows differ by platform

| Platform | Window | Impact |
|---|---|---|
| TikTok Ads | 7-day click, 1-day view | Standard |
| Meta/Instagram | 7-day click, 1-day view | Standard |
| Reddit Ads | **30-day click** | Reddit installs **will appear 4× higher** relative to spend if compared directly to TikTok/Meta installs |
| AppsFlyer MMP | Configurable per source | MMP is the authoritative install count — do not add platform-reported installs |

**Rule:** Never SUM installs across platforms from `cdm_paid_media_daily`. Use `cdm_installs` (AppsFlyer) as the single source of truth for install counts and attribution.

### 2. Instagram watch time is in milliseconds

`avg_watch_time_ms` from the Instagram Graph API is in **milliseconds**.  
The transformer converts this to seconds before writing to `cdm_organic_daily`.  
The CDM column `avg_watch_time_s` is always in **seconds** for all platforms.  
If you see Instagram values of 14,000+ seconds — the conversion was not applied.

### 3. Reddit `score` ≠ likes

Reddit's `score` field is `upvotes - downvotes` (net). It can be negative.  
The CDM maps this to the `likes` column with a note in `raw_source = reddit_api_v1`.  
Do not compare absolute like counts between Instagram/TikTok and Reddit directly.

### 4. SKAN installs have no user-level data

iOS installs where `att_status = denied` are attributed via SKAdNetwork (SKAN).  
These rows have `is_skan = TRUE` and an empty `customer_user_id`.  
**Do not JOIN SKAN rows to `cdm_funnel_events`** — no user-level match is possible.  
SKAN installs are real and should be counted, but funnel analysis must exclude them or handle nulls.

### 5. MMP vs platform install counts

AppsFlyer (MMP) and ad platforms will report different install numbers for the same campaign. This is expected:
- Platforms count clicks/view-through conversions within their attribution window
- MMP applies last-touch deduplication across all sources
- MMP is always the authoritative source for install counts in this pipeline

---

## cdm_paid_media_daily

One row per **platform × campaign × ad_group × ad × day**.

| Column | Type | Description | Platform notes |
|---|---|---|---|
| `date` | DATE | UTC date of ad delivery | Normalized from: `stat_time_day` (TikTok), `date_start` (Meta), `date[:10]` (Reddit ISO→date) |
| `platform` | STRING | `tiktok` / `instagram_meta` / `reddit` | |
| `channel_type` | STRING | Always `paid` for this table | |
| `campaign_id` | STRING | Platform campaign ID | |
| `campaign_name` | STRING | Platform campaign name | |
| `objective` | STRING | Campaign objective | TikTok: `APP_INSTALL`, Meta: `APP_INSTALLS`, Reddit: `app_installs` — normalized to lowercase |
| `ad_group_id` | STRING | Ad group / adset ID | TikTok: `adgroup_id` → renamed. Meta: `adset_id` → renamed. Reddit: `ad_group_id` (same) |
| `ad_group_name` | STRING | Ad group / adset name | Same rename as above |
| `ad_id` | STRING | Ad-level ID | |
| `ad_name` | STRING | Ad creative name | |
| `country` | STRING | 2-letter ISO country code | TikTok: `country_code`. Meta: `country`. Reddit: `country` |
| `impressions` | INT | Ad impressions served | |
| `clicks` | INT | Clicks on ad | Meta uses `link_clicks` (not total `clicks`) for install-intent click count |
| `cost_usd` | FLOAT | Spend in USD | Reddit source field is `spend_usd` → renamed to `cost_usd` |
| `installs` | INT | App installs attributed | TikTok: `app_install` (singular). Meta: `mobile_app_installs`. Reddit: `app_installs` (plural) |
| `conversions` | INT | Post-install conversions | Reddit: always 0 (not natively available) |
| `conversion_value_usd` | FLOAT | Revenue from conversions | Reddit: always 0 |
| `video_views` | INT | Video play/view count | TikTok: `video_play_actions`. Meta: `video_views`. Reddit: `video_views` |
| `video_completions` | INT | Full video watches | TikTok: `video_views_p100`. Meta: `video_thruplay_watched_actions`. Reddit: always 0 |
| `engagements` | INT | Sum of social interactions | TikTok: likes+comments+shares+follows. Meta: reactions+comments+shares+saves. Reddit: upvotes+comments+shares |
| `likes` | INT | Likes / reactions / upvotes | TikTok: `likes`. Meta: `post_reactions`. Reddit: `upvotes` |
| `cpm` | FLOAT | Cost per 1,000 impressions | Recomputed as `cost_usd / impressions * 1000`. TikTok/Meta: `cpm`. Reddit: `ecpm` → renamed |
| `cpc` | FLOAT | Cost per click | Reddit source: `ecpc` → renamed |
| `ctr` | FLOAT | Click-through rate | 0–1 float (not percentage) |
| `cpi` | FLOAT | Cost per install | Computed: `cost_usd / installs` |
| `cps` | FLOAT | Cost per subscription | Reddit: always 0 |
| `roas` | FLOAT | Return on ad spend | Reddit: always 0 (not natively available). Meta: from `purchase_roas` action list |
| `attribution_window` | STRING | Attribution window used | `7d_click_1d_view` (TikTok, Meta) or `30d_click` (Reddit) |
| `raw_source` | STRING | Source API identifier | e.g. `tiktok_ads_api_v1.3` |

---

## cdm_installs

One row per **app install**. Primary source of truth for all funnel and LTV analysis.

| Column | Type | Description |
|---|---|---|
| `appsflyer_id` | STRING | AppsFlyer device ID. Primary join key for MMP data. Pseudonymous — no PII |
| `customer_user_id` | STRING | Your app's internal user ID. Empty until registration. Empty for SKAN rows |
| `install_date` | DATE | UTC date of first app open (stripped from full timestamp) |
| `install_timestamp` | STRING | Full UTC datetime of install |
| `media_source` | STRING | AppsFlyer raw source name (e.g. `tiktokglobal_int`, `facebook`, `organic`) |
| `channel` | STRING | Human-readable channel name (e.g. `TikTok Ads`, `Meta Ads`) |
| `campaign_id` | STRING | Campaign ID — joinable to `cdm_paid_media_daily.campaign_id` |
| `platform` | STRING | `ios` or `android` |
| `country` | STRING | 2-letter ISO country |
| `att_status` | STRING | iOS ATT consent: `authorized` / `denied` / `not_determined` / `not_applicable` (Android) |
| `has_idfa` | BOOL | True if IDFA available (iOS ATT authorized only) |
| `is_skan` | BOOL | True if attributed via SKAdNetwork (iOS ATT denied). No user-level join possible |
| `is_retargeting` | BOOL | True for re-engagement installs (existing users). Filtered out in staging |
| `registered` | BOOL | Account created post-install |
| `first_activity` | BOOL | First GPS activity uploaded (activation event) |
| `paywall_viewed` | BOOL | Premium upgrade screen shown |
| `trial_started` | BOOL | Free trial initiated |
| `subscribed` | BOOL | Paid subscription activated |
| `subscription_type` | STRING | `monthly` / `annual` / `family` |
| `subscription_value_usd` | FLOAT | Revenue attributed to this install |
| `days_to_subscribe` | INT | Days from install to first payment. NULL if not subscribed |

---

## cdm_organic_daily

One row per **published post**. No spend, no campaign hierarchy, no direct attribution.

| Column | Type | Description | Platform notes |
|---|---|---|---|
| `date` | DATE | Post publication date | Instagram: from `timestamp`. TikTok: from Unix `create_time`. Reddit: from Unix `created_utc` |
| `platform` | STRING | `instagram` / `tiktok` / `reddit` | |
| `post_id` | STRING | Platform post identifier | Instagram: `media_id`. TikTok: `video_id`. Reddit: `t3_xxx` format |
| `content_type` | STRING | Content format | Instagram: `REELS/IMAGE/STORY/CAROUSEL_ALBUM`. TikTok: `Standard Video/Duet/Stitch`. Reddit: `Text Post/Image Post` |
| `reach` | INT | Unique accounts reached | Reddit: `unique_views` (requires mod access) |
| `impressions` | INT | Total views (includes repeat views) | Reddit: same as reach (no separate impressions) |
| `likes` | INT | Likes / reactions / upvotes | Reddit: `score` (net upvotes — can be negative) |
| `saves` | INT | Saves / bookmarks / awards | TikTok: `collect_count`. Reddit: `awards_count` as proxy |
| `engagements` | INT | likes + comments + shares + saves | |
| `blended_engagement_rate` | FLOAT | `engagements / reach` | Computed consistently. Use this for cross-platform comparison |
| `video_views` | INT | Video plays | TikTok: `play_count`. Reddit: only for Video Post type |
| `avg_watch_time_s` | FLOAT | Average watch duration in **seconds** | Instagram: converted from milliseconds (`avg_watch_time_ms / 1000`). Reddit: always NULL |
| `video_completion_rate` | FLOAT | % of viewers who watched full video | TikTok: `full_video_watched_rate`. Instagram/Reddit: NULL |
| `follower_delta` | INT | Net follower change | Instagram: `follows` (per-post). TikTok: `fans_delta`. Reddit: `subscriber_delta` (subreddit-level) |
| `outbound_clicks` | INT | Clicks to external URL | Instagram: `website_clicks`. Reddit: `outbound_clicks`. TikTok: not available |
| `platform_engagement_rate` | FLOAT | Platform's own native engagement rate | TikTok: native field. Reddit: `upvote_ratio` as quality proxy. Instagram: NULL |

---

## cdm_funnel_summary

Aggregated daily funnel rates by channel. Powers the funnel dashboard.

| Column | Type | Description |
|---|---|---|
| `install_date` | DATE | Day of install |
| `channel` | STRING | Acquisition channel |
| `installs` | INT | Total installs |
| `registrations` | INT | Users who registered |
| `first_activities` | INT | Users who uploaded first activity |
| `paywall_views` | INT | Users who saw paywall |
| `trials` | INT | Users who started trial |
| `subscriptions` | INT | Users who subscribed |
| `revenue_usd` | FLOAT | Revenue from subscriptions |
| `reg_rate` | FLOAT | `registrations / installs` |
| `act_rate` | FLOAT | `first_activities / registrations` |
| `paywall_rate` | FLOAT | `paywall_views / first_activities` |
| `trial_rate` | FLOAT | `trials / paywall_views` |
| `sub_rate` | FLOAT | `subscriptions / trials` |
| `overall_cvr` | FLOAT | `subscriptions / installs` — headline metric |
| `ltv_per_install` | FLOAT | `revenue_usd / installs` |
| `arpu` | FLOAT | `revenue_usd / subscriptions` |
| `ios_att_rate` | FLOAT | Share of iOS installs with ATT authorized |
| `skan_pct` | FLOAT | Share of installs via SKAN (limited attribution) |

---

## Team ownership

| Dataset / Table prefix | Owner | Description |
|---|---|---|
| `cdm_paid_media_*` | Growth / UA team | All paid media ETL and reporting |
| `cdm_organic_*` | Content / Social team | Organic content performance |
| `cdm_installs`, `cdm_funnel_*` | Product Analytics | MMP attribution and funnel events |
| `cdm_ltv_cohort`, `mart_ltv_*` | Finance / Growth | Subscriber LTV and revenue reporting |
| `mart_*`, `rpt_*` | Shared / BI | dbt marts and report views |

---

*Last updated: generated by ETL pipeline | pipeline_version tracked in `_pipeline_version` column*
