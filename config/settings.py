"""
config/settings.py
──────────────────
Central configuration for the Social Fitness Tracking App
Media Intelligence ETL pipeline.

In production: load secrets from GCP Secret Manager or environment variables.
In development: .env file via python-dotenv.
"""

import os
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path

# ── Project root ──────────────────────────────────────────────────────────────
ROOT_DIR      = Path(__file__).parent.parent
DATA_RAW_DIR  = ROOT_DIR / "data" / "raw"
DATA_CDM_DIR  = ROOT_DIR / "data" / "processed"
LOG_DIR       = ROOT_DIR / "logs"

for d in [DATA_RAW_DIR, DATA_CDM_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)


# ── Date windows ──────────────────────────────────────────────────────────────
DEFAULT_LOOKBACK_DAYS = 1          # daily incremental default
BACKFILL_START        = date(2024, 10, 1)
BACKFILL_END          = date(2025, 3, 31)


# ── GCP ───────────────────────────────────────────────────────────────────────
GCP_PROJECT_ID      = os.getenv("GCP_PROJECT_ID",      "social-fitness-prod")
GCS_RAW_BUCKET      = os.getenv("GCS_RAW_BUCKET",      "social-fitness-raw")
BQ_DATASET_STAGING  = os.getenv("BQ_DATASET_STAGING",  "social_fitness_staging")
BQ_DATASET_MARTS    = os.getenv("BQ_DATASET_MARTS",    "social_fitness")
BQ_LOCATION         = os.getenv("BQ_LOCATION",         "US")


# ── TikTok Ads API ────────────────────────────────────────────────────────────
@dataclass
class TikTokConfig:
    app_id:           str = os.getenv("TIKTOK_APP_ID",       "your_app_id")
    secret:           str = os.getenv("TIKTOK_SECRET",       "your_secret")
    access_token:     str = os.getenv("TIKTOK_ACCESS_TOKEN", "your_access_token")
    advertiser_id:    str = os.getenv("TIKTOK_ADVERTISER_ID","your_advertiser_id")
    api_version:      str = "v1.3"
    base_url:         str = "https://business-api.tiktok.com/open_api"
    # Fields requested from the Reporting API
    dimensions:  list = field(default_factory=lambda: [
        "stat_time_day", "campaign_id", "adgroup_id", "ad_id",
        "country_code", "placement",
    ])
    metrics: list = field(default_factory=lambda: [
        "campaign_name", "adgroup_name", "ad_name", "objective_type",
        "impressions", "clicks", "spend", "cpm", "cpc", "ctr",
        "app_install", "conversion", "cost_per_conversion", "value",
        "video_play_actions", "video_watched_2s", "video_watched_6s",
        "video_views_p25", "video_views_p50", "video_views_p75", "video_views_p100",
        "likes", "comments", "shares", "follows",
    ])
    attribution_window: str = "7D_CLICK,1D_VIEW"
    page_size:          int = 1000


# ── Meta Marketing API ────────────────────────────────────────────────────────
@dataclass
class MetaConfig:
    app_id:        str = os.getenv("META_APP_ID",        "your_app_id")
    app_secret:    str = os.getenv("META_APP_SECRET",    "your_app_secret")
    access_token:  str = os.getenv("META_ACCESS_TOKEN",  "your_access_token")
    ad_account_id: str = os.getenv("META_AD_ACCOUNT_ID", "act_your_account_id")
    api_version:   str = "v19.0"
    base_url:      str = "https://graph.facebook.com"
    fields: list = field(default_factory=lambda: [
        "date_start", "date_stop",
        "campaign_id", "campaign_name", "objective",
        "adset_id", "adset_name", "ad_id", "ad_name",
        "country", "publisher_platform", "platform_position",
        "impressions", "reach", "frequency",
        "clicks", "link_clicks", "spend", "cpm", "cpc", "ctr",
        "mobile_app_installs", "cost_per_app_install",
        "purchases", "purchase_roas",
        "video_views", "video_thruplay_watched_actions",
        "video_p25_watched_actions", "video_p50_watched_actions",
        "video_p75_watched_actions", "video_p100_watched_actions",
        "post_engagement", "post_reactions", "post_comments",
        "post_shares", "post_saves",
    ])
    breakdowns:         list  = field(default_factory=lambda: ["country", "publisher_platform", "platform_position"])
    attribution_window: list  = field(default_factory=lambda: ["7d_click", "1d_view"])
    level:              str   = "ad"
    limit:              int   = 500


# ── Reddit Ads API ────────────────────────────────────────────────────────────
@dataclass
class RedditConfig:
    client_id:     str = os.getenv("REDDIT_CLIENT_ID",     "your_client_id")
    client_secret: str = os.getenv("REDDIT_CLIENT_SECRET", "your_client_secret")
    username:      str = os.getenv("REDDIT_USERNAME",      "your_username")
    password:      str = os.getenv("REDDIT_PASSWORD",      "your_password")
    account_id:    str = os.getenv("REDDIT_ACCOUNT_ID",    "your_account_id")
    api_version:   str = "v3"
    base_url:      str = "https://ads-api.reddit.com"
    # Reddit attribution window is fixed at 30d click — stored as metadata
    attribution_window: str = "30d_click"
    columns: list = field(default_factory=lambda: [
        "date", "campaign_id", "campaign_name", "objective",
        "ad_group_id", "ad_group_name", "ad_id", "ad_name",
        "subreddit", "country", "device",
        "impressions", "clicks", "spend_usd", "ecpm", "ecpc", "ctr",
        "app_installs", "upvotes", "comments", "shares", "video_views",
    ])
    page_size: int = 500


# ── Pipeline metadata ─────────────────────────────────────────────────────────
PIPELINE_VERSION = "1.0.0"
PIPELINE_NAME    = "social-fitness-paid-media-etl"

# CDM table names in BigQuery
CDM_PAID_MEDIA_TABLE  = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.cdm_paid_media_daily"
CDM_ORGANIC_TABLE     = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.cdm_organic_daily"
CDM_INSTALLS_TABLE    = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.cdm_installs"
CDM_FUNNEL_TABLE      = f"{GCP_PROJECT_ID}.{BQ_DATASET_STAGING}.cdm_funnel_events"

TIKTOK_CONFIG = TikTokConfig()
META_CONFIG   = MetaConfig()
REDDIT_CONFIG = RedditConfig()
