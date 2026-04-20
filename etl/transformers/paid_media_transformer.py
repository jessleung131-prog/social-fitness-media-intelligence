"""
etl/transformers/paid_media_transformer.py
───────────────────────────────────────────
Transforms raw platform-specific payloads into the Common Data Model (CDM)
for paid media: cdm_paid_media_daily.

Handles all schema differences between TikTok, Meta, and Reddit:
  ┌─────────────────┬───────────────────────┬───────────────────────┬──────────────────────┐
  │ Concept         │ TikTok raw field       │ Meta raw field        │ Reddit raw field     │
  ├─────────────────┼───────────────────────┼───────────────────────┼──────────────────────┤
  │ Date            │ stat_time_day          │ date_start            │ date[:10] (strip UTC)│
  │ Ad group        │ adgroup_id/name        │ adset_id/name         │ ad_group_id/name     │
  │ Spend           │ spend                  │ spend                 │ spend_usd            │
  │ Installs        │ app_install            │ mobile_app_installs   │ app_installs         │
  │ ROAS            │ value/spend            │ purchase_roas (list)  │ not available        │
  │ CPM             │ cpm                    │ cpm                   │ ecpm                 │
  │ CPC             │ cpc                    │ cpc                   │ ecpc                 │
  │ Likes           │ likes                  │ post_reactions        │ upvotes              │
  │ Attribution     │ 7d_click_1d_view       │ 7d_click_1d_view      │ 30d_click (fixed)    │
  └─────────────────┴───────────────────────┴───────────────────────┴──────────────────────┘

Output schema (cdm_paid_media_daily):
  date, platform, channel_type, campaign_id, campaign_name, objective,
  ad_group_id, ad_group_name, ad_id, ad_name, country,
  impressions, clicks, cost_usd, installs, conversions, conversion_value_usd,
  video_views, video_completions, engagements, likes, comments, shares,
  cpm, cpc, ctr, cpi, cps, roas,
  attribution_window, raw_source, _extracted_at, _pipeline_version
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import DATA_CDM_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# CDM column definition and ordering
# ─────────────────────────────────────────────────────────────────────────────

CDM_COLUMNS = [
    "date", "platform", "channel_type",
    "campaign_id", "campaign_name", "objective",
    "ad_group_id", "ad_group_name", "ad_id", "ad_name",
    "country",
    "impressions", "clicks", "cost_usd",
    "installs", "conversions", "conversion_value_usd",
    "video_views", "video_completions", "engagements",
    "likes", "comments", "shares",
    "cpm", "cpc", "ctr", "cpi", "cps", "roas",
    "attribution_window", "raw_source",
    "_extracted_at", "_pipeline_version",
]

CDM_DTYPES = {
    "impressions":           "int64",
    "clicks":                "int64",
    "cost_usd":              "float64",
    "installs":              "int64",
    "conversions":           "int64",
    "conversion_value_usd":  "float64",
    "video_views":           "int64",
    "video_completions":     "int64",
    "engagements":           "int64",
    "likes":                 "int64",
    "comments":              "int64",
    "shares":                "int64",
    "cpm":                   "float64",
    "cpc":                   "float64",
    "ctr":                   "float64",
    "cpi":                   "float64",
    "cps":                   "float64",
    "roas":                  "float64",
}


# ─────────────────────────────────────────────────────────────────────────────
# Platform-specific transformers
# ─────────────────────────────────────────────────────────────────────────────

class PaidMediaTransformer:
    """
    Orchestrates transformation of all paid media sources into a single CDM table.

    Usage:
        transformer = PaidMediaTransformer()

        tiktok_cdm = transformer.transform_tiktok(raw_rows)
        meta_cdm   = transformer.transform_meta(raw_rows)
        reddit_cdm = transformer.transform_reddit(raw_rows, attribution_window="30d_click")

        combined   = transformer.combine([tiktok_cdm, meta_cdm, reddit_cdm])
        transformer.save(combined, date(2025, 1, 1))
    """

    def __init__(self):
        self._extracted_at      = datetime.now(timezone.utc).isoformat()
        self._pipeline_version  = PIPELINE_VERSION

    # ── TikTok transformer ────────────────────────────────────────────────────

    def transform_tiktok(self, raw_rows: list[dict]) -> pd.DataFrame:
        """
        Maps TikTok raw API fields → CDM.

        Field mapping notes:
          stat_time_day     → date
          adgroup_id/name   → ad_group_id/name  (TikTok calls these 'adgroup')
          app_install       → installs           (singular, not plural)
          conversion        → conversions
          value             → conversion_value_usd
          video_play_actions→ video_views
          video_views_p100  → video_completions
          likes/comments/shares/follows → engagements (sum)
        """
        log.info(f"Transforming TikTok: {len(raw_rows):,} raw rows")
        rows = []

        for r in raw_rows:
            spend    = _safe_float(r.get("spend", 0))
            installs = _safe_int(r.get("app_install", 0))
            convs    = _safe_int(r.get("conversion", 0))
            impr     = _safe_int(r.get("impressions", 0))
            clicks   = _safe_int(r.get("clicks", 0))
            likes    = _safe_int(r.get("likes", 0))
            comments = _safe_int(r.get("comments", 0))
            shares   = _safe_int(r.get("shares", 0))
            follows  = _safe_int(r.get("follows", 0))

            rows.append({
                "date":                 r["stat_time_day"],
                "platform":             "tiktok",
                "channel_type":         "paid",
                "campaign_id":          r.get("campaign_id", ""),
                "campaign_name":        r.get("campaign_name", ""),
                "objective":            r.get("objective_type", ""),
                "ad_group_id":          r.get("adgroup_id", ""),       # adgroup → ad_group
                "ad_group_name":        r.get("adgroup_name", ""),
                "ad_id":                r.get("ad_id", ""),
                "ad_name":              r.get("ad_name", ""),
                "country":              r.get("country_code", ""),     # country_code → country
                "impressions":          impr,
                "clicks":               clicks,
                "cost_usd":             spend,
                "installs":             installs,                       # app_install → installs
                "conversions":          convs,                         # conversion → conversions
                "conversion_value_usd": _safe_float(r.get("value", 0)),
                "video_views":          _safe_int(r.get("video_play_actions", 0)),
                "video_completions":    _safe_int(r.get("video_views_p100", 0)),
                "engagements":          likes + comments + shares + follows,
                "likes":                likes,
                "comments":             comments,
                "shares":               shares,
                "cpm":                  _safe_div(spend, impr) * 1000,
                "cpc":                  _safe_div(spend, clicks),
                "ctr":                  _safe_div(clicks, impr),
                "cpi":                  _safe_div(spend, installs),
                "cps":                  _safe_div(spend, convs),
                "roas":                 _safe_div(_safe_float(r.get("value", 0)), spend),
                "attribution_window":   "7d_click_1d_view",
                "raw_source":           "tiktok_ads_api_v1.3",
                "_extracted_at":        self._extracted_at,
                "_pipeline_version":    self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df = self._apply_dtypes(df)
        log.info(f"TikTok transformed: {len(df):,} CDM rows")
        return df

    # ── Meta transformer ──────────────────────────────────────────────────────

    def transform_meta(self, raw_rows: list[dict]) -> pd.DataFrame:
        """
        Maps Meta raw API fields → CDM.

        Field mapping notes:
          date_start             → date
          adset_id/name          → ad_group_id/name  (Meta calls these 'adset')
          mobile_app_installs    → installs
          purchases              → conversions
          purchase_roas          → roas  (Meta returns as list of dicts OR float)
          post_reactions         → likes
          cpm/cpc                → cpm/cpc  (same field names as TikTok — no change needed)
          video_thruplay_watched_actions → video_completions
        """
        log.info(f"Transforming Meta: {len(raw_rows):,} raw rows")
        rows = []

        for r in raw_rows:
            spend    = _safe_float(r.get("spend", 0))
            installs = _safe_int(r.get("mobile_app_installs", 0))    # different from TikTok
            purch    = _safe_int(r.get("purchases", 0))
            impr     = _safe_int(r.get("impressions", 0))
            clicks   = _safe_int(r.get("link_clicks", 0))
            reactions= _safe_int(r.get("post_reactions", 0))         # reactions = likes proxy
            comments = _safe_int(r.get("post_comments", 0))
            shares   = _safe_int(r.get("post_shares", 0))
            saves    = _safe_int(r.get("post_saves", 0))

            # Meta returns purchase_roas as a list of action dicts OR as a float
            # e.g. [{"action_type": "offsite_conversion.fb_pixel_purchase", "value": "1.23"}]
            roas_raw = r.get("purchase_roas", 0)
            if isinstance(roas_raw, list):
                roas = _safe_float(roas_raw[0].get("value", 0)) if roas_raw else 0.0
            else:
                roas = _safe_float(roas_raw)

            rows.append({
                "date":                 r["date_start"],               # date_start → date
                "platform":             "instagram_meta",
                "channel_type":         "paid",
                "campaign_id":          r.get("campaign_id", ""),
                "campaign_name":        r.get("campaign_name", ""),
                "objective":            r.get("objective", ""),
                "ad_group_id":          r.get("adset_id", ""),         # adset_id → ad_group_id
                "ad_group_name":        r.get("adset_name", ""),       # adset_name → ad_group_name
                "ad_id":                r.get("ad_id", ""),
                "ad_name":              r.get("ad_name", ""),
                "country":              r.get("country", ""),
                "impressions":          impr,
                "clicks":               clicks,
                "cost_usd":             spend,
                "installs":             installs,
                "conversions":          purch,
                "conversion_value_usd": round(purch * 9.99, 2),        # derived from purchases
                "video_views":          _safe_int(r.get("video_views", 0)),
                "video_completions":    _safe_int(r.get("video_thruplay_watched_actions", 0)),
                "engagements":          reactions + comments + shares + saves,
                "likes":                reactions,                     # reactions → likes
                "comments":             comments,
                "shares":               shares,
                "cpm":                  _safe_div(spend, impr) * 1000,
                "cpc":                  _safe_div(spend, clicks),
                "ctr":                  _safe_div(clicks, impr),
                "cpi":                  _safe_div(spend, installs),
                "cps":                  _safe_div(spend, purch),
                "roas":                 roas,
                "attribution_window":   "7d_click_1d_view",
                "raw_source":           "meta_marketing_api_v19.0",
                "_extracted_at":        self._extracted_at,
                "_pipeline_version":    self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df = self._apply_dtypes(df)
        log.info(f"Meta transformed: {len(df):,} CDM rows")
        return df

    # ── Reddit transformer ────────────────────────────────────────────────────

    def transform_reddit(self, raw_rows: list[dict],
                         attribution_window: str = "30d_click") -> pd.DataFrame:
        """
        Maps Reddit raw API fields → CDM.

        Field mapping notes:
          date[:10]     → date  (strip ISO UTC timestamp to plain date)
          spend_usd     → cost_usd  (explicit currency suffix → standard name)
          app_installs  → installs  (plural, not singular like TikTok)
          ecpm/ecpc     → cpm/cpc   (effective CPM/CPC → standard names)
          upvotes       → likes     (Reddit equivalent)
          attribution_window: sourced from _meta (not per row) — passed as param
          roas: not available — set to 0, flagged in notes

        IMPORTANT: Reddit's 30d attribution window means installs will be
        overcounted relative to TikTok/Meta (7d). Do not sum installs across
        platforms without normalizing for window differences.
        """
        log.info(f"Transforming Reddit: {len(raw_rows):,} raw rows")
        rows = []

        for r in raw_rows:
            # Strip ISO timestamp → plain date: "2025-01-01T00:00:00Z" → "2025-01-01"
            raw_date = r.get("date", "")
            date_str = raw_date[:10] if raw_date else ""

            spend    = _safe_float(r.get("spend_usd", 0))             # spend_usd → cost_usd
            installs = _safe_int(r.get("app_installs", 0))            # app_installs (plural)
            impr     = _safe_int(r.get("impressions", 0))
            clicks   = _safe_int(r.get("clicks", 0))
            upvotes  = _safe_int(r.get("upvotes", 0))                 # upvotes → likes
            comments = _safe_int(r.get("comments", 0))
            shares   = _safe_int(r.get("shares", 0))

            rows.append({
                "date":                 date_str,
                "platform":             "reddit",
                "channel_type":         "paid",
                "campaign_id":          r.get("campaign_id", ""),
                "campaign_name":        r.get("campaign_name", ""),
                "objective":            r.get("objective", ""),
                "ad_group_id":          r.get("ad_group_id", ""),
                "ad_group_name":        r.get("ad_group_name", ""),
                "ad_id":                r.get("ad_id", ""),
                "ad_name":              r.get("ad_name", ""),
                "country":              r.get("country", ""),
                "impressions":          impr,
                "clicks":               clicks,
                "cost_usd":             spend,                         # spend_usd → cost_usd
                "installs":             installs,
                "conversions":          0,                             # not native in Reddit
                "conversion_value_usd": 0.0,
                "video_views":          _safe_int(r.get("video_views", 0)),
                "video_completions":    0,                             # not available
                "engagements":          upvotes + comments + shares,
                "likes":                upvotes,                       # upvotes → likes
                "comments":             comments,
                "shares":               shares,
                "cpm":                  _safe_float(r.get("ecpm", 0)),  # ecpm → cpm
                "cpc":                  _safe_float(r.get("ecpc", 0)),  # ecpc → cpc
                "ctr":                  _safe_float(r.get("ctr", 0)),
                "cpi":                  _safe_div(spend, installs),
                "cps":                  0.0,                           # not available
                "roas":                 0.0,                           # not available natively
                "attribution_window":   attribution_window,            # from _meta, not per row
                "raw_source":           "reddit_ads_api_v3",
                "_extracted_at":        self._extracted_at,
                "_pipeline_version":    self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df = self._apply_dtypes(df)
        log.info(f"Reddit transformed: {len(df):,} CDM rows")
        return df

    # ── Combine & save ────────────────────────────────────────────────────────

    def combine(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        """
        Concatenates all platform CDM DataFrames, enforces column order,
        parses date column, and sorts chronologically.
        """
        combined = pd.concat(dfs, ignore_index=True)
        combined["date"] = pd.to_datetime(combined["date"])
        combined = combined[CDM_COLUMNS]
        combined = combined.sort_values(["date", "platform", "campaign_id", "ad_id"])
        combined = combined.reset_index(drop=True)
        log.info(f"Combined CDM: {len(combined):,} total rows across {combined['platform'].nunique()} platforms")
        return combined

    def save(self, df: pd.DataFrame, run_date: date) -> Path:
        """
        Saves the CDM DataFrame to data/processed/cdm_paid_media_daily.csv
        Also saves a date-partitioned copy for incremental loads.
        """
        # Full table (append or overwrite)
        full_path = DATA_CDM_DIR / "cdm_paid_media_daily.csv"
        df.to_csv(full_path, index=False)

        # Date-partitioned copy (for BigQuery incremental loads)
        part_dir  = DATA_CDM_DIR / "partitioned" / f"run_date={run_date}"
        part_dir.mkdir(parents=True, exist_ok=True)
        part_path = part_dir / "cdm_paid_media_daily.csv"
        df.to_csv(part_path, index=False)

        log.info(f"CDM saved: {full_path} ({len(df):,} rows)")
        log.info(f"Partition: {part_path}")
        return full_path

    # ── Internal helpers ──────────────────────────────────────────────────────

    @staticmethod
    def _apply_dtypes(df: pd.DataFrame) -> pd.DataFrame:
        for col, dtype in CDM_DTYPES.items():
            if col in df.columns:
                df[col] = df[col].fillna(0).astype(dtype)
        return df


# ── Scalar helpers ────────────────────────────────────────────────────────────

def _safe_float(val: Any, default: float = 0.0) -> float:
    try:
        return float(val)
    except (ValueError, TypeError):
        return default

def _safe_int(val: Any, default: int = 0) -> int:
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return default

def _safe_div(numerator: float, denominator: float, default: float = 0.0) -> float:
    try:
        return numerator / denominator if denominator and denominator != 0 else default
    except (ZeroDivisionError, TypeError):
        return default


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from extractors.tiktok_extractor import TikTokExtractor
    from extractors.meta_extractor   import MetaExtractor
    from extractors.reddit_extractor import RedditExtractor

    start, end = date(2025, 1, 1), date(2025, 1, 7)

    # Extract
    tt_rows = TikTokExtractor().extract(start, end)
    mt_rows = MetaExtractor().extract(start, end)
    rd_rows = RedditExtractor().extract(start, end)

    # Transform
    transformer = PaidMediaTransformer()
    tt_cdm = transformer.transform_tiktok(tt_rows)
    mt_cdm = transformer.transform_meta(mt_rows)
    rd_cdm = transformer.transform_reddit(rd_rows)

    combined = transformer.combine([tt_cdm, mt_cdm, rd_cdm])
    transformer.save(combined, date.today())

    print(f"\n── CDM shape: {combined.shape} ──")
    print(combined.dtypes.to_string())
    print(f"\n── Platform row counts ──")
    print(combined.groupby("platform")[["impressions","clicks","cost_usd","installs"]].sum().round(2))
    print(f"\n── Attribution windows ──")
    print(combined.groupby(["platform","attribution_window"]).size().rename("rows"))
