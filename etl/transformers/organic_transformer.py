"""
etl/transformers/organic_transformer.py
─────────────────────────────────────────
Transforms raw organic content from Instagram, TikTok, and Reddit
into cdm_organic_daily — a unified content performance table.

Critical normalization decisions:

  WATCH TIME UNITS
  ─────────────────
  Instagram returns avg_watch_time in MILLISECONDS (avg_watch_time_ms).
  TikTok returns average_time_watched in SECONDS.
  CDM stores in SECONDS. Transform divides Instagram value by 1000.
  Failure to do this gives Instagram an apparent 1000x advantage in watch time.

  ENGAGEMENT DENOMINATOR
  ───────────────────────
  Instagram: engagement_rate = interactions / reach
  TikTok:    engagement_rate = (likes+comments+shares) / play_count  (native field)
  Reddit:    engagement_rate = (score+comments+shares) / unique_views
  CDM stores blended_engagement_rate computed consistently as:
    (likes + comments + shares + saves) / reach (or play_count for TikTok)
  This allows cross-platform comparison. Platform-native rates stored separately.

  LIKE PROXY
  ───────────
  Instagram: like_count
  TikTok:    like_count
  Reddit:    score (net upvotes, not raw likes — affected by downvotes)
  CDM maps all to 'likes' column. Reddit 'score' noted in raw_source metadata.

  VIDEO METRICS
  ─────────────
  Instagram: video_views + avg_watch_time_ms (convert to seconds)
  TikTok:    play_count + average_time_watched (seconds) + full_video_watched_rate
  Reddit:    video_views (only for Video Post type, no watch time)
  CDM stores video_views, avg_watch_time_s, video_completion_rate for all.
  Reddit completion_rate always null (not available).

  FOLLOWER DELTA
  ───────────────
  Instagram: follows (per-post delta, via /insights)
  TikTok:    fans_delta (per-video delta)
  Reddit:    subscriber_delta (subreddit-level, not per-post)
  CDM stores as follower_delta for all three.

  NO SPEND / NO CAMPAIGN / NO ATTRIBUTION
  ────────────────────────────────────────
  Organic rows have no cost_usd, campaign_id, or attribution_window.
  These columns are NULL in organic CDM rows.
  Do not JOIN organic content to the paid media CDM on campaign_id.
  Organic → install attribution goes through UTM/deep link tracking in MMP.
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import DATA_CDM_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)

CDM_COLUMNS = [
    "date", "platform", "channel_type",
    "post_id", "content_type", "topic", "is_viral",
    "reach", "impressions",
    "likes", "comments", "shares", "saves",
    "engagements", "blended_engagement_rate",
    "video_views", "avg_watch_time_s", "video_completion_rate",
    "follower_delta", "outbound_clicks", "profile_visits",
    "platform_engagement_rate",   # platform's own native rate
    "raw_source", "_extracted_at", "_pipeline_version",
]


class OrganicTransformer:
    """
    Normalizes Instagram, TikTok, Reddit organic content into cdm_organic_daily.

    Usage:
        t       = OrganicTransformer()
        ig_cdm  = t.transform_instagram(raw_ig_rows)
        tt_cdm  = t.transform_tiktok(raw_tt_rows)
        rd_cdm  = t.transform_reddit(raw_rd_rows)
        combined= t.combine([ig_cdm, tt_cdm, rd_cdm])
        t.save(combined)
    """

    def __init__(self):
        self._extracted_at     = datetime.now(timezone.utc).isoformat()
        self._pipeline_version = PIPELINE_VERSION

    # ── Instagram ─────────────────────────────────────────────────────────────

    def transform_instagram(self, raw_rows: list[dict]) -> pd.DataFrame:
        """
        Instagram Graph API → CDM.

        Critical conversions:
          avg_watch_time_ms ÷ 1000 → avg_watch_time_s
          timestamp[:10]            → date
          total_interactions        → engagements
          like_count                → likes
          saved                     → saves
          follows                   → follower_delta
          website_clicks            → outbound_clicks
        """
        log.info(f"Transforming Instagram organic: {len(raw_rows):,} rows")
        rows = []

        for r in raw_rows:
            reach    = _int(r.get("reach", 0))
            likes    = _int(r.get("like_count", 0))
            comments = _int(r.get("comments_count", 0))
            shares   = _int(r.get("shares", 0))
            saves    = _int(r.get("saved", 0))          # 'saved' → saves
            eng      = likes + comments + shares + saves
            vv       = _int(r.get("video_views", 0))

            # CRITICAL: avg_watch_time from Instagram is in MILLISECONDS
            # Must divide by 1000 to get seconds for CDM
            watch_ms = _float(r.get("avg_watch_time_ms", 0))
            watch_s  = round(watch_ms / 1000, 1)          # ms → s

            # No native completion rate from Instagram API
            # (thruplay not available at post level via Graph API)
            completion_rate = None

            # Blended engagement rate: consistent across platforms
            beng_rate = round(eng / reach, 5) if reach > 0 else 0.0

            rows.append({
                "date":                   r.get("timestamp", "")[:10],  # strip time
                "platform":               "instagram",
                "channel_type":           "organic",
                "post_id":                r.get("media_id", r.get("id", "")),
                "content_type":           r.get("media_type", ""),      # REELS/IMAGE/STORY etc.
                "topic":                  r.get("topic", ""),
                "is_viral":               bool(r.get("is_viral", False)),
                "reach":                  reach,
                "impressions":            _int(r.get("impressions", 0)),
                "likes":                  likes,
                "comments":               comments,
                "shares":                 shares,
                "saves":                  saves,
                "engagements":            eng,
                "blended_engagement_rate":beng_rate,
                "video_views":            vv,
                "avg_watch_time_s":       watch_s,       # converted from ms
                "video_completion_rate":  completion_rate,
                "follower_delta":         _int(r.get("follows", 0)),    # 'follows' → follower_delta
                "outbound_clicks":        _int(r.get("website_clicks", 0)),
                "profile_visits":         _int(r.get("profile_visits", 0)),
                "platform_engagement_rate": None,         # Instagram doesn't return this natively
                "raw_source":             "instagram_graph_api_v19",
                "_extracted_at":          self._extracted_at,
                "_pipeline_version":      self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        log.info(f"Instagram transformed: {len(df):,} CDM rows")
        return df

    # ── TikTok ────────────────────────────────────────────────────────────────

    def transform_tiktok(self, raw_rows: list[dict]) -> pd.DataFrame:
        """
        TikTok Display API → CDM.

        Key differences from Instagram:
          play_count              → video_views (TikTok primary metric)
          average_time_watched    → avg_watch_time_s (already in seconds)
          full_video_watched_rate → video_completion_rate
          collect_count           → saves (TikTok bookmarks)
          fans_delta              → follower_delta
          engagement_rate         → platform_engagement_rate (keep native)
          create_time (Unix ts)   → date
        """
        log.info(f"Transforming TikTok organic: {len(raw_rows):,} rows")
        rows = []

        for r in raw_rows:
            plays    = _int(r.get("play_count", r.get("view_count", 0)))
            likes    = _int(r.get("like_count", 0))
            comments = _int(r.get("comment_count", 0))
            shares   = _int(r.get("share_count", 0))
            saves    = _int(r.get("collect_count", 0))   # collect_count → saves
            eng      = likes + comments + shares + saves
            reach    = _int(r.get("reach", int(plays * 0.72)))

            # TikTok watch time is already in seconds (unlike Instagram ms)
            watch_s  = _float(r.get("average_time_watched", 0))

            # Completion rate: TikTok returns as 0-1 float
            completion = _float(r.get("full_video_watched_rate", 0))

            # TikTok returns engagement_rate natively — store as platform rate
            plat_eng_rate = _float(r.get("engagement_rate", 0))

            # Blended rate (consistent denominator across platforms)
            beng_rate = round(eng / plays, 5) if plays > 0 else 0.0

            # Convert Unix timestamp to date
            create_ts = r.get("create_time", 0)
            try:
                post_date = str(datetime.utcfromtimestamp(int(create_ts)).date())
            except (ValueError, TypeError, OSError):
                post_date = ""

            rows.append({
                "date":                   post_date,
                "platform":               "tiktok",
                "channel_type":           "organic",
                "post_id":                r.get("id", r.get("video_id", "")),
                "content_type":           r.get("content_type", "Standard Video"),
                "topic":                  r.get("topic", ""),
                "is_viral":               bool(r.get("is_viral", False)),
                "reach":                  reach,
                "impressions":            plays,          # TikTok: impressions ≈ plays
                "likes":                  likes,
                "comments":               comments,
                "shares":                 shares,
                "saves":                  saves,
                "engagements":            eng,
                "blended_engagement_rate":beng_rate,
                "video_views":            plays,          # play_count is the video view metric
                "avg_watch_time_s":       watch_s,        # already in seconds (no conversion needed)
                "video_completion_rate":  completion,
                "follower_delta":         _int(r.get("fans_delta", 0)),  # fans_delta → follower_delta
                "outbound_clicks":        0,               # not available in Display API
                "profile_visits":         _int(r.get("profile_visits", 0)),
                "platform_engagement_rate": plat_eng_rate,
                "raw_source":             "tiktok_display_api_v2",
                "_extracted_at":          self._extracted_at,
                "_pipeline_version":      self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        log.info(f"TikTok transformed: {len(df):,} CDM rows")
        return df

    # ── Reddit ────────────────────────────────────────────────────────────────

    def transform_reddit(self, raw_rows: list[dict]) -> pd.DataFrame:
        """
        Reddit API → CDM.

        Key differences from Instagram and TikTok:
          score       → likes (net upvotes, not raw — can be negative)
          num_comments→ comments (same field name luckily)
          upvote_ratio→ stored in platform_engagement_rate as proxy
          unique_views→ reach (mod access required for real data)
          outbound_clicks → outbound_clicks (mod dashboard only)
          created_utc (Unix ts) → date
          No avg_watch_time, no video_completion_rate
          subscriber_delta → follower_delta (subreddit-level, not post-level)
        """
        log.info(f"Transforming Reddit organic: {len(raw_rows):,} rows")
        rows = []

        for r in raw_rows:
            score    = _int(r.get("score", 0))           # net upvotes
            comments = _int(r.get("num_comments", 0))
            shares   = _int(r.get("num_shares", 0))
            reach    = _int(r.get("unique_views", max(score * 4, 1)))
            eng      = score + comments + shares

            # Reddit upvote_ratio as proxy for platform engagement quality
            upvote_ratio = _float(r.get("upvote_ratio", 0))

            # Blended engagement rate
            beng_rate = round(eng / reach, 5) if reach > 0 else 0.0

            # Convert Unix timestamp to date
            created_utc = r.get("created_utc", 0)
            try:
                post_date = str(datetime.utcfromtimestamp(int(created_utc)).date())
            except (ValueError, TypeError, OSError):
                post_date = ""

            rows.append({
                "date":                   post_date,
                "platform":               "reddit",
                "channel_type":           "organic",
                "post_id":                r.get("post_id", r.get("id", "")),
                "content_type":           r.get("post_type", "Post"),
                "topic":                  r.get("topic", ""),
                "is_viral":               bool(r.get("is_viral", False)),
                "reach":                  reach,
                "impressions":            reach,           # no separate impressions on Reddit
                "likes":                  score,           # score = net upvotes → likes proxy
                "comments":               comments,
                "shares":                 shares,
                "saves":                  _int(r.get("awards_count", 0)),  # awards as proxy for saves
                "engagements":            eng,
                "blended_engagement_rate":beng_rate,
                "video_views":            _int(r.get("video_views", 0)),
                "avg_watch_time_s":       None,            # not available on Reddit
                "video_completion_rate":  None,            # not available on Reddit
                "follower_delta":         _int(r.get("subscriber_delta", 0)),
                "outbound_clicks":        _int(r.get("outbound_clicks", 0)),
                "profile_visits":         0,               # not available
                "platform_engagement_rate": upvote_ratio, # upvote_ratio as quality proxy
                "raw_source":             "reddit_api_v1",
                "_extracted_at":          self._extracted_at,
                "_pipeline_version":      self._pipeline_version,
            })

        df = pd.DataFrame(rows)
        df["date"] = pd.to_datetime(df["date"])
        log.info(f"Reddit transformed: {len(df):,} CDM rows")
        return df

    # ── Combine & save ────────────────────────────────────────────────────────

    def combine(self, dfs: list[pd.DataFrame]) -> pd.DataFrame:
        combined = pd.concat(dfs, ignore_index=True)
        # Ensure all CDM columns present
        for col in CDM_COLUMNS:
            if col not in combined.columns:
                combined[col] = None
        combined = combined[CDM_COLUMNS]
        combined = combined.sort_values(["date", "platform"]).reset_index(drop=True)
        log.info(f"Organic CDM combined: {len(combined):,} rows across "
                 f"{combined['platform'].nunique()} platforms")
        return combined

    def save(self, df: pd.DataFrame) -> Path:
        path = DATA_CDM_DIR / "cdm_organic_daily.csv"
        df.to_csv(path, index=False)
        log.info(f"Saved: cdm_organic_daily.csv ({len(df):,} rows)")
        return path


# ── Helpers ───────────────────────────────────────────────────────────────────

def _int(val, default=0) -> int:
    try: return int(float(val))
    except: return default

def _float(val, default=0.0) -> float:
    try: return float(val)
    except: return default


# ── CLI ───────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from extractors.organic_extractor import OrganicExtractor

    start, end = date(2024, 10, 1), date(2025, 3, 31)
    ext = OrganicExtractor()
    ig  = ext.extract_instagram(start, end)
    tt  = ext.extract_tiktok(start, end)
    rd  = ext.extract_reddit(start, end)

    t       = OrganicTransformer()
    ig_cdm  = t.transform_instagram(ig)
    tt_cdm  = t.transform_tiktok(tt)
    rd_cdm  = t.transform_reddit(rd)
    combined= t.combine([ig_cdm, tt_cdm, rd_cdm])
    t.save(combined)

    print(f"\n── cdm_organic_daily: {combined.shape} ──")
    print(combined.groupby("platform").agg(
        posts          =("post_id",          "count"),
        viral_posts    =("is_viral",         "sum"),
        total_reach    =("reach",            "sum"),
        total_eng      =("engagements",      "sum"),
        avg_eng_rate   =("blended_engagement_rate", "mean"),
        avg_watch_s    =("avg_watch_time_s", "mean"),
        follower_growth=("follower_delta",   "sum"),
    ).round(3))

    print(f"\n── Watch time units verification ──")
    print("  Instagram avg_watch_time_s (converted from ms):",
          round(ig_cdm["avg_watch_time_s"].mean(), 1), "seconds")
    print("  TikTok avg_watch_time_s (already in seconds):  ",
          round(tt_cdm["avg_watch_time_s"].mean(), 1), "seconds")
    print("  Reddit avg_watch_time_s (not available):       ",
          rd_cdm["avg_watch_time_s"].isna().all())

    print(f"\n── Engagement rate methodology ──")
    print("  All platforms use blended_engagement_rate = (likes+comments+shares+saves)/reach")
    print("  Platform-native rates stored in platform_engagement_rate separately")
    print(f"  TikTok native: {tt_cdm['platform_engagement_rate'].mean():.4f}")
    print(f"  Reddit upvote_ratio proxy: {rd_cdm['platform_engagement_rate'].mean():.4f}")
