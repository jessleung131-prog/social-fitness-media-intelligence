"""
etl/extractors/meta_extractor.py
──────────────────────────────────
Production extractor for the Meta (Instagram/Facebook) Marketing API v19.0
Insights endpoint: /act_{ad_account_id}/insights

Handles:
  • Long-lived access token validation
  • Async insight job creation (Meta requires async for large date ranges)
  • Cursor-based pagination
  • Breakdown by country × publisher_platform × platform_position
  • Rate limit handling (Business API: 200 score/hour per app-user pair)
  • Automatic fallback to simulated data when no credentials present

Key difference from TikTok/Reddit:
  Meta uses an ASYNC job pattern for large insight queries.
  You POST to create a job, poll until status = "Job Completed",
  then GET the results. This extractor handles the full polling loop.

API reference:
  https://developers.facebook.com/docs/marketing-api/insights
"""

import json
import time
import logging
import hashlib
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import MetaConfig, DATA_RAW_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)


class MetaExtractor:
    """
    Extracts ad-level daily performance from Meta Marketing API.

    Breakdown: country × publisher_platform × platform_position
    Level: ad (most granular — includes adset and campaign IDs)

    In production:  creates async insight jobs and polls for completion.
    In development: returns simulated data matching real Meta API schema.

    Usage:
        extractor = MetaExtractor()
        rows = extractor.extract(start_date=date(2025,1,1), end_date=date(2025,1,31))
        extractor.save_raw(rows, date(2025,1,1), date(2025,1,31))
    """

    # Meta API constraints
    MAX_WINDOW_DAYS      = 37     # Insights API max range per job
    JOB_POLL_INTERVAL    = 5     # seconds between async job status checks
    JOB_POLL_MAX_WAIT    = 300   # seconds before timeout
    PAGE_LIMIT           = 500
    MAX_RETRIES          = 3
    RETRY_BACKOFF        = [3, 8, 20]

    def __init__(self, config: MetaConfig = None):
        self.cfg          = config or MetaConfig()
        self._is_simulated = self._detect_placeholder_credentials()
        if self._is_simulated:
            log.info("MetaExtractor: no real credentials — using simulated data")
        else:
            log.info("MetaExtractor: live credentials — using real API")

    # ── Credential detection ──────────────────────────────────────────────────

    def _detect_placeholder_credentials(self) -> bool:
        return (
            self.cfg.access_token.startswith("your_")
            or self.cfg.ad_account_id.startswith("act_your")
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        """
        Pull all ad-level daily insights for the given date range.
        Chunks requests at Meta's 37-day window limit.
        """
        log.info(f"Meta extract: {start_date} → {end_date}")
        all_rows = []

        for chunk_start, chunk_end in self._chunk_dates(start_date, end_date):
            if self._is_simulated:
                rows = self._simulate_response(chunk_start, chunk_end)
            else:
                rows = self._fetch_from_api(chunk_start, chunk_end)
            all_rows.extend(rows)
            log.info(f"  chunk {chunk_start}→{chunk_end}: {len(rows):,} rows")

        log.info(f"Meta extract complete: {len(all_rows):,} total rows")
        return all_rows

    def save_raw(self, rows: list[dict], start_date: date, end_date: date) -> Path:
        """
        Saves raw Meta API response in the GCS-mirror directory.
        Path: data/raw/meta/YYYY-MM-DD/meta_ads_raw.json
        """
        out_dir  = DATA_RAW_DIR / "meta" / start_date.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "meta_ads_raw.json"

        payload = {
            "_meta": {
                "source":           "meta_marketing_api_v19.0",
                "extracted_at":     _utcnow(),
                "date_range":       f"{start_date}/{end_date}",
                "ad_account_id":    self.cfg.ad_account_id,
                "level":            self.cfg.level,
                "breakdowns":       self.cfg.breakdowns,
                "attribution_window": self.cfg.attribution_window,
                "simulated":        self._is_simulated,
                "pipeline_version": PIPELINE_VERSION,
                "row_count":        len(rows),
            },
            "data":   rows,
            "paging": {"cursors": {}},
        }

        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)

        log.info(f"Meta raw saved: {out_path} ({len(rows):,} rows)")
        return out_path

    # ── Live API (async job pattern) ──────────────────────────────────────────

    def _fetch_from_api(self, start_date: date, end_date: date) -> list[dict]:
        """
        Meta Insights require an async job for ad-level queries.
        Flow: POST /insights → poll GET /{job_id} → paginate GET results
        """
        # Step 1: Create async insight job
        job_id = self._create_insight_job(start_date, end_date)
        log.info(f"  Meta async job created: {job_id}")

        # Step 2: Poll until complete
        self._wait_for_job(job_id)
        log.info(f"  Meta job {job_id} complete — fetching results")

        # Step 3: Paginate results
        return self._paginate_job_results(job_id)

    def _create_insight_job(self, start_date: date, end_date: date) -> str:
        url    = f"{self.cfg.base_url}/{self.cfg.api_version}/{self.cfg.ad_account_id}/insights"
        params = {
            "access_token":          self.cfg.access_token,
            "level":                 self.cfg.level,
            "fields":                ",".join(self.cfg.fields),
            "breakdowns":            ",".join(self.cfg.breakdowns),
            "time_range":            json.dumps({"since": str(start_date), "until": str(end_date)}),
            "time_increment":        1,   # daily breakdown
            "attribution_windows":   json.dumps(self.cfg.attribution_window),
            "limit":                 self.PAGE_LIMIT,
            "use_unified_attribution_setting": True,
        }
        resp = self._request_with_retry("POST", url, params=params)
        return resp.json()["report_run_id"]

    def _wait_for_job(self, job_id: str) -> None:
        url     = f"{self.cfg.base_url}/{self.cfg.api_version}/{job_id}"
        params  = {"access_token": self.cfg.access_token}
        elapsed = 0

        while elapsed < self.JOB_POLL_MAX_WAIT:
            resp   = self._request_with_retry("GET", url, params=params)
            status = resp.json().get("async_status")
            pct    = resp.json().get("async_percent_completion", 0)

            log.debug(f"  Job {job_id}: {status} ({pct}%)")

            if status == "Job Completed":
                return
            if status in ("Job Failed", "Job Skipped"):
                raise RuntimeError(f"Meta insight job {job_id} failed: {status}")

            time.sleep(self.JOB_POLL_INTERVAL)
            elapsed += self.JOB_POLL_INTERVAL

        raise TimeoutError(f"Meta job {job_id} timed out after {elapsed}s")

    def _paginate_job_results(self, job_id: str) -> list[dict]:
        url    = f"{self.cfg.base_url}/{self.cfg.api_version}/{job_id}/insights"
        params = {"access_token": self.cfg.access_token, "limit": self.PAGE_LIMIT}
        rows   = []

        while url:
            resp = self._request_with_retry("GET", url, params=params)
            data = resp.json()
            rows.extend(data.get("data", []))

            # Cursor-based pagination
            next_cursor = data.get("paging", {}).get("cursors", {}).get("after")
            if next_cursor and data.get("paging", {}).get("next"):
                params = {"access_token": self.cfg.access_token,
                          "after": next_cursor, "limit": self.PAGE_LIMIT}
            else:
                break

        return rows

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.request(method, url, timeout=60, **kwargs)
                # Meta uses 400 for rate limit with specific error codes
                if resp.status_code == 400:
                    err_code = resp.json().get("error", {}).get("code", 0)
                    if err_code in (4, 17, 32, 613):   # rate limit error codes
                        wait = self.RETRY_BACKOFF[attempt]
                        log.warning(f"Meta rate limit (code {err_code}) — waiting {wait}s")
                        time.sleep(wait)
                        continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                time.sleep(self.RETRY_BACKOFF[attempt])

    # ── Simulation layer ──────────────────────────────────────────────────────

    def _simulate_response(self, start_date: date, end_date: date) -> list[dict]:
        """
        Generates realistic Meta API insight rows.
        Mirrors the real Meta Marketing API v19.0 field names and types exactly.

        Key schema differences from TikTok:
          - date_start + date_stop instead of stat_time_day
          - adset_id/adset_name instead of adgroup_id/adgroup_name
          - mobile_app_installs instead of app_install
          - purchase_roas is a list of action dicts: [{"action_type":"offsite_conversion.fb_pixel_purchase","value":"1.23"}]
          - post_reactions/comments/shares are action type breakdowns
          - publisher_platform field (instagram vs facebook)
          - platform_position field (feed/story/reels/explore)
        """
        random.seed(int(hashlib.md5(str(start_date).encode()).hexdigest(), 16) % 10000)

        CAMPAIGNS = [
            {"id": "FB_C_001", "name": "Fitness Enthusiasts - Awareness",  "obj": "BRAND_AWARENESS"},
            {"id": "FB_C_002", "name": "App Installs - US/UK/AU",          "obj": "APP_INSTALLS"},
            {"id": "FB_C_003", "name": "Lookalike - Premium Subscribers",  "obj": "CONVERSIONS"},
        ]
        FORMATS    = ["Reel", "Story", "Feed Video", "Carousel", "Collection"]
        COUNTRIES  = ["US", "GB", "AU", "CA", "DE", "FR"]
        PUBLISHERS = ["instagram", "facebook"]
        POSITIONS  = ["feed", "story", "reels", "explore"]

        rows = []
        current = start_date
        while current <= end_date:
            day_mult = 0.80 if current.weekday() >= 5 else 1.0

            for camp in CAMPAIGNS:
                for as_idx in range(1, 4):
                    as_id = f"FB_AS_{camp['id'][-3:]}_{as_idx:02d}"
                    for ad_idx in range(1, 3):
                        fmt      = random.choice(FORMATS)
                        pub      = random.choice(PUBLISHERS)
                        pos      = random.choice(POSITIONS)
                        impr     = max(0, int(_jitter(18000 * day_mult * as_idx * 0.55)))
                        reach    = int(impr * _jitter(0.78))
                        l_clicks = int(impr * _jitter(0.014))
                        installs = int(l_clicks * _jitter(0.11)) if camp["obj"] == "APP_INSTALLS" else 0
                        purch    = int(installs * _jitter(0.06)) if camp["obj"] == "CONVERSIONS" else 0
                        spend    = round(_jitter(320 * day_mult * as_idx * 0.5), 2)
                        vv       = int(impr * _jitter(0.42)) if fmt in ["Reel","Feed Video","Story"] else 0
                        thruplays= int(vv * _jitter(0.28))
                        eng      = int(_jitter(l_clicks * 1.8))
                        reactions= int(_jitter(l_clicks * 0.9))
                        comments = int(_jitter(l_clicks * 0.08))
                        shares   = int(_jitter(l_clicks * 0.12))
                        saves    = int(_jitter(l_clicks * 0.22))

                        # Meta wraps ROAS as an action-value list
                        purchase_roas_val = round(purch * 9.99 / spend, 4) if spend else 0
                        purchase_roas = [{
                            "action_type": "offsite_conversion.fb_pixel_purchase",
                            "value":       str(purchase_roas_val)
                        }] if purchase_roas_val else []

                        rows.append({
                            # Time
                            "date_start":          current.strftime("%Y-%m-%d"),
                            "date_stop":           current.strftime("%Y-%m-%d"),
                            # Campaign hierarchy
                            "campaign_id":         camp["id"],
                            "campaign_name":       camp["name"],
                            "objective":           camp["obj"],
                            "adset_id":            as_id,
                            "adset_name":          f"{camp['name']} - AdSet {as_idx}",
                            "ad_id":               f"FB_AD_{as_id[-5:]}_{ad_idx}",
                            "ad_name":             f"{fmt} Creative v{ad_idx}",
                            # Breakdowns
                            "country":             random.choice(COUNTRIES),
                            "publisher_platform":  pub,
                            "platform_position":   pos,
                            # Reach & frequency
                            "impressions":         impr,
                            "reach":               reach,
                            "frequency":           round(impr / reach, 3) if reach else 0,
                            # Click metrics
                            "clicks":              l_clicks + int(_jitter(l_clicks * 0.3)),
                            "link_clicks":         l_clicks,
                            "spend":               spend,
                            "cpm":                 round(spend / impr * 1000, 4) if impr else 0,
                            "cpc":                 round(spend / l_clicks, 4) if l_clicks else 0,
                            "ctr":                 round(l_clicks / impr, 6) if impr else 0,
                            # App install metrics
                            "mobile_app_installs":  installs,
                            "cost_per_app_install": round(spend / installs, 4) if installs else 0,
                            # Purchase metrics (Meta native format)
                            "purchases":                     purch,
                            "purchase_roas":                 purchase_roas,
                            # Video metrics
                            "video_views":                   vv,
                            "video_thruplay_watched_actions": thruplays,
                            "video_p25_watched_actions":     int(vv * _jitter(0.70)),
                            "video_p50_watched_actions":     int(vv * _jitter(0.52)),
                            "video_p75_watched_actions":     int(vv * _jitter(0.38)),
                            "video_p100_watched_actions":    thruplays,
                            # Engagement actions (Meta returns these as integer counts)
                            "post_engagement":     eng,
                            "post_reactions":      reactions,
                            "post_comments":       comments,
                            "post_shares":         shares,
                            "post_saves":          saves,
                        })
            current += timedelta(days=1)

        return rows

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _chunk_dates(start: date, end: date, max_days: int = 37) -> Iterator[tuple[date, date]]:
        """Split date range into ≤37-day chunks (Meta API constraint)."""
        current = start
        while current <= end:
            chunk_end = min(current + timedelta(days=max_days - 1), end)
            yield current, chunk_end
            current = chunk_end + timedelta(days=1)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _jitter(base: float, pct: float = 0.25) -> float:
    return max(0.0, base * random.uniform(1 - pct, 1 + pct))

def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    ext  = MetaExtractor()
    rows = ext.extract(date(2025, 1, 1), date(2025, 1, 7))
    path = ext.save_raw(rows, date(2025, 1, 1), date(2025, 1, 7))

    print(f"\n── Sample row (first record) ──")
    # Show purchase_roas format difference vs TikTok
    sample = rows[0]
    print(json.dumps({k: sample[k] for k in [
        "date_start", "campaign_id", "adset_id", "ad_id",
        "impressions", "spend", "mobile_app_installs", "purchase_roas"
    ]}, indent=2))
    print(f"\n── Saved: {path} ({len(rows):,} rows) ──")
