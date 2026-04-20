"""
etl/extractors/tiktok_extractor.py
────────────────────────────────────
Production extractor for the TikTok Business API v1.3
Reporting endpoint: /open_api/v1.3/report/integrated/get/

Handles:
  • OAuth2 token management
  • Paginated report requests (1,000 rows/page)
  • Date-range chunking (API max window: 30 days)
  • Rate limit backoff (429 / 40 req/min limit)
  • Automatic fallback to simulated data when no credentials present
  • Raw response saved to GCS-structured local path for audit trail

API reference:
  https://business-api.tiktok.com/portal/docs?id=1740302848100353
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

# ── Internal imports ──────────────────────────────────────────────────────────
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import TikTokConfig, DATA_RAW_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# TikTok Ads Extractor
# ─────────────────────────────────────────────────────────────────────────────

class TikTokExtractor:
    """
    Extracts daily ad performance from TikTok Business API.

    In production:  makes real HTTP requests using credentials from settings.
    In development: detects placeholder credentials and returns simulated data
                    that exactly mirrors the real API response schema.

    Usage:
        extractor = TikTokExtractor()
        rows = extractor.extract(start_date=date(2025,1,1), end_date=date(2025,1,31))
        extractor.save_raw(rows, date(2025,1,1), date(2025,1,31))
    """

    # TikTok API limits
    MAX_WINDOW_DAYS  = 30     # max date range per single request
    PAGE_SIZE        = 1000   # rows per page
    RATE_LIMIT_RPS   = 0.67   # ~40 requests/min → 1 req per 1.5s
    MAX_RETRIES      = 3
    RETRY_BACKOFF    = [2, 5, 15]   # seconds between retries

    def __init__(self, config: TikTokConfig = None):
        self.cfg = config or TikTokConfig()
        self._is_simulated = self._detect_placeholder_credentials()
        if self._is_simulated:
            log.info("TikTokExtractor: no real credentials found — using simulated data")
        else:
            log.info("TikTokExtractor: live credentials detected — using real API")

    # ── Credential detection ──────────────────────────────────────────────────

    def _detect_placeholder_credentials(self) -> bool:
        return (
            self.cfg.access_token.startswith("your_")
            or self.cfg.advertiser_id.startswith("your_")
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        """
        Pull all ad-level daily performance rows for the given date range.
        Automatically chunks requests if range exceeds API's 30-day maximum.

        Returns a flat list of row dicts matching the raw TikTok API schema.
        """
        log.info(f"TikTok extract: {start_date} → {end_date}")
        all_rows = []

        for chunk_start, chunk_end in self._chunk_dates(start_date, end_date):
            if self._is_simulated:
                rows = self._simulate_response(chunk_start, chunk_end)
            else:
                rows = self._fetch_from_api(chunk_start, chunk_end)
            all_rows.extend(rows)
            log.info(f"  chunk {chunk_start}→{chunk_end}: {len(rows):,} rows")

        log.info(f"TikTok extract complete: {len(all_rows):,} total rows")
        return all_rows

    def save_raw(self, rows: list[dict], start_date: date, end_date: date) -> Path:
        """
        Persist raw API response to the local GCS-mirror directory.
        Path convention: data/raw/tiktok/YYYY-MM-DD/tiktok_ads_raw.json

        In production this would upload to:
        gs://{GCS_RAW_BUCKET}/tiktok/{start_date}/tiktok_ads_raw.json
        """
        out_dir  = DATA_RAW_DIR / "tiktok" / start_date.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "tiktok_ads_raw.json"

        payload = {
            "code":    0,
            "message": "OK",
            "_meta": {
                "source":           "tiktok_ads_api_v1.3",
                "extracted_at":     _utcnow(),
                "date_range":       f"{start_date}/{end_date}",
                "advertiser_id":    self.cfg.advertiser_id,
                "simulated":        self._is_simulated,
                "pipeline_version": PIPELINE_VERSION,
                "row_count":        len(rows),
            },
            "data": {
                "page_info": {"total_number": len(rows), "page": 1, "page_size": self.PAGE_SIZE},
                "list":      rows,
            },
        }

        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)

        log.info(f"TikTok raw saved: {out_path} ({len(rows):,} rows)")
        return out_path

    # ── Live API call ─────────────────────────────────────────────────────────

    def _fetch_from_api(self, start_date: date, end_date: date) -> list[dict]:
        """
        Calls TikTok Reporting API with pagination.

        Endpoint: POST /open_api/v1.3/report/integrated/get/
        Auth:     Access-Token header
        """
        url     = f"{self.cfg.base_url}/{self.cfg.api_version}/report/integrated/get/"
        headers = {
            "Access-Token": self.cfg.access_token,
            "Content-Type": "application/json",
        }
        all_rows = []
        page     = 1

        while True:
            payload = {
                "advertiser_id":     self.cfg.advertiser_id,
                "report_type":       "BASIC",
                "dimensions":        self.cfg.dimensions,
                "metrics":           self.cfg.metrics,
                "start_date":        start_date.strftime("%Y-%m-%d"),
                "end_date":          end_date.strftime("%Y-%m-%d"),
                "attribution_window": self.cfg.attribution_window,
                "order_field":       "stat_time_day",
                "order_type":        "ASC",
                "page":              page,
                "page_size":         self.PAGE_SIZE,
            }

            response = self._request_with_retry("POST", url, headers=headers, json=payload)
            data     = response.json()

            if data.get("code") != 0:
                raise RuntimeError(
                    f"TikTok API error {data.get('code')}: {data.get('message')}"
                )

            rows       = data["data"]["list"]
            page_info  = data["data"]["page_info"]
            all_rows.extend(rows)

            log.debug(f"  page {page}: {len(rows)} rows, total so far: {len(all_rows)}")

            # Check if more pages exist
            if len(all_rows) >= page_info["total_number"]:
                break
            page += 1
            time.sleep(1.0 / self.RATE_LIMIT_RPS)

        return all_rows

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Retries on 429 (rate limit) and 5xx (server errors)."""
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 429:
                    wait = self.RETRY_BACKOFF[attempt]
                    log.warning(f"TikTok rate limit hit — waiting {wait}s (attempt {attempt+1})")
                    time.sleep(wait)
                    continue
                resp.raise_for_status()
                return resp
            except requests.RequestException as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                wait = self.RETRY_BACKOFF[attempt]
                log.warning(f"TikTok request failed: {e} — retry in {wait}s")
                time.sleep(wait)

    # ── Simulation layer ──────────────────────────────────────────────────────

    def _simulate_response(self, start_date: date, end_date: date) -> list[dict]:
        """
        Generates realistic TikTok API response rows with correct field names,
        types, and value ranges — matching the real v1.3 schema exactly.
        """
        random.seed(int(hashlib.md5(str(start_date).encode()).hexdigest(), 16) % 10000)

        CAMPAIGNS = [
            {"id": "TT_C_001", "name": "Brand Awareness - Running Q4",   "obj": "REACH"},
            {"id": "TT_C_002", "name": "App Install - Cycling Audience",  "obj": "APP_INSTALL"},
            {"id": "TT_C_003", "name": "Subscription Retargeting",        "obj": "CONVERSIONS"},
        ]
        AD_FORMATS = ["TopView", "In-Feed Ad", "Branded Hashtag Challenge", "Spark Ad"]
        COUNTRIES  = ["US", "GB", "AU", "CA", "DE", "FR"]

        rows = []
        current = start_date
        while current <= end_date:
            day_mult = 0.75 if current.weekday() >= 5 else 1.0

            for camp in CAMPAIGNS:
                for ag_idx in range(1, 4):
                    ag_id = f"TT_AG_{camp['id'][-3:]}_{ag_idx:02d}"
                    for ad_idx in range(1, 3):
                        impr     = max(0, int(_jitter(12000 * day_mult * ag_idx * 0.6)))
                        clicks   = int(impr * _jitter(0.018))
                        installs = int(clicks * _jitter(0.09)) if camp["obj"] == "APP_INSTALL" else 0
                        convs    = int(installs * _jitter(0.07)) if camp["obj"] == "CONVERSIONS" else 0
                        spend    = round(_jitter(280 * day_mult * ag_idx * 0.5), 2)
                        vv       = int(impr * _jitter(0.55))

                        rows.append({
                            # Dimensions
                            "stat_time_day":    current.strftime("%Y-%m-%d"),
                            "campaign_id":      camp["id"],
                            "campaign_name":    camp["name"],
                            "objective_type":   camp["obj"],
                            "adgroup_id":       ag_id,
                            "adgroup_name":     f"{camp['name']} - AdGroup {ag_idx}",
                            "ad_id":            f"TT_AD_{ag_id[-5:]}_{ad_idx}",
                            "ad_name":          f"{random.choice(AD_FORMATS)} v{ad_idx}",
                            "country_code":     random.choice(COUNTRIES),
                            "placement":        "TikTok",
                            # Volume metrics
                            "impressions":      impr,
                            "clicks":           clicks,
                            "spend":            spend,
                            # Efficiency metrics (TikTok computes these server-side)
                            "cpm":              round(spend / impr * 1000, 4) if impr else 0,
                            "cpc":              round(spend / clicks, 4) if clicks else 0,
                            "ctr":              round(clicks / impr, 6) if impr else 0,
                            # Conversion metrics
                            "app_install":              installs,
                            "conversion":               convs,
                            "cost_per_conversion":      round(spend / convs, 4) if convs else 0,
                            "value":                    round(convs * random.uniform(6.67, 11.99), 2),
                            # Video metrics
                            "video_play_actions":       vv,
                            "video_watched_2s":         int(vv * _jitter(0.88)),
                            "video_watched_6s":         int(vv * _jitter(0.60)),
                            "video_views_p25":          int(vv * _jitter(0.72)),
                            "video_views_p50":          int(vv * _jitter(0.58) * 0.72),
                            "video_views_p75":          int(vv * _jitter(0.45) * 0.58 * 0.72),
                            "video_views_p100":         int(vv * _jitter(0.35) * 0.45 * 0.58 * 0.72),
                            # Engagement metrics
                            "likes":            int(_jitter(clicks * 0.35)),
                            "comments":         int(_jitter(clicks * 0.04)),
                            "shares":           int(_jitter(clicks * 0.06)),
                            "follows":          int(_jitter(clicks * 0.02)),
                        })
            current += timedelta(days=1)

        return rows

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _chunk_dates(start: date, end: date, max_days: int = 30) -> Iterator[tuple[date, date]]:
        """Split a date range into ≤30-day chunks (TikTok API constraint)."""
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
    from datetime import date

    ext   = TikTokExtractor()
    rows  = ext.extract(date(2025, 1, 1), date(2025, 1, 7))
    path  = ext.save_raw(rows, date(2025, 1, 1), date(2025, 1, 7))

    print(f"\n── Sample row (first record) ──")
    print(json.dumps(rows[0], indent=2))
    print(f"\n── Saved: {path} ({len(rows):,} rows) ──")
