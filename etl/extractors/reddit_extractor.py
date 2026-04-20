"""
etl/extractors/reddit_extractor.py
────────────────────────────────────
Production extractor for the Reddit Ads API v3.
Reporting endpoint: /adAccounts/{id}/campaigns/{id}/reports

Handles:
  • OAuth2 client_credentials token flow (Reddit uses standard OAuth2)
  • Token refresh on expiry (tokens expire every 1 hour)
  • Campaign-level report requests (Reddit requires per-campaign calls)
  • Pagination via offset/limit
  • Automatic fallback to simulated data when no credentials present

Key schema differences vs TikTok and Meta:
  • date field is ISO-8601 with UTC timestamp, not plain date
  • spend field is spend_usd (explicit currency suffix)
  • No native revenue/conversion-value field — Reddit doesn't support
    value-based optimization natively; conversions tracked via pixel only
  • Attribution window is fixed at 30d click (cannot be changed per request)
  • Reports are per-campaign, not account-wide — requires looping over campaigns
  • subreddit is a dimension (not available in TikTok/Meta)

API reference:
  https://ads-api.reddit.com/docs/
"""

import json
import time
import logging
import hashlib
import random
from base64 import b64encode
from datetime import date, timedelta
from pathlib import Path

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import RedditConfig, DATA_RAW_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)


class RedditExtractor:
    """
    Extracts ad performance from Reddit Ads API v3.

    Notable quirks vs TikTok/Meta:
      1. Auth:   OAuth2 Basic Auth (client_id:client_secret as Basic header)
      2. Scope:  Requests must loop per campaign — no account-wide endpoint
      3. Date:   Returns ISO timestamps, not plain dates
      4. Budget: spend_usd not spend — explicit currency in field name
      5. ROAS:   Not returned — must be computed from pixel conversion value
      6. Window: Always 30d click — stored in _meta, not in each row

    In production:  makes real HTTP calls with OAuth2 token management.
    In development: returns simulated data with exact Reddit schema.

    Usage:
        extractor = RedditExtractor()
        rows = extractor.extract(start_date=date(2025,1,1), end_date=date(2025,1,31))
        extractor.save_raw(rows, date(2025,1,1), date(2025,1,31))
    """

    TOKEN_URL     = "https://www.reddit.com/api/v1/access_token"
    PAGE_SIZE     = 500
    MAX_RETRIES   = 3
    RETRY_BACKOFF = [2, 6, 15]

    def __init__(self, config: RedditConfig = None):
        self.cfg           = config or RedditConfig()
        self._is_simulated = self._detect_placeholder_credentials()
        self._access_token  = None
        self._token_expiry  = 0

        if self._is_simulated:
            log.info("RedditExtractor: no real credentials — using simulated data")
        else:
            log.info("RedditExtractor: live credentials — using real API")

    # ── Credential detection ──────────────────────────────────────────────────

    def _detect_placeholder_credentials(self) -> bool:
        return (
            self.cfg.client_id.startswith("your_")
            or self.cfg.account_id.startswith("your_")
        )

    # ── Public interface ──────────────────────────────────────────────────────

    def extract(self, start_date: date, end_date: date) -> list[dict]:
        """
        Pull all ad-level performance data for the given date range.
        Loops over all campaigns in the account.
        """
        log.info(f"Reddit extract: {start_date} → {end_date}")

        if self._is_simulated:
            rows = self._simulate_response(start_date, end_date)
        else:
            rows = self._fetch_from_api(start_date, end_date)

        log.info(f"Reddit extract complete: {len(rows):,} total rows")
        return rows

    def save_raw(self, rows: list[dict], start_date: date, end_date: date) -> Path:
        """
        Saves raw Reddit API response.
        Path: data/raw/reddit/YYYY-MM-DD/reddit_ads_raw.json

        IMPORTANT: Reddit's fixed 30d attribution window is stored in _meta,
        not in individual rows. The transformer must read this and attach it
        as a column when building the CDM.
        """
        out_dir  = DATA_RAW_DIR / "reddit" / start_date.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "reddit_ads_raw.json"

        payload = {
            "_meta": {
                "source":             "reddit_ads_api_v3",
                "extracted_at":       _utcnow(),
                "date_range":         f"{start_date}/{end_date}",
                "account_id":         self.cfg.account_id,
                "attribution_window": self.cfg.attribution_window,  # always "30d_click"
                "note":               "Reddit does not support configurable attribution windows. All conversions use 30d click. This differs from TikTok (7d) and Meta (7d) — do not compare conversion metrics directly.",
                "simulated":          self._is_simulated,
                "pipeline_version":   PIPELINE_VERSION,
                "row_count":          len(rows),
            },
            "data":     rows,
            "metadata": {"total": len(rows)},
        }

        with open(out_path, "w") as f:
            json.dump(payload, f, indent=2)

        log.info(f"Reddit raw saved: {out_path} ({len(rows):,} rows)")
        return out_path

    # ── Live API ──────────────────────────────────────────────────────────────

    def _get_token(self) -> str:
        """
        Reddit OAuth2: client_credentials flow.
        Tokens expire every 3600s — refreshed automatically.
        """
        if self._access_token and time.time() < self._token_expiry - 60:
            return self._access_token

        credentials  = b64encode(
            f"{self.cfg.client_id}:{self.cfg.client_secret}".encode()
        ).decode()
        headers = {"Authorization": f"Basic {credentials}",
                   "User-Agent": "social-fitness-etl/1.0"}
        data    = {"grant_type": "password",
                   "username": self.cfg.username,
                   "password": self.cfg.password}

        resp = requests.post(self.TOKEN_URL, headers=headers, data=data, timeout=15)
        resp.raise_for_status()
        token_data = resp.json()

        self._access_token = token_data["access_token"]
        self._token_expiry = time.time() + token_data.get("expires_in", 3600)
        log.debug("Reddit token refreshed")
        return self._access_token

    def _fetch_from_api(self, start_date: date, end_date: date) -> list[dict]:
        """
        Reddit requires separate report requests per campaign.
        1. List all campaigns in the account
        2. For each campaign, request the report
        3. Merge all rows
        """
        campaigns = self._list_campaigns()
        log.info(f"  Reddit: found {len(campaigns)} campaigns")

        all_rows = []
        for camp_id in campaigns:
            rows = self._fetch_campaign_report(camp_id, start_date, end_date)
            all_rows.extend(rows)
            log.debug(f"  campaign {camp_id}: {len(rows)} rows")
            time.sleep(0.5)   # conservative rate limiting

        return all_rows

    def _list_campaigns(self) -> list[str]:
        token = self._get_token()
        url   = f"{self.cfg.base_url}/{self.cfg.api_version}/adAccounts/{self.cfg.account_id}/campaigns"
        headers = {"Authorization": f"Bearer {token}",
                   "User-Agent": "social-fitness-etl/1.0"}

        resp = self._request_with_retry("GET", url, headers=headers)
        return [c["id"] for c in resp.json().get("data", [])]

    def _fetch_campaign_report(self, campaign_id: str, start_date: date, end_date: date) -> list[dict]:
        token   = self._get_token()
        url     = f"{self.cfg.base_url}/{self.cfg.api_version}/adAccounts/{self.cfg.account_id}/campaigns/{campaign_id}/reports"
        headers = {"Authorization": f"Bearer {token}",
                   "User-Agent": "social-fitness-etl/1.0"}

        all_rows = []
        offset   = 0

        while True:
            params = {
                "columns":   ",".join(self.cfg.columns),
                "startDate": start_date.isoformat(),
                "endDate":   end_date.isoformat(),
                "breakdown": "day,ad,subreddit,country,device",
                "limit":     self.PAGE_SIZE,
                "offset":    offset,
            }
            resp   = self._request_with_retry("GET", url, headers=headers, params=params)
            data   = resp.json()
            rows   = data.get("data", [])
            all_rows.extend(rows)

            if len(rows) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

        return all_rows

    def _request_with_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.request(method, url, timeout=30, **kwargs)
                if resp.status_code == 429:
                    wait = self.RETRY_BACKOFF[attempt]
                    log.warning(f"Reddit rate limit — waiting {wait}s")
                    time.sleep(wait)
                    continue
                if resp.status_code == 401:
                    # Token expired mid-request — force refresh
                    self._access_token = None
                    kwargs.get("headers", {})["Authorization"] = f"Bearer {self._get_token()}"
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
        Generates realistic Reddit Ads API response rows.

        Key schema differences captured in simulation:
          - date is ISO-8601 with UTC time suffix (not plain YYYY-MM-DD)
          - spend field is spend_usd (not spend)
          - efficiency fields use ecpm/ecpc (not cpm/cpc)
          - subreddit is a native dimension
          - No revenue/ROAS field — Reddit doesn't return this natively
          - app_installs (not app_install or mobile_app_installs)
          - upvotes replaces likes
        """
        random.seed(int(hashlib.md5(str(start_date).encode()).hexdigest(), 16) % 10000)

        CAMPAIGNS = [
            {"id": "RD_C_001", "name": "r/running Community Takeover",  "obj": "awareness"},
            {"id": "RD_C_002", "name": "App Install - r/cycling",        "obj": "app_installs"},
        ]
        AD_FORMATS  = ["Promoted Post", "Video Ad", "Conversation Ad"]
        COUNTRIES   = ["US", "GB", "AU", "CA", "DE", "FR"]
        DEVICES     = ["iOS", "Android", "desktop"]
        SUBREDDITS  = ["r/running", "r/cycling", "r/fitness", "r/triathlon", "r/Strava"]

        rows = []
        current = start_date
        while current <= end_date:
            # Reddit date format: ISO-8601 with Z suffix
            date_str = f"{current.strftime('%Y-%m-%d')}T00:00:00Z"

            for camp in CAMPAIGNS:
                for ag_idx in range(1, 3):
                    ag_id = f"RD_AG_{camp['id'][-3:]}_{ag_idx:02d}"
                    for ad_idx in range(1, 3):
                        impr     = max(0, int(_jitter(6500 * ag_idx * 0.7)))
                        clicks   = int(impr * _jitter(0.012))
                        installs = int(clicks * _jitter(0.08)) if camp["obj"] == "app_installs" else 0
                        # Reddit: spend_usd not spend
                        spend_usd = round(_jitter(180 * ag_idx * 0.6), 2)
                        vv        = int(impr * _jitter(0.38)) if ad_idx == 1 else 0

                        rows.append({
                            # Reddit date format: ISO timestamp, not plain date
                            "date":              date_str,
                            # Campaign hierarchy (same logical structure as TikTok/Meta)
                            "campaign_id":       camp["id"],
                            "campaign_name":     camp["name"],
                            "objective":         camp["obj"],
                            "ad_group_id":       ag_id,
                            "ad_group_name":     f"{camp['name']} - Group {ag_idx}",
                            "ad_id":             f"RD_AD_{ag_id[-5:]}_{ad_idx}",
                            "ad_name":           f"{random.choice(AD_FORMATS)} v{ad_idx}",
                            # Reddit-specific dimensions
                            "subreddit":         random.choice(SUBREDDITS),
                            "country":           random.choice(COUNTRIES),
                            "device":            random.choice(DEVICES),
                            # Volume — NOTE: spend_usd not spend
                            "impressions":       impr,
                            "clicks":            clicks,
                            "spend_usd":         spend_usd,
                            # Efficiency — NOTE: ecpm/ecpc not cpm/cpc
                            "ecpm":              round(spend_usd / impr * 1000, 4) if impr else 0,
                            "ecpc":              round(spend_usd / clicks, 4) if clicks else 0,
                            "ctr":               round(clicks / impr, 6) if impr else 0,
                            # Conversions — NOTE: app_installs not app_install or mobile_app_installs
                            "app_installs":      installs,
                            # NOTE: No revenue/ROAS field in Reddit API
                            # Engagement — NOTE: upvotes not likes
                            "upvotes":           int(_jitter(clicks * 0.45)),
                            "comments":          int(_jitter(clicks * 0.07)),
                            "shares":            int(_jitter(clicks * 0.04)),
                            "video_views":       vv,
                        })
            current += timedelta(days=1)

        return rows

    # ── Utilities ─────────────────────────────────────────────────────────────

    @staticmethod
    def _chunk_dates(start: date, end: date, max_days: int = 90) -> list[tuple]:
        chunks, current = [], start
        while current <= end:
            chunk_end = min(current + timedelta(days=max_days - 1), end)
            chunks.append((current, chunk_end))
            current = chunk_end + timedelta(days=1)
        return chunks


# ── Helpers ───────────────────────────────────────────────────────────────────

def _jitter(base: float, pct: float = 0.25) -> float:
    return max(0.0, base * random.uniform(1 - pct, 1 + pct))

def _utcnow() -> str:
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    ext  = RedditExtractor()
    rows = ext.extract(date(2025, 1, 1), date(2025, 1, 7))
    path = ext.save_raw(rows, date(2025, 1, 1), date(2025, 1, 7))

    print(f"\n── Sample row (first record) ──")
    print(json.dumps(rows[0], indent=2))
    print(f"\n── Schema notes ──")
    print("  date field:    ISO-8601 UTC timestamp (not plain date)")
    print("  spend field:   spend_usd (not 'spend')")
    print("  efficiency:    ecpm/ecpc (not cpm/cpc)")
    print("  installs:      app_installs (not app_install or mobile_app_installs)")
    print("  engagement:    upvotes (not likes)")
    print("  attribution:   always 30d click — stored in _meta, not per row")
    print(f"\n── Saved: {path} ({len(rows):,} rows) ──")
