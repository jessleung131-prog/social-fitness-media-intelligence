"""
etl/extractors/appsflyer_extractor.py
───────────────────────────────────────
Production extractor for AppsFlyer Pull API v5.

Report types extracted:
  installs_report       — one row per install, attributed to media source
  in_app_events_report  — one row per in-app event (activity, subscribe, etc.)

Key fields unique to AppsFlyer vs paid media platforms:
  appsflyer_id      — device-level MMP identifier (pseudonymous)
  customer_user_id  — your app's internal user ID (set via SDK at registration)
  media_source      — AppsFlyer's normalized source name (tiktokglobal_int, etc.)
  att_status        — iOS App Tracking Transparency consent
  skad_campaign_id  — SKAdNetwork campaign ID (iOS ATT-denied users only)
  is_retargeting    — true for re-engagement (not new user) campaigns
  install_time      — UTC timestamp of first app open (not a plain date)

Attribution model: last-touch, with SKAN probabilistic fallback for iOS ATT-denied.
Do NOT sum installs from MMP + ad platforms — leads to double counting.
MMP is the single source of truth for install attribution.

API reference:
  https://support.appsflyer.com/hc/en-us/articles/208387843
"""

import csv
import hashlib
import json
import logging
import random
import time
from datetime import date, datetime, timedelta, timezone
from io import StringIO
from pathlib import Path
from typing import Iterator

import requests

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import DATA_RAW_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)

MEDIA_SOURCES = [
    ("tiktokglobal_int",  "TikTok Ads",  0.31),
    ("facebook",          "Meta Ads",    0.38),
    ("reddit_int",        "Reddit Ads",  0.09),
    ("googleadwords_int", "Google UAC",  0.14),
    ("organic",           "Organic",     0.08),
]

SUB_PLANS = [
    ("monthly",  11.99,  0.45),
    ("annual",   79.99,  0.40),
    ("family",  139.99,  0.15),
]

COUNTRIES = ["US", "GB", "AU", "CA", "DE", "FR"]
DEVICES   = ["iOS", "Android"]


class AppsFlyerExtractor:
    """
    Extracts install-level attribution and in-app event data from AppsFlyer.

    In production:  calls AppsFlyer Pull API with API token.
    In development: generates simulated data with exact AppsFlyer schema.

    Usage:
        ext      = AppsFlyerExtractor()
        installs = ext.extract_installs(date(2025,1,1), date(2025,1,31))
        events   = ext.extract_events(date(2025,1,1), date(2025,1,31))
        ext.save_raw(installs, events, date(2025,1,1), date(2025,1,31))
    """

    PULL_API_BASE = "https://hq1.appsflyer.com/api/raw-data/export/app"
    MAX_RETRIES   = 3
    RETRY_BACKOFF = [5, 15, 30]
    REQUEST_DELAY = 1.1   # 60 req/min rate limit

    def __init__(self,
                 app_id:    str = "com.socialfitness.app",
                 api_token: str = "your_api_token"):
        self.app_id         = app_id
        self.api_token      = api_token
        self._is_simulated  = api_token.startswith("your_") or not api_token
        mode = "simulated" if self._is_simulated else "live API"
        log.info(f"AppsFlyerExtractor: {mode} | app_id={app_id}")

    # ── Public interface ──────────────────────────────────────────────────────

    def extract_installs(self, start_date: date, end_date: date) -> list[dict]:
        log.info(f"AppsFlyer installs: {start_date} → {end_date}")
        all_rows = []
        for day in _date_range(start_date, end_date):
            rows = (self._simulate_installs(day) if self._is_simulated
                    else self._fetch_installs_api(day))
            all_rows.extend(rows)
        log.info(f"AppsFlyer installs complete: {len(all_rows):,} rows")
        return all_rows

    def extract_events(self, start_date: date, end_date: date) -> list[dict]:
        log.info(f"AppsFlyer in-app events: {start_date} → {end_date}")
        all_rows = []
        for day in _date_range(start_date, end_date):
            rows = (self._simulate_events(day) if self._is_simulated
                    else self._fetch_events_api(day))
            all_rows.extend(rows)
        log.info(f"AppsFlyer events complete: {len(all_rows):,} rows")
        return all_rows

    def save_raw(self, installs: list[dict], events: list[dict],
                 start_date: date, end_date: date) -> tuple:
        out_dir = DATA_RAW_DIR / "appsflyer" / start_date.strftime("%Y-%m-%d")
        out_dir.mkdir(parents=True, exist_ok=True)

        installs_path = out_dir / "installs_report.csv"
        events_path   = out_dir / "in_app_events_report.csv"
        _write_csv(installs, installs_path)
        _write_csv(events,   events_path)

        meta = {
            "source":           "appsflyer_pull_api_v5",
            "app_id":           self.app_id,
            "extracted_at":     _utcnow(),
            "date_range":       f"{start_date}/{end_date}",
            "simulated":        self._is_simulated,
            "pipeline_version": PIPELINE_VERSION,
            "install_rows":     len(installs),
            "event_rows":       len(events),
            "attribution_note": (
                "Last-touch attribution. iOS ATT-denied users use SKAN "
                "(aggregated, no user-level data). MMP is single source "
                "of truth — do not add ad platform install counts."
            ),
        }
        with open(out_dir / "_meta.json", "w") as f:
            json.dump(meta, f, indent=2)

        log.info(f"AppsFlyer raw saved: installs={len(installs):,} | events={len(events):,}")
        return installs_path, events_path

    # ── Live API ──────────────────────────────────────────────────────────────

    def _fetch_installs_api(self, day: date) -> list[dict]:
        url = (f"{self.PULL_API_BASE}/{self.app_id}/installs_report/v5"
               f"?from={day}&to={day}&timezone=UTC&category=standard")
        resp = self._request_with_retry("GET", url,
                                        headers={"authorization": f"Bearer {self.api_token}"})
        return list(csv.DictReader(StringIO(resp.text)))

    def _fetch_events_api(self, day: date) -> list[dict]:
        events = ("registration,first_activity_uploaded,paywall_viewed,"
                  "trial_started,subscription_started,subscription_renewed,"
                  "subscription_cancelled,share_to_social,friend_invited")
        url = (f"{self.PULL_API_BASE}/{self.app_id}/in_app_events_report/v5"
               f"?from={day}&to={day}&timezone=UTC&event_name={events}")
        resp = self._request_with_retry("GET", url,
                                        headers={"authorization": f"Bearer {self.api_token}"})
        return list(csv.DictReader(StringIO(resp.text)))

    def _request_with_retry(self, method, url, **kwargs):
        for attempt in range(self.MAX_RETRIES):
            try:
                resp = requests.request(method, url, timeout=60, **kwargs)
                if resp.status_code == 429:
                    time.sleep(self.RETRY_BACKOFF[attempt])
                    continue
                resp.raise_for_status()
                time.sleep(self.REQUEST_DELAY)
                return resp
            except requests.RequestException as e:
                if attempt == self.MAX_RETRIES - 1:
                    raise
                time.sleep(self.RETRY_BACKOFF[attempt])

    # ── Simulation: installs ──────────────────────────────────────────────────

    def _simulate_installs(self, day: date) -> list[dict]:
        random.seed(int(hashlib.md5(str(day).encode()).hexdigest(), 16) % 99991)
        n_installs = int(_jitter(320, 0.18))
        weights    = [s[2] for s in MEDIA_SOURCES]
        rows       = []

        for _ in range(n_installs):
            src    = random.choices(MEDIA_SOURCES, weights=weights)[0]
            device = random.choice(DEVICES)
            h, m, s = random.randint(0, 23), random.randint(0, 59), random.randint(0, 59)

            af_id  = _fake_id("af_")
            # customer_user_id only populated after registration (~5% never register)
            reg    = random.random() < 0.82
            cu_id  = _fake_id("cu_") if reg else ""

            # iOS ATT consent
            att    = ("not_applicable" if device == "Android"
                      else random.choices(["authorized","denied","not_determined"],
                                         weights=[0.45, 0.38, 0.17])[0])
            skan   = (f"SKAN_{random.randint(100,999)}"
                      if device == "iOS" and att == "denied" else "")

            # Campaign IDs for paid sources
            if src[0] == "organic":
                camp_id = adset_id = ad_id = camp_name = ""
            else:
                plat_map  = {"tiktokglobal_int":"TT","facebook":"FB",
                             "reddit_int":"RD","googleadwords_int":"GG"}
                prefix    = plat_map.get(src[0], "XX")
                n_camps   = 3 if src[0] != "reddit_int" else 2
                sfx       = f"C_00{random.randint(1, n_camps)}"
                camp_id   = f"{prefix}_{sfx}"
                camp_name = f"{src[1]} - {sfx}"
                adset_id  = f"{prefix}_AS_{sfx}_{random.randint(1,3):02d}"
                ad_id     = f"{prefix}_AD_{random.randint(100,999)}"

            funnel = self._sim_funnel(reg)

            rows.append({
                # Identity
                "appsflyer_id":           af_id,
                "customer_user_id":       cu_id,
                "app_id":                 self.app_id,
                "app_version":            random.choice(["4.1.0","4.2.0","4.3.1"]),
                # Timing
                "install_time":           f"{day} {h:02d}:{m:02d}:{s:02d}",
                "touch_time":             f"{day} {max(0,h-1):02d}:{m:02d}:{s:02d}",
                # Attribution
                "media_source":           src[0],
                "channel":                src[1],
                "campaign":               camp_name,
                "campaign_id":            camp_id,
                "adset":                  adset_id,
                "ad":                     ad_id,
                "is_retargeting":         "false",
                "is_primary_attribution": "true",
                "attribution_type":       "last_touch",
                # Device
                "platform":               device,
                "os_version":             (f"{random.randint(15,17)}.{random.randint(0,4)}"
                                          if device=="iOS" else f"{random.randint(12,14)}.0"),
                "device_type":            random.choice(
                    ["iPhone 14","iPhone 15","iPhone 13"] if device=="iOS"
                    else ["Samsung Galaxy S23","Google Pixel 7","OnePlus 11"]),
                "country_code":           random.choice(COUNTRIES),
                # iOS privacy
                "att_status":             att,
                "idfa":                   _fake_id("IDFA-") if att=="authorized" else "",
                "idfv":                   _fake_id("IDFV-"),
                "skad_campaign_id":       skan,
                "skad_conversion_value":  str(random.randint(0,63)) if skan else "",
                # Funnel flags (post-install outcomes — joined from events report)
                "registration":            str(funnel["registered"]).lower(),
                "first_activity_uploaded": str(funnel["first_activity"]).lower(),
                "paywall_viewed":          str(funnel["paywall_viewed"]).lower(),
                "trial_started":           str(funnel["trial_started"]).lower(),
                "subscribed":              str(funnel["subscribed"]).lower(),
                "subscription_type":       funnel["sub_type"],
                "subscription_value_usd":  str(funnel["sub_value"]),
                "days_to_subscribe":       str(funnel["days_to_sub"]),
            })

        return rows

    def _sim_funnel(self, registered: bool) -> dict:
        first_activity = registered and random.random() < 0.71
        paywall_viewed = first_activity and random.random() < 0.55
        trial_started  = paywall_viewed and random.random() < 0.38
        subscribed     = trial_started and random.random() < 0.52
        if subscribed:
            plan = random.choices(SUB_PLANS, weights=[p[2] for p in SUB_PLANS])[0]
            # Heaviest concentration on Day 0 (41% subscribe same day)
            days = random.choices(range(15), weights=[41,18,8,7,5,4,4,4,2,2,1,1,1,1,1])[0]
        else:
            plan = ("", 0.0)
            days = ""
        return {
            "registered":    registered,
            "first_activity": first_activity,
            "paywall_viewed": paywall_viewed,
            "trial_started":  trial_started,
            "subscribed":     subscribed,
            "sub_type":       plan[0] if subscribed else "",
            "sub_value":      plan[1] if subscribed else 0.0,
            "days_to_sub":    days,
        }

    # ── Simulation: in-app events ─────────────────────────────────────────────

    def _simulate_events(self, day: date) -> list[dict]:
        random.seed(int(hashlib.md5(f"ev_{day}".encode()).hexdigest(), 16) % 99991)
        EVENT_TYPES = [
            ("activity_uploaded",      0.28, {"sport_type":["running","cycling","swimming","hiking","yoga"],"distance_km":None,"duration_min":None}),
            ("kudos_given",            0.18, {}),
            ("kudos_received",         0.15, {}),
            ("segment_attempt",        0.09, {"segment_id":None,"is_pr":["true","false"]}),
            ("club_joined",            0.04, {"club_id":None,"club_type":["running_club","cycling_club","company_team"]}),
            ("challenge_joined",       0.05, {"challenge_id":None,"sponsor":["NorthFace","Garmin","Nike","Adidas"]}),
            ("route_saved",            0.04, {"route_type":["road","trail","gravel"]}),
            ("paywall_viewed",         0.06, {"plan_highlighted":["annual","monthly","family"]}),
            ("subscription_started",   0.03, {"plan":["monthly","annual","family"],"value_usd":[11.99,79.99,139.99],"trial_converted":"true"}),
            ("subscription_renewed",   0.02, {"plan":["monthly","annual","family"],"value_usd":[11.99,79.99,139.99]}),
            ("subscription_cancelled", 0.01, {"reason":["too_expensive","not_using","switching_app","pausing"]}),
            ("share_to_social",        0.03, {"platform":["instagram","tiktok","whatsapp"],"content_type":["activity","route","badge"]}),
            ("friend_invited",         0.02, {"channel":["in_app","share_link","contact_import"]}),
        ]
        weights = [e[1] for e in EVENT_TYPES]
        rows    = []

        for _ in range(int(_jitter(4800, 0.15))):
            et   = random.choices(EVENT_TYPES, weights=weights)[0]
            meta = {}
            for k, v in et[2].items():
                if v is None:
                    if "distance" in k: meta[k] = round(random.uniform(1, 42.2), 2)
                    elif "duration" in k: meta[k] = random.randint(10, 240)
                    else: meta[k] = f"ID_{random.randint(1000,9999)}"
                elif isinstance(v, list):
                    meta[k] = random.choice(v)
                else:
                    meta[k] = v

            rows.append({
                "event_time":       f"{day} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}",
                "appsflyer_id":     _fake_id("af_"),
                "customer_user_id": _fake_id("cu_"),
                "app_id":           self.app_id,
                "event_name":       et[0],
                "event_value":      json.dumps(meta),
                "platform":         random.choice(DEVICES),
                "country_code":     random.choice(COUNTRIES),
                "app_version":      random.choice(["4.1.0","4.2.0","4.3.1"]),
                "media_source":     random.choice([s[0] for s in MEDIA_SOURCES]),
                "is_retargeting":   "false",
            })

        return rows


# ── Helpers ───────────────────────────────────────────────────────────────────

def _date_range(start: date, end: date) -> Iterator[date]:
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)

def _jitter(base: float, pct: float = 0.20) -> float:
    return max(0.0, base * random.uniform(1 - pct, 1 + pct))

def _fake_id(prefix: str = "") -> str:
    return prefix + hashlib.md5(str(random.random()).encode()).hexdigest()[:12]

def _utcnow() -> str:
    return datetime.now(timezone.utc).isoformat()

def _write_csv(rows: list[dict], path: Path) -> None:
    if not rows:
        path.write_text("")
        return
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    ext      = AppsFlyerExtractor()
    installs = ext.extract_installs(date(2025, 1, 1), date(2025, 1, 7))
    events   = ext.extract_events(date(2025, 1, 1), date(2025, 1, 7))
    ext.save_raw(installs, events, date(2025, 1, 1), date(2025, 1, 7))
    print(f"\n── Install sample ──")
    print(json.dumps(installs[0], indent=2))
    print(f"\n── Event sample ──")
    print(json.dumps(events[0], indent=2))
