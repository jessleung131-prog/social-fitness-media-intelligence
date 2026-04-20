"""
simulate_platform_data.py
─────────────────────────
Generates realistic simulated raw API payloads for:
  • TikTok Ads API
  • Meta (Instagram) Marketing API
  • Reddit Ads API
  • AppsFlyer MMP (mobile attribution)
  • App event stream (in-app funnel events)
  • Organic content (Instagram, TikTok, Reddit)

Data mirrors real API field names, types, and quirks.
Run standalone to write all raw JSON/CSV to data/raw/.
"""

import json
import csv
import random
import hashlib
import os
from datetime import date, timedelta
from pathlib import Path

random.seed(42)

# ── Config ────────────────────────────────────────────────────────────────────
START_DATE = date(2024, 10, 1)
END_DATE   = date(2025, 3, 31)
RAW_DIR    = Path(__file__).parent.parent / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

CAMPAIGNS = {
    "tiktok": [
        {"campaign_id": "TT_C_001", "campaign_name": "Brand Awareness - Running Q4",  "objective": "REACH"},
        {"campaign_id": "TT_C_002", "campaign_name": "App Install - Cycling Audience", "objective": "APP_INSTALL"},
        {"campaign_id": "TT_C_003", "campaign_name": "Subscription Retargeting",       "objective": "CONVERSIONS"},
    ],
    "meta": [
        {"campaign_id": "FB_C_001", "campaign_name": "Fitness Enthusiasts - Awareness",  "objective": "BRAND_AWARENESS"},
        {"campaign_id": "FB_C_002", "campaign_name": "App Installs - US/UK/AU",          "objective": "APP_INSTALLS"},
        {"campaign_id": "FB_C_003", "campaign_name": "Lookalike - Premium Subscribers",  "objective": "CONVERSIONS"},
    ],
    "reddit": [
        {"campaign_id": "RD_C_001", "campaign_name": "r/running Community Takeover",    "objective": "awareness"},
        {"campaign_id": "RD_C_002", "campaign_name": "App Install - r/cycling",          "objective": "app_installs"},
    ],
}

AD_FORMATS = {
    "tiktok": ["TopView", "In-Feed Ad", "Branded Hashtag Challenge", "Spark Ad"],
    "meta":   ["Reel", "Story", "Feed Video", "Carousel", "Collection"],
    "reddit": ["Promoted Post", "Video Ad", "Conversation Ad"],
}

COUNTRIES = ["US", "GB", "AU", "CA", "DE", "FR"]
DEVICES   = ["iOS", "Android"]


def date_range(start: date, end: date):
    d = start
    while d <= end:
        yield d
        d += timedelta(days=1)


def fake_user_id(prefix="u"):
    return prefix + hashlib.md5(str(random.random()).encode()).hexdigest()[:12]


def jitter(base, pct=0.25):
    return max(0, base * random.uniform(1 - pct, 1 + pct))


# ── 1. TikTok Ads API ─────────────────────────────────────────────────────────
def simulate_tiktok_ads():
    """
    Mirrors TikTok Business API v1.3 /report/integrated/get/ response.
    Granularity: campaign × ad_group × ad × day
    """
    rows = []
    for day in date_range(START_DATE, END_DATE):
        is_weekend = day.weekday() >= 5
        day_mult   = 0.75 if is_weekend else 1.0
        for camp in CAMPAIGNS["tiktok"]:
            for ad_group_idx in range(1, 4):
                ad_group_id   = f"TT_AG_{camp['campaign_id'][-3:]}_{ad_group_idx:02d}"
                ad_group_name = f"{camp['campaign_name']} - AdGroup {ad_group_idx}"
                for ad_idx in range(1, 3):
                    ad_id   = f"TT_AD_{ad_group_id[-5:]}_{ad_idx}"
                    ad_name = f"{random.choice(AD_FORMATS['tiktok'])} v{ad_idx}"

                    impressions = int(jitter(12000 * day_mult * ad_group_idx * 0.6))
                    clicks      = int(impressions * jitter(0.018))
                    installs    = int(clicks * jitter(0.09))  if camp["objective"] == "APP_INSTALL"   else 0
                    conversions = int(installs * jitter(0.07)) if camp["objective"] == "CONVERSIONS"  else 0
                    spend       = round(jitter(280 * day_mult * ad_group_idx * 0.5), 2)
                    video_views = int(impressions * jitter(0.55))
                    vv_25       = int(video_views * jitter(0.72))
                    vv_50       = int(vv_25 * jitter(0.58))
                    vv_75       = int(vv_50 * jitter(0.45))
                    vv_100      = int(vv_75 * jitter(0.35))

                    rows.append({
                        "stat_time_day":       day.strftime("%Y-%m-%d"),
                        "campaign_id":         camp["campaign_id"],
                        "campaign_name":       camp["campaign_name"],
                        "objective_type":      camp["objective"],
                        "adgroup_id":          ad_group_id,
                        "adgroup_name":        ad_group_name,
                        "ad_id":               ad_id,
                        "ad_name":             ad_name,
                        "country_code":        random.choice(COUNTRIES),
                        "placement":           "TikTok",
                        "impressions":         impressions,
                        "clicks":              clicks,
                        "spend":               spend,
                        "cpm":                 round(spend / impressions * 1000, 4) if impressions else 0,
                        "cpc":                 round(spend / clicks, 4) if clicks else 0,
                        "ctr":                 round(clicks / impressions, 6) if impressions else 0,
                        "app_install":         installs,
                        "conversion":          conversions,
                        "cost_per_conversion": round(spend / conversions, 4) if conversions else 0,
                        "value":               round(conversions * random.uniform(6.67, 11.99), 2),
                        "video_play_actions":  video_views,
                        "video_watched_2s":    int(video_views * jitter(0.88)),
                        "video_watched_6s":    int(video_views * jitter(0.60)),
                        "video_views_p25":     vv_25,
                        "video_views_p50":     vv_50,
                        "video_views_p75":     vv_75,
                        "video_views_p100":    vv_100,
                        "likes":               int(jitter(clicks * 0.35)),
                        "comments":            int(jitter(clicks * 0.04)),
                        "shares":              int(jitter(clicks * 0.06)),
                        "follows":             int(jitter(clicks * 0.02)),
                    })

    out = RAW_DIR / "tiktok_ads_raw.json"
    with open(out, "w") as f:
        json.dump({"code": 0, "message": "OK", "data": {"list": rows, "page_info": {"total_number": len(rows)}}}, f, indent=2)
    print(f"  ✓ TikTok Ads: {len(rows):,} rows → {out.name}")
    return rows


# ── 2. Meta (Instagram) Marketing API ────────────────────────────────────────
def simulate_meta_ads():
    """
    Mirrors Meta Marketing API v19.0 /act_{ad_account_id}/insights response.
    Granularity: campaign × adset × ad × day
    """
    rows = []
    for day in date_range(START_DATE, END_DATE):
        is_weekend = day.weekday() >= 5
        day_mult   = 0.80 if is_weekend else 1.0
        for camp in CAMPAIGNS["meta"]:
            for adset_idx in range(1, 4):
                adset_id   = f"FB_AS_{camp['campaign_id'][-3:]}_{adset_idx:02d}"
                adset_name = f"{camp['campaign_name']} - AdSet {adset_idx}"
                for ad_idx in range(1, 3):
                    ad_id   = f"FB_AD_{adset_id[-5:]}_{ad_idx}"
                    fmt     = random.choice(AD_FORMATS["meta"])

                    impressions       = int(jitter(18000 * day_mult * adset_idx * 0.55))
                    reach             = int(impressions * jitter(0.78))
                    link_clicks       = int(impressions * jitter(0.014))
                    mobile_app_installs = int(link_clicks * jitter(0.11)) if camp["objective"] == "APP_INSTALLS" else 0
                    purchases         = int(mobile_app_installs * jitter(0.06)) if camp["objective"] == "CONVERSIONS" else 0
                    spend             = round(jitter(320 * day_mult * adset_idx * 0.5), 2)
                    video_views       = int(impressions * jitter(0.42)) if fmt in ["Reel", "Feed Video", "Story"] else 0
                    thruplay          = int(video_views * jitter(0.28))

                    rows.append({
                        "date_start":              day.strftime("%Y-%m-%d"),
                        "date_stop":               day.strftime("%Y-%m-%d"),
                        "campaign_id":             camp["campaign_id"],
                        "campaign_name":           camp["campaign_name"],
                        "objective":               camp["objective"],
                        "adset_id":                adset_id,
                        "adset_name":              adset_name,
                        "ad_id":                   ad_id,
                        "ad_name":                 f"{fmt} Creative v{ad_idx}",
                        "country":                 random.choice(COUNTRIES),
                        "publisher_platform":      random.choice(["instagram", "facebook"]),
                        "platform_position":       random.choice(["feed", "story", "reels", "explore"]),
                        "impressions":             impressions,
                        "reach":                   reach,
                        "frequency":               round(impressions / reach, 3) if reach else 0,
                        "clicks":                  link_clicks + int(jitter(link_clicks * 0.3)),
                        "link_clicks":             link_clicks,
                        "spend":                   spend,
                        "cpm":                     round(spend / impressions * 1000, 4) if impressions else 0,
                        "cpc":                     round(spend / link_clicks, 4) if link_clicks else 0,
                        "ctr":                     round(link_clicks / impressions, 6) if impressions else 0,
                        "mobile_app_installs":     mobile_app_installs,
                        "cost_per_app_install":    round(spend / mobile_app_installs, 4) if mobile_app_installs else 0,
                        "purchases":               purchases,
                        "purchase_roas":           round(purchases * 9.99 / spend, 4) if spend else 0,
                        "video_views":             video_views,
                        "video_thruplay_watched_actions": thruplay,
                        "video_p25_watched_actions": int(video_views * jitter(0.70)),
                        "video_p50_watched_actions": int(video_views * jitter(0.52)),
                        "video_p75_watched_actions": int(video_views * jitter(0.38)),
                        "video_p100_watched_actions": thruplay,
                        "post_engagement":         int(jitter(link_clicks * 1.8)),
                        "post_reactions":          int(jitter(link_clicks * 0.9)),
                        "post_comments":           int(jitter(link_clicks * 0.08)),
                        "post_shares":             int(jitter(link_clicks * 0.12)),
                        "post_saves":              int(jitter(link_clicks * 0.22)),
                        "fb_login_id":             fake_user_id("fb_"),
                    })

    out = RAW_DIR / "meta_ads_raw.json"
    with open(out, "w") as f:
        json.dump({"data": rows, "paging": {"cursors": {}}}, f, indent=2)
    print(f"  ✓ Meta Ads:   {len(rows):,} rows → {out.name}")
    return rows


# ── 3. Reddit Ads API ─────────────────────────────────────────────────────────
def simulate_reddit_ads():
    """
    Mirrors Reddit Ads API v3 /adAccounts/{id}/campaigns/{id}/reports response.
    """
    rows = []
    for day in date_range(START_DATE, END_DATE):
        for camp in CAMPAIGNS["reddit"]:
            for ag_idx in range(1, 3):
                ag_id = f"RD_AG_{camp['campaign_id'][-3:]}_{ag_idx:02d}"
                for ad_idx in range(1, 3):
                    impressions = int(jitter(6500 * ag_idx * 0.7))
                    clicks      = int(impressions * jitter(0.012))
                    installs    = int(clicks * jitter(0.08)) if camp["objective"] == "app_installs" else 0
                    spend_usd   = round(jitter(180 * ag_idx * 0.6), 2)

                    rows.append({
                        "date":              day.strftime("%Y-%m-%dT00:00:00Z"),
                        "campaign_id":       camp["campaign_id"],
                        "campaign_name":     camp["campaign_name"],
                        "objective":         camp["objective"],
                        "ad_group_id":       ag_id,
                        "ad_group_name":     f"{camp['campaign_name']} - Group {ag_idx}",
                        "ad_id":             f"RD_AD_{ag_id[-5:]}_{ad_idx}",
                        "ad_name":           f"{random.choice(AD_FORMATS['reddit'])} v{ad_idx}",
                        "subreddit":         random.choice(["r/running", "r/cycling", "r/fitness", "r/triathlon"]),
                        "country":           random.choice(COUNTRIES),
                        "device":            random.choice(DEVICES),
                        "impressions":       impressions,
                        "clicks":            clicks,
                        "spend_usd":         spend_usd,
                        "ecpm":              round(spend_usd / impressions * 1000, 4) if impressions else 0,
                        "ecpc":              round(spend_usd / clicks, 4) if clicks else 0,
                        "ctr":               round(clicks / impressions, 6) if impressions else 0,
                        "app_installs":      installs,
                        "upvotes":           int(jitter(clicks * 0.45)),
                        "comments":          int(jitter(clicks * 0.07)),
                        "shares":            int(jitter(clicks * 0.04)),
                        "video_views":       int(impressions * jitter(0.38)) if ad_idx == 1 else 0,
                        "reddit_user_id":    fake_user_id("rd_"),
                    })

    out = RAW_DIR / "reddit_ads_raw.json"
    with open(out, "w") as f:
        json.dump({"data": rows, "metadata": {"total": len(rows)}}, f, indent=2)
    print(f"  ✓ Reddit Ads: {len(rows):,} rows → {out.name}")
    return rows


# ── 4. AppsFlyer MMP Attribution ──────────────────────────────────────────────
def simulate_appsflyer():
    """
    Mirrors AppsFlyer Pull API / Data Locker export schema.
    One row per attributed install event with downstream funnel events.
    """
    rows = []
    media_sources = [
        ("tiktokglobal_int", "TikTok Ads", 0.31),
        ("facebook",         "Meta Ads",   0.38),
        ("reddit_int",       "Reddit Ads", 0.09),
        ("googleadwords_int","Google UAC", 0.14),
        ("organic",          "Organic",    0.08),
    ]

    for day in date_range(START_DATE, END_DATE):
        daily_installs = int(jitter(320))
        for _ in range(daily_installs):
            src = media_sources[random.choices(range(len(media_sources)), weights=[s[2] for s in media_sources])[0]]
            device   = random.choice(DEVICES)
            country  = random.choice(COUNTRIES)

            install_time = f"{day.strftime('%Y-%m-%d')} {random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            appsflyer_id = fake_user_id("af_")
            customer_uid = fake_user_id("cu_") if random.random() > 0.05 else ""

            # Funnel: install → register → first_activity → paywall → subscribe
            registered        = random.random() < 0.82
            first_activity    = registered and random.random() < 0.71
            paywall_viewed    = first_activity and random.random() < 0.55
            trial_started     = paywall_viewed and random.random() < 0.38
            subscribed        = trial_started and random.random() < 0.52
            sub_value         = random.choice([6.67, 11.99, 79.99, 139.99]) if subscribed else 0
            sub_type          = random.choice(["monthly", "monthly", "annual", "family"]) if subscribed else ""

            camp_id = ""
            adset_id = ""
            ad_id = ""
            if src[0] != "organic":
                plat = src[0].split("_")[0]
                camp_id  = random.choice([c["campaign_id"] for c in CAMPAIGNS.get(
                    "tiktok" if "tiktok" in src[0] else "meta" if "facebook" in src[0] else "reddit", CAMPAIGNS["meta"]
                )])

            rows.append({
                "appsflyer_id":            appsflyer_id,
                "customer_user_id":        customer_uid,
                "install_time":            install_time,
                "event_time":              install_time,
                "event_name":              "install",
                "media_source":            src[0],
                "channel":                 src[1],
                "campaign":                camp_id,
                "adset":                   adset_id,
                "ad":                      ad_id,
                "platform":                device,
                "country_code":            country,
                "app_id":                  "com.socialfitness.app",
                "app_version":             random.choice(["4.1.0", "4.2.0", "4.3.1"]),
                "os_version":              f"{random.randint(14,17)}.{random.randint(0,4)}" if device == "iOS" else f"{random.randint(11,14)}.0",
                "device_type":             random.choice(["iPhone", "Samsung Galaxy", "Google Pixel", "iPad"]),
                "is_retargeting":          str(random.random() < 0.12).lower(),
                "registration_event":      "true" if registered else "false",
                "registration_time":       install_time if registered else "",
                "first_activity_uploaded": "true" if first_activity else "false",
                "paywall_viewed":          "true" if paywall_viewed else "false",
                "trial_started":           "true" if trial_started else "false",
                "subscribed":              "true" if subscribed else "false",
                "subscription_type":       sub_type,
                "subscription_value_usd":  sub_value,
                "days_to_subscribe":       random.randint(0, 14) if subscribed else "",
                "is_primary_attribution":  "true",
                "att_status":              random.choice(["authorized", "denied", "not_determined"]) if device == "iOS" else "n/a",
                "skad_campaign_id":        f"SKAN_{random.randint(100,999)}" if device == "iOS" else "",
            })

    out = RAW_DIR / "appsflyer_raw.csv"
    with open(out, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=rows[0].keys())
        w.writeheader()
        w.writerows(rows)
    print(f"  ✓ AppsFlyer:  {len(rows):,} rows → {out.name}")
    return rows


# ── 5. In-App Event Stream ────────────────────────────────────────────────────
def simulate_app_events():
    """
    Simulates server-side app event log (e.g. from Firebase / Segment / Amplitude).
    Covers the full post-install engagement lifecycle.
    """
    event_types = [
        ("activity_uploaded",    0.28, {"sport_type": ["running","cycling","swimming","hiking","yoga"], "distance_km": None, "duration_min": None}),
        ("kudos_given",          0.18, {}),
        ("kudos_received",       0.15, {}),
        ("segment_attempt",      0.09, {"segment_id": None}),
        ("club_joined",          0.04, {"club_id": None}),
        ("challenge_joined",     0.05, {"challenge_id": None, "sponsor": ["NorthFace","GarminConnect","Nike","Adidas"]}),
        ("route_saved",          0.04, {}),
        ("paywall_viewed",       0.06, {"plan_shown": ["monthly","annual","family"]}),
        ("subscription_started", 0.03, {"plan": ["monthly","annual","family"], "value_usd": [11.99, 79.99, 139.99]}),
        ("subscription_renewed", 0.02, {}),
        ("subscription_cancelled",0.01, {"reason": ["too_expensive","not_using","switching_app","pausing"]}),
        ("share_to_social",      0.03, {"platform": ["instagram","tiktok","twitter","whatsapp"]}),
        ("friend_invited",       0.02, {}),
    ]

    rows = []
    user_pool = [fake_user_id("cu_") for _ in range(2500)]

    for day in date_range(START_DATE, END_DATE):
        n_events = int(jitter(4800))
        for _ in range(n_events):
            evt_name, _, meta_template = random.choices(
                event_types, weights=[e[1] for e in event_types]
            )[0]
            # re-draw properly
            et = random.choices(event_types, weights=[e[1] for e in event_types])[0]
            evt_name = et[0]
            meta_t   = et[2]

            meta = {}
            for k, v in meta_t.items():
                if v is None:
                    if k == "distance_km":  meta[k] = round(random.uniform(1, 42), 2)
                    elif k == "duration_min": meta[k] = random.randint(10, 240)
                    elif k == "segment_id": meta[k] = f"SEG_{random.randint(1000,9999)}"
                    elif k == "club_id":    meta[k] = f"CLUB_{random.randint(100,999)}"
                    elif k == "challenge_id": meta[k] = f"CH_{random.randint(10,99)}"
                else:
                    meta[k] = random.choice(v)

            rows.append({
                "event_id":        fake_user_id("ev_"),
                "user_id":         random.choice(user_pool),
                "event_name":      evt_name,
                "event_timestamp": f"{day.strftime('%Y-%m-%d')}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}Z",
                "platform":        random.choice(DEVICES),
                "app_version":     random.choice(["4.1.0", "4.2.0", "4.3.1"]),
                "session_id":      fake_user_id("ss_"),
                "properties":      json.dumps(meta),
            })

    out = RAW_DIR / "app_events_raw.json"
    with open(out, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"  ✓ App Events: {len(rows):,} rows → {out.name}")
    return rows


# ── 6. Organic Content Performance ───────────────────────────────────────────
def simulate_organic():
    """
    Simulates organic post performance from Instagram Graph API,
    TikTok Display API, and Reddit (manual/scrape).
    """
    content_types = {
        "instagram": ["Reel", "Feed Post", "Story", "Carousel"],
        "tiktok":    ["Standard Video", "Duet", "Stitch", "Live"],
        "reddit":    ["Text Post", "Image Post", "Link Post"],
    }
    topics = ["route_share", "race_recap", "training_tip", "club_spotlight",
              "challenge_completion", "gear_review", "athlete_story", "product_feature"]

    rows = []
    post_id = 1000

    for platform, ctypes in content_types.items():
        # ~3 posts per week per platform
        for day in date_range(START_DATE, END_DATE):
            if random.random() > 0.43:
                continue
            ctype   = random.choice(ctypes)
            topic   = random.choice(topics)
            is_viral = random.random() < 0.08

            base_reach = 45000 if is_viral else int(jitter(8500))
            eng_rate   = jitter(0.042 if is_viral else 0.022)

            row = {
                "post_id":         f"{platform[:2].upper()}_{post_id}",
                "platform":        platform,
                "content_type":    ctype,
                "topic":           topic,
                "published_date":  day.strftime("%Y-%m-%d"),
                "is_viral":        is_viral,
                "reach":           base_reach,
                "impressions":     int(base_reach * jitter(1.35)),
                "engagement_rate": round(eng_rate, 5),
            }

            if platform == "instagram":
                eng = int(base_reach * eng_rate)
                row.update({
                    "media_id":        f"IG_{post_id}",
                    "likes":           int(eng * 0.62),
                    "comments":        int(eng * 0.06),
                    "saves":           int(eng * 0.18),
                    "shares":          int(eng * 0.14),
                    "video_views":     int(base_reach * jitter(0.55)) if ctype in ["Reel","Story"] else 0,
                    "avg_watch_time_s":round(jitter(14.5), 1) if ctype == "Reel" else 0,
                    "profile_visits":  int(eng * 0.09),
                    "follower_delta":  int(jitter(12)),
                })
            elif platform == "tiktok":
                plays = int(base_reach * jitter(1.8))
                eng   = int(plays * eng_rate)
                row.update({
                    "video_id":          f"TT_{post_id}",
                    "total_play":        plays,
                    "likes":             int(eng * 0.71),
                    "comments":          int(eng * 0.05),
                    "shares":            int(eng * 0.12),
                    "avg_watch_time_s":  round(jitter(9.2), 1),
                    "completion_rate":   round(jitter(0.31), 4),
                    "profile_visits":    int(plays * jitter(0.04)),
                    "fans_delta":        int(jitter(28)),
                    "duets":             int(jitter(8)) if ctype == "Duet" else 0,
                    "stitches":          int(jitter(5)) if ctype == "Stitch" else 0,
                })
            else:  # reddit
                eng = int(base_reach * eng_rate)
                row.update({
                    "reddit_post_id":  f"RD_{post_id}",
                    "subreddit":       random.choice(["r/running","r/cycling","r/fitness","r/Strava"]),
                    "upvotes":         int(eng * 0.75),
                    "upvote_ratio":    round(jitter(0.88), 3),
                    "comments":        int(eng * 0.18),
                    "shares":          int(eng * 0.07),
                    "unique_views":    base_reach,
                    "outbound_clicks": int(base_reach * jitter(0.03)),
                    "subscriber_delta":int(jitter(4)),
                })

            rows.append(row)
            post_id += 1

    out = RAW_DIR / "organic_content_raw.json"
    with open(out, "w") as f:
        json.dump(rows, f, indent=2)
    print(f"  ✓ Organic:    {len(rows):,} rows → {out.name}")
    return rows


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n📡 Simulating platform data (Oct 2024 → Mar 2025)...\n")
    simulate_tiktok_ads()
    simulate_meta_ads()
    simulate_reddit_ads()
    simulate_appsflyer()
    simulate_app_events()
    simulate_organic()
    print(f"\n✅ All raw data written to {RAW_DIR}\n")
