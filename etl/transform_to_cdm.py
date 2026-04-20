"""
transform_to_cdm.py
────────────────────
Reads raw platform JSON/CSV from data/raw/ and transforms each source
into a standardized Common Data Model (CDM) Parquet/CSV in data/processed/.

CDM tables produced:
  • cdm_paid_media_daily     — unified paid ads (TikTok + Meta + Reddit)
  • cdm_organic_daily        — unified organic post performance
  • cdm_installs             — attributed install events (AppsFlyer)
  • cdm_funnel_events        — in-app lifecycle events
  • cdm_funnel_summary       — daily funnel conversion rates by channel
"""

import json, csv, os
import pandas as pd
from pathlib import Path
from datetime import datetime

RAW_DIR       = Path(__file__).parent.parent / "data" / "raw"
PROCESSED_DIR = Path(__file__).parent.parent / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def load_json(filename):
    with open(RAW_DIR / filename) as f:
        return json.load(f)

def load_csv(filename):
    with open(RAW_DIR / filename) as f:
        return list(csv.DictReader(f))

def save(df, name):
    path = PROCESSED_DIR / f"{name}.csv"
    df.to_csv(path, index=False)
    print(f"  ✓ {name}: {len(df):,} rows → {path.name}")
    return df


# ── 1. CDM: Paid Media Daily ──────────────────────────────────────────────────
"""
Target schema for cdm_paid_media_daily:
  date DATE, platform STRING, channel_type STRING (paid),
  campaign_id, campaign_name, ad_group_id, ad_group_name, ad_id, ad_name,
  country STRING, impressions INT, clicks INT, cost_usd FLOAT,
  installs INT, conversions INT, conversion_value_usd FLOAT,
  video_views INT, engagements INT,
  cpm FLOAT, cpc FLOAT, ctr FLOAT, cpi FLOAT, cps FLOAT, roas FLOAT,
  attribution_window STRING, raw_source STRING
"""

def transform_tiktok():
    raw = load_json("tiktok_ads_raw.json")["data"]["list"]
    rows = []
    for r in raw:
        spend = float(r["spend"])
        installs = int(r["app_install"])
        convs    = int(r["conversion"])
        impr     = int(r["impressions"])
        clicks   = int(r["clicks"])
        eng      = int(r["likes"]) + int(r["comments"]) + int(r["shares"])

        rows.append({
            "date":                  r["stat_time_day"],
            "platform":              "tiktok",
            "channel_type":          "paid",
            "campaign_id":           r["campaign_id"],
            "campaign_name":         r["campaign_name"],
            "objective":             r["objective_type"],
            "ad_group_id":           r["adgroup_id"],
            "ad_group_name":         r["adgroup_name"],
            "ad_id":                 r["ad_id"],
            "ad_name":               r["ad_name"],
            "country":               r["country_code"],
            "impressions":           impr,
            "clicks":                clicks,
            "cost_usd":              spend,
            "installs":              installs,
            "conversions":           convs,
            "conversion_value_usd":  float(r["value"]),
            "video_views":           int(r["video_play_actions"]),
            "video_completions":     int(r["video_views_p100"]),
            "engagements":           eng,
            "cpm":                   round(spend / impr * 1000, 4) if impr else 0,
            "cpc":                   round(spend / clicks, 4) if clicks else 0,
            "ctr":                   round(clicks / impr, 6) if impr else 0,
            "cpi":                   round(spend / installs, 4) if installs else 0,
            "cps":                   round(spend / convs, 4) if convs else 0,
            "roas":                  round(float(r["value"]) / spend, 4) if spend else 0,
            "attribution_window":    "7d_click_1d_view",
            "raw_source":            "tiktok_ads_api_v1.3",
        })
    return pd.DataFrame(rows)


def transform_meta():
    raw = load_json("meta_ads_raw.json")["data"]
    rows = []
    for r in raw:
        spend    = float(r["spend"])
        installs = int(r["mobile_app_installs"])
        purchases= int(r["purchases"])
        impr     = int(r["impressions"])
        clicks   = int(r["link_clicks"])
        eng      = int(r["post_reactions"]) + int(r["post_comments"]) + int(r["post_shares"]) + int(r["post_saves"])

        rows.append({
            "date":                  r["date_start"],
            "platform":              "instagram_meta",
            "channel_type":          "paid",
            "campaign_id":           r["campaign_id"],
            "campaign_name":         r["campaign_name"],
            "objective":             r["objective"],
            "ad_group_id":           r["adset_id"],
            "ad_group_name":         r["adset_name"],
            "ad_id":                 r["ad_id"],
            "ad_name":               r["ad_name"],
            "country":               r["country"],
            "impressions":           impr,
            "clicks":                clicks,
            "cost_usd":              spend,
            "installs":              installs,
            "conversions":           purchases,
            "conversion_value_usd":  round(purchases * 9.99, 2),
            "video_views":           int(r["video_views"]),
            "video_completions":     int(r["video_thruplay_watched_actions"]),
            "engagements":           eng,
            "cpm":                   round(spend / impr * 1000, 4) if impr else 0,
            "cpc":                   round(spend / clicks, 4) if clicks else 0,
            "ctr":                   round(clicks / impr, 6) if impr else 0,
            "cpi":                   round(spend / installs, 4) if installs else 0,
            "cps":                   round(spend / purchases, 4) if purchases else 0,
            "roas":                  float(r["purchase_roas"]),
            "attribution_window":    "7d_click_1d_view",
            "raw_source":            "meta_marketing_api_v19",
        })
    return pd.DataFrame(rows)


def transform_reddit():
    raw = load_json("reddit_ads_raw.json")["data"]
    rows = []
    for r in raw:
        spend    = float(r["spend_usd"])
        installs = int(r["app_installs"])
        impr     = int(r["impressions"])
        clicks   = int(r["clicks"])
        eng      = int(r["upvotes"]) + int(r["comments"]) + int(r["shares"])

        rows.append({
            "date":                  r["date"][:10],   # strip time component
            "platform":              "reddit",
            "channel_type":          "paid",
            "campaign_id":           r["campaign_id"],
            "campaign_name":         r["campaign_name"],
            "objective":             r["objective"],
            "ad_group_id":           r["ad_group_id"],
            "ad_group_name":         r["ad_group_name"],
            "ad_id":                 r["ad_id"],
            "ad_name":               r["ad_name"],
            "country":               r["country"],
            "impressions":           impr,
            "clicks":                clicks,
            "cost_usd":              spend,
            "installs":              installs,
            "conversions":           0,
            "conversion_value_usd":  0.0,
            "video_views":           int(r["video_views"]),
            "video_completions":     0,
            "engagements":           eng,
            "cpm":                   round(spend / impr * 1000, 4) if impr else 0,
            "cpc":                   round(spend / clicks, 4) if clicks else 0,
            "ctr":                   round(clicks / impr, 6) if impr else 0,
            "cpi":                   round(spend / installs, 4) if installs else 0,
            "cps":                   0,
            "roas":                  0.0,
            "attribution_window":    "30d_click",       # Reddit default
            "raw_source":            "reddit_ads_api_v3",
        })
    return pd.DataFrame(rows)


def build_paid_media_cdm():
    df = pd.concat([transform_tiktok(), transform_meta(), transform_reddit()], ignore_index=True)
    df["date"] = pd.to_datetime(df["date"])
    df = df.sort_values(["date", "platform", "campaign_id"])
    return save(df, "cdm_paid_media_daily")


# ── 2. CDM: Organic Daily ─────────────────────────────────────────────────────
def build_organic_cdm():
    raw = load_json("organic_content_raw.json")
    rows = []
    for r in raw:
        plat = r["platform"]
        rows.append({
            "date":             r["published_date"],
            "platform":         plat,
            "channel_type":     "organic",
            "post_id":          r["post_id"],
            "content_type":     r["content_type"],
            "topic":            r["topic"],
            "is_viral":         r["is_viral"],
            "reach":            r["reach"],
            "impressions":      r["impressions"],
            "engagement_rate":  r["engagement_rate"],
            "likes":            r.get("likes", 0),
            "comments":         r.get("comments", 0),
            "shares":           r.get("shares", 0),
            "saves":            r.get("saves", 0),
            "video_views":      r.get("video_views", r.get("total_play", 0)),
            "avg_watch_time_s": r.get("avg_watch_time_s", 0),
            "video_completion_rate": r.get("completion_rate", 0),
            "profile_visits":   r.get("profile_visits", 0),
            "follower_delta":   r.get("follower_delta", r.get("fans_delta", r.get("subscriber_delta", 0))),
            "outbound_clicks":  r.get("outbound_clicks", 0),
            "raw_source":       f"{plat}_organic_api",
        })
    df = pd.DataFrame(rows)
    df["date"] = pd.to_datetime(df["date"])
    df["engagements"] = df["likes"] + df["comments"] + df["shares"] + df["saves"]
    df = df.sort_values(["date", "platform"])
    return save(df, "cdm_organic_daily")


# ── 3. CDM: Installs (AppsFlyer) ─────────────────────────────────────────────
def build_installs_cdm():
    raw = load_csv("appsflyer_raw.csv")
    rows = []
    for r in raw:
        rows.append({
            "install_date":          r["install_time"][:10],
            "appsflyer_id":          r["appsflyer_id"],
            "customer_user_id":      r["customer_user_id"],
            "media_source":          r["media_source"],
            "channel":               r["channel"],
            "campaign_id":           r["campaign"],
            "platform":              r["platform"].lower(),
            "country":               r["country_code"],
            "device_type":           r["device_type"],
            "app_version":           r["app_version"],
            "is_retargeting":        r["is_retargeting"] == "true",
            "att_status":            r["att_status"],
            "registered":            r["registration_event"] == "true",
            "first_activity":        r["first_activity_uploaded"] == "true",
            "paywall_viewed":        r["paywall_viewed"] == "true",
            "trial_started":         r["trial_started"] == "true",
            "subscribed":            r["subscribed"] == "true",
            "subscription_type":     r["subscription_type"],
            "subscription_value_usd":float(r["subscription_value_usd"] or 0),
            "days_to_subscribe":     int(r["days_to_subscribe"]) if r["days_to_subscribe"] else None,
            "raw_source":            "appsflyer_pull_api",
        })
    df = pd.DataFrame(rows)
    df["install_date"] = pd.to_datetime(df["install_date"])
    return save(df, "cdm_installs")


# ── 4. CDM: Funnel Events ─────────────────────────────────────────────────────
def build_funnel_events_cdm():
    raw = load_json("app_events_raw.json")
    rows = []
    for r in raw:
        rows.append({
            "event_id":        r["event_id"],
            "user_id":         r["user_id"],
            "event_name":      r["event_name"],
            "event_date":      r["event_timestamp"][:10],
            "event_timestamp": r["event_timestamp"],
            "platform":        r["platform"].lower(),
            "app_version":     r["app_version"],
            "session_id":      r["session_id"],
            "properties":      r["properties"],
        })
    df = pd.DataFrame(rows)
    df["event_date"] = pd.to_datetime(df["event_date"])
    return save(df, "cdm_funnel_events")


# ── 5. CDM: Funnel Summary (daily × channel) ──────────────────────────────────
def build_funnel_summary(installs_df):
    """Aggregate install-level attribution into a daily funnel by channel."""
    grp = installs_df.groupby(["install_date", "channel"]).agg(
        installs         = ("appsflyer_id",          "count"),
        registrations    = ("registered",             "sum"),
        first_activities = ("first_activity",         "sum"),
        paywall_views    = ("paywall_viewed",          "sum"),
        trials           = ("trial_started",           "sum"),
        subscriptions    = ("subscribed",              "sum"),
        revenue_usd      = ("subscription_value_usd",  "sum"),
    ).reset_index()

    grp["reg_rate"]     = (grp["registrations"]    / grp["installs"]).round(4)
    grp["act_rate"]     = (grp["first_activities"] / grp["registrations"].replace(0,1)).round(4)
    grp["paywall_rate"] = (grp["paywall_views"]    / grp["first_activities"].replace(0,1)).round(4)
    grp["trial_rate"]   = (grp["trials"]           / grp["paywall_views"].replace(0,1)).round(4)
    grp["sub_rate"]     = (grp["subscriptions"]    / grp["trials"].replace(0,1)).round(4)
    grp["overall_cvr"]  = (grp["subscriptions"]    / grp["installs"]).round(4)

    return save(grp, "cdm_funnel_summary")


# ── Main ──────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("\n🔄 Transforming raw data → Common Data Model...\n")
    build_paid_media_cdm()
    build_organic_cdm()
    installs = build_installs_cdm()
    build_funnel_events_cdm()
    build_funnel_summary(installs)
    print(f"\n✅ All CDM tables written to {PROCESSED_DIR}\n")
