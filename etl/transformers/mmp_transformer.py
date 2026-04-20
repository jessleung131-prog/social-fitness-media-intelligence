"""
etl/transformers/mmp_transformer.py
Transforms AppsFlyer install + event data into CDM tables:
  cdm_installs, cdm_funnel_events, cdm_funnel_summary, cdm_ltv_cohort
"""
import json, logging
from datetime import date, datetime, timezone
from pathlib import Path
import pandas as pd
import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import DATA_CDM_DIR, PIPELINE_VERSION

log = logging.getLogger(__name__)

MEDIA_SOURCE_MAP = {
    "tiktokglobal_int": "TikTok Ads", "facebook": "Meta Ads",
    "reddit_int": "Reddit Ads", "googleadwords_int": "Google UAC",
    "organic": "Organic", "apple_search_ads": "Apple Search Ads",
}

class MMPTransformer:
    def __init__(self):
        self._extracted_at = datetime.now(timezone.utc).isoformat()
        self._pipeline_version = PIPELINE_VERSION

    def transform_installs(self, raw_rows):
        log.info(f"Transforming installs: {len(raw_rows):,} raw rows")
        rows = []
        for r in raw_rows:
            raw_src  = r.get("media_source", "unknown")
            channel  = MEDIA_SOURCE_MAP.get(raw_src, raw_src)
            sub_val  = _float(r.get("subscription_value_usd", 0))
            rows.append({
                "appsflyer_id":          r.get("appsflyer_id", ""),
                "customer_user_id":      r.get("customer_user_id", ""),
                "app_id":                r.get("app_id", ""),
                "install_date":          r.get("install_time", "")[:10],
                "install_timestamp":     r.get("install_time", ""),
                "media_source":          raw_src,
                "channel":               channel,
                "campaign_id":           r.get("campaign_id", ""),
                "campaign_name":         r.get("campaign", ""),
                "adset_id":              r.get("adset", ""),
                "ad_id":                 r.get("ad", ""),
                "is_retargeting":        _bool(r.get("is_retargeting", "false")),
                "is_primary_attribution":_bool(r.get("is_primary_attribution", "true")),
                "platform":              r.get("platform", "").lower(),
                "os_version":            r.get("os_version", ""),
                "device_type":           r.get("device_type", ""),
                "country":               r.get("country_code", ""),
                "att_status":            r.get("att_status", "not_applicable"),
                "has_idfa":              bool(r.get("idfa", "")),
                "is_skan":               bool(r.get("skad_campaign_id", "")),
                "skad_campaign_id":      r.get("skad_campaign_id", ""),
                "registered":            _bool(r.get("registration", "false")),
                "first_activity":        _bool(r.get("first_activity_uploaded", "false")),
                "paywall_viewed":        _bool(r.get("paywall_viewed", "false")),
                "trial_started":         _bool(r.get("trial_started", "false")),
                "subscribed":            _bool(r.get("subscribed", "false")),
                "subscription_type":     r.get("subscription_type", ""),
                "subscription_value_usd":sub_val,
                "days_to_subscribe":     _int_or_none(r.get("days_to_subscribe", "")),
                "raw_source":            "appsflyer_pull_api_v5",
                "_extracted_at":         self._extracted_at,
                "_pipeline_version":     self._pipeline_version,
            })
        df = pd.DataFrame(rows)
        df["install_date"] = pd.to_datetime(df["install_date"])
        before = len(df)
        df = df[df["is_primary_attribution"]].reset_index(drop=True)
        if before > len(df):
            log.info(f"  Deduped {before-len(df):,} non-primary attribution rows")
        log.info(f"Installs transformed: {len(df):,} CDM rows")
        return df

    def transform_events(self, raw_rows):
        log.info(f"Transforming events: {len(raw_rows):,} raw rows")
        rows = []
        for r in raw_rows:
            raw_val = r.get("event_value", "{}")
            try:
                props = json.loads(raw_val) if raw_val else {}
            except (json.JSONDecodeError, TypeError):
                props = {}
            rows.append({
                "event_date":        r.get("event_time","")[:10],
                "event_timestamp":   r.get("event_time",""),
                "appsflyer_id":      r.get("appsflyer_id",""),
                "customer_user_id":  r.get("customer_user_id",""),
                "app_id":            r.get("app_id",""),
                "event_name":        r.get("event_name",""),
                "revenue_usd":       _float(props.get("value_usd", props.get("revenue", 0))),
                "sport_type":        props.get("sport_type",""),
                "distance_km":       _float(props.get("distance_km",0)),
                "duration_min":      _int_or_none(props.get("duration_min","")),
                "subscription_plan": props.get("plan",""),
                "share_platform":    props.get("platform",""),
                "cancel_reason":     props.get("reason",""),
                "event_properties":  raw_val,
                "platform":          r.get("platform","").lower(),
                "country":           r.get("country_code",""),
                "media_source":      r.get("media_source",""),
                "channel":           MEDIA_SOURCE_MAP.get(r.get("media_source",""), r.get("media_source","")),
                "raw_source":        "appsflyer_pull_api_v5",
                "_extracted_at":     self._extracted_at,
                "_pipeline_version": self._pipeline_version,
            })
        df = pd.DataFrame(rows)
        df["event_date"] = pd.to_datetime(df["event_date"])
        log.info(f"Events transformed: {len(df):,} CDM rows")
        return df

    def build_funnel_summary(self, installs_df):
        log.info("Building funnel summary...")
        grp = installs_df.groupby([installs_df["install_date"].dt.date, "channel"]).agg(
            installs          =("appsflyer_id",          "count"),
            registrations     =("registered",             "sum"),
            first_activities  =("first_activity",         "sum"),
            paywall_views     =("paywall_viewed",          "sum"),
            trials            =("trial_started",           "sum"),
            subscriptions     =("subscribed",              "sum"),
            revenue_usd       =("subscription_value_usd",  "sum"),
            ios_att_authorized=("att_status", lambda x: (x=="authorized").sum()),
            ios_att_denied    =("att_status", lambda x: (x=="denied").sum()),
            skan_installs     =("is_skan",                "sum"),
        ).reset_index()
        grp.columns = ["install_date","channel"] + list(grp.columns[2:])
        grp["reg_rate"]        = _sdiv(grp["registrations"],   grp["installs"])
        grp["act_rate"]        = _sdiv(grp["first_activities"], grp["registrations"])
        grp["paywall_rate"]    = _sdiv(grp["paywall_views"],    grp["first_activities"])
        grp["trial_rate"]      = _sdiv(grp["trials"],           grp["paywall_views"])
        grp["sub_rate"]        = _sdiv(grp["subscriptions"],    grp["trials"])
        grp["overall_cvr"]     = _sdiv(grp["subscriptions"],    grp["installs"])
        grp["ltv_per_install"] = _sdiv(grp["revenue_usd"],      grp["installs"])
        grp["arpu"]            = _sdiv(grp["revenue_usd"],      grp["subscriptions"])
        grp["ios_att_rate"]    = _sdiv(grp["ios_att_authorized"], grp["ios_att_authorized"]+grp["ios_att_denied"])
        grp["skan_pct"]        = _sdiv(grp["skan_installs"],    grp["installs"])
        rate_cols = ["reg_rate","act_rate","paywall_rate","trial_rate","sub_rate","overall_cvr","ios_att_rate","skan_pct"]
        grp[rate_cols] = grp[rate_cols].round(4)
        grp[["ltv_per_install","arpu","revenue_usd"]] = grp[["ltv_per_install","arpu","revenue_usd"]].round(2)
        grp["_extracted_at"]     = self._extracted_at
        grp["_pipeline_version"] = self._pipeline_version
        log.info(f"Funnel summary: {len(grp):,} rows")
        return grp

    def build_ltv_cohort(self, installs_df):
        log.info("Building LTV cohort table...")
        df = installs_df.copy()
        df["cohort_month"] = df["install_date"].dt.to_period("M").dt.to_timestamp()
        cohort = df.groupby(["cohort_month","channel","subscription_type"]).agg(
            installs        =("appsflyer_id","count"),
            subscribers     =("subscribed","sum"),
            revenue_usd     =("subscription_value_usd","sum"),
            avg_days_to_sub =("days_to_subscribe", lambda x: pd.to_numeric(x,errors="coerce").mean()),
        ).reset_index()
        cohort["sub_rate"]        = _sdiv(cohort["subscribers"],  cohort["installs"]).round(4)
        cohort["arpu"]            = _sdiv(cohort["revenue_usd"],  cohort["subscribers"]).round(2)
        cohort["ltv_per_install"] = _sdiv(cohort["revenue_usd"],  cohort["installs"]).round(2)
        cohort["avg_days_to_sub"] = cohort["avg_days_to_sub"].round(1)
        cohort["funnel_status"]   = cohort["sub_rate"].apply(
            lambda r: "healthy" if r>=0.05 else ("watch" if r>=0.03 else "at_risk"))
        cohort["_extracted_at"]     = self._extracted_at
        cohort["_pipeline_version"] = self._pipeline_version
        log.info(f"LTV cohort: {len(cohort):,} rows")
        return cohort

    def save_all(self, installs, events, cohort, summary):
        for name, df in [("cdm_installs",installs),("cdm_funnel_events",events),
                         ("cdm_ltv_cohort",cohort),("cdm_funnel_summary",summary)]:
            path = DATA_CDM_DIR / f"{name}.csv"
            df.to_csv(path, index=False)
            log.info(f"Saved: {name}.csv ({len(df):,} rows)")

def _bool(val):
    if isinstance(val, bool): return val
    return str(val).strip().lower() in ("true","1","yes")

def _float(val, default=0.0):
    try: return float(val)
    except: return default

def _int_or_none(val):
    try: return int(float(val))
    except: return None

def _sdiv(num, den):
    return num.div(den.replace(0, float("nan"))).fillna(0)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from extractors.appsflyer_extractor import AppsFlyerExtractor
    start, end = date(2024,10,1), date(2025,3,31)
    ext = AppsFlyerExtractor()
    raw_inst = ext.extract_installs(start, end)
    raw_evts = ext.extract_events(start, end)
    t        = MMPTransformer()
    installs = t.transform_installs(raw_inst)
    events   = t.transform_events(raw_evts)
    cohort   = t.build_ltv_cohort(installs)
    summary  = t.build_funnel_summary(installs)
    t.save_all(installs, events, cohort, summary)
    print(f"\n── cdm_installs: {installs.shape} ──")
    print(installs.groupby("channel").agg(installs=("appsflyer_id","count"), subscribed=("subscribed","sum"), revenue=("subscription_value_usd","sum"), cvr=("subscribed","mean")).round(3))
    print(f"\n── LTV cohort (head) ──")
    print(cohort[["cohort_month","channel","installs","subscribers","revenue_usd","ltv_per_install","funnel_status"]].head(8).to_string(index=False))
    print(f"\n── ATT consent ──")
    print(installs.groupby("att_status").size().rename("installs"))
