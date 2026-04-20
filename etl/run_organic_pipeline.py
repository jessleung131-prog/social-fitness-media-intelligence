"""
etl/run_organic_pipeline.py
─────────────────────────────
Runs the organic content ETL pipeline:
  Instagram + TikTok + Reddit → cdm_organic_daily

Usage:
  python etl/run_organic_pipeline.py
  python etl/run_organic_pipeline.py --start 2024-10-01 --end 2025-03-31
"""

import argparse, json, logging, sys, traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
log = logging.getLogger("organic_pipeline")

sys.path.insert(0, str(Path(__file__).parent.parent))
from etl.extractors.organic_extractor      import OrganicExtractor
from etl.transformers.organic_transformer  import OrganicTransformer
from config.settings                       import BACKFILL_START, BACKFILL_END


def run_pipeline(start_date: date, end_date: date, dry_run: bool = False) -> dict:
    t0     = datetime.now(timezone.utc)
    result = {"pipeline":"organic_etl","start_date":str(start_date),
              "end_date":str(end_date),"dry_run":dry_run,
              "started_at":t0.isoformat(),"status":"running"}

    log.info(f"\n{'='*60}\nORGANIC CONTENT ETL PIPELINE\n"
             f"Date range: {start_date} → {end_date}\n{'='*60}")
    try:
        # Extract
        log.info("\n── STEP 1: EXTRACT ──────────────────────────────────")
        ext = OrganicExtractor()
        ig  = ext.extract_instagram(start_date, end_date)
        tt  = ext.extract_tiktok(start_date, end_date)
        rd  = ext.extract_reddit(start_date, end_date)
        if not dry_run:
            ext.save_raw(ig, tt, rd, start_date)
        log.info(f"Extract: IG={len(ig):,} | TT={len(tt):,} | RD={len(rd):,}")

        # Transform
        log.info("\n── STEP 2: TRANSFORM ────────────────────────────────")
        t        = OrganicTransformer()
        ig_cdm   = t.transform_instagram(ig)
        tt_cdm   = t.transform_tiktok(tt)
        rd_cdm   = t.transform_reddit(rd)
        combined = t.combine([ig_cdm, tt_cdm, rd_cdm])
        log.info(f"Transform: {len(combined):,} CDM rows")

        # Save
        log.info("\n── STEP 3: SAVE ─────────────────────────────────────")
        if not dry_run:
            t.save(combined)

        # Preview
        log.info("\n── STEP 4: PREVIEW ──────────────────────────────────")
        _print_organic_preview(combined, ig_cdm, tt_cdm, rd_cdm)

        result.update({"status":"success","cdm_rows":len(combined),
                       "by_platform":combined.groupby("platform").size().to_dict()})

    except Exception as e:
        log.error(f"Pipeline failed: {e}\n{traceback.format_exc()}")
        result["status"] = "error"; result["error"] = str(e); return result

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    result["elapsed_sec"]  = round(elapsed, 2)
    result["completed_at"] = datetime.now(timezone.utc).isoformat()
    log.info(f"\n{'='*60}\nORGANIC PIPELINE COMPLETE ✅  {elapsed:.1f}s\n{'='*60}\n")
    return result


def _print_organic_preview(combined, ig_cdm, tt_cdm, rd_cdm):
    print(f"\n{'─'*60}\nORGANIC CONTENT CDM PREVIEW\n{'─'*60}")
    print(f"\n[1] Summary by platform:")
    print(combined.groupby("platform").agg(
        posts         =("post_id",               "count"),
        viral         =("is_viral",              "sum"),
        total_reach   =("reach",                 "sum"),
        total_eng     =("engagements",           "sum"),
        avg_eng_rate  =("blended_engagement_rate","mean"),
        follower_growth=("follower_delta",        "sum"),
    ).round(4).to_string())

    print(f"\n[2] Watch time normalization check:")
    print(f"    Instagram avg_watch_time_s: {ig_cdm['avg_watch_time_s'].mean():.1f}s  (converted from ms)")
    print(f"    TikTok avg_watch_time_s:    {tt_cdm['avg_watch_time_s'].mean():.1f}s  (native seconds)")
    print(f"    Reddit avg_watch_time_s:    NULL  (not available)")

    print(f"\n[3] Engagement rate methodology:")
    for plat, df in [("Instagram",ig_cdm),("TikTok",tt_cdm),("Reddit",rd_cdm)]:
        b = df["blended_engagement_rate"].mean()
        p = df["platform_engagement_rate"].mean() if "platform_engagement_rate" in df else None
        p_str = f"{p:.4f}" if p is not None and p > 0 else "N/A"
        print(f"    {plat}: blended={b:.4f} | platform_native={p_str}")

    print(f"\n[4] Content type breakdown:")
    print(combined.groupby(["platform","content_type"]).size()
          .rename("posts").to_string())

    print(f"\n[5] Schema — null columns by platform (expected):")
    for plat, df in [("Instagram",ig_cdm),("TikTok",tt_cdm),("Reddit",rd_cdm)]:
        nulls = [c for c in ["avg_watch_time_s","video_completion_rate",
                              "platform_engagement_rate"]
                 if df[c].isna().all()]
        print(f"    {plat}: {nulls if nulls else 'none'}")
    print(f"{'─'*60}\n")


def parse_args():
    p = argparse.ArgumentParser(description="Organic Content ETL Pipeline")
    p.add_argument("--date",  type=str, default=None)
    p.add_argument("--start", type=str, default=None)
    p.add_argument("--end",   type=str, default=None)
    p.add_argument("--backfill", action="store_true")
    p.add_argument("--dry-run",  action="store_true")
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    if args.backfill:
        s, e = BACKFILL_START, BACKFILL_END
    elif args.date:
        s = e = date.fromisoformat(args.date)
    elif args.start:
        s = date.fromisoformat(args.start)
        e = date.fromisoformat(args.end) if args.end else date.today()
    else:
        s = e = date.today() - timedelta(days=1)

    result = run_pipeline(s, e, dry_run=args.dry_run)
    print(json.dumps({k:v for k,v in result.items() if k not in ("steps",)},
                     indent=2, default=str))
    sys.exit(0 if result["status"] == "success" else 2)
