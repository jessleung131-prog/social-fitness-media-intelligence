"""
etl/run_mmp_pipeline.py
─────────────────────────
Runs the MMP attribution ETL pipeline:
  AppsFlyer installs + events → cdm_installs, cdm_funnel_events,
                                 cdm_funnel_summary, cdm_ltv_cohort

Usage:
  python etl/run_mmp_pipeline.py
  python etl/run_mmp_pipeline.py --start 2024-10-01 --end 2025-03-31
  python etl/run_mmp_pipeline.py --date 2025-01-15
"""

import argparse, json, logging, sys, traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s")
log = logging.getLogger("mmp_pipeline")

sys.path.insert(0, str(Path(__file__).parent.parent))
from etl.extractors.appsflyer_extractor    import AppsFlyerExtractor
from etl.transformers.mmp_transformer       import MMPTransformer
from etl.loaders.bigquery_loader            import BigQueryLoader
from config.settings                        import BACKFILL_START, BACKFILL_END


def run_pipeline(start_date: date, end_date: date, dry_run: bool = False) -> dict:
    t0     = datetime.now(timezone.utc)
    result = {"pipeline":"mmp_etl","start_date":str(start_date),
              "end_date":str(end_date),"dry_run":dry_run,
              "started_at":t0.isoformat(),"steps":{},"status":"running"}

    log.info(f"\n{'='*60}\nMMP ATTRIBUTION ETL PIPELINE\n"
             f"Date range: {start_date} → {end_date}\n{'='*60}")
    try:
        # ── Extract ───────────────────────────────────────────────────────────
        log.info("\n── STEP 1: EXTRACT ──────────────────────────────────")
        ext      = AppsFlyerExtractor()
        installs = ext.extract_installs(start_date, end_date)
        events   = ext.extract_events(start_date, end_date)
        if not dry_run:
            ext.save_raw(installs, events, start_date, end_date)
        result["steps"]["extract"] = {"installs":len(installs),"events":len(events)}
        log.info(f"Extract: {len(installs):,} installs | {len(events):,} events")

        # ── Transform ─────────────────────────────────────────────────────────
        log.info("\n── STEP 2: TRANSFORM ────────────────────────────────")
        t          = MMPTransformer()
        inst_cdm   = t.transform_installs(installs)
        evts_cdm   = t.transform_events(events)
        cohort_cdm = t.build_ltv_cohort(inst_cdm)
        summ_cdm   = t.build_funnel_summary(inst_cdm)
        result["steps"]["transform"] = {
            "installs":len(inst_cdm),"events":len(evts_cdm),
            "cohort_rows":len(cohort_cdm),"summary_rows":len(summ_cdm)
        }
        log.info(f"Transform: {len(inst_cdm):,} installs | {len(evts_cdm):,} events")

        # ── Save ──────────────────────────────────────────────────────────────
        log.info("\n── STEP 3: SAVE ─────────────────────────────────────")
        if not dry_run:
            t.save_all(inst_cdm, evts_cdm, cohort_cdm, summ_cdm)

        # ── Preview ───────────────────────────────────────────────────────────
        log.info("\n── STEP 4: PREVIEW ──────────────────────────────────")
        _print_mmp_preview(inst_cdm, cohort_cdm)

    except Exception as e:
        log.error(f"Pipeline failed: {e}\n{traceback.format_exc()}")
        result["status"] = "error"; result["error"] = str(e); return result

    elapsed = (datetime.now(timezone.utc) - t0).total_seconds()
    result.update({"status":"success","elapsed_sec":round(elapsed,2),
                   "completed_at":datetime.now(timezone.utc).isoformat()})
    log.info(f"\n{'='*60}\nMMP PIPELINE COMPLETE ✅  {elapsed:.1f}s\n{'='*60}\n")
    return result


def _print_mmp_preview(installs, cohort):
    print(f"\n{'─'*60}\nMMP ATTRIBUTION PREVIEW\n{'─'*60}")
    print(f"\n[1] Installs by channel:")
    print(installs.groupby("channel").agg(
        installs    =("appsflyer_id","count"),
        subscribed  =("subscribed","sum"),
        revenue_usd =("subscription_value_usd","sum"),
        overall_cvr =("subscribed","mean"),
    ).round(3).to_string())

    print(f"\n[2] ATT consent breakdown (iOS attribution quality):")
    print(installs.groupby("att_status").size().rename("installs").to_string())

    print(f"\n[3] SKAN rows (no user-level data available):")
    skan_pct = installs["is_skan"].mean()
    print(f"    {installs['is_skan'].sum():,} SKAN installs ({skan_pct:.1%} of total)")

    print(f"\n[4] LTV cohort (top 6 rows by revenue):")
    top = cohort.nlargest(6, "revenue_usd")
    print(top[["cohort_month","channel","installs","subscribers",
               "revenue_usd","ltv_per_install","funnel_status"]].to_string(index=False))

    print(f"\n[5] Funnel health status:")
    print(cohort.groupby("funnel_status").agg(
        rows=("installs","count"), installs=("installs","sum"),
        revenue=("revenue_usd","sum")).to_string())
    print(f"{'─'*60}\n")


def parse_args():
    p = argparse.ArgumentParser(description="MMP Attribution ETL Pipeline")
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
        yesterday = date.today() - timedelta(days=1)
        s = e = yesterday

    result = run_pipeline(s, e, dry_run=args.dry_run)
    print(json.dumps({k:v for k,v in result.items() if k!="steps"},
                     indent=2, default=str))
    sys.exit(0 if result["status"]=="success" else 2)
