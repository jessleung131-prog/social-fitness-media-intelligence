"""
etl/run_paid_media_pipeline.py
────────────────────────────────
Main entry point for the paid media ETL pipeline.

Orchestrates the full workflow:
  Extract → Validate Raw → Transform → Validate CDM → Load → Log

Can be run:
  • Locally:          python etl/run_paid_media_pipeline.py
  • Incremental:      python etl/run_paid_media_pipeline.py --date 2025-01-15
  • Backfill:         python etl/run_paid_media_pipeline.py --start 2024-10-01 --end 2025-03-31
  • Dry run:          python etl/run_paid_media_pipeline.py --dry-run
  • As Cloud Function: triggered by Cloud Scheduler (see deploy/cloud_function.py)

Exit codes:
  0 = success
  1 = validation failed (data not loaded)
  2 = extraction error
  3 = transform error
"""

import argparse
import json
import logging
import sys
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# ── Logging setup ─────────────────────────────────────────────────────────────
LOG_FORMAT = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("paid_media_pipeline")

# ── Internal imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).parent.parent))

from etl.extractors.tiktok_extractor          import TikTokExtractor
from etl.extractors.meta_extractor            import MetaExtractor
from etl.extractors.reddit_extractor          import RedditExtractor
from etl.transformers.paid_media_transformer  import PaidMediaTransformer
from etl.validators.paid_media_validator      import PaidMediaValidator
from etl.loaders.bigquery_loader              import BigQueryLoader
from config.settings                          import BACKFILL_START, BACKFILL_END


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline
# ─────────────────────────────────────────────────────────────────────────────

def run_pipeline(start_date: date, end_date: date, dry_run: bool = False) -> dict:
    """
    Executes the full paid media ETL pipeline for the given date range.

    Returns a result dict with status, row counts, and timing.
    """
    pipeline_start = datetime.now(timezone.utc)
    log.info(f"{'='*60}")
    log.info(f"PAID MEDIA ETL PIPELINE")
    log.info(f"Date range:  {start_date} → {end_date}")
    log.info(f"Dry run:     {dry_run}")
    log.info(f"{'='*60}")

    result = {
        "pipeline":   "paid_media_etl",
        "start_date": str(start_date),
        "end_date":   str(end_date),
        "dry_run":    dry_run,
        "started_at": pipeline_start.isoformat(),
        "steps":      {},
        "status":     "running",
    }

    try:
        # ── Step 1: Extract ───────────────────────────────────────────────────
        log.info("\n── STEP 1: EXTRACT ──────────────────────────────────")

        tt_extractor = TikTokExtractor()
        mt_extractor = MetaExtractor()
        rd_extractor = RedditExtractor()

        tt_rows = tt_extractor.extract(start_date, end_date)
        mt_rows = mt_extractor.extract(start_date, end_date)
        rd_rows = rd_extractor.extract(start_date, end_date)

        # Save raw files (audit trail)
        if not dry_run:
            tt_extractor.save_raw(tt_rows, start_date, end_date)
            mt_extractor.save_raw(mt_rows, start_date, end_date)
            rd_extractor.save_raw(rd_rows, start_date, end_date)

        result["steps"]["extract"] = {
            "status":      "success",
            "tiktok_rows": len(tt_rows),
            "meta_rows":   len(mt_rows),
            "reddit_rows": len(rd_rows),
            "total_raw":   len(tt_rows) + len(mt_rows) + len(rd_rows),
        }
        log.info(
            f"Extract complete: TikTok={len(tt_rows):,} "
            f"Meta={len(mt_rows):,} Reddit={len(rd_rows):,}"
        )

        # ── Step 2: Transform ─────────────────────────────────────────────────
        log.info("\n── STEP 2: TRANSFORM ────────────────────────────────")

        transformer = PaidMediaTransformer()

        tt_cdm = transformer.transform_tiktok(tt_rows)
        mt_cdm = transformer.transform_meta(mt_rows)
        rd_cdm = transformer.transform_reddit(
            rd_rows,
            attribution_window=rd_extractor.cfg.attribution_window,
        )

        combined = transformer.combine([tt_cdm, mt_cdm, rd_cdm])

        result["steps"]["transform"] = {
            "status":    "success",
            "cdm_rows":  len(combined),
            "platforms": combined["platform"].value_counts().to_dict(),
            "date_range": f"{combined['date'].min().date()} → {combined['date'].max().date()}",
        }
        log.info(f"Transform complete: {len(combined):,} CDM rows across {combined['platform'].nunique()} platforms")

        # ── Step 3: Validate ──────────────────────────────────────────────────
        log.info("\n── STEP 3: VALIDATE ─────────────────────────────────")

        validator = PaidMediaValidator()
        report    = validator.validate(combined, run_date=start_date)
        report.print_report()

        result["steps"]["validate"] = {
            "status":        "passed" if report.passed else "failed",
            "checks_run":    len(report.results),
            "checks_passed": sum(1 for r in report.results if r.passed),
            "errors":        report.error_count,
            "warnings":      report.warning_count,
        }

        if not report.passed:
            log.error("Validation FAILED — aborting load")
            result["status"] = "failed_validation"
            return result

        # ── Step 4: Save / Load ───────────────────────────────────────────────
        log.info("\n── STEP 4: LOAD ─────────────────────────────────────")

        # Always save to local CSV (for preview and fallback)
        transformer.save(combined, start_date)

        # Load to BigQuery
        loader        = BigQueryLoader(dry_run=dry_run)
        load_result   = loader.load_paid_media(combined, run_date=start_date)

        result["steps"]["load"] = load_result
        log.info(f"Load complete: status={load_result['status']}")

        # ── Step 5: Preview ───────────────────────────────────────────────────
        log.info("\n── STEP 5: PREVIEW ──────────────────────────────────")
        _print_preview(combined)

    except Exception as e:
        log.error(f"Pipeline failed: {e}")
        log.error(traceback.format_exc())
        result["status"] = "error"
        result["error"]  = str(e)
        return result

    # ── Finalize ──────────────────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - pipeline_start).total_seconds()
    result["status"]       = "success"
    result["elapsed_sec"]  = round(elapsed, 2)
    result["completed_at"] = datetime.now(timezone.utc).isoformat()

    log.info(f"\n{'='*60}")
    log.info(f"PIPELINE COMPLETE  ✅  {elapsed:.1f}s")
    log.info(f"{'='*60}\n")

    return result


# ─────────────────────────────────────────────────────────────────────────────
# Preview printer
# ─────────────────────────────────────────────────────────────────────────────

def _print_preview(df) -> None:
    """Prints a structured data preview after transformation."""
    import pandas as pd

    print(f"\n{'─'*60}")
    print(f"DATA PREVIEW: cdm_paid_media_daily")
    print(f"{'─'*60}")

    print(f"\n[1] Shape: {df.shape[0]:,} rows × {df.shape[1]} columns")

    print(f"\n[2] Date range:")
    print(f"    {df['date'].min().date()}  →  {df['date'].max().date()}")

    print(f"\n[3] Platform summary:")
    summary = df.groupby("platform").agg(
        rows        =("campaign_id",    "count"),
        spend_usd   =("cost_usd",       "sum"),
        impressions =("impressions",    "sum"),
        installs    =("installs",       "sum"),
        avg_cpi     =("cpi",            "mean"),
        avg_ctr     =("ctr",            "mean"),
    ).round(2)
    summary["spend_usd"] = summary["spend_usd"].apply(lambda x: f"${x:,.0f}")
    print(summary.to_string())

    print(f"\n[4] Attribution windows (IMPORTANT for cross-platform comparison):")
    windows = df.groupby(["platform", "attribution_window"]).size().rename("rows")
    print(windows.to_string())

    print(f"\n[5] Sample rows (first 3):")
    sample_cols = ["date","platform","campaign_name","impressions","clicks","cost_usd","installs","cpi","attribution_window"]
    print(df[sample_cols].head(3).to_string(index=False))

    print(f"\n[6] Schema (column types):")
    for col, dtype in df.dtypes.items():
        print(f"    {col:<30} {dtype}")

    print(f"\n[7] Null counts (columns with any nulls):")
    null_counts = df.isnull().sum()
    null_counts = null_counts[null_counts > 0]
    if len(null_counts) == 0:
        print("    None — all columns complete")
    else:
        print(null_counts.to_string())
    print(f"{'─'*60}\n")


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(
        description="Social Fitness App — Paid Media ETL Pipeline"
    )
    parser.add_argument(
        "--date", type=str, default=None,
        help="Single date to process (YYYY-MM-DD). Default: yesterday."
    )
    parser.add_argument(
        "--start", type=str, default=None,
        help="Backfill start date (YYYY-MM-DD)."
    )
    parser.add_argument(
        "--end", type=str, default=None,
        help="Backfill end date (YYYY-MM-DD). Default: today."
    )
    parser.add_argument(
        "--backfill", action="store_true",
        help=f"Run full backfill: {BACKFILL_START} → {BACKFILL_END}"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and transform but do not write to BigQuery."
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.backfill:
        start_date = BACKFILL_START
        end_date   = BACKFILL_END
    elif args.date:
        start_date = end_date = date.fromisoformat(args.date)
    elif args.start:
        start_date = date.fromisoformat(args.start)
        end_date   = date.fromisoformat(args.end) if args.end else date.today()
    else:
        # Default: yesterday (standard daily incremental run)
        yesterday  = date.today() - timedelta(days=1)
        start_date = end_date = yesterday

    result = run_pipeline(start_date, end_date, dry_run=args.dry_run)

    # Print final result summary
    print(json.dumps({
        k: v for k, v in result.items()
        if k not in ("steps",)   # steps already printed inline
    }, indent=2, default=str))

    # Exit code
    if result["status"] == "success":
        sys.exit(0)
    elif result["status"] == "failed_validation":
        sys.exit(1)
    else:
        sys.exit(2)
