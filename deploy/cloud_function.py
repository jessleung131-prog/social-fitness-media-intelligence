"""
deploy/cloud_function.py
─────────────────────────
Google Cloud Function entry point for the automated daily ETL pipeline.

Deployment:
  gcloud functions deploy social-fitness-etl \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=run_etl \
    --trigger-topic=social-fitness-etl-trigger \
    --set-env-vars GCP_PROJECT_ID=your-project \
    --memory=1024MB \
    --timeout=540s

Cloud Scheduler trigger (runs daily at 04:00 UTC):
  gcloud scheduler jobs create pubsub social-fitness-etl-daily \
    --schedule="0 4 * * *" \
    --topic=social-fitness-etl-trigger \
    --message-body='{"pipeline":"all","lookback_days":1}' \
    --time-zone="UTC"

Pipeline execution order (all daily, incremental):
  1. paid_media_etl    (TikTok + Meta + Reddit → cdm_paid_media_daily)
  2. mmp_etl           (AppsFlyer → cdm_installs + cdm_funnel_cohorts + cdm_ltv_cohorts)
  3. organic_etl       (Instagram + TikTok + Reddit organic → cdm_organic_daily)
  4. dbt_run           (staging → marts → reports in BigQuery)
  5. notify            (Slack pipeline summary)
"""

import base64
import json
import logging
import os
from datetime import date, timedelta, datetime, timezone
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))

log = logging.getLogger("cloud_function")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


# ─────────────────────────────────────────────────────────────────────────────
# Cloud Function entry point
# ─────────────────────────────────────────────────────────────────────────────

def run_etl(event: dict, context) -> str:
    """
    Cloud Function triggered by Pub/Sub message from Cloud Scheduler.

    event["data"] contains base64-encoded JSON:
      {"pipeline": "all" | "paid_media" | "mmp" | "organic",
       "lookback_days": 1,
       "start_date": "YYYY-MM-DD",   # optional override
       "end_date": "YYYY-MM-DD"}     # optional override
    """
    # Decode Pub/Sub message
    if "data" in event:
        message = json.loads(base64.b64decode(event["data"]).decode())
    else:
        message = {}

    pipeline     = message.get("pipeline", "all")
    lookback     = int(message.get("lookback_days", 1))
    start_str    = message.get("start_date")
    end_str      = message.get("end_date")

    # Determine date range
    if start_str and end_str:
        start_date = date.fromisoformat(start_str)
        end_date   = date.fromisoformat(end_str)
    else:
        end_date   = date.today() - timedelta(days=1)   # yesterday
        start_date = end_date - timedelta(days=lookback - 1)

    log.info(f"Cloud Function triggered: pipeline={pipeline} {start_date}→{end_date}")

    results = {}
    errors  = []

    try:
        if pipeline in ("all", "paid_media"):
            results["paid_media"] = _run_paid_media(start_date, end_date)

        if pipeline in ("all", "mmp"):
            results["mmp"] = _run_mmp(start_date, end_date)

        if pipeline in ("all", "organic"):
            results["organic"] = _run_organic(start_date, end_date)

        if pipeline in ("all", "dbt"):
            results["dbt"] = _run_dbt()

    except Exception as e:
        log.error(f"Pipeline error: {e}", exc_info=True)
        errors.append(str(e))

    # Send Slack notification
    summary = _build_summary(results, errors, start_date, end_date)
    _notify_slack(summary)

    if errors:
        raise RuntimeError(f"Pipeline completed with errors: {errors}")

    return json.dumps({"status": "success", "summary": summary})


# ─────────────────────────────────────────────────────────────────────────────
# Sub-pipeline runners
# ─────────────────────────────────────────────────────────────────────────────

def _run_paid_media(start_date: date, end_date: date) -> dict:
    from etl.extractors.tiktok_extractor         import TikTokExtractor
    from etl.extractors.meta_extractor           import MetaExtractor
    from etl.extractors.reddit_extractor         import RedditExtractor
    from etl.transformers.paid_media_transformer import PaidMediaTransformer
    from etl.validators.paid_media_validator     import PaidMediaValidator
    from etl.loaders.bigquery_loader             import BigQueryLoader

    log.info("Running paid media ETL...")

    tt_rows = TikTokExtractor().extract(start_date, end_date)
    mt_rows = MetaExtractor().extract(start_date, end_date)
    rd_rows = RedditExtractor().extract(start_date, end_date)

    t       = PaidMediaTransformer()
    combined = t.combine([
        t.transform_tiktok(tt_rows),
        t.transform_meta(mt_rows),
        t.transform_reddit(rd_rows),
    ])

    report = PaidMediaValidator().validate(combined, start_date)
    if not report.passed:
        raise ValueError(f"Paid media validation failed: {report.error_count} errors")

    result = BigQueryLoader().load_paid_media(combined, start_date)
    log.info(f"Paid media ETL complete: {len(combined):,} rows → {result['status']}")
    return {"rows": len(combined), "status": result["status"]}


def _run_mmp(start_date: date, end_date: date) -> dict:
    from etl.extractors.appsflyer_extractor  import AppsFlyerExtractor
    from etl.transformers.mmp_transformer    import MMPTransformer
    from etl.loaders.bigquery_loader         import BigQueryLoader

    log.info("Running MMP ETL...")

    ext       = AppsFlyerExtractor()
    installs  = ext.extract_installs(start_date, end_date)
    events    = ext.extract_in_app_events(start_date, end_date)
    skan      = ext.extract_skan(start_date, end_date)
    ext.save_raw(installs, events, skan, start_date, end_date)

    t            = MMPTransformer()
    installs_df  = t.transform_installs(installs, skan)
    cohorts_df   = t.build_funnel_cohorts(installs_df)
    ltv_df       = t.build_ltv_cohorts(installs_df)

    loader = BigQueryLoader()
    loader.load_table(installs_df, "cdm_installs",       [], start_date)
    loader.load_table(cohorts_df,  "cdm_funnel_cohorts", [], start_date)
    loader.load_table(ltv_df,      "cdm_ltv_cohorts",    [], start_date, write_mode="truncate")

    log.info(f"MMP ETL complete: {len(installs_df):,} installs")
    return {"installs": len(installs_df), "events": len(events), "skan": len(skan)}


def _run_organic(start_date: date, end_date: date) -> dict:
    from etl.extractors.organic_extractor import OrganicExtractor, OrganicTransformer
    from etl.loaders.bigquery_loader      import BigQueryLoader

    log.info("Running organic ETL...")

    ext     = OrganicExtractor()
    ig_rows = ext.extract_instagram(start_date, end_date)
    tt_rows = ext.extract_tiktok(start_date, end_date)
    rd_rows = ext.extract_reddit(start_date, end_date)
    ext.save_raw(ig_rows, tt_rows, rd_rows, start_date, end_date)

    t  = OrganicTransformer()
    df = t.transform(ig_rows, tt_rows, rd_rows)
    BigQueryLoader().load_table(df, "cdm_organic_daily", [], start_date)

    log.info(f"Organic ETL complete: {len(df):,} posts")
    return {"posts": len(df), "by_platform": df["platform"].value_counts().to_dict()}


def _run_dbt() -> dict:
    """
    Runs dbt models in BigQuery after all CDM tables are loaded.
    In production this calls the dbt Cloud API or runs dbt locally via subprocess.
    """
    import subprocess
    log.info("Running dbt models...")

    cmd    = ["dbt", "run", "--profiles-dir", "/app/dbt", "--target", "prod"]
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)

    if result.returncode != 0:
        log.error(f"dbt failed: {result.stderr}")
        raise RuntimeError(f"dbt run failed: {result.stderr[-500:]}")

    # Also run dbt tests
    test_result = subprocess.run(
        ["dbt", "test", "--profiles-dir", "/app/dbt", "--target", "prod"],
        capture_output=True, text=True, timeout=120
    )
    if test_result.returncode != 0:
        log.warning(f"dbt tests failed: {test_result.stderr}")

    log.info("dbt run complete")
    return {"status": "success", "tests_passed": test_result.returncode == 0}


# ─────────────────────────────────────────────────────────────────────────────
# Slack notification
# ─────────────────────────────────────────────────────────────────────────────

def _build_summary(results: dict, errors: list, start: date, end: date) -> str:
    status = "✅ SUCCESS" if not errors else "❌ FAILED"
    lines  = [
        f"*Social Fitness App · ETL Pipeline* {status}",
        f"Date range: `{start}` → `{end}`",
        f"Run time: `{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}`",
    ]
    for pipeline, result in results.items():
        lines.append(f"• *{pipeline}*: {json.dumps(result)}")
    if errors:
        for e in errors:
            lines.append(f"• ⚠️ Error: {e}")
    return "\n".join(lines)


def _notify_slack(message: str) -> None:
    webhook_url = os.getenv("SLACK_WEBHOOK_URL", "")
    if not webhook_url:
        log.info(f"Slack notification (no webhook configured):\n{message}")
        return
    try:
        import requests
        resp = requests.post(webhook_url, json={"text": message}, timeout=10)
        resp.raise_for_status()
        log.info("Slack notification sent")
    except Exception as e:
        log.warning(f"Slack notification failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Local runner (for testing without Cloud Function trigger)
# ─────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    # Simulate what Cloud Scheduler sends
    message_body = {
        "pipeline":      "all",
        "lookback_days": 1,
        "start_date":    "2024-10-01",
        "end_date":      "2025-03-31",
    }
    encoded = base64.b64encode(json.dumps(message_body).encode()).decode()
    fake_event = {"data": encoded}

    result = run_etl(fake_event, context=None)
    print(f"\n── Cloud Function result ──")
    print(json.dumps(json.loads(result), indent=2, default=str))
