"""
deploy/cloud_functions/main.py
────────────────────────────────
Cloud Function entry point for the Social Fitness App ETL pipeline.
Triggered by Cloud Scheduler via Pub/Sub message.

Deployment:
  gcloud functions deploy social-fitness-etl \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=. \
    --entry-point=run_etl \
    --trigger-topic=social-fitness-etl-trigger \
    --memory=2048MB \
    --timeout=540s \
    --set-env-vars=GCP_PROJECT_ID=your-project \
    --service-account=social-fitness-etl@your-project.iam.gserviceaccount.com

Environment variables (set via Secret Manager in production):
  GCP_PROJECT_ID       — GCP project
  GCS_RAW_BUCKET       — bucket for raw data landing
  BQ_DATASET_STAGING   — BigQuery staging dataset
  TIKTOK_ACCESS_TOKEN  — TikTok Ads API
  META_ACCESS_TOKEN    — Meta Marketing API
  META_AD_ACCOUNT_ID   — Meta ad account
  REDDIT_CLIENT_ID     — Reddit OAuth2 client
  REDDIT_CLIENT_SECRET — Reddit OAuth2 secret
  APPSFLYER_API_TOKEN  — AppsFlyer Pull API
  SLACK_WEBHOOK_URL    — pipeline alerting
  DBT_TARGET           — dbt target (prod or staging)

Pub/Sub message schema:
  {
    "pipeline":   "all" | "paid_media" | "mmp" | "organic",
    "date":       "YYYY-MM-DD" (optional, defaults to yesterday),
    "start_date": "YYYY-MM-DD" (optional, for backfill),
    "end_date":   "YYYY-MM-DD" (optional, for backfill),
    "dry_run":    false
  }
"""

import base64
import json
import logging
import os
import sys
import traceback
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

# Cloud Functions logger (structured JSON to Cloud Logging)
logging.basicConfig(
    level=logging.INFO,
    format='{"severity":"%(levelname)s","message":"%(message)s",'
           '"logger":"%(name)s","timestamp":"%(asctime)s"}',
)
log = logging.getLogger("social-fitness-etl")

# Add project root to path (Cloud Functions deploy entire source dir)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))


def run_etl(event, context):
    """
    Cloud Function entry point (Gen2 Pub/Sub trigger).

    event:   dict with 'data' key containing base64-encoded JSON message
    context: Cloud Functions context (event_id, timestamp, etc.)
    """
    start_time = datetime.now(timezone.utc)
    log.info(f"ETL triggered | event_id={context.event_id} | "
             f"timestamp={context.timestamp}")

    # ── Parse Pub/Sub message ─────────────────────────────────────────────────
    try:
        message_data = base64.b64decode(event.get("data", "e30=")).decode("utf-8")
        params       = json.loads(message_data)
    except Exception as e:
        log.warning(f"Could not parse Pub/Sub message, using defaults: {e}")
        params = {}

    pipeline   = params.get("pipeline", "all")
    dry_run    = params.get("dry_run", False)
    start_date, end_date = _resolve_dates(params)

    log.info(f"Pipeline={pipeline} | {start_date} → {end_date} | dry_run={dry_run}")

    # ── Load secrets from Secret Manager ─────────────────────────────────────
    _load_secrets()

    # ── Run pipelines ─────────────────────────────────────────────────────────
    results = {}
    errors  = []

    if pipeline in ("all", "paid_media"):
        results["paid_media"] = _run_safe("paid_media", start_date, end_date, dry_run)
        if results["paid_media"]["status"] != "success":
            errors.append("paid_media")

    if pipeline in ("all", "mmp"):
        results["mmp"] = _run_safe("mmp", start_date, end_date, dry_run)
        if results["mmp"]["status"] != "success":
            errors.append("mmp")

    if pipeline in ("all", "organic"):
        results["organic"] = _run_safe("organic", start_date, end_date, dry_run)
        if results["organic"]["status"] != "success":
            errors.append("organic")

    # ── Run dbt after all pipelines succeed ───────────────────────────────────
    if not errors and not dry_run:
        results["dbt"] = _run_dbt()
        if results["dbt"]["status"] != "success":
            errors.append("dbt")

    # ── Notify Slack ──────────────────────────────────────────────────────────
    elapsed    = (datetime.now(timezone.utc) - start_time).total_seconds()
    final_status = "success" if not errors else "failed"
    _notify_slack(final_status, results, errors, start_date, end_date, elapsed)

    # ── Log final summary ──────────────────────────────────────────────────────
    summary = {
        "status":       final_status,
        "pipeline":     pipeline,
        "date_range":   f"{start_date}/{end_date}",
        "elapsed_sec":  round(elapsed, 1),
        "errors":       errors,
        "results":      {k: v.get("status") for k, v in results.items()},
    }
    log.info(f"ETL complete: {json.dumps(summary)}")

    # Raise on failure so Cloud Functions marks the invocation as failed
    # and Cloud Scheduler can retry
    if errors:
        raise RuntimeError(f"ETL failed for: {errors}")

    return summary


# ── Pipeline runners ──────────────────────────────────────────────────────────

def _run_safe(name: str, start_date: date, end_date: date, dry_run: bool) -> dict:
    """Run a pipeline, catching all exceptions so one failure doesn't block others."""
    try:
        if name == "paid_media":
            from etl.run_paid_media_pipeline import run_pipeline
        elif name == "mmp":
            from etl.run_mmp_pipeline import run_pipeline
        elif name == "organic":
            from etl.run_organic_pipeline import run_pipeline
        else:
            return {"status": "error", "error": f"Unknown pipeline: {name}"}

        result = run_pipeline(start_date, end_date, dry_run=dry_run)
        log.info(f"{name} pipeline: {result['status']}")
        return result

    except Exception as e:
        log.error(f"{name} pipeline FAILED: {e}\n{traceback.format_exc()}")
        return {"status": "error", "error": str(e), "pipeline": name}


def _run_dbt() -> dict:
    """
    Run dbt models via subprocess after all ETL pipelines complete.
    dbt must be installed in the Cloud Function container.
    Uses dbt Cloud or dbt Core depending on deployment.
    """
    import subprocess

    log.info("Running dbt models...")
    dbt_target = os.getenv("DBT_TARGET", "prod")

    try:
        result = subprocess.run(
            ["dbt", "run", "--target", dbt_target,
             "--select", "staging+ marts+ reports+",
             "--fail-fast"],
            capture_output=True, text=True, timeout=300,
        )

        if result.returncode == 0:
            log.info(f"dbt run succeeded:\n{result.stdout[-2000:]}")
            # Run dbt tests after successful run
            test_result = subprocess.run(
                ["dbt", "test", "--target", dbt_target],
                capture_output=True, text=True, timeout=180,
            )
            test_status = "passed" if test_result.returncode == 0 else "failed"
            log.info(f"dbt test: {test_status}")
            return {"status": "success", "dbt_tests": test_status}
        else:
            log.error(f"dbt run failed:\n{result.stderr[-2000:]}")
            return {"status": "error", "error": result.stderr[-500:]}

    except subprocess.TimeoutExpired:
        return {"status": "error", "error": "dbt run timed out after 300s"}
    except FileNotFoundError:
        log.warning("dbt not installed — skipping dbt run")
        return {"status": "skipped", "reason": "dbt not installed"}


# ── Secret Manager ────────────────────────────────────────────────────────────

def _load_secrets():
    """
    Load secrets from GCP Secret Manager into environment variables.
    In local dev, env vars are already set via .env / shell exports.
    On Cloud Functions, this pulls from Secret Manager at runtime.
    """
    try:
        from google.cloud import secretmanager

        project_id = os.getenv("GCP_PROJECT_ID")
        if not project_id:
            log.warning("GCP_PROJECT_ID not set — skipping Secret Manager")
            return

        client  = secretmanager.SecretManagerServiceClient()
        secrets = [
            "TIKTOK_ACCESS_TOKEN", "TIKTOK_ADVERTISER_ID",
            "META_ACCESS_TOKEN", "META_AD_ACCOUNT_ID",
            "REDDIT_CLIENT_ID", "REDDIT_CLIENT_SECRET",
            "REDDIT_USERNAME", "REDDIT_PASSWORD",
            "APPSFLYER_API_TOKEN",
            "SLACK_WEBHOOK_URL",
        ]

        for secret_id in secrets:
            if os.getenv(secret_id):
                continue   # already set (e.g. from --set-env-vars in deploy)
            try:
                name     = f"projects/{project_id}/secrets/{secret_id}/versions/latest"
                response = client.access_secret_version(request={"name": name})
                os.environ[secret_id] = response.payload.data.decode("utf-8")
            except Exception as e:
                log.warning(f"Could not load secret {secret_id}: {e}")

        log.info("Secrets loaded from Secret Manager")

    except ImportError:
        log.info("google-cloud-secret-manager not installed — using env vars")


# ── Slack notifications ───────────────────────────────────────────────────────

def _notify_slack(status: str, results: dict, errors: list,
                  start_date: date, end_date: date, elapsed: float):
    """Posts pipeline summary to Slack webhook."""
    import urllib.request

    webhook_url = os.getenv("SLACK_WEBHOOK_URL")
    if not webhook_url:
        return

    icon   = "✅" if status == "success" else "❌"
    color  = "#36a64f" if status == "success" else "#dc3545"

    rows_by_pipeline = {
        k: v.get("steps", {}).get("transform", {}).get("cdm_rows", "—")
        for k, v in results.items()
        if isinstance(v, dict)
    }

    blocks = {
        "attachments": [{
            "color": color,
            "blocks": [
                {
                    "type": "header",
                    "text": {"type": "plain_text",
                             "text": f"{icon} Social Fitness ETL — {status.upper()}"}
                },
                {
                    "type": "section",
                    "fields": [
                        {"type": "mrkdwn", "text": f"*Date range*\n{start_date} → {end_date}"},
                        {"type": "mrkdwn", "text": f"*Elapsed*\n{elapsed:.0f}s"},
                        {"type": "mrkdwn", "text": f"*Pipelines*\n{', '.join(results.keys())}"},
                        {"type": "mrkdwn", "text": f"*Errors*\n{', '.join(errors) if errors else 'None'}"},
                    ]
                },
                {
                    "type": "section",
                    "text": {"type": "mrkdwn",
                             "text": "\n".join(
                                 f"• *{k}*: {v.get('status','?')} "
                                 f"({rows_by_pipeline.get(k,'—')} rows)"
                                 for k, v in results.items()
                             )}
                }
            ]
        }]
    }

    try:
        req  = urllib.request.Request(
            webhook_url,
            data=json.dumps(blocks).encode(),
            headers={"Content-Type": "application/json"},
        )
        urllib.request.urlopen(req, timeout=5)
    except Exception as e:
        log.warning(f"Slack notification failed: {e}")


# ── Date resolution ───────────────────────────────────────────────────────────

def _resolve_dates(params: dict) -> tuple[date, date]:
    """
    Resolve start/end dates from message params.
    Defaults to yesterday (standard daily incremental run).
    """
    yesterday = date.today() - timedelta(days=1)

    if params.get("start_date") and params.get("end_date"):
        return (date.fromisoformat(params["start_date"]),
                date.fromisoformat(params["end_date"]))

    if params.get("date"):
        d = date.fromisoformat(params["date"])
        return d, d

    return yesterday, yesterday


# ── Local development entry point ─────────────────────────────────────────────

if __name__ == "__main__":
    """Run locally by simulating a Cloud Function invocation."""
    import argparse

    parser = argparse.ArgumentParser(description="Run ETL Cloud Function locally")
    parser.add_argument("--pipeline", default="all",
                        choices=["all","paid_media","mmp","organic"])
    parser.add_argument("--start",   default=None)
    parser.add_argument("--end",     default=None)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    # Simulate Pub/Sub event
    message = {"pipeline": args.pipeline, "dry_run": args.dry_run}
    if args.start: message["start_date"] = args.start
    if args.end:   message["end_date"]   = args.end

    encoded = base64.b64encode(json.dumps(message).encode()).decode()

    class FakeContext:
        event_id  = "local-test-001"
        timestamp = datetime.now(timezone.utc).isoformat()

    result = run_etl({"data": encoded}, FakeContext())
    print(json.dumps(result, indent=2, default=str))
