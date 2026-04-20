#!/bin/bash
# deploy/setup_gcp.sh
# ─────────────────────────────────────────────────────────────────────────────
# One-time GCP infrastructure setup for the Social Fitness App ETL pipeline.
# Run this script once to provision all required GCP resources.
#
# Prerequisites:
#   gcloud CLI installed and authenticated
#   Billing enabled on the project
#   Owner or Editor + required IAM permissions
#
# Usage:
#   export PROJECT_ID=your-gcp-project-id
#   export REGION=us-central1
#   bash deploy/setup_gcp.sh
# ─────────────────────────────────────────────────────────────────────────────

set -euo pipefail

PROJECT_ID=${PROJECT_ID:?'Set PROJECT_ID env var'}
REGION=${REGION:-us-central1}
SA_NAME="social-fitness-etl"
SA_EMAIL="${SA_NAME}@${PROJECT_ID}.iam.gserviceaccount.com"
GCS_RAW_BUCKET="${PROJECT_ID}-social-fitness-raw"
PUBSUB_TOPIC="social-fitness-etl-trigger"
FUNCTION_NAME="social-fitness-etl"
BQ_DATASET_STAGING="social_fitness_staging"
BQ_DATASET_MARTS="social_fitness"

echo "════════════════════════════════════════════════════════"
echo "  Social Fitness App — GCP Infrastructure Setup"
echo "  Project: ${PROJECT_ID} | Region: ${REGION}"
echo "════════════════════════════════════════════════════════"

# ── 1. Enable required APIs ───────────────────────────────────────────────────
echo ""
echo "── 1. Enabling GCP APIs..."
gcloud services enable \
  cloudfunctions.googleapis.com \
  cloudbuild.googleapis.com \
  cloudscheduler.googleapis.com \
  pubsub.googleapis.com \
  bigquery.googleapis.com \
  storage.googleapis.com \
  secretmanager.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  --project="${PROJECT_ID}"
echo "   ✓ APIs enabled"

# ── 2. Create service account ─────────────────────────────────────────────────
echo ""
echo "── 2. Creating service account..."
gcloud iam service-accounts create "${SA_NAME}" \
  --display-name="Social Fitness ETL Pipeline" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "   (already exists)"

# Grant required roles
ROLES=(
  "roles/bigquery.dataEditor"
  "roles/bigquery.jobUser"
  "roles/storage.objectAdmin"
  "roles/secretmanager.secretAccessor"
  "roles/pubsub.subscriber"
  "roles/logging.logWriter"
  "roles/monitoring.metricWriter"
)
for ROLE in "${ROLES[@]}"; do
  gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
    --member="serviceAccount:${SA_EMAIL}" \
    --role="${ROLE}" \
    --quiet 2>/dev/null
done
echo "   ✓ Service account: ${SA_EMAIL}"

# ── 3. Create GCS raw data bucket ─────────────────────────────────────────────
echo ""
echo "── 3. Creating GCS bucket..."
gsutil mb -p "${PROJECT_ID}" -l "${REGION}" -b on \
  "gs://${GCS_RAW_BUCKET}" 2>/dev/null || echo "   (already exists)"

# Lifecycle rule: delete raw files older than 90 days
cat > /tmp/lifecycle.json << 'EOF'
{
  "lifecycle": {
    "rule": [{
      "action": {"type": "Delete"},
      "condition": {"age": 90}
    }]
  }
}
EOF
gsutil lifecycle set /tmp/lifecycle.json "gs://${GCS_RAW_BUCKET}"
echo "   ✓ Bucket: gs://${GCS_RAW_BUCKET} (90-day lifecycle)"

# ── 4. Create BigQuery datasets ───────────────────────────────────────────────
echo ""
echo "── 4. Creating BigQuery datasets..."
bq --project_id="${PROJECT_ID}" mk \
  --dataset \
  --location=US \
  --description="Social fitness ETL staging tables" \
  "${PROJECT_ID}:${BQ_DATASET_STAGING}" 2>/dev/null || echo "   (staging already exists)"

bq --project_id="${PROJECT_ID}" mk \
  --dataset \
  --location=US \
  --description="Social fitness analytics marts" \
  "${PROJECT_ID}:${BQ_DATASET_MARTS}" 2>/dev/null || echo "   (marts already exists)"
echo "   ✓ Datasets: ${BQ_DATASET_STAGING}, ${BQ_DATASET_MARTS}"

# ── 5. Store secrets in Secret Manager ───────────────────────────────────────
echo ""
echo "── 5. Setting up Secret Manager..."
SECRETS=(
  "TIKTOK_ACCESS_TOKEN"
  "TIKTOK_ADVERTISER_ID"
  "META_ACCESS_TOKEN"
  "META_AD_ACCOUNT_ID"
  "REDDIT_CLIENT_ID"
  "REDDIT_CLIENT_SECRET"
  "REDDIT_USERNAME"
  "REDDIT_PASSWORD"
  "APPSFLYER_API_TOKEN"
  "SLACK_WEBHOOK_URL"
)
for SECRET in "${SECRETS[@]}"; do
  gcloud secrets create "${SECRET}" \
    --replication-policy="automatic" \
    --project="${PROJECT_ID}" 2>/dev/null || echo "   (${SECRET} already exists)"
done
echo "   ✓ Secret Manager secrets created"
echo "   → Run: gcloud secrets versions add SECRET_NAME --data-file=<(echo 'value')"
echo "   → For each secret above, add your real API credentials"

# ── 6. Create Pub/Sub topic ───────────────────────────────────────────────────
echo ""
echo "── 6. Creating Pub/Sub topic..."
gcloud pubsub topics create "${PUBSUB_TOPIC}" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "   (already exists)"
echo "   ✓ Topic: ${PUBSUB_TOPIC}"

# ── 7. Deploy Cloud Function ──────────────────────────────────────────────────
echo ""
echo "── 7. Deploying Cloud Function..."
gcloud functions deploy "${FUNCTION_NAME}" \
  --gen2 \
  --runtime=python311 \
  --region="${REGION}" \
  --source="$(pwd)/deploy/cloud_functions" \
  --entry-point=run_etl \
  --trigger-topic="${PUBSUB_TOPIC}" \
  --memory=2048MB \
  --timeout=540s \
  --min-instances=0 \
  --max-instances=3 \
  --service-account="${SA_EMAIL}" \
  --set-env-vars="GCP_PROJECT_ID=${PROJECT_ID},\
GCS_RAW_BUCKET=${GCS_RAW_BUCKET},\
BQ_DATASET_STAGING=${BQ_DATASET_STAGING},\
BQ_DATASET_MARTS=${BQ_DATASET_MARTS},\
DBT_TARGET=prod" \
  --project="${PROJECT_ID}"
echo "   ✓ Cloud Function deployed: ${FUNCTION_NAME}"

# ── 8. Create Cloud Scheduler jobs ───────────────────────────────────────────
echo ""
echo "── 8. Creating Cloud Scheduler jobs..."

# Daily full pipeline — 04:00 UTC
gcloud scheduler jobs create pubsub social-fitness-etl-daily \
  --schedule="0 4 * * *" \
  --topic="${PUBSUB_TOPIC}" \
  --message-body='{"pipeline":"all","dry_run":false}' \
  --time-zone="UTC" \
  --description="Daily ETL: all pipelines, yesterday data" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || \
gcloud scheduler jobs update pubsub social-fitness-etl-daily \
  --schedule="0 4 * * *" \
  --topic="${PUBSUB_TOPIC}" \
  --message-body='{"pipeline":"all","dry_run":false}' \
  --location="${REGION}" \
  --project="${PROJECT_ID}"

# Paid media only — 02:00 UTC (earlier, smaller payload)
gcloud scheduler jobs create pubsub social-fitness-paid-media-daily \
  --schedule="0 2 * * *" \
  --topic="${PUBSUB_TOPIC}" \
  --message-body='{"pipeline":"paid_media","dry_run":false}' \
  --time-zone="UTC" \
  --description="Daily ETL: paid media only" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "   (paid-media job already exists)"

# Weekly backfill check (Sunday 06:00 UTC) — dry run to validate
gcloud scheduler jobs create pubsub social-fitness-weekly-dry-run \
  --schedule="0 6 * * 0" \
  --topic="${PUBSUB_TOPIC}" \
  --message-body='{"pipeline":"all","dry_run":true}' \
  --time-zone="UTC" \
  --description="Weekly dry run — schema validation" \
  --location="${REGION}" \
  --project="${PROJECT_ID}" 2>/dev/null || echo "   (weekly dry-run job already exists)"

echo "   ✓ Scheduler jobs created"

# ── 9. Summary ────────────────────────────────────────────────────────────────
echo ""
echo "════════════════════════════════════════════════════════"
echo "  ✅ GCP SETUP COMPLETE"
echo "════════════════════════════════════════════════════════"
echo ""
echo "  Next steps:"
echo "  1. Add API credentials to Secret Manager:"
echo "     gcloud secrets versions add TIKTOK_ACCESS_TOKEN --data-file=<(echo 'your-token')"
echo ""
echo "  2. Test the Cloud Function:"
echo "     gcloud pubsub topics publish ${PUBSUB_TOPIC} \\"
echo "       --message='{\"pipeline\":\"paid_media\",\"date\":\"2025-01-01\",\"dry_run\":true}'"
echo ""
echo "  3. Monitor execution:"
echo "     gcloud functions logs read ${FUNCTION_NAME} --region=${REGION} --limit=50"
echo ""
echo "  4. Deploy dbt:"
echo "     cd dbt && dbt run --target prod"
echo ""
echo "  Scheduler jobs (run 'gcloud scheduler jobs list'):"
echo "    social-fitness-etl-daily         → 04:00 UTC daily (full pipeline)"
echo "    social-fitness-paid-media-daily  → 02:00 UTC daily (paid media)"
echo "    social-fitness-weekly-dry-run    → 06:00 UTC Sunday (validation)"
echo ""
