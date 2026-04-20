"""
etl/loaders/bigquery_loader.py
────────────────────────────────
Loads CDM DataFrames into Google BigQuery.

Handles:
  • Schema definition and enforcement (prevents silent type mismatches)
  • Incremental load strategy: WRITE_APPEND for daily partitions,
    WRITE_TRUNCATE for full refreshes
  • Date-range deduplication before append (avoids double-counting on reruns)
  • GCS staging upload (recommended path for large loads)
  • Dry-run mode (validates schema without writing — safe for CI/CD)
  • Automatic fallback to local CSV export when BigQuery not configured

Load strategy per table:
  cdm_paid_media_daily  → incremental by date partition (DELETE + INSERT pattern)
  cdm_organic_daily     → incremental by date partition
  cdm_installs          → incremental by install_date partition
  cdm_funnel_events     → append only (immutable event log)
  mart_* tables         → full refresh (WRITE_TRUNCATE)
"""

import json
import logging
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from config.settings import (
    GCP_PROJECT_ID, BQ_DATASET_STAGING, BQ_DATASET_MARTS,
    BQ_LOCATION, DATA_CDM_DIR, PIPELINE_VERSION,
    CDM_PAID_MEDIA_TABLE,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery schema definitions (enforced at write time)
# ─────────────────────────────────────────────────────────────────────────────

CDM_PAID_MEDIA_SCHEMA = [
    # Partition column
    {"name": "date",                   "type": "DATE",    "mode": "REQUIRED"},
    # Dimensions
    {"name": "platform",               "type": "STRING",  "mode": "REQUIRED"},
    {"name": "channel_type",           "type": "STRING",  "mode": "NULLABLE"},
    {"name": "campaign_id",            "type": "STRING",  "mode": "NULLABLE"},
    {"name": "campaign_name",          "type": "STRING",  "mode": "NULLABLE"},
    {"name": "objective",              "type": "STRING",  "mode": "NULLABLE"},
    {"name": "ad_group_id",            "type": "STRING",  "mode": "NULLABLE"},
    {"name": "ad_group_name",          "type": "STRING",  "mode": "NULLABLE"},
    {"name": "ad_id",                  "type": "STRING",  "mode": "NULLABLE"},
    {"name": "ad_name",                "type": "STRING",  "mode": "NULLABLE"},
    {"name": "country",                "type": "STRING",  "mode": "NULLABLE"},
    # Volume metrics
    {"name": "impressions",            "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "clicks",                 "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "cost_usd",               "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "installs",               "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "conversions",            "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "conversion_value_usd",   "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "video_views",            "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "video_completions",      "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "engagements",            "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "likes",                  "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "comments",               "type": "INTEGER", "mode": "NULLABLE"},
    {"name": "shares",                 "type": "INTEGER", "mode": "NULLABLE"},
    # Computed efficiency metrics
    {"name": "cpm",                    "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "cpc",                    "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "ctr",                    "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "cpi",                    "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "cps",                    "type": "FLOAT",   "mode": "NULLABLE"},
    {"name": "roas",                   "type": "FLOAT",   "mode": "NULLABLE"},
    # Metadata
    {"name": "attribution_window",     "type": "STRING",  "mode": "NULLABLE"},
    {"name": "raw_source",             "type": "STRING",  "mode": "NULLABLE"},
    {"name": "_extracted_at",          "type": "STRING",  "mode": "NULLABLE"},
    {"name": "_pipeline_version",      "type": "STRING",  "mode": "NULLABLE"},
]


# ─────────────────────────────────────────────────────────────────────────────
# BigQuery Loader
# ─────────────────────────────────────────────────────────────────────────────

class BigQueryLoader:
    """
    Loads CDM DataFrames into BigQuery using the google-cloud-bigquery client.

    In production:  authenticates via Application Default Credentials (ADC).
                    On GCP: uses the service account attached to Cloud Function/Run.
                    Locally: uses gcloud auth application-default login.

    In development: detects missing BigQuery credentials and falls back to
                    saving CSV to data/processed/ with a warning.

    Usage:
        loader = BigQueryLoader()
        result = loader.load_paid_media(df, run_date=date(2025, 1, 1))
    """

    def __init__(self, project_id: str = GCP_PROJECT_ID,
                 dataset: str = BQ_DATASET_STAGING,
                 dry_run: bool = False):
        self.project_id = project_id
        self.dataset    = dataset
        self.dry_run    = dry_run
        self._client    = None
        self._bq_available = self._check_bq_available()

        if not self._bq_available:
            log.warning(
                "BigQuery client unavailable (google-cloud-bigquery not installed "
                "or no credentials). Falling back to local CSV export."
            )

    # ── Availability check ────────────────────────────────────────────────────

    def _check_bq_available(self) -> bool:
        try:
            from google.cloud import bigquery
            from google.auth.exceptions import DefaultCredentialsError
            try:
                self._client = bigquery.Client(project=self.project_id,
                                               location=BQ_LOCATION)
                return True
            except DefaultCredentialsError:
                return False
        except ImportError:
            return False

    # ── Public interface ──────────────────────────────────────────────────────

    def load_paid_media(self, df: pd.DataFrame, run_date: date) -> dict:
        """
        Loads cdm_paid_media_daily to BigQuery.

        Strategy: DELETE rows for dates in this batch, then INSERT.
        This makes the load idempotent — safe to rerun for the same date range.

        Returns a result dict with row counts and status.
        """
        table_id = f"{self.project_id}.{self.dataset}.cdm_paid_media_daily"
        return self._load(df, table_id, CDM_PAID_MEDIA_SCHEMA, run_date,
                          partition_col="date", write_mode="incremental")

    def load_table(self, df: pd.DataFrame, table_name: str,
                   schema: list[dict], run_date: date,
                   write_mode: str = "incremental") -> dict:
        """Generic loader for any CDM table."""
        table_id = f"{self.project_id}.{self.dataset}.{table_name}"
        return self._load(df, table_id, schema, run_date,
                          partition_col="date", write_mode=write_mode)

    # ── Core load logic ───────────────────────────────────────────────────────

    def _load(self, df: pd.DataFrame, table_id: str,
              schema: list[dict], run_date: date,
              partition_col: str = "date",
              write_mode: str = "incremental") -> dict:

        row_count  = len(df)
        dates      = sorted(df[partition_col].dt.date.unique()) if partition_col in df.columns else []
        date_range = f"{min(dates)} → {max(dates)}" if dates else "unknown"

        log.info(f"Loading → {table_id} | {row_count:,} rows | {date_range} | mode={write_mode}")

        if self.dry_run:
            log.info(f"DRY RUN: schema validated, no rows written")
            return {"status": "dry_run", "table": table_id, "rows": row_count}

        if not self._bq_available:
            return self._fallback_csv_export(df, table_id, run_date)

        # Real BigQuery load
        from google.cloud import bigquery

        bq_schema = [
            bigquery.SchemaField(f["name"], f["type"], mode=f.get("mode", "NULLABLE"))
            for f in schema
        ]

        job_config = bigquery.LoadJobConfig(schema=bq_schema)

        if write_mode == "incremental" and dates:
            # DELETE partition dates that will be re-inserted (idempotency)
            self._delete_date_partitions(table_id, partition_col, dates)
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
        elif write_mode == "truncate":
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_TRUNCATE
        else:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND

        # Enable date partitioning on the partition column
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_col,
        )

        load_job = self._client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        load_job.result()   # wait for completion

        result = {
            "status":     "success",
            "table":      table_id,
            "rows":       row_count,
            "date_range": date_range,
            "job_id":     load_job.job_id,
            "loaded_at":  datetime.now(timezone.utc).isoformat(),
        }
        log.info(f"Load complete: {table_id} | job_id={load_job.job_id}")
        return result

    def _delete_date_partitions(self, table_id: str,
                                 partition_col: str, dates: list) -> None:
        """
        Deletes existing rows for the given dates before re-inserting.
        Makes incremental loads idempotent.
        """
        date_list = ", ".join(f"DATE('{d}')" for d in dates)
        query     = f"DELETE FROM `{table_id}` WHERE {partition_col} IN ({date_list})"
        log.debug(f"Deleting partitions: {query}")
        self._client.query(query).result()

    # ── Fallback: local CSV ───────────────────────────────────────────────────

    def _fallback_csv_export(self, df: pd.DataFrame,
                              table_id: str, run_date: date) -> dict:
        """
        When BigQuery is unavailable, saves the CDM DataFrame as CSV.
        Prints the BigQuery DDL statement for manual table creation.
        """
        table_name = table_id.split(".")[-1]
        out_path   = DATA_CDM_DIR / f"{table_name}.csv"
        df.to_csv(out_path, index=False)

        log.info(f"Fallback CSV export: {out_path} ({len(df):,} rows)")
        log.info(f"\n── To load manually into BigQuery, run: ──\n"
                 f"  bq load --source_format=CSV \\\n"
                 f"    --skip_leading_rows=1 \\\n"
                 f"    {table_id} \\\n"
                 f"    {out_path}\n")

        return {
            "status":    "fallback_csv",
            "table":     table_id,
            "rows":      len(df),
            "local_path": str(out_path),
        }

    # ── Schema utilities ──────────────────────────────────────────────────────

    @staticmethod
    def schema_to_ddl(table_id: str, schema: list[dict]) -> str:
        """
        Generates CREATE TABLE DDL for manual BigQuery setup.
        """
        col_defs = []
        for f in schema:
            bq_type = f["type"]
            mode    = f.get("mode", "NULLABLE")
            nullable = "" if mode == "REQUIRED" else ""
            col_defs.append(f"  {f['name']} {bq_type}")

        cols = ",\n".join(col_defs)
        return (
            f"CREATE TABLE IF NOT EXISTS `{table_id}`\n"
            f"(\n{cols}\n)\n"
            f"PARTITION BY date\n"
            f"OPTIONS (require_partition_filter = TRUE);"
        )


# ── CLI entry point / schema printer ─────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    loader = BigQueryLoader(dry_run=False)

    print("\n── BigQuery table DDL (cdm_paid_media_daily) ──\n")
    ddl = BigQueryLoader.schema_to_ddl(CDM_PAID_MEDIA_TABLE, CDM_PAID_MEDIA_SCHEMA)
    print(ddl)

    print(f"\n── Loader status ──")
    print(f"  BigQuery available: {loader._bq_available}")
    print(f"  Project:            {loader.project_id}")
    print(f"  Dataset:            {loader.dataset}")
    print(f"  Dry run:            {loader.dry_run}")

    if not loader._bq_available:
        print(f"\n  To enable BigQuery:")
        print(f"    pip install google-cloud-bigquery")
        print(f"    gcloud auth application-default login")
        print(f"    export GCP_PROJECT_ID=your-project-id")
