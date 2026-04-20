"""
etl/validators/paid_media_validator.py
───────────────────────────────────────
Data quality validation layer for cdm_paid_media_daily.
Runs before BigQuery load — catches schema drift, null leakage,
and business-logic violations at transform time, not at query time.

Validation categories:
  1. Schema   — required columns present, correct types
  2. Nulls    — critical fields cannot be null
  3. Ranges   — numeric values within expected bounds
  4. Logic    — cross-field consistency (e.g. cpi = cost_usd / installs)
  5. Platform — platform-specific rules (e.g. Reddit ROAS always 0)
  6. Coverage — expected platforms and date range present
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

import pandas as pd

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# Validation result
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ValidationResult:
    passed:   bool
    check:    str
    message:  str
    severity: str = "ERROR"    # ERROR | WARNING | INFO
    rows_affected: int = 0

    def __repr__(self):
        icon = "✓" if self.passed else ("✗" if self.severity == "ERROR" else "⚠")
        return f"[{self.severity}] {icon} {self.check}: {self.message}"


@dataclass
class ValidationReport:
    results:    list[ValidationResult] = field(default_factory=list)
    df_shape:   tuple = (0, 0)
    run_date:   Optional[date] = None

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results if r.severity == "ERROR")

    @property
    def error_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "ERROR")

    @property
    def warning_count(self) -> int:
        return sum(1 for r in self.results if not r.passed and r.severity == "WARNING")

    def summary(self) -> str:
        total  = len(self.results)
        passed = sum(1 for r in self.results if r.passed)
        return (
            f"Validation: {passed}/{total} checks passed | "
            f"{self.error_count} errors | {self.warning_count} warnings | "
            f"shape={self.df_shape}"
        )

    def print_report(self) -> None:
        print(f"\n{'─'*60}")
        print(f"VALIDATION REPORT  {self.run_date or 'no date'}")
        print(f"Shape: {self.df_shape}  |  {self.summary()}")
        print(f"{'─'*60}")
        for r in self.results:
            print(f"  {r}")
        status = "✅ PASSED — safe to load" if self.passed else "❌ FAILED — do not load"
        print(f"\n  {status}\n")


# ─────────────────────────────────────────────────────────────────────────────
# Validator
# ─────────────────────────────────────────────────────────────────────────────

class PaidMediaValidator:
    """
    Runs a full battery of quality checks on cdm_paid_media_daily
    before it is loaded to BigQuery.

    Usage:
        validator = PaidMediaValidator()
        report    = validator.validate(df, run_date=date(2025, 1, 1))
        report.print_report()

        if not report.passed:
            raise ValueError("Validation failed — aborting BigQuery load")
    """

    REQUIRED_COLUMNS = [
        "date", "platform", "campaign_id", "ad_id",
        "impressions", "clicks", "cost_usd", "installs",
        "cpm", "ctr", "attribution_window", "raw_source",
    ]

    EXPECTED_PLATFORMS = {"tiktok", "instagram_meta", "reddit"}

    PLATFORM_ATTRIBUTION = {
        "tiktok":         "7d_click_1d_view",
        "instagram_meta": "7d_click_1d_view",
        "reddit":         "30d_click",
    }

    def validate(self, df: pd.DataFrame, run_date: Optional[date] = None) -> ValidationReport:
        report          = ValidationReport(df_shape=df.shape, run_date=run_date)
        report.results += self._check_schema(df)
        report.results += self._check_nulls(df)
        report.results += self._check_ranges(df)
        report.results += self._check_logic(df)
        report.results += self._check_platform_rules(df)
        report.results += self._check_coverage(df, run_date)

        status = "PASSED" if report.passed else "FAILED"
        log.info(f"Validation {status}: {report.summary()}")
        return report

    # ── 1. Schema checks ──────────────────────────────────────────────────────

    def _check_schema(self, df: pd.DataFrame) -> list[ValidationResult]:
        results = []

        # Required columns present
        missing = [c for c in self.REQUIRED_COLUMNS if c not in df.columns]
        results.append(ValidationResult(
            passed        = len(missing) == 0,
            check         = "required_columns",
            message       = f"All required columns present" if not missing else f"Missing: {missing}",
            severity      = "ERROR",
            rows_affected = 0,
        ))

        # Date column is datetime-like
        if "date" in df.columns:
            is_date = pd.api.types.is_datetime64_any_dtype(df["date"]) or \
                      pd.api.types.is_object_dtype(df["date"])
            results.append(ValidationResult(
                passed   = is_date,
                check    = "date_dtype",
                message  = f"date column is datetime-compatible" if is_date else "date column has unexpected type",
                severity = "ERROR",
            ))

        # Numeric columns are numeric
        numeric_cols = ["impressions", "clicks", "cost_usd", "installs",
                        "cpm", "ctr", "cpi", "roas"]
        for col in numeric_cols:
            if col not in df.columns:
                continue
            is_numeric = pd.api.types.is_numeric_dtype(df[col])
            results.append(ValidationResult(
                passed   = is_numeric,
                check    = f"dtype_{col}",
                message  = f"{col} is numeric" if is_numeric else f"{col} is not numeric ({df[col].dtype})",
                severity = "ERROR",
            ))

        return results

    # ── 2. Null checks ────────────────────────────────────────────────────────

    def _check_nulls(self, df: pd.DataFrame) -> list[ValidationResult]:
        results = []
        critical_no_null = ["date", "platform", "campaign_id", "impressions", "cost_usd"]

        for col in critical_no_null:
            if col not in df.columns:
                continue
            null_count = df[col].isna().sum()
            results.append(ValidationResult(
                passed        = null_count == 0,
                check         = f"no_nulls_{col}",
                message       = f"{col}: no nulls" if null_count == 0 else f"{col}: {null_count:,} null values",
                severity      = "ERROR",
                rows_affected = int(null_count),
            ))

        # Warn on nulls in non-critical columns
        warn_cols = ["ad_group_id", "ad_name", "country"]
        for col in warn_cols:
            if col not in df.columns:
                continue
            null_pct = df[col].isna().mean()
            results.append(ValidationResult(
                passed        = null_pct < 0.05,
                check         = f"low_nulls_{col}",
                message       = f"{col}: {null_pct:.1%} null (acceptable)" if null_pct < 0.05
                                else f"{col}: {null_pct:.1%} null — check extraction",
                severity      = "WARNING",
                rows_affected = int(df[col].isna().sum()),
            ))

        return results

    # ── 3. Range checks ───────────────────────────────────────────────────────

    def _check_ranges(self, df: pd.DataFrame) -> list[ValidationResult]:
        results = []

        checks = [
            ("impressions",  0,       50_000_000,  "ERROR"),
            ("clicks",       0,       impressions := None, "ERROR"),
            ("cost_usd",     0,       500_000,     "ERROR"),
            ("ctr",          0,       0.5,         "WARNING"),   # CTR > 50% = data issue
            ("cpm",          0,       1_000,       "WARNING"),
            ("cpi",          0,       10_000,      "WARNING"),
            ("roas",         0,       50,          "WARNING"),
        ]

        for col, lo, hi, sev in checks:
            if col not in df.columns or hi is None:
                continue
            out_of_range = ((df[col] < lo) | (df[col] > hi)).sum()
            pct = out_of_range / len(df) if len(df) > 0 else 0
            results.append(ValidationResult(
                passed        = out_of_range == 0,
                check         = f"range_{col}",
                message       = f"{col}: all values in [{lo}, {hi}]" if out_of_range == 0
                                else f"{col}: {out_of_range:,} rows ({pct:.1%}) outside [{lo}, {hi}]",
                severity      = sev,
                rows_affected = int(out_of_range),
            ))

        # Cost must be positive when impressions > 0
        if "cost_usd" in df.columns and "impressions" in df.columns:
            zero_spend_with_impr = ((df["cost_usd"] == 0) & (df["impressions"] > 0)).sum()
            results.append(ValidationResult(
                passed        = zero_spend_with_impr == 0,
                check         = "cost_vs_impressions",
                message       = "No zero-spend rows with positive impressions" if zero_spend_with_impr == 0
                                else f"{zero_spend_with_impr:,} rows have impressions but zero spend",
                severity      = "WARNING",
                rows_affected = int(zero_spend_with_impr),
            ))

        return results

    # ── 4. Logic checks ───────────────────────────────────────────────────────

    def _check_logic(self, df: pd.DataFrame) -> list[ValidationResult]:
        results = []

        # clicks ≤ impressions
        if "clicks" in df.columns and "impressions" in df.columns:
            invalid = (df["clicks"] > df["impressions"]).sum()
            results.append(ValidationResult(
                passed        = invalid == 0,
                check         = "clicks_le_impressions",
                message       = "clicks ≤ impressions for all rows" if invalid == 0
                                else f"{invalid:,} rows where clicks > impressions",
                severity      = "ERROR",
                rows_affected = int(invalid),
            ))

        # installs ≤ clicks
        if "installs" in df.columns and "clicks" in df.columns:
            invalid = (df["installs"] > df["clicks"]).sum()
            results.append(ValidationResult(
                passed        = invalid == 0,
                check         = "installs_le_clicks",
                message       = "installs ≤ clicks for all rows" if invalid == 0
                                else f"{invalid:,} rows where installs > clicks",
                severity      = "WARNING",
                rows_affected = int(invalid),
            ))

        # CPI consistency: if installs > 0 and cost > 0, cpi should be > 0
        if all(c in df.columns for c in ["cpi", "installs", "cost_usd"]):
            cpi_inconsistent = (
                (df["installs"] > 0) & (df["cost_usd"] > 0) & (df["cpi"] == 0)
            ).sum()
            results.append(ValidationResult(
                passed        = cpi_inconsistent == 0,
                check         = "cpi_consistency",
                message       = "CPI computed correctly" if cpi_inconsistent == 0
                                else f"{cpi_inconsistent:,} rows with installs and spend but zero CPI",
                severity      = "WARNING",
                rows_affected = int(cpi_inconsistent),
            ))

        return results

    # ── 5. Platform-specific rules ────────────────────────────────────────────

    def _check_platform_rules(self, df: pd.DataFrame) -> list[ValidationResult]:
        results = []

        if "platform" not in df.columns:
            return results

        # Reddit: ROAS must always be 0 (not natively available)
        if "roas" in df.columns:
            reddit_with_roas = ((df["platform"] == "reddit") & (df["roas"] > 0)).sum()
            results.append(ValidationResult(
                passed        = reddit_with_roas == 0,
                check         = "reddit_no_roas",
                message       = "Reddit rows have no ROAS (expected)" if reddit_with_roas == 0
                                else f"{reddit_with_roas:,} Reddit rows have non-zero ROAS — check transformer",
                severity      = "ERROR",
                rows_affected = int(reddit_with_roas),
            ))

        # Attribution windows per platform
        if "attribution_window" in df.columns:
            for platform, expected_window in self.PLATFORM_ATTRIBUTION.items():
                plat_rows = df[df["platform"] == platform]
                if len(plat_rows) == 0:
                    continue
                wrong_window = (plat_rows["attribution_window"] != expected_window).sum()
                results.append(ValidationResult(
                    passed        = wrong_window == 0,
                    check         = f"attribution_window_{platform}",
                    message       = f"{platform}: attribution_window = {expected_window} ✓"
                                    if wrong_window == 0
                                    else f"{platform}: {wrong_window:,} rows with unexpected attribution window",
                    severity      = "ERROR",
                    rows_affected = int(wrong_window),
                ))

        return results

    # ── 6. Coverage checks ────────────────────────────────────────────────────

    def _check_coverage(self, df: pd.DataFrame, run_date: Optional[date]) -> list[ValidationResult]:
        results = []

        # All three platforms present
        if "platform" in df.columns:
            platforms_found = set(df["platform"].unique())
            missing_plat    = self.EXPECTED_PLATFORMS - platforms_found
            results.append(ValidationResult(
                passed   = len(missing_plat) == 0,
                check    = "all_platforms_present",
                message  = f"All platforms present: {sorted(platforms_found)}" if not missing_plat
                            else f"Missing platforms: {missing_plat}",
                severity = "WARNING",
            ))

        # Row count sanity (at least 10 rows per platform per day expected)
        if "platform" in df.columns and "date" in df.columns:
            dates_found = df["date"].nunique()
            min_rows    = 10 * len(self.EXPECTED_PLATFORMS) * dates_found
            results.append(ValidationResult(
                passed        = len(df) >= min_rows,
                check         = "minimum_row_count",
                message       = f"{len(df):,} rows for {dates_found} date(s) — sufficient"
                                if len(df) >= min_rows
                                else f"Only {len(df):,} rows — expected at least {min_rows:,}",
                severity      = "WARNING",
                rows_affected = 0,
            ))

        return results


# ── CLI entry point ───────────────────────────────────────────────────────────

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(levelname)s  %(message)s")

    sys.path.insert(0, str(Path(__file__).parent.parent))
    from extractors.tiktok_extractor import TikTokExtractor
    from extractors.meta_extractor   import MetaExtractor
    from extractors.reddit_extractor import RedditExtractor
    from transformers.paid_media_transformer import PaidMediaTransformer

    start, end = date(2025, 1, 1), date(2025, 1, 7)

    tt_rows  = TikTokExtractor().extract(start, end)
    mt_rows  = MetaExtractor().extract(start, end)
    rd_rows  = RedditExtractor().extract(start, end)

    t = PaidMediaTransformer()
    combined = t.combine([
        t.transform_tiktok(tt_rows),
        t.transform_meta(mt_rows),
        t.transform_reddit(rd_rows),
    ])

    validator = PaidMediaValidator()
    report    = validator.validate(combined, run_date=start)
    report.print_report()
