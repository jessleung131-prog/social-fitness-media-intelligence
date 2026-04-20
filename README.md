# Social Fitness Tracker App · Media Intelligence Platform

> End-to-end ETL pipeline and analytics system for a social fitness tracking app — ingesting paid and organic media data from TikTok, Instagram/Meta, Reddit, and AppsFlyer into a unified BigQuery data model. Tracks the full user lifecycle from brand impression to long-term subscriber LTV.

**[🔴 Live Demo →](https://jessleung131-prog.github.io/social-fitness-media-intelligence)**

---

## What this project demonstrates

| Area | What's built |
|---|---|
| **Paid media ETL** | Realistic API simulation + normalization for TikTok Ads, Meta Marketing API, Reddit Ads |
| **MMP attribution** | AppsFlyer install-level data with full funnel flags (register → activate → paywall → subscribe) |
| **Common Data Model** | Unified schema that absorbs platform-specific field name and type differences |
| **BigQuery SQL pipeline** | Staging → Mart → Report layer with dbt-style modular SQL |
| **Organic content** | Instagram Graph API, TikTok Display API, Reddit post analytics normalized into CDM |
| **Full-funnel metrics** | Install → Registration → First Activity → Paywall → Trial → Subscription → Referral |
| **LTV cohort analysis** | Monthly cohort tables tracking 30/60/90-day subscriber revenue by acquisition channel |
| **GitHub Pages dashboard** | Live interactive showcase with Chart.js visualizations |

---

## Data sources simulated

```
TikTok Ads API v1.3          →  3,276 rows   (campaign × ad_group × ad × day)
Meta Marketing API v19       →  3,276 rows   (campaign × adset × ad × day)
Reddit Ads API v3            →  1,456 rows   (campaign × ad_group × ad × day)
AppsFlyer Pull API           →  58,814 rows  (one row per attributed install)
App event stream (Firebase)  →  878,315 rows (in-app lifecycle events)
Organic content APIs         →  228 rows     (posts across 3 platforms)
```
*Data covers Oct 2024 – Mar 2025. All data is simulated for demonstration.*

---

## Repository structure

```
social-fitness-media-intelligence/
│
├── etl/
│   ├── simulate_platform_data.py   # Generates raw API payloads per platform
│   ├── transform_to_cdm.py         # Normalizes all sources into CDM
│   ├── extractors/                 # One extractor per platform (real API clients)
│   ├── transformers/               # Field mapping + type casting per source
│   └── loaders/                    # GCS upload + BigQuery load functions
│
├── sql/
│   ├── bigquery_pipeline.sql       # Full SQL pipeline (staging → marts → reports)
│   ├── staging/                    # stg_paid_media, stg_organic, stg_installs
│   ├── marts/                      # mart_channel_performance, mart_ltv_cohort
│   └── utils/                      # Shared macros and utility functions
│
├── data/
│   ├── raw/                        # Platform JSON/CSV (gitignored for large files)
│   └── processed/                  # CDM CSVs (sample files committed)
│
├── docs/
│   ├── linkedin_thumbnail.png      # LinkedIn project card thumbnail
│   ├── data_dictionary.md          # Full CDM field documentation
│   └── architecture.png            # Pipeline architecture diagram
│
├── tests/                          # dbt-style data quality tests
├── index.html                      # GitHub Pages live showcase
└── README.md
```

---

## CDM: Schema differences absorbed at transform time

One of the core challenges this project addresses is that each platform uses different field names, date formats, and attribution windows for the same logical concepts.

| Concept | TikTok field | Meta field | Reddit field | CDM field |
|---|---|---|---|---|
| Spend | `spend` | `spend` | `spend_usd` | `cost_usd` |
| App installs | `app_install` | `mobile_app_installs` | `app_installs` | `installs` |
| Date | `stat_time_day` | `date_start` | `date` (ISO UTC) | `date` (DATE, UTC) |
| Attribution | 7d click / 1d view | 7d click / 1d view | 30d click | stored as metadata |
| Revenue | `value` | `purchase_roas × spend` | not native | `conversion_value_usd` |

---

## Full-funnel event model

```
[Ad Impression]
      ↓
[App Store Click] → CPI tracked via MMP (AppsFlyer)
      ↓
[App Install]     → install event, device + channel attributed
      ↓
[Registration]    → account_created event (82% of installs)
      ↓
[First Activity]  → activity_uploaded event (71% of registered)
      ↓
[Paywall Viewed]  → paywall_view event (55% of activated)
      ↓
[Trial Started]   → trial_start event (38% of paywall views)
      ↓
[Subscribed]      → purchase event, revenue attributed (52% of trials)
      ↓
[Retention]       → renewal, churn, win-back events
      ↓
[Referral]        → share_to_social, friend_invited events
```

---

## GCP automation pipeline

```python
# Daily trigger: Cloud Scheduler → Cloud Function → GCS → BigQuery → dbt → Dashboard

SCHEDULE: "0 4 * * *"  # 04:00 UTC daily

STEPS:
  1. Extract  → fetch yesterday's data from each platform API
  2. Land     → upload raw JSON/CSV to GCS bucket (partitioned by date)
  3. Transform → run transform_to_cdm.py to normalize fields and types
  4. Load     → BigQuery: raw tables → CDM → staging views
  5. Model    → dbt run: staging → marts → report views
  6. Alert    → Slack notification with pipeline summary metrics
```

---

## Team ownership model

| Team | Owns | Schema |
|---|---|---|
| Growth / UA | `etl/paid_*`, paid mart SQL, CPI/ROAS reporting | `paid_media.*` |
| Organic / Content | `etl/organic_*`, organic mart SQL, engagement reporting | `organic.*` |
| Product Analytics | `etl/app_events_*`, funnel events, DAU/MAU | `app_events.*` |
| Finance | `sql/marts/ltv_cohort*`, MRR, churn, subscriber LTV | `subscriptions.*` |

All teams share the CDM layer. Schema changes go through PR review with dbt tests required to pass before merge.

---

## Key metrics tracked

**Paid media:** Impressions · Clicks · CPI · CTR · CPM · ROAS · Video view rate  
**Funnel:** Install→Register rate · Activation rate · Paywall view rate · Trial CVR · Subscribe CVR · Overall CVR  
**LTV:** 30/60/90-day revenue per cohort · LTV/CAC ratio · Days to subscribe · Annual vs monthly mix  
**Organic:** Reach · Engagement rate · Video completion rate · Follower growth · Viral coefficient  
**Retention:** D30/D90 retention · Monthly churn · MRR · Win-back rate

---

## Quick start

```bash
git clone https://github.com/jessleung131-prog/social-fitness-media-intelligence
cd social-fitness-media-intelligence

pip install pandas

# Generate simulated platform data
python etl/simulate_platform_data.py

# Transform to Common Data Model
python etl/transform_to_cdm.py

# Processed CDM tables are now in data/processed/
# Run bigquery_pipeline.sql against your BigQuery project
```

---

*Built by [Jess L.](https://github.com/jessleung131-prog) · Social Fitness Tracker App — generalized ETL template · All data simulated.*
