# Design Document — Wistia Video Analytics Pipeline

Author: Vidya Sagar Nattala · Last updated: 2026-10-08

## 1. Goal

The marketing team publishes two videos through Wistia (one on Facebook, one on YouTube) and wants to compare how each channel performs: how many people see the video, how many start it, how long they watch, and where viewers come from. This project builds a daily, automated pipeline that collects the data from the Wistia Stats API, stores it in a data lake on AWS, models it as a star schema, and makes it queryable with SQL.

No SME was assigned. Open questions were answered with documented **assumptions** (section 7), to be confirmed later.

## 2. Requirements mapping

| Requirement | How it is met |
|---|---|
| FR1–FR3 Connect to the Wistia API, authenticate | Python `requests` in Lambda; Bearer token read from Secrets Manager (`wistia/api-token`) |
| FR4–FR5 Collect media metadata and engagement metrics | 5 endpoints: media stats, media metadata, daily stats (`by_date`), visitors, events |
| FR6 Pagination | Visitors and events are paged (`page`, `per_page=100`) until an empty page or the watermark is reached; `MAX_PAGES = 50` safety cap |
| FR7 Incremental loads | DynamoDB watermarks per endpoint; only new days/events/visitors are fetched |
| FR8 Daily schedule, 7-day run | EventBridge Scheduler → Step Functions, daily 6:00 AM Central; run Oct 8–14, 2026 |
| FR9 CI/CD | GitHub Actions: CI checks + CD deploy via OIDC role |
| FR10 Data model | Gold star schema in S3 (Parquet), Glue Data Catalog, Athena |
| FR11 Dashboard (optional) | Athena queries in `sql/athena_queries.sql`; dashboard not built (possible extension) |
| FR12 Documentation | README, this document, runbook, demo script |
| Error handling / logging | Retries with backoff, failures never move watermarks, structured logs in CloudWatch, Step Functions catch → SNS email |

## 3. Architecture

```
EventBridge Scheduler (06:00 America/Chicago)
  → Step Functions wistia-pipeline
      1. Lambda wistia-ingest        Wistia API → S3 Bronze (raw JSON, load_date partitions)
      2. Glue wistia-silver-clean    Bronze → Silver (Parquet: types, dedupe, privacy)
      3. Glue wistia-gold-build      Silver → Gold (star schema + reconciliation checks)
      4. Glue crawler                Gold → Data Catalog → Athena
      any failure → SNS wistia-pipeline-alerts (email) → execution marked Failed
GitHub Actions (OIDC) deploys: Lambda code, Glue scripts, state machine definition
```

S3 bucket `wistia-data-222238779166`: `bronze/`, `silver/`, `gold/`, `scripts/`, `athena-results/`, `quarantine/` (reserved).

## 4. Key decisions and trade-offs

| Area | Decision | Why | Trade-off / alternative |
|---|---|---|---|
| Cloud | AWS only | Brief allows AWS or Azure; Databricks is a separate vendor | Glue is AWS's managed Spark |
| Ingestion compute | AWS Lambda | Small daily volume (~5 s/run), near-zero cost | 15-minute limit → the one-time full-history backfill (~70 min) ran on a laptop |
| Incremental state | DynamoDB watermark table | Atomic, simple key-value | An S3 JSON file would be cheaper but less safe |
| Write order | Save to Bronze first, then move the watermark | A failure never loses data; the next run catches up | Possible duplicates on re-runs → handled in Silver |
| Clock | UTC everywhere | Wistia and AWS use UTC; laptop and cloud produce identical results | Local-time reports need conversion |
| Daily stats end date | Yesterday (UTC) | Never store partial "today" numbers | Data is one day behind |
| Layers | Bronze (raw, append-only) → Silver (clean per source) → Gold (business model) | Raw history kept; each layer has one job | More storage (small) |
| Transform | PySpark, developed locally, run on Glue | Brief requires PySpark; local dev is free and fast | Two environments to keep in sync (`--base_path` parameter) |
| Two Glue jobs | Silver and Gold separate | Re-run Gold alone; failures pinpointed per step | Two job start-ups (~1 min each) |
| Data model | Star schema, 2 facts sharing `dim_media` | Loads exist only per video/day; plays/watch time per visitor | A single fact would mix grains |
| SCD | Type 1 for both dimensions | Attributes are stable; per-play location is kept at event level | SCD2 possible later from Bronze daily snapshots |
| Gold writes | Full rebuild (`overwrite`) each run | Simple and idempotent at this volume (seconds) | At scale: incremental partitions or MERGE on `event_key` |
| Query layer | Athena on Parquet | No load step, pay per query (KBs scanned) | Redshift Serverless if a warehouse is required |
| Alerts | Failure-only email | Avoid alert fatigue | Success visible in Step Functions history |
| Deploy security | GitHub OIDC role, least privilege | No long-lived keys in GitHub | — |
| Infrastructure setup | Console + CLI, documented in the runbook | Learning focus, exact repeatable commands | Next step: Terraform / CloudFormation |

## 5. Transformations

### Silver (cleaning, one table per source)
| Table | Rows | Cleaning |
|---|---|---|
| `media` | 6 → 2 | Latest daily snapshot per video; rename to brief names; text → timestamps |
| `events` | 1,837 → 1,233 | Text → timestamp; dedupe on `event_key`; **mask IP** (last octet → `XXX`); keep 12 needed columns; flatten browser info; empty text → NULL |
| `visitors` | 101,421 → 101,108 | Timestamps; dedupe on `visitor_key` (latest `last_active_at`); drop `visitor_identity` (name/email/org) |
| `daily_stats` | 1,504 → 1,490 | Text → date; **`media_id` derived from the Bronze file name** (the API rows don't contain it); dedupe on `media_id + date` |

Duplicates came from test runs plus the full backfill saving the same records twice; Bronze is append-only by design and Silver removes them.

### Gold (business rules)
| Table | Business rule |
|---|---|
| `dim_media` | `channel`: 8hunphufxp → Facebook, 9k4tbcdfg0 → YouTube (assumption) |
| `fact_media_daily` | `play_rate = plays / loads` (0 when no loads), rounded to 4 decimals |
| `dim_visitor` | Built from visitors who **played our videos** (1,158, not the 101,108 account-wide list); location = their **most recent play**; browser info joined from Silver visitors |
| `fact_media_engagement` | Grain video × visitor × day: `play_count = COUNT(*)`, `watched_percent = AVG(percent_viewed)`, `total_watch_time = SUM(percent_viewed × duration_seconds)` |

## 6. Data quality and reconciliation

Automatic checks in `gold_build.py` (the job fails, and an alert is sent, if a check fails):
- Sum of `play_count` in `fact_media_engagement` = number of events in Silver (1,233 = 1,233)
- Every visitor in the fact exists in `dim_visitor` (0 missing)

Manual reconciliation:
| Check | Result |
|---|---|
| Visitors after dedupe vs ingestion log | 101,108 = 101,108 |
| Daily rows vs calendar | 849 days (2024-06-10 → 2026-10-06) + 641 days (2025-01-04 → 2026-10-06) |
| Events vs Wistia lifetime plays | 908 vs 947 and 325 vs 328 (lifetime includes the current day) |
| Avg watched % vs Wistia dashboard engagement | 25.8% vs 25.6% (YouTube), 14.1% vs 14.4% (Facebook) |
| Athena row counts vs laptop run | Identical (2 / 1,158 / 1,490 / 1,170) |
| Viewers per channel | 280 + 880 = 1,160 vs 1,158 → 2 visitors watched both videos |

## 7. Assumptions and SME questions

| # | Question | Assumption used |
|---|---|---|
| 1 | Which video is on which channel? (no channel field in the API) | Vertical 8hunphufxp = Facebook; horizontal 9k4tbcdfg0 = YouTube |
| 2 | IP addresses: mask, hash or drop? | Masked (last octet `XXX`); full raw data only in Bronze |
| 3 | Brief says "Basic Auth" but the snippet uses Bearer | Bearer token (works) |
| 4 | API version `/v1` or `/modern` | `/v1` (matches the brief's snippet) |
| 5 | Is Athena on S3 acceptable for FR10, or is Redshift required? | Athena (Redshift Serverless as alternative) |
| 6 | Play rate definition | plays ÷ loads (Wistia's dashboard uses its own formula, e.g. 42.2% vs 35.0%) |

## 8. Security and privacy
- API token only in Secrets Manager; never in code or Git (checked before the first commit)
- Least-privilege IAM roles: `wistia-lambda-role`, `wistia-glue-role`, `wistia-sfn-role`, scheduler role, `wistia-github-deploy-role` (OIDC, `main` branch of this repo only)
- Personal data minimized in Silver: IP masked; email, names, organization, exact lat/lon, ISP dropped
- S3 Block Public Access on; default encryption

## 9. Cost (approximate)
Lambda: seconds per day (free tier). Glue: 2 jobs × 2 G.1X workers × ~1–2 min per day ≈ a few cents per day. Athena: KBs scanned per query (fractions of a cent). S3, DynamoDB, SNS, Step Functions: negligible at this volume.

## 10. Insights
- Facebook: 118,459 loads, 944 plays (0.8% play rate), 14.1% watched on average → reach without attention.
- YouTube: 915 loads, 320 plays (35.0% play rate), 25.8% watched on average → more total watch time (51 h vs 36 h) with one third of the plays.
- Many viewers stop within seconds; in-app browsers (Facebook, Instagram, TikTok) are common → social sharing beyond the two channels.
- 9k4tbcdfg0 had no new views since 2026-07-26.

## 11. Future improvements
- Infrastructure as Code (Terraform or CloudFormation) for all resources
- Incremental Gold loads (date partitions / MERGE) when volume grows
- SCD2 on `dim_media` from Bronze daily snapshots if attribute history is needed
- Dashboard (Streamlit or QuickSight) on the Athena queries (FR11)
- Data quality framework (e.g. Glue Data Quality rules) and a quarantine table
- Success notifications or a daily summary email during the first weeks
