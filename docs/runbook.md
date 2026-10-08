# Runbook — Wistia Video Analytics Pipeline

Last updated: 2026-10-08 (UTC)
Status: complete build runbook (Phases 1–9) + operations and teardown. Copy kept in the repo as `docs/runbook.md`.

## Contents
1. Phase 1 — API exploration (Postman) ✅
2. Phase 2 — Architecture design ✅ (no SME assigned → decisions recorded as assumptions)
3. Phase 3 — AWS setup ✅ (S3, Secrets Manager, DynamoDB, Lambda role)
4. Phase 4 — Python ingestion → S3 Bronze → AWS Lambda ✅ (incl. full-history backfill)
5. Phase 5 — PySpark Bronze → Silver → Gold ✅ (local dev → `silver_clean.py` + `gold_build.py` → Glue jobs `wistia-silver-clean`, `wistia-gold-build`)
6. Phase 6 — Athena ✅ (database `wistia_gold`, crawler `wistia-gold-crawler`, workgroup `wistia-workgroup`, reconciled queries)
7. Phase 7 — Orchestration + alerts + daily schedule ✅ (`wistia-pipeline`, `wistia-pipeline-daily`; 7-day run Oct 8–14)
8. Phase 8 — CI/CD ✅ (private repo `wistia-video-analytics-pipeline`, OIDC role `wistia-github-deploy-role`, workflow `ci-cd.yml`)
9. Phase 9 — Documentation ✅ (README, design doc, runbook, demo script, Athena queries)
10. Operate · Pause or teardown

---

## Phase 1 — API exploration (Postman) ✅

**Goal:** confirm authentication, see what each Wistia endpoint returns, and test pagination before designing anything.

### Prerequisites
- Postman installed; workspace `sagarDE`, collection `SagarDE`
- Wistia API token (from the project brief — never commit it to Git or paste it in screenshots)
- Media IDs: `8hunphufxp`, `9k4tbcdfg0`
- API docs: https://docs.wistia.com/reference/get_stats-medias-mediaid (version dropdown → v1.0 shows `/v1` paths; the default v2026.09 shows the newer `/modern/...` paths). Each endpoint page lists **Path Params** (go inside the URL) and **Query Params** (go after `?`).

### 1.1 Create the first request (media stats)
1. Postman → `+` (new tab) → method **GET**
2. URL: `https://api.wistia.com/v1/stats/medias/8hunphufxp.json`
3. Send without auth → **401 Unauthorized** `{"code":"unauthorized_credentials"}` (expected; proves auth is required)
4. **Authorization** tab → Auth Type **Bearer Token** → paste the token (the 64-char string, not the label text)
5. Send → **200 OK**
6. Save → name `Wistia - media stats - 8hunphufxp` → collection `SagarDE`

### 1.2 Second media stats
Duplicate 1.1 → change ID to `9k4tbcdfg0` → Send → rename `Wistia - media stats - 9k4tbcdfg0` → Ctrl+S

### 1.3 Media metadata
Duplicate → URL `https://api.wistia.com/v1/medias/{id}.json` (no `/stats`) → Send → save as `Wistia - media metadata - {id}` for both IDs

### 1.4 Daily stats
Duplicate → URL `https://api.wistia.com/v1/stats/medias/8hunphufxp/by_date.json?start_date=2026-09-28&end_date=2026-10-04` → Send → save as `Wistia - daily stats - 8hunphufxp`

### 1.5 Visitors + pagination test
1. Duplicate → URL `https://api.wistia.com/v1/stats/visitors.json?page=1&per_page=5` → Send → save as `Wistia - visitors`
2. Pagination test: **Params** tab → `page = 2` (keep `per_page = 5`) → **Send again** → first `visitor_key` must differ from page 1
3. Reset `page = 1` → Ctrl+S

### 1.6 Events
Duplicate visitors → URL `https://api.wistia.com/v1/stats/events.json?media_id=8hunphufxp&page=1&per_page=3` → Send → save as `Wistia - events`

### Endpoint reference
| # | Endpoint | Path params | Query params | Paginated | Feeds |
|---|---|---|---|---|---|
| 1 | `/v1/stats/medias/{id}.json` | media hashed id | — | No | Reconciliation (lifetime totals) |
| 2 | `/v1/medias/{id}.json` | media hashed id | — | No | dim_media |
| 3 | `/v1/stats/medias/{id}/by_date.json` | media hashed id | `start_date`, `end_date` (YYYY-MM-DD; default yesterday+today) | No | fact_media_daily |
| 4 | `/v1/stats/visitors.json` | — | `page`, `per_page` (max 100) | Yes | dim_visitor |
| 5 | `/v1/stats/events.json` | — | `media_id`, `page`, `per_page` | Yes | fact_media_engagement; dim_visitor IP/country |

### Key findings (details in `progress.md`)
- Auth: Bearer token works (brief says "Basic Auth" — open question)
- Lifetime stats have no date → daily numbers come from `by_date`
- `by_date` has no play_rate/engagement/visitors per day → derive play_rate = play_count / load_count; it also has **no media_id** in its rows
- Metadata has **no channel field** → Facebook/YouTube mapping is an assumption (vertical 8hunphufxp = Facebook, horizontal 9k4tbcdfg0 = YouTube)
- API uses `created`/`updated`; brief uses `created_at`/`updated_at` → rename in PySpark
- Visitors: account-wide (all media in the account), no IP/country, not linked to a media; sorted newest first by `last_active_at`; plain list (no total or next link) → loop pages until `[]`; dedupe on `visitor_key`
- Events: one viewing session (play) each; `event_key`, `received_at`, `media_id`, `visitor_key`, `ip`, `country/region/city`, `percent_viewed` (0–1); IP is personal data → mask/hash before Gold; watch_seconds = percent_viewed × media duration; `embed_url` empty
- Load vs play: a **load** = the player was shown on someone's screen (e.g. scrolling past it); a **play** = the video actually started. play_rate = plays ÷ loads

### Troubleshooting (Phase 1)
| Symptom | Cause | Fix |
|---|---|---|
| 401 Unauthorized | No token, or the label text "Wistia API token" pasted instead of the token | Authorization → Bearer Token → paste the 64-char token |
| Page 2 returns the same visitors as page 1 | Wrong param edited (`per_page` changed instead of `page`), or response not re-sent | Check URL shows `page=2&per_page=5`; click **Send** again (same ms/KB = old response) |
| Orange dot on a tab | Unsaved changes | Ctrl+S |
| Stray `GET http://embed...` tab | An asset link was clicked | Close → Don't save |

---

## Phase 2 — Architecture design ✅

No SME is assigned right now, so design decisions are made by the developer and recorded as **assumptions** in the design document (the brief asks for decisions, assumptions and tradeoffs to be documented).

### Flow
```
EventBridge Scheduler (daily)
   ▼
Step Functions
   ▼
Lambda wistia-ingest (Python): media → daily stats → events → visitors
   (token from Secrets Manager, watermarks in DynamoDB)
   ▼
S3 Bronze (raw JSON, load_date partitions, UTC)
   ▼  Glue job wistia-silver-clean (silver_clean.py)
S3 Silver (clean, typed, deduped, IP masked)
   ▼  Glue job wistia-gold-build (gold_build.py)
S3 Gold (star schema: dim_media, dim_visitor, fact_media_engagement, fact_media_daily)
   ▼
Glue Catalog + Athena  (→ optional Streamlit dashboard)
```

### Decisions
| Area | Decision | Why / tradeoff |
|---|---|---|
| Cloud | AWS | Brief allows AWS or Azure only; reuses restaurant-project patterns. Databricks is a separate vendor → not allowed; AWS Glue is the AWS equivalent |
| Ingestion | AWS Lambda (Python `requests`) | Wistia is on the internet, so no laptop extract; small daily volume (~5–25 s per run); near-zero cost. 15-min limit → the one-time full-history backfill ran locally (~70 min) |
| Watermarks | DynamoDB table, one row per endpoint | Same pattern as healthcare project; safe atomic updates (vs S3 JSON file) |
| Secrets | AWS Secrets Manager | Token never in code or Git |
| API version | `/v1` | Matches the brief's snippet; `/modern` is the newer style |
| Clock | **UTC everywhere** (`load_date`, `end_date`, `run_id`, log times, Spark session time zone) | Lambda and Wistia timestamps are UTC; laptop and AWS must produce the same folders/bookmarks/values |
| Storage | S3 Bronze/Silver/Gold, `load_date=YYYY-MM-DD` partitions in Bronze | Keeps each day's raw pull, easy re-run of one day, proves the 7-day run |
| Transform | PySpark, developed locally in VS Code on a synced Bronze copy, then run as two AWS Glue jobs | Brief requires PySpark; no dbt. Local dev is free and fast; the same scripts run in Glue (`--base_path` job parameter) |
| Silver vs Gold | Silver = cleaning (types, duplicates, privacy, column names), one table per source; Gold = business rules + star schema, built **from Silver**; separate scripts/jobs | Same layering as the restaurant project; Gold re-runnable alone; failures pinpointed per step |
| Data model (FR10) | **Star schema**: dims `dim_media`, `dim_visitor`; facts `fact_media_engagement` (media × visitor × date) and `fact_media_daily` (media × date) | Two grains because loads/play_rate exist only per media per day (daily_stats), while plays/watched % exist per visitor (events) |
| SCD | All dimensions **SCD Type 1** | Attributes are stable; no report needs past values; location history is kept per event; raw daily snapshots stay in Bronze if SCD2 is ever needed |
| Privacy | IP masked (last octet → `XXX`); visitor name/email/org, lat/lon, ISP dropped in Silver | Data minimization; full raw data stays in Bronze |
| Gold write | Full rebuild each run (`mode("overwrite")`) | Small volume → simple and idempotent; at scale → incremental by date partition or MERGE on `event_key` |
| Query layer | Parquet + Glue Catalog + Athena | No load step, near-zero cost; alternative: Redshift Serverless if a real DWH is required |
| Schedule (FR8) | EventBridge Scheduler daily → Step Functions; SNS alerts | Same as restaurant project |
| CI/CD (FR9) | GitHub Actions + OIDC deploy role | Same as restaurant project |

### Requirement mapping
FR6 pagination + FR7 incremental → Lambda · FR8 → scheduler · FR9 → GitHub Actions · FR10 → Gold + Athena · FR11 → Streamlit (optional) · FR12 → repo + README + diagram + this runbook

### S3 layout
```
s3://wistia-data-222238779166/
   bronze/<endpoint>/load_date=YYYY-MM-DD/   (media, media_stats, daily_stats, visitors, events)
   silver/<source>/                          (media, daily_stats, events, visitors)
   gold/<table>/                             (dim_media, dim_visitor, fact_media_engagement, fact_media_daily)
   quarantine/
   scripts/                                  (silver_clean.py, gold_build.py, temp/)
   athena-results/
```

---

## Phase 3 — AWS setup ✅

Account 222238779166, region **us-east-1 (N. Virginia)**, admin user `sagar-user`.

### 3.1 S3 bucket
1. Console → **S3** → **Create bucket**
2. Name `wistia-data-222238779166` (rules: lowercase, numbers, hyphens; 3–63 chars; globally unique — account ID suffix keeps it unique), region us-east-1, other settings default (Block Public Access on, default encryption)
3. Open the bucket → **Create folder** ×6: `bronze`, `silver`, `gold`, `quarantine`, `scripts`, `athena-results`

Note: the **bucket** is `wistia-data-222238779166`; `bronze/` etc. are folders (key prefixes) inside it.

### 3.2 Secrets Manager — API token
1. Console → **Secrets Manager** → **Store a new secret**
2. Secret type **Other type of secret**; Key/value: key **`api-token`** (hyphen) = the 64-char Wistia token (from the brief or Postman → Authorization tab)
3. Encryption key: `aws/secretsmanager` (default) → Next
4. Secret name `wistia/api-token`; description "Wistia Stats API bearer token for the video analytics pipeline" → Next
5. Automatic rotation **off** → Next → Store
6. To check the key name later: secret → **Retrieve secret value** → **Secret key** column (never share the value column)

### 3.3 DynamoDB — watermark table
1. Console → **DynamoDB** → **Tables** → **Create table**
2. Table name `wistia_watermark`; Partition key `endpoint` (String); no sort key
3. Table settings **Default settings** (already On-demand capacity) → Create table → wait for **Active**

### 3.4 IAM role for Lambda
1. Console → **IAM** → **Roles** → **Create role**
2. Trusted entity **AWS service** → Use case **Lambda** → Next
3. Attach managed policy `AWSLambdaBasicExecutionRole` (CloudWatch logs) → Next
4. Role name `wistia-lambda-role` → check the policy is listed under Step 2 → Create role
5. Open the role → **Add permissions** → **Create inline policy** → JSON:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ReadWistiaToken", "Effect": "Allow",
      "Action": "secretsmanager:GetSecretValue",
      "Resource": "arn:aws:secretsmanager:us-east-1:222238779166:secret:wistia/api-token-*" },
    { "Sid": "Watermarks", "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:PutItem", "dynamodb:UpdateItem"],
      "Resource": "arn:aws:dynamodb:us-east-1:222238779166:table/wistia_watermark" },
    { "Sid": "WriteBronze", "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:DeleteObject"],
      "Resource": "arn:aws:s3:::wistia-data-222238779166/bronze/*" },
    { "Sid": "ListBronze", "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::wistia-data-222238779166",
      "Condition": { "StringLike": { "s3:prefix": "bronze/*" } } }
  ]
}
```
6. Policy name `wistia-lambda-access` → Create policy
7. Check: role **Permissions** tab shows 2 policies — `AWSLambdaBasicExecutionRole` (AWS managed) and `wistia-lambda-access` (Customer inline)

Least privilege: the Lambda can read only its own secret, update only its watermark table, and write/list only under `bronze/`. (The Glue role `wistia-glue-role` is created in §5.6.)

### Troubleshooting (Phase 3)
| Symptom | Cause | Fix |
|---|---|---|
| Rotation page shows a Lambda (e.g. healthcare-ingest-drive-to-s3) in the dropdown | Fields are inactive while Automatic rotation is off | Ignore; leave rotation off |
| New DynamoDB table not in the list | Create table not clicked yet / still creating | Click Create table; refresh until **Active** |

---

## Phase 4 — Python ingestion → S3 Bronze → AWS Lambda ✅

Code is written and tested locally in VS Code first, then packaged and deployed to Lambda. Local runs use the laptop's AWS default profile (`sagar-user`); Lambda uses `wistia-lambda-role`. Lambda runs only the **ingestion** script; the PySpark scripts run in **Glue** (Lambda has no Spark, 15-min limit).

### 4.1 Local project setup
Folder `C:\projects\wistia-video-analytics-pipeline` (separate from the restaurant project). In the VS Code terminal:
```
deactivate                                   # only if another project's (.venv) is active
mkdir C:\projects\wistia-video-analytics-pipeline
# File → Open Folder → wistia-video-analytics-pipeline
python -m venv .venv
.venv\Scripts\activate                       # prompt must start with (.venv)
pip install requests boto3 python-dotenv
pip freeze > requirements.txt                # no space in the file name
mkdir ingestion
New-Item ingestion\wistia_client.py
```
If activation is blocked: `Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned`.
Turn on **File → Auto Save** in VS Code.

### 4.2 `ingestion/wistia_client.py` (final state)
- Imports: `json`, `time`, `logging`, `boto3`, `requests`, `from datetime import date, datetime, timedelta, timezone`
- Logging: `basicConfig(INFO, "%(asctime)s %(levelname)s %(message)s")`, `log = getLogger("wistia")`, **`log.setLevel(logging.INFO)`** (needed in Lambda), `logging.Formatter.converter = time.gmtime` (UTC log times on the laptop)
- Constants: `BASE_URL`, `MEDIA_IDS = ["8hunphufxp", "9k4tbcdfg0"]`, `BUCKET`, **`MAX_PAGES = 50`** (daily safety cap; 2000 was used for the backfill)
- `get_token()` — Secrets Manager `wistia/api-token`, key `api-token`
- `call_wistia(token, url, max_retries=3)` — the **only** place that calls `requests.get`: Bearer header, `timeout=30`, retries with backoff
- Endpoint functions (each builds the URL and returns `call_wistia(token, url)`): `get_media_stat`, `get_media_metadata`, `get_daily_stats`, `get_visitors_page`, `get_events_page`
- `get_all_visitors(..., since=None)` / `get_all_events(..., since=None)` — loop pages; stop at the first record ≤ bookmark or an empty page; return `None` if any page fails
- `save_to_bronze(data, endpoint, load_date, file_name)` → `s3://<bucket>/bronze/<endpoint>/load_date=<date>/<file_name>.json`
- `get_watermark(endpoint)` / `set_watermark(endpoint, value)` — DynamoDB `wistia_watermark`
- `run()` — the pipeline: `now = datetime.now(timezone.utc)` → `load_date` (UTC today), `end_date` (UTC yesterday), `run_id` (UTC HHMMSS); loop over `MEDIA_IDS` (stats, metadata, daily incremental, events incremental); then visitors incremental; collects `failures`; at the end raises `RuntimeError` if any failure, else logs `run finished`
- `lambda_handler(event, context)` — AWS entry point → `run()`, returns `{"status": "ok"}`
- `if __name__ == "__main__": run()` — laptop entry point

### 4.3 Incremental logic (FR7)
| Endpoint | Watermark key | Watermark value | Next run fetches | Bronze file name |
|---|---|---|---|---|
| daily stats | `daily_stats#<media_id>` | last complete UTC day (yesterday) | day after watermark → yesterday; first run: media `created` date → yesterday; skip if start > end | `<media_id>_<run_id>.json` |
| events | `events#<media_id>` | newest `received_at` saved | pages until first event ≤ watermark | `<media_id>_<run_id>.json` |
| visitors | `visitors` | newest `last_active_at` saved | pages until first visitor ≤ watermark | `visitors_<run_id>.json` |
| media, media_stats | — | — | full snapshot every run (tiny) | `<media_id>.json` (latest snapshot of the day) |

Rules:
- Save to Bronze **first**, then `set_watermark` — if anything fails, the watermark doesn't move and the next run catches up (no gaps).
- End at **yesterday (UTC)** for daily stats so partial "today" numbers are never frozen.
- Events/visitors lists are newest first → `[0]` is the newest → new watermark.
- Incremental files include `run_id`, so same-day re-runs **add** files instead of overwriting (Bronze is append-only; Silver dedupes on `event_key` / `visitor_key` / `media_id + date`).
- The media ID in the daily_stats file name is what Silver uses to add `media_id` (the by_date rows don't contain it).

### 4.4 Error handling, retries, logging
| Situation | Behaviour |
|---|---|
| Network error, timeout, 429, 5xx | `call_wistia` retries up to 3 times, waiting 2 s then 4 s (`log.warning`); then gives up (`log.error`) and returns `None` |
| 401 / 404 | No retry (`log.error`), returns `None` |
| stats call fails | Stats not saved; recorded in `failures`; rest of the run continues |
| metadata call fails | `log.error`, recorded, `continue` → skip the rest of that media (daily needs `created`); next media and visitors still run |
| daily / events / visitors fail (incl. a failed page mid-paging) | Nothing saved, **bookmark not moved** (`log.error`), recorded; next run retries the whole range |
| End of run with failures | `log.error("run finished with failures: [...]")` + `raise RuntimeError` → Lambda reports **failed** → (Phase 7) Step Functions → SNS alert |
| Logging | Every line: timestamp + level (INFO / WARNING / ERROR). In Lambda these go to CloudWatch Logs `/aws/lambda/wistia-ingest` → filter on `ERROR` |

Tests done (2026-10-06): bad media ID → `ERROR error 404 (not retrying)`; unreachable host `api.wistia.invalid` → 3 attempts, waits 2 s / 4 s, `giving up`; normal run → all lines `INFO`, `run started` … `run finished`.

### 4.5 Full-history backfill (one time, laptop) ✅
Testing used `max_pages=3`, so events/visitors bookmarks only covered the newest ~300 rows. Done on 2026-10-06:
1. Delete the test bookmarks (keep `daily_stats#…`, already full history):
   ```
   aws dynamodb delete-item --table-name wistia_watermark --key '{"endpoint": {"S": "events#8hunphufxp"}}'
   aws dynamodb delete-item --table-name wistia_watermark --key '{"endpoint": {"S": "events#9k4tbcdfg0"}}'
   aws dynamodb delete-item --table-name wistia_watermark --key '{"endpoint": {"S": "visitors"}}'
   aws dynamodb scan --table-name wistia_watermark      # only the 2 daily_stats rows left
   ```
2. `MAX_PAGES = 2000`, then run with a saved log:
   ```
   python ingestion\wistia_client.py 2>&1 | Tee-Object backfill_log.txt
   Select-String -Path backfill_log.txt -Pattern "new events|new visitors|run finished|ERROR"
   ```
3. Result: events full history for both media (`8hunphufxp_182738.json` ~1 MB ≈ plays count; `9k4tbcdfg0_182738.json` ~371 KB); visitors **1,012 pages → 101,108 visitors** (account-wide, stopped at the real end of the list), ~70 minutes, no errors.
4. Set `MAX_PAGES = 50` for daily production.

Why on the laptop: the visitors full load (~70 min) exceeds Lambda's 15-minute maximum; it's a one-time job. Daily runs read 1–2 pages.

Note: the red text in the terminal during the backfill is just PowerShell colouring the log channel (stderr); the lines are INFO.

### 4.6 Lambda deployment ✅
**Package (Linux libraries, built on Windows):**
```
mkdir build
pip install requests -t build --platform manylinux2014_x86_64 --only-binary=:all: --python-version 3.12
Copy-Item ingestion\wistia_client.py build\ -Force
Compress-Archive -Path build\* -DestinationPath wistia_lambda.zip -Force
Get-ChildItem build                      # wistia_client.py + requests, urllib3, idna, certifi, charset_normalizer
Compare-Object (Get-Content ingestion\wistia_client.py) (Get-Content build\wistia_client.py)   # no output = identical
```
`boto3` is built into Lambda, so only `requests` (+ helpers) is packaged. `--platform manylinux...` matters because `charset_normalizer` has a compiled part and Lambda runs on Linux. `wistia_client.py` must be at the **top level** of the zip (`build\*`, not `build`).

**Create the function (console, first time only):**
1. Lambda → **Create function** → **Author from scratch**
2. Name `wistia-ingest`; Runtime **Python 3.12**; ARM64 **off** (x86_64)
3. Additional settings → **Custom execution role** → `wistia-lambda-role` → **Create function**
4. Code → **Update ▼ → Update from a .zip file** → `wistia_lambda.zip` → Save (uploading a zip deploys automatically; **Deploy** is only for console edits)
5. Code → **Runtime settings → Edit → Handler** `wistia_client.lambda_handler` → Save
6. Configuration → **General configuration → Edit** → Timeout **5 min**, Memory **256 MB** → Save
7. Test → event `manual-run` `{}` → **Test**

**Results:** succeeded, `{"status": "ok"}`; 128 MB → 9.8 s; 256 MB → ~5 s (more memory = more CPU). Logs visible in the test output and CloudWatch `/aws/lambda/wistia-ingest`.

**Redeploy after any code change:** edit in VS Code (the one true copy) → rebuild zip (Copy-Item + Compress-Archive) → Update from a .zip file → Test. Later automated by GitHub Actions (Phase 8). Never leave a fix only in the console editor (drift: the next zip upload overwrites it).

### 4.7 Verified results
- 2026-10-05: token length 64; all 5 endpoints; first Bronze files
- 2026-10-06: daily stats full history 848 rows (8hunphufxp, 2024-06-10→2026-10-05) and 640 rows (9k4tbcdfg0, 2025-01-04→2026-10-05); second runs skip (`already up to date`, page 1 only)
- 2026-10-06: full-history backfill (see 4.5)
- 2026-10-07 01:15 UTC (laptop, UTC change): `load_date=2026-10-07`, daily range 2026-10-06→2026-10-06 → 1 row per media; 1 new event; 5 new visitors; ~25 s
- 2026-10-07 01:18 UTC (Lambda): succeeded, ~4.8 s, 256 MB, everything up to date
- Live check: 8hunphufxp plays 943 → 947 over two days

Useful commands:
```
python ingestion\wistia_client.py
aws s3 ls s3://wistia-data-222238779166/bronze/ --recursive --human-readable
aws dynamodb scan --table-name wistia_watermark
aws dynamodb delete-item --table-name wistia_watermark --key '{"endpoint": {"S": "<name>"}}'   # PowerShell 7: no \" escaping
Select-String -Path ingestion\wistia_client.py -Pattern "print\("                              # should find nothing
Select-String -Path ingestion\wistia_client.py -Pattern "MAX_PAGES" -CaseSensitive
```

### Troubleshooting (Phase 4)
| Symptom | Cause | Fix |
|---|---|---|
| `C:\...` "not recognized as a cmdlet" | Typed a folder path as a command | `mkdir <path>`, then File → Open Folder |
| `logging.Formatter...` / `Spark version: ...` "not recognized" | Python code (or expected output) typed in the terminal | Python goes in the .py file; terminal is for commands |
| `Get -childItem` not recognized | Space inside a cmdlet name | `Get-ChildItem` (no space; Tab completes) |
| No `(.venv)` in prompt; packages went to main Python | venv not activated | `.venv\Scripts\activate` and reinstall |
| `requirements` file with no extension | Space in `requirements .txt` | `Remove-Item requirements`; `pip freeze > requirements.txt` |
| Crossed-out red tab still shows old content | File was deleted; editor buffer is stale | Close tab → Don't Save; check with `Get-Content` |
| `KeyError: 'api_token'` | Secret key is `api-token` (hyphen) | Use `secret["api-token"]`; check key in console |
| Debug `print` never runs | Placed after `return` | Put it before `return` |
| `NameError: name 'secret'` / `'metadata'` / `'load_date'` | Variable used before it's created, or outside the function/block where it exists | Create first, then use; keep lines in the right block |
| `NameError: name 'get_all_events'` / wavy line under `run()` | Function called before (or without) its `def` | Define it (all `def`s above the entry points) |
| 404 `Route not found` | URL typo (`daily.json` instead of `by_date.json`; events under `/medias/`) | Match the endpoint reference table exactly |
| `TypeError: 'NoneType' object is not iterable` | Previous call failed and returned `None` | Check for `None` before using the result |
| `TypeError: 'dict' object is not callable` | `event("received_at")` with round brackets | Dictionary fields use square brackets: `event["received_at"]` |
| `KeyError: 'created_date'` | Wrong field name | Wistia metadata field is `created` |
| Output unchanged after editing / new code "missing" | File not saved | Ctrl+S / Auto Save; verify with `Select-String` or `Get-Content` |
| `Select-String "MAX_PAGES"` also matches `max_pages` | Search ignores case by default | Add `-CaseSensitive` |
| Run still used 3 pages after setting `MAX_PAGES` | The **call** in `run()` still passed `max_pages=3` (call values override definition defaults) | Change the calls (events + visitors lines) |
| `AttributeError ... statys_code` / `status_codes` / `isformat` / `now.date.today()` | Typo | `status_code` / `isoformat` / `now.date()` |
| Every call 401 after refactor | Header name typo (`Authorizied`) | `"Authorization"` |
| Error text appended to the list letter by letter | Function `return`ed an error string | Log the error, `return None` |
| `ParamValidationError: Unknown parameter "BUCKET"/"key"` | boto3 parameter names are case-sensitive | `Bucket=BUCKET`, `Key=key` |
| All saves go to the same S3 file | File name / folder typed by hand (`"9k4tbcdfg0"`, `"media"`) | Use `media_id` (no quotes) and the right folder name per endpoint |
| Same-day re-run replaced earlier Bronze file | File name without run time | Add `_{run_id}` to incremental file names |
| Laptop and Lambda produce different dates / run_ids | Local time vs UTC | `now = datetime.now(timezone.utc)` for load_date, end_date, run_id |
| Visitors fetched twice | Visitors lines indented inside the media loop | 4 spaces, after the loop (account-wide, not per media) |
| Watermark always `None` after writing | Name or field mismatch (`test#demo:` vs `test#demo`; `Last_value` vs `last_value`) | Exact same text in set and get; check with `aws dynamodb scan` |
| Printed `start_date` / `end_date` literally | Variables inside quotes, or missing `f` before an f-string | Remove quotes / add `f` |
| `events: 3` instead of 300 | `append` outside the `for` (or `break` outside the `if`) | Indent: `for` 8, `if`/`append` 12, `stop`/`break` 16 (Tab / Ctrl+]) |
| `if stats is None:` saved nothing / crashed | Condition reversed | `if stats is not None:` (body = good path) |
| Retry waits skipped after network errors | `if attempt < max_retries` indented inside `else` | Same level as `try` |
| `SyntaxError` in a call | Missing comma between parameters (`max_pages=3 per_page=100`) | Add the comma |
| `delete-item` "Invalid JSON" | `\"` escaping in PowerShell 7 | Use plain `'{"endpoint": {"S": "..."}}'` |
| Lambda editor: "file was not found" after zip upload | Old `lambda_function.py` tab still open | Close the tab; set Handler `wistia_client.lambda_handler` |
| Lambda log output shows only START/END/REPORT | Lambda pre-configures logging at WARNING; `basicConfig` is ignored | `log.setLevel(logging.INFO)` |
| Lambda memory still 128 MB | Only timeout was saved | Configuration → General configuration → Edit → 256 MB |
| `INFO Found credentials in shared credentials file` | boto3's own log line | Harmless |
| Red dot next to a line number | Breakpoint clicked by accident | Click it to remove (no effect on `python` runs) |

---

## Phase 5 — PySpark Bronze → Silver → Gold ✅ (local + AWS Glue)

**Approach:** develop on the laptop (VS Code + local PySpark) against a synced copy of Bronze; once all tables work, put the same logic into two clean scripts and run them as AWS Glue jobs on S3. Start each table by **looking at the real data** first (DESCRIBE / SELECT), then build. `transform/explore_bronze.py` = Babu's learning/exploration file (kept); `silver_clean.py` + `gold_build.py` = the final production scripts.

### 5.1 Local Spark setup ✅
```
pip install pyspark==3.5.*        # 3.5 line = AWS Glue 5.0
java -version                     # Temurin 11 (already installed for the restaurant project)
python -c "import os,sys; os.environ['PYSPARK_PYTHON']=sys.executable; from pyspark.sql import SparkSession; s=SparkSession.builder.master('local[*]').getOrCreate(); print('Spark version:', s.version); s.stop()"
aws s3 sync s3://wistia-data-222238779166/bronze/ data\bronze\     # 30 files, ~57 MB; re-run copies only new files
mkdir transform
New-Item transform\explore_bronze.py
python -m json.tool data\bronze\media\load_date=2026-10-07\8hunphufxp.json | Select-Object -First 12   # view a raw file formatted
```
Spark reports 3.5.5 (a `SPARK_HOME` from an earlier setup) — fine, same 3.5 line.
`ERROR/SUCCESS: The process with PID ... could not be terminated` at the end of every run = harmless Windows message when Spark shuts down. `[Stage N:> (0 + 1) / 1]` = Spark progress bar, not an error. `spark-warehouse/` folder = auto-created by Spark, empty (→ `.gitignore`).

### 5.2 Techniques used (in `explore_bronze.py`, written by Babu)
- `SparkSession` + `spark.sparkContext.setLogLevel("ERROR")` + **`spark.conf.set("spark.sql.session.timeZone", "UTC")`**
- Read Bronze: `spark.read.option("multiLine", True).json("data/bronze/<endpoint>")` (Spark adds `load_date` from the folder name). Read Silver/Gold: `spark.read.parquet(...)` (types are kept)
- Explore with Spark SQL: `df.createOrReplaceTempView("name")`; `spark.sql("DESCRIBE name").show(50, truncate=False)`; duplicate check `COUNT(*)` vs `COUNT(DISTINCT key)`
- Dedupe: `Window.partitionBy(key).orderBy(...desc())` + `F.row_number()` → `filter(rn == 1)` → `drop("rn")` (SQL: `ROW_NUMBER() OVER (...) ... WHERE rn = 1`)
- Rename/pick: `select(F.col(...).alias(...))`, `withColumnRenamed`; types: `F.to_timestamp`, `F.to_date`; rules: `F.when(...).otherwise(...)`; joins: `df.join(other, on="key", how="left")`; aggregation: `groupBy(...).agg(F.count, F.avg, F.sum)`
- Save: `df.write.mode("overwrite").parquet(...)`

### 5.3 Silver tables ✅
| Silver table | Raw → Silver rows | Cleaning |
|---|---|---|
| `media` | 6 → **2** | latest snapshot per `hashed_id` (by `load_date`); rename (hashed_id→media_id, name→title, created→created_at, share_link.url→url, duration→duration_seconds, updated→updated_at); `F.to_timestamp` |
| `events` | 1,837 → **1,233** (908 + 325) | `received_at` → timestamp; dedupe per `event_key` (newest `load_date`); mask IP `F.regexp_replace(ip, r"\.\d+$", ".XXX")`; 12 columns: event_key, media_id, visitor_key, event_at (=received_at), percent_viewed, ip_masked, country, region, city, browser, platform, is_mobile (from `user_agent_details.*`); `replace("", None)` |
| `visitors` | 101,421 → **101,108** | `created_at`, `last_active_at` → timestamp; dedupe per `visitor_key` (newest `last_active_at`); 8 columns: visitor_key, created_at, last_active_at, load_count, play_count, browser, platform, is_mobile; `visitor_identity` (name/email/org) dropped; `replace("", None)` |
| `daily_stats` | 1,504 → **1,490** (849 + 641) | `date` → `F.to_date`; **`media_id` from the file name**: `F.regexp_extract(F.input_file_name(), r"([a-z0-9]{10})(_\d+)?\.json$", 1)` (the by_date rows have no media_id); dedupe per `media_id + date` (newest `load_date`); 5 columns: media_id, date, load_count, play_count, hours_watched |

Dropped from events for data minimization: lat, lon, org, email, thumbnail, iframe_heatmap_url, embed_url, conversion_*, media_name, media_url.

Reconciliation: visitors 101,108 = backfill `new visitors`; daily 849 days (2024-06-10→2026-10-06) and 641 days (2025-01-04→2026-10-06); events 325 vs 328 lifetime plays (9k4tbcdfg0), 908 vs 947 (8hunphufxp — local sync predates the newest events).

Duplicate sources: early test runs + the full backfill saved the same records twice (9k4tbcdfg0 had no new views since 2026-07-26, so its ~300 events repeated); the day-2 7-day daily test (2026-09-28→10-04) repeats inside the full daily history.

### 5.4 Gold tables ✅ (star schema)
| Gold table | Grain | Built from | Business rule | Rows |
|---|---|---|---|---|
| `dim_media` (SCD1) | video | silver media | `channel` = CASE 8hunphufxp→Facebook, 9k4tbcdfg0→YouTube, else Unknown (assumption) | 2 |
| `fact_media_daily` | video × day | silver daily_stats | `play_rate` = `round(when(load_count > 0, play_count / load_count).otherwise(0), 4)` | 1,490 |
| `dim_visitor` (SCD1) | visitor who played our videos | silver events (latest play per visitor → ip_masked, country, region, city) LEFT JOIN silver visitors (browser, platform, is_mobile, created_at, last_active_at, load/play_count); key renamed `visitor_id` | location = **most recent play** | 1,158 |
| `fact_media_engagement` | video × visitor × day (aggregated) | silver events + `dim_media.duration_seconds`; `date = to_date(event_at)` | `play_count = COUNT(*)`, `watched_percent = AVG(percent_viewed)`, `total_watch_time = SUM(percent_viewed × duration_seconds)` (seconds) | 1,170 |

Why `dim_visitor` = 1,158 and not 101,108: the visitors endpoint is account-wide (viewers of all videos in the Wistia account); only 1,158 visitors played our 2 videos. Building it from events guarantees every fact visitor exists in the dimension (referential integrity).

Why 1,170 fact rows from 1,233 plays: 63 times the same visitor played the same video again on the same day → combined into one row with `play_count` ≥ 2.

Checks (built into `gold_build.py`, the run fails if either fails): `SUM(play_count)` in the fact = 1,233 = events in Silver ✅; fact visitors missing from `dim_visitor` (left_anti join) = 0 ✅.

Insights for the report: most viewers stop within seconds (e.g. 0.6% of a 16-minute video); in-app browsers seen: Facebook, Instagram, **TikTok**, Chrome Webview, UIWebView.

### 5.5 Final scripts (`transform/`)
| Script | Does | Helpers |
|---|---|---|
| `silver_clean.py` | Bronze → Silver (4 tables), prints row counts | `read_bronze()`, `keep_latest(df, keys, order_col)`, `save_silver()` |
| `gold_build.py` | Silver → Gold (4 tables) + 2 reconciliation checks (`RuntimeError` on failure) | `read_silver()`, `save_gold()` |

Glue-ready changes (same in both files):
```python
if os.name == "nt":                                  # Windows laptop only; Glue runs on Linux
    os.environ["PYSPARK_PYTHON"] = sys.executable

BASE_PATH = "data"                                   # laptop default
if "--base_path" in sys.argv:                        # Glue job parameter
    BASE_PATH = sys.argv[sys.argv.index("--base_path") + 1]

spark = SparkSession.builder.appName("wistia_silver_clean").getOrCreate()   # no .master("local[*]")
```
Why split into two scripts: Silver = data quality per source, Gold = business rules; Gold can be re-run alone; each becomes one Glue job, so Step Functions shows exactly which step failed (same as restaurant `rci-silver-clean` / `rci-gold-build`).

Laptop run (verified 2026-10-07):
```
python transform\silver_clean.py   → media 2, events 1233, visitors 101108, daily_stats 1490
python transform\gold_build.py     → dim_media 2, fact_media_daily 1490, dim_visitor 1158, fact_media_engagement 1170
                                     check: plays in fact = 1233, events in silver = 1233
                                     check: fact visitors missing from dim_visitor = 0
                                     gold build finished successfully
```

### 5.6 AWS Glue jobs ✅
**IAM role `wistia-glue-role`** (console): IAM → Roles → Create role → AWS service → **Glue** → attach `AWSGlueServiceRole` → name `wistia-glue-role` → inline policy `wistia-glue-access`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "ReadBronzeAndScripts", "Effect": "Allow",
      "Action": "s3:GetObject",
      "Resource": [
        "arn:aws:s3:::wistia-data-222238779166/bronze/*",
        "arn:aws:s3:::wistia-data-222238779166/scripts/*"
      ] },
    { "Sid": "ReadWriteSilverGold", "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::wistia-data-222238779166/silver/*",
        "arn:aws:s3:::wistia-data-222238779166/gold/*",
        "arn:aws:s3:::wistia-data-222238779166/scripts/temp/*"
      ] },
    { "Sid": "ListBucket", "Effect": "Allow",
      "Action": "s3:ListBucket",
      "Resource": "arn:aws:s3:::wistia-data-222238779166" }
  ]
}
```
(`DeleteObject` is needed because `mode("overwrite")` removes the old Parquet files.)

**Upload scripts:**
```
aws s3 cp transform\silver_clean.py s3://wistia-data-222238779166/scripts/silver_clean.py
aws s3 cp transform\gold_build.py   s3://wistia-data-222238779166/scripts/gold_build.py
aws s3 ls s3://wistia-data-222238779166/scripts/
```

**Create the jobs (CLI, so the console script editor can't overwrite the uploaded script):**
```
aws glue create-job --name wistia-silver-clean --role wistia-glue-role --glue-version 5.0 --worker-type G.1X --number-of-workers 2 --timeout 30 --command Name=glueetl,ScriptLocation=s3://wistia-data-222238779166/scripts/silver_clean.py,PythonVersion=3 --default-arguments '{"--base_path":"s3://wistia-data-222238779166","--TempDir":"s3://wistia-data-222238779166/scripts/temp/","--enable-continuous-cloudwatch-log":"true"}'

aws glue create-job --name wistia-gold-build --role wistia-glue-role --glue-version 5.0 --worker-type G.1X --number-of-workers 2 --timeout 30 --command Name=glueetl,ScriptLocation=s3://wistia-data-222238779166/scripts/gold_build.py,PythonVersion=3 --default-arguments '{"--base_path":"s3://wistia-data-222238779166","--TempDir":"s3://wistia-data-222238779166/scripts/temp/","--enable-continuous-cloudwatch-log":"true"}'
```
Settings: Glue 5.0 (Spark 3.5), 2 × G.1X workers (smallest), 30-min timeout (cost safety), `--base_path` points the scripts at S3, `print` lines go to CloudWatch (run → Output logs).

**Run (manual test, 2026-10-07):** Glue → ETL jobs → job → **Run** → Runs tab → **Succeeded** (~2–4 min each). Silver first, then Gold. Check:
```
aws s3 ls s3://wistia-data-222238779166/silver/    # daily_stats/ events/ media/ visitors/
aws s3 ls s3://wistia-data-222238779166/gold/      # dim_media/ dim_visitor/ fact_media_daily/ fact_media_engagement/
```
Gold **Succeeded** also means both reconciliation checks passed. Counts on S3 can be slightly higher than the laptop's (S3 Bronze has newer data than the local sync).

Redeploy after a script change: edit in VS Code → test on the laptop → `aws s3 cp` to `scripts/` → run the job (Phase 8 automates this).

Scheduling is NOT done per job: in Phase 7 one EventBridge schedule starts Step Functions, which runs Lambda → silver job → gold job → crawler in order, stops on failure and alerts via SNS.

### 5.7 Modeling notes (for the design doc / interviews)
- **Silver** = cleaning per source (types, duplicates, privacy, names); **Gold** = business rules (channel, play_rate, latest location, aggregations) + star schema, always read from Silver.
- **Star schema** (two facts sharing `dim_media` = "galaxy"/fact constellation); `channel` is a column in `dim_media` (a separate `dim_channel` would make it a snowflake). Star vs snowflake = structure; SCD1 vs SCD2 = history inside a dimension; independent choices.
- **SCD applies only to dimensions**, chosen per table by business need ("does any report need the old value?"). Facts never use SCD; they only grow with new rows.
- All dims **SCD1**: media attributes stable; visitor location history is captured per event (country/region on each play), so SCD2 on `dim_visitor` isn't needed. SCD2 would add `effective_from/effective_to/is_current` + surrogate key and a point-in-time join from the fact; possible later from Bronze daily snapshots.
- `fact_media_engagement` is an aggregated fact (grain video × visitor × day, the brief's grain); play-level detail stays in Silver events.
- `mode("overwrite")` = full rebuild each run (idempotent at this volume); `append` would duplicate rows on every run. Facts in real projects: mostly append; late/corrected data via MERGE or partition overwrite.
- Select only business-needed columns in Silver (data minimization); full raw data stays in Bronze.

### Troubleshooting (Phase 5)
| Symptom | Cause | Fix |
|---|---|---|
| Search panel / `Search: {...}` tab instead of files | Search view opened by accident | Close the tab; Explorer = top icon in the left bar (Ctrl+Shift+E) |
| `>>>` prompt in the terminal | Python started without a file name (interactive mode) | `exit()` |
| `ImportError ... Sparksession` / `window` | Wrong capitals | `SparkSession`, `Window` |
| `'SparkSession' object has no attribute 'sparkcontent'` | Typo | `spark.sparkContext.setLogLevel("ERROR")` |
| `w = ...` after `spark.stop()` | Code after stop can't use Spark | `spark.stop()` is always the last line |
| `UNRESOLVED_COLUMN created_at` / `precent_viewed` | Selected the new name before renaming / typo | Use the raw name (`created`) + `.alias(...)`; copy names from the output |
| `url` column shows `{hash, https://..., unlocked}` | Selected the whole struct `share_link` | `F.col("share_link.url")` |
| Red wavy lines under `.when(...)` | Extra `)` closed `withColumn(` early | One `)` per open bracket; click a bracket to see its pair |
| `SyntaxError: ... forgot a comma?` inside `.agg(...)` | Missing comma between aggregations | Comma after each `F.count/avg/sum(...)` |
| Timestamps shifted 5 h (created 2024-06-09 23:56 instead of 06-10 04:56) | Spark shows timestamps in the laptop time zone | `spark.conf.set("spark.sql.session.timeZone", "UTC")` right after creating the session |
| `.show(50)` "top 50 records" | `show(n)` = up to n output rows (for DESCRIBE, n columns) | Comment it as display limit |
| `got an unexpected keyword argument 'trucncate'` | Typo | `truncate=False` |
| `spark.sql(...)` prints nothing | No `.show()` | `spark.sql("...").show()` |
| `createOrReplaceTempview` / `createOrReplaceTemp` error | Capital V / incomplete name | `createOrReplaceTempView("name")` |
| `print(df)` prints `DataFrame[col: type, ...]` | Printing a DataFrame shows its description, not data | `df.show()` / `df.count()` |
| `print.schema()` error | `print` has no `.schema` | `df.printSchema()` |
| `[PARSE_SYNTAX_ERROR] ... near '"'` | 4 quotes `""""` opening a multi-line SQL string | Exactly 3 quotes `"""` |
| `DISCTINCT` syntax error | Typo | `DISTINCT` |
| `TABLE_OR_VIEW_NOT_FOUND daily_stats` / `events_clean` | SQL name not registered (folder names aren't SQL tables) | `df.createOrReplaceTempView("<name>")` before the query |
| Dedupe step wrong table / error | Built on raw `events` instead of `events_clean`, or `filter` inside `withColumn(...)` | `events_clean.withColumn("rn", F.row_number().over(w)).filter(...).drop("rn")` |
| `select(...)` lost columns | `select` keeps **only** listed columns | List unchanged columns too (no alias) |
| `.mode(...)` AttributeError | Missing `.write` | `df.write.mode("overwrite").parquet(...)` |
| `NOT_COLUMN_OR_STR ... got int` | Used `regexp_replace` instead of `regexp_extract` | `F.regexp_extract(col, pattern, 1)` |
| `PATH_NOT_FOUND .../silver/dim_media` | dim_media is a Gold table | `data/gold/dim_media` |
| `PATH_NOT_FOUND .../gold/fact_media_engagement` | Saved under a misspelled folder name | Correct the folder name in the save; delete the wrong folder |
| `data\gold\dim_media` not found | Wrote to `data/gold/media` | Gold folder = model name `dim_media`; Silver folder = source name `media` |
| New scripts not found in `transform\` | Downloaded files landed in the project root / Downloads | `Move-Item <file> transform\` |
| S3 `silver/` / `gold/` empty | Local dev writes to `data/...` on the laptop | Expected until the Glue jobs run |
| Parquet file unreadable in the editor | Binary columnar format | Read it with `spark.read.parquet(...).show()` |

---

## Phase 6 — Athena ✅ (FR10: Gold queryable with SQL)
Goal: register the Gold Parquet folders as tables in the Glue Data Catalog and query them with SQL in Athena. Done 2026-10-08. All objects were created with the AWS CLI (same result as the console; faster, exact, repeatable, and the command is the documentation).

### 6.1 Glue database
```
aws glue create-database --database-input '{"Name":"wistia_gold"}'
```
No output = success. Check: Glue → Data Catalog → Databases → `wistia_gold`. A database is only a container for table definitions (like `CREATE DATABASE`); the data stays in S3.

### 6.2 Crawler
```
aws glue create-crawler --name wistia-gold-crawler --role wistia-glue-role --database-name wistia_gold --targets '{"S3Targets":[{"Path":"s3://wistia-data-222238779166/gold/","Exclusions":["**/_SUCCESS"]}]}' --configuration '{"Version":1.0,"Grouping":{"TableLevelConfiguration":3}}'
aws glue start-crawler --name wistia-gold-crawler
aws glue get-crawler --name wistia-gold-crawler --query "Crawler.State"        # READY = idle (finished, or never started)
aws glue get-tables --database-name wistia_gold --query "TableList[].Name"
```
- Role `wistia-glue-role` (already reads `gold/`); `**/_SUCCESS` excluded (Spark's empty marker files)
- `TableLevelConfiguration: 3` → level 1 bucket, 2 `gold`, 3 table folder → **each folder = one table**
- Result: `dim_media`, `dim_visitor`, `fact_media_daily`, `fact_media_engagement` (columns + types read from Parquet)
- Re-run the crawler only when a table's **columns** change (or a new table is added); new rows in existing folders are visible to Athena immediately. Phase 7 runs it after each Gold build anyway.

### 6.3 Athena workgroup
```
aws athena create-work-group --name wistia-workgroup --configuration '{"ResultConfiguration":{"OutputLocation":"s3://wistia-data-222238779166/athena-results/"},"EnforceWorkGroupConfiguration":true}'
```
Console equivalent: Athena → Administration → Workgroups → Create workgroup → name, query result location, tick **Override client-side settings**. Keeps this project's queries/results separate from `rci-workgroup` etc.

### 6.4 Query in the console
Athena → Query editor → Workgroup **`wistia-workgroup`** (Acknowledge) → Data source `AwsDataCatalog`, Database **`wistia_gold`** → run.

Queries (save in the repo as `sql/athena_queries.sql`):
```sql
-- 1. Row counts (reconciliation with the Glue run / laptop)
SELECT 'dim_media' AS table_name, COUNT(*) AS row_count FROM dim_media
UNION ALL SELECT 'dim_visitor', COUNT(*) FROM dim_visitor
UNION ALL SELECT 'fact_media_daily', COUNT(*) FROM fact_media_daily
UNION ALL SELECT 'fact_media_engagement', COUNT(*) FROM fact_media_engagement;

-- 2. Play rate per channel (fact_media_daily + dim_media)
SELECT m.channel,
       SUM(f.load_count)                                               AS loads,
       SUM(f.play_count)                                               AS plays,
       ROUND(CAST(SUM(f.play_count) AS DOUBLE) / SUM(f.load_count), 4) AS play_rate
FROM fact_media_daily f
JOIN dim_media m ON f.media_id = m.media_id
GROUP BY m.channel;

-- 3. Engagement per channel (fact_media_engagement + dim_media)
SELECT m.channel,
       COUNT(DISTINCT e.visitor_id)             AS viewers,
       SUM(e.play_count)                        AS plays,
       ROUND(AVG(e.watched_percent) * 100, 1)   AS avg_watched_pct,
       ROUND(SUM(e.total_watch_time) / 3600, 1) AS hours_watched
FROM fact_media_engagement e
JOIN dim_media m ON e.media_id = m.media_id
GROUP BY m.channel;
```
`CAST(... AS DOUBLE)`: integer ÷ integer is integer in Athena (947 / 118580 = 0), same as SQL Server.

### 6.5 Results (2026-10-08) and reconciliation
| Check | Result |
|---|---|
| Row counts | dim_media 2 · dim_visitor 1,158 · fact_media_daily 1,490 · fact_media_engagement 1,170 = **identical to the laptop** (no new ingestion since the local sync) |
| `SELECT * FROM dim_media` | 2 rows, timestamps in UTC; 0.50 KB scanned, < 0.5 s |

| Channel | Loads | Plays (daily) | Play rate | Viewers | Plays (events) | Avg watched | Hours watched |
|---|---|---|---|---|---|---|---|
| YouTube (9k4tbcdfg0) | 915 | 320 | **35.0%** | 280 | 325 | **25.8%** | **51.2** |
| Facebook (8hunphufxp) | 118,459 | 944 | **0.8%** | 880 | 908 | **14.1%** | 35.8 |

- **Engagement reconciles with Wistia's own dashboard:** Wistia engagement 25.6% (YouTube) / 14.4% (Facebook) vs pipeline 25.8% / 14.1%, built independently from raw events.
- Plays: daily sum 944 / 320 vs Wistia lifetime 947 / 328 → daily stats stop at yesterday (complete days only); lifetime includes today.
- Wistia's dashboard play rate (42.2% YouTube) uses Wistia's own formula; ours is defined as plays ÷ loads (documented).
- 280 + 880 = 1,160 viewers vs 1,158 in dim_visitor → **2 visitors watched both videos** (consistent).

**Business story:** Facebook reaches ~130× more screens (loads) and 3× more viewers, but < 1% press play and viewers watch ~14% (~2.3 min of a 16-min video) — typical autoplay-in-feed behaviour. YouTube reaches few people, but 1 in 3 watch and they watch ~26% (~9.5 min of a 37-min video) → YouTube produced **more total watch time** (51 h vs 36 h) with one third of the plays.

### Troubleshooting (Phase 6)
| Symptom | Cause | Fix |
|---|---|---|
| Crawler state `READY` but no tables | New crawlers are also `READY` (never started) | `aws glue start-crawler ...`, wait 1–2 min, check `get-tables` |
| Athena asks for a query result location | Primary workgroup selected | Switch the Workgroup dropdown to `wistia-workgroup` |
| Ratio shows 0 | Integer ÷ integer | `CAST(... AS DOUBLE)` |
| Create command "already exists" | Object created twice (CLI + console) | Use one way only; it already exists |

## Phase 7 — Orchestration, alerts, daily schedule ✅ (7-day run Oct 8 → Oct 14, FR8)
```
EventBridge Scheduler  wistia-pipeline-daily   cron(0 6 * * ? *) America/Chicago (= 11:00 UTC)
   → Step Functions    wistia-pipeline
        Ingest          Lambda wistia-ingest          (lambda:invoke; retries Lambda.ServiceException/TooManyRequests ×2)
        SilverClean     Glue wistia-silver-clean      (glue:startJobRun.sync = waits for the job)
        GoldBuild       Glue wistia-gold-build        (.sync; reconciliation checks fail the job)
        RefreshCatalog  crawler wistia-gold-crawler   (aws-sdk:glue:startCrawler)
        Catch States.ALL on every step → NotifyFailure (SNS email) → PipelineFailed (Fail state)
```
Alerts are **failure-only** (no email on success; success emails considered and declined for now). Daily check: Step Functions → `wistia-pipeline` → Executions → today's row **Succeeded**; the Executions list is the FR8 evidence.

### 7.1 SNS topic + email
```
aws sns create-topic --name wistia-pipeline-alerts
aws sns subscribe --topic-arn arn:aws:sns:us-east-1:222238779166:wistia-pipeline-alerts --protocol email --notification-endpoint <email>
aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:us-east-1:222238779166:wistia-pipeline-alerts --query "Subscriptions[].SubscriptionArn"
```
Safe confirmation: right-click "Confirm subscription" → copy link → SNS → Subscriptions → Pending → **Confirm subscription** → paste (makes unsubscribe require login; Gmail's link scanner once unsubscribed the healthcare alerts). 2026-10-07: link was clicked directly instead (works, unprotected) → re-check the subscription still exists during the 7-day run.

### 7.2 IAM role `wistia-sfn-role`
IAM → Create role → AWS service → **Step Functions** (AWS auto-adds `AWSLambdaRole`; optional to remove later) → name `wistia-sfn-role` → inline policy `wistia-sfn-access`:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    { "Sid": "RunIngestLambda", "Effect": "Allow", "Action": "lambda:InvokeFunction",
      "Resource": "arn:aws:lambda:us-east-1:222238779166:function:wistia-ingest*" },
    { "Sid": "RunGlueJobs", "Effect": "Allow",
      "Action": ["glue:StartJobRun", "glue:GetJobRun", "glue:GetJobRuns", "glue:BatchStopJobRun"],
      "Resource": ["arn:aws:glue:us-east-1:222238779166:job/wistia-silver-clean",
                   "arn:aws:glue:us-east-1:222238779166:job/wistia-gold-build"] },
    { "Sid": "RunCrawler", "Effect": "Allow", "Action": ["glue:StartCrawler", "glue:GetCrawler"],
      "Resource": "arn:aws:glue:us-east-1:222238779166:crawler/wistia-gold-crawler" },
    { "Sid": "SendAlerts", "Effect": "Allow", "Action": "sns:Publish",
      "Resource": "arn:aws:sns:us-east-1:222238779166:wistia-pipeline-alerts" }
  ]
}
```

### 7.3 State machine `wistia-pipeline`
Definition in the repo: `orchestration/state_machine.json` (validate: `python -m json.tool orchestration\state_machine.json > $null; if ($?) { "JSON OK" }`).
```
mkdir orchestration
New-Item orchestration\state_machine.json        # paste the definition, Ctrl+S
aws stepfunctions create-state-machine --name wistia-pipeline --role-arn arn:aws:iam::222238779166:role/wistia-sfn-role --definition file://orchestration/state_machine.json
# after editing the JSON later:
aws stepfunctions update-state-machine --state-machine-arn arn:aws:states:us-east-1:222238779166:stateMachine:wistia-pipeline --definition file://orchestration/state_machine.json
```
Failure message: `States.Format('Wistia pipeline failed. Execution: {}. Error: {}', $$.Execution.Id, States.JsonToString($.error))`, subject "Wistia pipeline FAILED".

### 7.4 Tests (2026-10-07)
| Run | Result |
|---|---|
| 1 | **SilverClean FAILED** → NotifyFailure → PipelineFailed (alert path proven). Cause: `wistia-glue-role` not allowed `s3:PutObject` on `silver_$folder$` (Spark/EMRFS folder-marker object at the bucket root). Fix: add `arn:aws:s3:::wistia-data-222238779166/*_$folder$` to the `ReadWriteSilverGold` resources of `wistia-glue-access` |
| 2 | **ExecutionSucceeded** in 2 min 55 s (all 4 steps green) |

### 7.5 Schedule
EventBridge → Scheduler → Schedules → Create: name `wistia-pipeline-daily`, Recurring, Cron `cron(0 6 * * ? *)`, time zone **America/Chicago**, flexible window Off, target **Step Functions StartExecution** → `wistia-pipeline`, input `{}`, permissions **Create new role** (`Amazon_EventBridge_Scheduler_SFN_...`). 6:00 Central = 11:00 UTC → "yesterday (UTC)" is a complete day for daily stats.

### Troubleshooting (Phase 7)
| Symptom | Cause | Fix |
|---|---|---|
| `NotFound ... Topic does not exist` on subscribe | Topic not created yet | `aws sns create-topic ...` first |
| ARN typed alone → "not recognized as a cmdlet" | An ARN is a value, not a command | Use it inside the full command after `--topic-arn` |
| `>>` prompt in PowerShell | Two commands pasted together / unfinished line | Ctrl+C, run one command per line |
| Glue job AccessDenied on `silver_$folder$` / `gold_$folder$` | Spark writes folder-marker objects next to the folder | Allow `.../*_$folder$` in the Glue role policy |

## Phase 8 — CI/CD (GitHub Actions + OIDC) ✅ (FR9)
Repo: **github.com/vidyasagar8686/wistia-video-analytics-pipeline** (private), branch `main`. First run 2026-10-07: CI ✅ + CD ✅ (1 min 10 s), OIDC login worked first time.

Workflow from now on: **edit in VS Code → `git add -A` → `git commit -m "..."` → `git push` → GitHub runs CI, then deploys to AWS.** No more manual zip uploads or `aws s3 cp` for code.

### 8.1 `.gitignore` + first commit
`.gitignore` contents: `.venv/ __pycache__/ *.pyc build/ *.zip data/ samples/ spark-warehouse/ metastore_db/ derby.log backfill_log.txt *.log .env *.pem .vscode/ .DS_Store Thumbs.db` (written with a PowerShell here-string: `@' ... '@ | Set-Content .gitignore`).
```
git init -b main
git status --short          # expect only: .gitignore ingestion/ orchestration/ requirements.txt transform/
Select-String -Path ingestion\*.py, transform\*.py, orchestration\*.json -Pattern "[A-Za-z0-9]{64}"   # no token in code → no output
git add -A
git commit -m "Wistia pipeline: Lambda ingestion, PySpark Silver/Gold, Step Functions orchestration"
```
`LF will be replaced by CRLF` warnings = harmless (Windows line endings).

### 8.2 GitHub repo + push
github.com/new → name `wistia-video-analytics-pipeline`, **Private**, **Add README OFF**, no .gitignore, no license (an empty repo; a GitHub-created README would make the first push be rejected). Then:
```
git remote add origin https://github.com/vidyasagar8686/wistia-video-analytics-pipeline.git
git push -u origin main
```

### 8.3 AWS OIDC deploy role `wistia-github-deploy-role`
The GitHub OIDC provider `token.actions.githubusercontent.com` already existed (restaurant project) → reused (`aws iam list-open-id-connect-providers`). Files in the repo: `iam/wistia-github-trust.json`, `iam/wistia-github-deploy-policy.json`.
- Trust: `sts:AssumeRoleWithWebIdentity` from the OIDC provider; `aud = sts.amazonaws.com`; `sub` StringLike `repo:vidyasagar8686*/wistia-video-analytics-pipeline*:ref:refs/heads/main` (the `*` accepts GitHub's newer sub format with numeric IDs, e.g. `vidyasagar8686@174058076/...@1405858555`, seen in the restaurant project)
- Permissions (inline `wistia-github-deploy-access`): `lambda:UpdateFunctionCode/GetFunction/GetFunctionConfiguration` on `wistia-ingest`; `s3:PutObject` on `scripts/silver_clean.py` and `scripts/gold_build.py` only; `states:UpdateStateMachine/DescribeStateMachine` on `wistia-pipeline`
```
aws iam create-role --role-name wistia-github-deploy-role --assume-role-policy-document file://iam/wistia-github-trust.json
aws iam put-role-policy --role-name wistia-github-deploy-role --policy-name wistia-github-deploy-access --policy-document file://iam/wistia-github-deploy-policy.json
aws iam get-role-policy --role-name wistia-github-deploy-role --policy-name wistia-github-deploy-access --query "PolicyDocument.Statement[].Sid"
```

### 8.4 Workflow `.github/workflows/ci-cd.yml`
Triggers: push to `main`, pull requests to `main`, manual `workflow_dispatch`.
| Job | When | Steps |
|---|---|---|
| **CI - checks** | every push / PR | checkout@v5 → setup-python@v6 (3.12) → `py_compile` ingestion + transform scripts → `json.tool` on `orchestration/*.json` and `iam/*.json` |
| **CD - deploy to AWS** | `needs: ci`, only `main`, not PRs; `permissions: id-token: write` | configure-aws-credentials@v5 (role-to-assume = deploy role) → build Lambda zip on Linux (`pip install requests -t build`, copy `wistia_client.py`, zip) → `aws lambda update-function-code` + `aws lambda wait function-updated` → `aws s3 cp` both Glue scripts → `aws stepfunctions update-state-machine --definition file://orchestration/state_machine.json` |

YAML basics: `key: value`; indentation (spaces) = "belongs to"; `-` = list item; `|` = multi-line commands; `uses:` = ready-made Marketplace step, `run:` = your own commands (the same CLI commands as the manual routine).

Annotations on run #1 (no action needed): Node.js 20 deprecation notice for `configure-aws-credentials@v5` (already forced to Node 24); `ubuntu-latest` moves to Ubuntu 26 from 2026-10-19.

### Troubleshooting (Phase 8)
| Symptom | Cause | Fix |
|---|---|---|
| `.gitignore` "not recognized as a cmdlet" | File name typed alone | `New-Item .gitignore` |
| File stays empty after pasting | Content pasted but not saved / pasted elsewhere | Paste in the open editor + Ctrl+S (no white dot on the tab), or `@' ... '@ \| Set-Content <file>` in the terminal |
| `Access to the path 'C:\Windows\System32\...' is denied` | Command run in a separate PowerShell window, not the VS Code terminal | `exit` that window; use the VS Code terminal (`(.venv) PS C:\projects\...>`) |
| A full-screen editor with File/Edit/View/Help opens in the terminal | Text starting with `edit` typed as a command (Microsoft Edit) | Ctrl+Q (don't save) |
| `Move-Item ... Downloads\ci-cd.yml` not found | Download landed in the project root | `Move-Item ci-cd.yml .github\workflows\` |
| Push rejected "remote contains work you do not have" | Repo created on GitHub with a README | Create repos empty (README off), or `git pull --rebase` first |

## Phase 9 — Documentation ✅ (FR12)
Files added to the repo (2026-10-08):
| File | Contents |
|---|---|
| `README.md` | Overview, architecture diagram (Mermaid, renders on GitHub), repo layout, data model ERD, row counts, results, how to run / query / develop / deploy |
| `docs/design_doc.md` | Requirements mapping (FR1–FR12), decisions & trade-offs, Silver/Gold transformations, data quality & reconciliation, assumptions/SME questions, security, cost, insights, future improvements |
| `docs/runbook.md` | This runbook (copy) |
| `docs/demo_script.md` | 8 scenes for the demo video + likely questions |
| `sql/athena_queries.sql` | 9 queries: row counts, play rate per channel, engagement per channel, monthly trend, top regions, platform mix, 3 data-quality checks |

Add them to the repo:
```
git add -A
git commit -m "Add README, design doc, runbook, demo script and Athena queries"
git push
```
(CI/CD runs; docs-only changes redeploy the same code, harmless.)

Optional (FR11): dashboard (Streamlit or QuickSight) on the Athena queries — not built.

---

## Operate
| Task | How |
|---|---|
| Daily check (7-day run Oct 8–14) | Step Functions → `wistia-pipeline` → **Executions** → today's row **Succeeded**; screenshot the list at the end as FR8 evidence |
| Failure email received | Open the execution → red step → error message; Lambda logs `/aws/lambda/wistia-ingest`, Glue job run → Output/Error logs |
| Re-run a day | Fix the cause → Start execution again (safe: watermarks only moved on success; Silver/Gold rebuilt fully) |
| Run one step only | Lambda → Test; Glue → job → Run; `aws glue start-crawler --name wistia-gold-crawler` |
| Check data freshness | Athena query 9 in `sql/athena_queries.sql` (latest day = yesterday UTC) |
| Check watermarks | `aws dynamodb scan --table-name wistia_watermark` |
| Check the alert subscription | `aws sns list-subscriptions-by-topic --topic-arn arn:aws:sns:us-east-1:222238779166:wistia-pipeline-alerts` |
| Change code | Edit in VS Code → test locally → commit → push (CI/CD deploys) |

### Common problems
| Symptom | Likely cause | Fix |
|---|---|---|
| Ingest step failed | Wistia API down / token changed (401) | Check Lambda logs; update the secret value in Secrets Manager if the token changed; re-run |
| Silver/Gold AccessDenied | Glue role missing an S3 path | Add the path to `wistia-glue-access` (see §7.4 `_$folder$`) |
| Gold failed "Reconciliation failed" | Data issue between Silver and Gold | Check the job log message; query Silver vs Gold counts |
| RefreshCatalog failed `CrawlerRunningException` | Crawler still running from a manual start | Wait and re-run; harmless for the data |
| No alert email on failure | Subscription removed (link scanner) | Re-subscribe and confirm via the console |

## Pause or teardown
**Pause (keep everything, stop daily runs/cost):** EventBridge → Scheduler → Schedules → `wistia-pipeline-daily` → **Disable**. (Re-enable to resume.)

**Teardown (after the project is accepted), in this order:**
```
# 1. Stop the schedule and orchestration
#    Console: EventBridge → Scheduler → wistia-pipeline-daily → Delete
aws stepfunctions delete-state-machine --state-machine-arn arn:aws:states:us-east-1:222238779166:stateMachine:wistia-pipeline
aws sns delete-topic --topic-arn arn:aws:sns:us-east-1:222238779166:wistia-pipeline-alerts

# 2. Query layer
aws athena delete-work-group --work-group wistia-workgroup --recursive-delete-option
aws glue delete-crawler --name wistia-gold-crawler
aws glue delete-database --name wistia_gold

# 3. Compute
aws glue delete-job --job-name wistia-silver-clean
aws glue delete-job --job-name wistia-gold-build
aws lambda delete-function --function-name wistia-ingest

# 4. State and secrets
aws dynamodb delete-table --table-name wistia_watermark
aws secretsmanager delete-secret --secret-id wistia/api-token --recovery-window-in-days 7

# 5. Data (irreversible!)
aws s3 rm s3://wistia-data-222238779166 --recursive
aws s3 rb s3://wistia-data-222238779166

# 6. IAM roles (console: IAM → Roles → delete): wistia-lambda-role, wistia-glue-role,
#    wistia-sfn-role, Amazon_EventBridge_Scheduler_SFN_..., wistia-github-deploy-role
#    Keep the GitHub OIDC provider (shared with the restaurant project).
```
Before step 5: download anything you want to keep (e.g. Gold Parquet, Athena results, screenshots).
