# Demo Video Script — Wistia Video Analytics Pipeline

Target length: 8–10 minutes. Record the screen with your voice. Have these tabs open before you start: GitHub repo, AWS console (Step Functions, S3, Glue, Athena), VS Code.

## Scene 1 — The problem (30 s)
**Show:** README top section.
**Say:** "The marketing team publishes two videos through Wistia, one on Facebook and one on YouTube. They want to compare the channels every day: how many people see the video, how many press play, how long they watch, and where they come from. I built a daily automated pipeline on AWS that answers these questions with SQL."

## Scene 2 — Architecture (1 min)
**Show:** the architecture diagram in the README.
**Say:** "Every morning at 6 AM Central, EventBridge Scheduler starts a Step Functions state machine. A Lambda function pulls only new data from the Wistia API into S3 Bronze. Two Glue PySpark jobs clean it into Silver and build a star schema in Gold. A crawler refreshes the Athena tables. If any step fails, I get an email. Code changes are deployed by GitHub Actions using OIDC, so no AWS keys are stored anywhere."

## Scene 3 — Ingestion (1.5 min)
**Show:** `ingestion/wistia_client.py` in VS Code (`call_wistia`, `get_all_events`, `run`), then DynamoDB `wistia_watermark`, then S3 `bronze/`.
**Say:**
- "The token is read from Secrets Manager."
- "`call_wistia` retries network errors and server errors with backoff, but not 401/404."
- "Visitors and events are paginated. Watermarks in DynamoDB make it incremental: each run only fetches what is new."
- "Data is saved to Bronze first, and only then the watermark moves, so a failure never loses data."
- "Bronze keeps the raw JSON, partitioned by load date, in UTC."

## Scene 4 — Transformation (2 min)
**Show:** `transform/silver_clean.py`, then `transform/gold_build.py`, then S3 `silver/` and `gold/`.
**Say:**
- "Silver cleans each source: correct types, duplicates removed with ROW_NUMBER, IP addresses masked, personal data dropped."
- "One surprise: the daily stats endpoint doesn't return the video ID, so I derive it from the file name."
- "Gold applies business rules: channel, play rate, each visitor's latest location, and aggregates plays per video, visitor and day."
- "Gold ends with two automatic checks: total plays must equal the events in Silver, and every visitor in the fact must exist in the dimension. If a check fails, the job fails and I get an alert."

## Scene 5 — Orchestration and the 7-day run (1.5 min)
**Show:** Step Functions graph, then the Executions list (7 green runs), then EventBridge schedule, then SNS topic.
**Say:** "Here is the daily state machine. Every step has a catch that sends an email and marks the run as failed. During testing, a permission issue made the Silver job fail, and the alert email arrived, so the failure path is proven. Here are seven consecutive successful daily runs."

## Scene 6 — Results in Athena (1.5 min)
**Show:** Athena, run queries 1, 2 and 3 from `sql/athena_queries.sql`.
**Say:**
- "Row counts match the Glue run."
- "Facebook: about 118 thousand loads but under 1% press play. YouTube: few loads but 35% play rate."
- "Facebook viewers watch 14% on average, YouTube viewers 26%. My numbers match Wistia's own engagement figures, 25.6 and 14.4 percent, which validates the pipeline."
- "YouTube produced more total watch time with a third of the plays."

## Scene 7 — CI/CD (45 s)
**Show:** GitHub Actions run (CI + CD green), `.github/workflows/ci-cd.yml`, `iam/wistia-github-trust.json`.
**Say:** "On every push, CI checks the code and JSON files. On main, CD logs in to AWS with a short-lived OIDC role, which only this repo's main branch can use, and deploys the Lambda, the Glue scripts and the state machine."

## Scene 8 — Wrap-up (30 s)
**Say:** "Key decisions and assumptions are in the design document, for example the channel mapping, IP masking and the play-rate definition, since no SME was assigned. Next steps would be Terraform for the infrastructure, incremental Gold loads at larger scale, and a dashboard. Thank you."

## Likely questions
| Question | Short answer |
|---|---|
| Why Lambda for ingestion? | Small daily volume (seconds); the 70-minute full backfill ran once locally because Lambda's limit is 15 minutes |
| Why two fact tables? | Loads exist only per video per day; plays and watch time per visitor. Different grains |
| Why SCD1? | Attributes are stable; per-play location is kept at event level; Bronze keeps daily snapshots if SCD2 is ever needed |
| What if a run fails mid-way? | Watermarks only move after data is saved; Gold is rebuilt fully; re-running is safe (idempotent) |
| How is personal data handled? | IP masked, identity fields and exact location dropped in Silver; raw only in Bronze |
| Why Athena, not Redshift? | No load step, pay per query, tiny volume; Redshift Serverless is the alternative |
