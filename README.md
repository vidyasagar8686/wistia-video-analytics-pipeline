# Wistia Video Analytics Pipeline

A small daily AWS ETL project: Wistia API → S3 Bronze → Glue PySpark Silver →
Gold star schema → Athena. The two videos have assumed Facebook/YouTube labels;
the API does not establish referral channel attribution.

## Review changes

This branch adds source fixes and tests. It does not establish deployment or
production compliance. Read [release migration](docs/release_migration.md) before
enabling CD: output paths, query database selection and runtime permissions change.

| Concern | Source change | Remaining boundary |
|---|---|---|
| Filename attribution | Explicit request metadata; validated legacy fallback; reject conflicts | Video IDs remain a configured two-video list |
| Concurrent progress | Consistent reads and compare-and-swap watermarks; unique raw filenames | No claim of lossless capture under arbitrary source edits or late arrival |
| Partial extraction | Cap exhaustion fails, logs and blocks progress for that batch | Operator-managed backfill retries from the prior watermark |
| Publication | Validate, stage immutable outputs, register one versioned database, conditionally switch release manifest | Readers must pin the manifest database; old saved queries require migration |
| Privacy | IPv4/IPv6 network masking and suppressed-visitor registry | Retention approval, reader audit and operational erasure still required |
| Testing and operations | Behavioral/Spark tests; selected schema contracts; additive IAM/alarm template | Full infrastructure provisioning and live alarm tests remain |
| Metric agreement | Versioned project definitions and matching-window SQL | Wistia UI formula and stakeholder sign-off are unresolved |

## Execution flow

EventBridge starts Step Functions at 6 AM America/Chicago. Ingestion reads five
endpoints, saves raw responses and advances completed-source watermarks
conditionally. A cap, source failure or conflict prevents downstream processing
through the workflow's SNS failure path.

Silver writes an isolated `silver_versions/<run_id>/` snapshot. Gold reads that
same version, checks reconciliation and references, writes
`gold_versions/<run_id>/`, verifies row counts and registers its four tables.
A conditional write to `releases/current.json` makes that database the release.
The old crawler is removed from the new workflow.

Athena consumers resolve the manifest once per report and query its database.
The legacy `wistia_gold` database does not switch automatically. Failed candidate
databases are not releases and should not be exposed as reporting datasets.

## Data model

| Table | Grain | Metrics or attributes |
|---|---|---|
| dim_media | Video | Current metadata and assumed channel |
| dim_visitor | Visitor in selected-video events | Latest location, coarse IP network and browser attributes |
| fact_media_daily | Video and UTC day | Loads, plays, hours watched, project play rate |
| fact_media_engagement | Video, visitor and UTC day | Play count, average watched fraction, estimated watch seconds |

Both dimensions use current attributes. Historical location analysis should use
event detail; current video duration is used to estimate watch seconds.

## Tests

Use Python 3.11 and Java 11 for the synthetic Spark tests:

```text
pip install boto3 requests pyspark==3.5.3
python -m unittest discover -s tests -v
```

Set RUN_SPARK_TESTS=1 to include real Spark subprocesses. Unit tests use mocked
AWS clients; Spark tests use synthetic local files, not production credentials.
CI sets this variable and runs the full suite. All runtime JSON is syntax-checked.
Tests cover capped paging, equal timestamps, failures, stale watermark writes,
IP masking, registry validation and conditional catalog publication. The Spark
test covers deduplication, suppressed visitors, envelope/legacy attribution,
successful publication and keeping the prior release after invalid attribution.

## Deployment and open decisions

[Release migration](docs/release_migration.md) describes the required permission,
catalog, registry and query changes. Deployment is disabled until repository
variable WISTIA_RELEASE_DEPLOY_ENABLED is explicitly set to true. The code still
uses existing Lambda/Glue resources; [the additive template](infra/hardening.json)
is not a complete recreation of the environment.

- [Privacy lifecycle procedure](privacy/README.md)
- [Metric definitions and reconciliation](docs/metrics.md)
- [Metric reconciliation SQL](sql/metric_reconciliation.sql)
- [Original design record](docs/design_doc.md)
- [Original build runbook](docs/runbook.md)

