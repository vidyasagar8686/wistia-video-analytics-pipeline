# Correctness changes and release migration

These source changes are not evidence of deployment or live AWS validation.
The older design document and runbook describe the original training deployment;
this guide takes precedence for the changed paths and behavior.

## What changed

- A page cap raises an extraction failure and emits `extraction_incomplete` in
  logs. No watermark is advanced for the incomplete source batch. Existing Step
  Functions failure handling stops downstream jobs and sends SNS when deployed.
- Watermarks use strongly consistent reads and conditional writes against the
  previously read value. A competing writer causes failure instead of overwriting
  progress. Equal timestamps are replayed; Silver deduplicates event keys.
- Daily response envelopes store request video ID and completion metadata separately
  from the unchanged API payload. Legacy filename attribution is allowed only for
  configured video IDs. Missing/conflicting IDs fail the Silver job.
- Silver writes `silver_versions/<run_id>/`; Gold reads that exact run. Required
  columns, event keys/timestamps/fractions, video attribution and daily nonnegative
  values are checked. This is a selected contract, not a complete schema registry.
- IPv4 uses /24 networks and IPv6 uses /48 networks. Invalid IPs become null.
- Gold validates before writing and verifies staged row counts. It then creates a
  new Glue database with all four external tables and conditionally publishes
  `releases/current.json`. Existing release locations are not overwritten.
- CI runs unit and synthetic Spark integration tests. Glue bookmarks are not used:
  each run fully rebuilds an isolated version, appropriate for this volume.

## Migration checklist

1. Pause the schedule and wait for active runs. Keep the existing Gold database
   available until a new release is tested. Back up configurations and watermarks.
2. Review/install `infra/hardening.json` with existing role/function/bucket names.
   It adds runtime permissions and alarms; it is not full infrastructure as code.
   Do not claim that all pre-existing resources or reader grants are covered.
3. Create `privacy/suppressed_visitors.json` in the bucket with
   `{"visitor_keys": []}` after checking whether any actual requests must be loaded.
   The new AWS Silver job fails if the registry is missing or unreadable.
4. Update the GitHub deploy policy to include scripts/pipeline_support.py. Upload
   that module with both Glue scripts. The state machine passes --extra-py-files
   and one run ID to both jobs; existing --base_path job defaults must stay set.
   Verify that the Gold runtime's boto3 S3 PutObject model supports IfMatch and
   IfNoneMatch. Upgrade its job dependency if needed before the first publication.
5. Disable the old Gold crawler schedule/trigger. The new Gold job registers its
   versioned tables directly. Do not point a crawler or old reporting database at
   the parent gold_versions prefix: it contains multiple releases.
6. Start a new Step Functions execution with a unique name containing only letters,
   numbers, hyphens or underscores. Resolve the manifest and query all four tables
   in its database. Verify counts, joins, cutoff, masked fields and assumptions.
7. Migrate Athena saved queries and readers to resolve the manifest ONCE and use
   that database throughout each report. Existing wistia_gold queries do not
   automatically switch. Restrict catalog/data access to approved release versions;
   candidate databases left by a failed publish are not reporting releases.
8. Test an intentional cap hit, a conditional conflict and a failed validation in
   a nonproduction setup. Verify the SNS subscription and alarm delivery.
9. Set repository variable WISTIA_RELEASE_DEPLOY_ENABLED=true only after migration
   is approved. CD is gated to avoid deploying incompatible path/permission changes
   automatically. No changes in this branch modify running AWS services by themselves.

## Recovery and limitations

A capped run has no completion checkpoint for that source. Re-run its original
watermark with a sufficient WISTIA_MAX_PAGES setting in a managed backfill process,
then restore the normal bound. Do not merely set the watermark to the newest page.
Offset pagination is mutable; a larger cap and deduplication do not solve arbitrary
late arrival or historical edits. Confirm source ordering guarantees and add an
overlap/reconciliation policy before promising lossless capture. Current code checks
observed ordering and replays equal watermark timestamps.

A failed publication leaves the prior current.json unchanged; a failed candidate
may leave versioned files/catalog objects. Remove them under an approved retention
policy, not by changing a valid release's table locations. Read the pointer before
building and compare its ETag on publish; a concurrent change requires a new run.
An execution start timestamp also prevents an older execution from publishing
after a newer execution has already become current.
Local CLI publication assumes a single writer. In AWS, also reserve ingestion
concurrency to one; conditional watermark writes remain the last line of defense.

Registry changes and erasure require the paused maintenance procedure in
privacy/README.md. Retention and end-to-end erasure are not automatically complete.
See docs/metrics.md for the unresolved stakeholder metric agreement.

## Suggested initial operational targets

These are proposed starting thresholds, not historical SLAs or measured costs:

| Signal | Starting threshold | Response |
|---|---|---|
| Incomplete extraction/cap hit | At least 1 in 5 minutes | SNS, stop downstream, investigate/backfill |
| Failed state machine | At least 1 in 5 minutes | SNS, inspect execution and logs |
| Execution duration | More than 10 minutes | Investigate page count, retries and Glue startup |
| Last successful execution | No success in a full 24-hour UTC period | Investigate schedule and freshness |
| Athena scan and monthly AWS cost | Owner-approved budget, still to be set | Review before asserting a spend SLA |

The alarms in infra/hardening.json must be deployed and tested. The success alarm
is a coarse schedule monitor; actual data freshness also needs date-window checks.
