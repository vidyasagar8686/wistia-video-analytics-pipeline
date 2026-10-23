# Raw data access retention and erasure

This repository does not establish legal compliance or verify the deployed AWS
access graph. IPv4 and IPv6 minimization is implemented downstream; visitor
identifiers and coarse locations remain personal data.

## Before enabling production

The data owner must approve a raw-data retention period, the purposes for keeping
visitor-level history, authorized reader roles, and the time allowed for erasure.
Do not choose a retention value solely to make a test pass. Restrict Athena users
to released Gold databases and outputs; grant Bronze access only to ingestion,
cleaning and designated privacy operators. Audit bucket/access-point policies,
identity policies, Lake Formation grants, assumed roles and account administrators.
Encryption and Block Public Access do not replace this audit.

## Erasure procedure

1. Verify the requester and identify their Wistia visitor keys through an approved
   process. Do not log email addresses, raw IPs or identity payloads.
2. Pause the schedule and ingestion. Wait for all active Lambda and Glue runs and
   reporting queries to finish. Restrict readers during the maintenance window.
3. Add the keys to `privacy/suppressed_visitors.json` in the data bucket as
   `{"visitor_keys": ["key1"]}`. This restricted registry must be retained for as
   long as source reimports could restore erased records. The Silver job requires
   this registry and excludes these keys before writing events and visitors.
4. Find and rewrite Bronze event/visitor JSON containing those keys, preserving
   unrelated records. Delete the old objects and **all affected noncurrent object
   versions** as approved. Account for replication, backups, exports, Athena
   results, local development copies and the source Wistia account. A delete
   marker alone is insufficient. Legal holds require an explicit owner decision.
5. Withdraw affected released databases and their pointers from readers. Remove
   affected legacy and versioned Silver/Gold copies, including noncurrent versions,
   then rebuild a new release with the suppression registry. A metadata-only
   crawler refresh does not erase data. Remove obsolete catalog tables/databases
   and verify no retained table points to an affected copy.
6. Test for the keys in raw records and every retained/rebuilt visitor-level table,
   using a restricted operator role. Inspect the backup/replication inventories.
   Record completion evidence without storing the erased identity in normal logs.
7. Resume reporting and the scheduler only after verification. Test restoration
   from backups using the suppression registry before allowing restored data back
   into reports.

This is an operator procedure, not an automated end-to-end erasure service. The
repository does not claim that deletion in external systems/backups is automated.

## Retention configuration

`retention-rule.example.json` is a parameterized S3 Lifecycle rule. Replace the
placeholder days with the owner's approved values and merge it into existing
rules; do not overwrite unrelated policies. It expires current and noncurrent
Bronze objects and aborts incomplete multipart uploads. Lifecycle expiry is
asynchronous and does not satisfy an immediate erasure request by itself.
Set separate retention rules for derived releases and query results after deciding
which published history consumers need. Time-limited Bronze retention reduces
the history available for full rebuilds; archive/rebuild requirements must be
resolved before turning expiry on.
