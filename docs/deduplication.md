# Business keys and duplicate conflict handling

Silver rejects invalid keys before grouping, then deduplicates the canonical
analytical columns that it will publish. File paths, load dates, ingestion IDs,
discarded identity fields and other raw metadata do not choose an event winner.
The previous SHA-256 tie-breaker has been removed.

## Identity and ordering contracts

| Source | Identity | Ordering | Same identity with different payloads |
|---|---|---|---|
| Media | API hashed_id, renamed media_id | API updated timestamp | Keep the greatest updated value; conflicting payloads tied there fail |
| Events | API event_key; visitor_key and media_id also required | No authoritative event revision field available | Conflicting canonical payloads fail, even if collected on different days |
| Visitors | API visitor_key | API last_active_at | Keep the snapshot with greatest activity time; conflicting payloads tied there fail |
| Daily statistics | Request media_id plus API date | Ingestion collected_at; load_date midnight for legacy files | Keep the latest observed snapshot; conflicting payloads tied there fail |

Wistia documents event and visitor keys as unique identifiers in its current
[event reference](https://docs.wistia.com/reference/get_stats-events-eventkey) and
[visitor reference](https://docs.wistia.com/reference/get_stats-visitors-visitorkey).
This project ingests /v1 in a single account. Those current /modern references
support the identity choice; they do not prove that every legacy row contains a
valid key or guarantee revision/ordering behavior. If accounts are combined,
namespace these identities by account rather than assuming cross-account scope.

The pipeline enforces key presence rather than assuming it. String identifiers
must be strings, nonempty, non-null and without outer whitespace. A missing key
is not reconstructed from a name, IP address, timestamp or unrelated field.
Daily dates are converted to dates and must be non-null. Missing required key
columns fail the contract. A batch containing one invalid key is rejected; a
valid duplicate elsewhere does not hide it. Opaque IDs are not trimmed or renamed.

## Canonical payload and exact copies

Canonical payloads contain the selected Silver fields after timestamp conversion,
field minimization, IP masking and empty-text normalization. Exact copies of all
these fields collapse to one record. Identical metrics with different valid keys
remain separate events. Differences only in discarded fields do not change the
published analytical record. Hashes are not used as identity or conflict authority.

The Spark helper uses [dropDuplicates](https://spark.apache.org/docs/3.5.6/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicates.html)
on all canonical fields, then selects the maximum ordering timestamp where an
ordering policy exists. If multiple distinct payloads still share one identity,
DataContractError fails Silver. Error messages contain source/field names, not
visitor IDs, emails, IP addresses or offending row payloads.

## What the ordering rules mean

An event's received_at describes event time, not a documented update version.
Later collection alone does not authorize changing its visitor, video or watched
fraction. Conflicts stop publication for investigation. Legitimate changes to a
viewing session may therefore block a batch until a source-backed correction
policy is established; they are never silently resolved by a hash.

Visitor last_active_at orders observed activity snapshots. It is not claimed to
version every browser or identity edit. Older profile snapshots are superseded;
equal latest activity timestamps with conflicting retained fields fail. Media
uses its source updated field rather than arrival date.

Daily collected_at is observation time, not a source revision guarantee. Legacy
files have only collection-day precision, so conflicts at that resolution fail.
Recollecting a row can update its observed metrics; this does not detect an edit
that ingestion never requests. Late-arrival discovery and scheduled historical
reconciliation remain separate work.

## Failure and recovery

Invalid keys, missing/invalid ordering timestamps, and unresolved conflicts fail
the source stage. No record-level quarantine is added by this change. Existing
versioned output and workflow sequencing keep a failed candidate from replacing
the released database. A failed stage can leave partial candidate Silver files;
retry under a new run ID after correcting the source/contract, without overwriting
an accepted release or moving watermarks merely to silence an error.

Investigate a conflict through restricted access to its raw records and verify
the source's intended identity/version semantics. Do not delete audit history or
invent a winner automatically. A correction registry or a trusted source revision
field would require a separate, explicit policy and tests.

## Verification

tests/test_deduplication.py runs the production helper functions on real Spark
DataFrames. It covers exact copies, distinct keys, null/blank/padded/missing/type-
invalid keys, event identity collisions, changed event metrics, ordered media and
profile snapshots, tied conflicts, invalid ordering, composite date keys and
redacted error messages. The full pipeline fixture also verifies that a null
visitor profile key or conflicting event payload fails Silver while retaining
the prior release pointer.
