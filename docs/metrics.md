# Metric agreement and reconciliation

The project metric definition is `wistia_project_v1`, recorded in each release
manifest. Rates are fractions, not percentages: multiply by 100 for display.

| Metric | Project definition | Limits |
|---|---|---|
| Play rate | Daily endpoint plays / loads; 0 when loads are 0 | Name it project play rate; do not call it Wistia's displayed rate |
| Average watched fraction | Event percent_viewed averaged per play | Across visitor-day facts, weight by play_count |
| Estimated event watch seconds | SUM(percent_viewed * current video duration) | Assumes duration is suitable for those historical plays |
| Daily hours watched | hours_watched from the daily endpoint | Keep separate from the event-derived estimate |
| Lifetime totals | Snapshot returned by the lifetime endpoint | Snapshot time differs from completed daily windows |

Daily requests end at the previous UTC day. The events endpoint is not restricted
to that cutoff during ingestion, so reporting must filter events to the same
window. Never explain a discrepancy as a cutoff issue without checking it.
Video-to-channel mappings remain assumptions about the two videos, not observed
referral channels.

## Resolving differences with a stakeholder

1. Choose a single video and identical inclusive start/exclusive end dates, with
   UTC stated explicitly. Capture the Wistia screenshot/export and retrieval time.
2. Run `sql/metric_reconciliation.sql` against one released database. Record its
   run ID and the exact numerator and denominator, not just rounded percentages.
3. Verify which Wistia endpoint/UI definition is being compared, its date/timezone
   filtering, unique-versus-total counting, exclusions and ingestion completeness.
   The repository does not contain a confirmed derivation of Wistia's UI formula.
4. Attribute a difference only when evidence supports the explanation. Keep daily
   endpoint totals, event totals and lifetime snapshots in separate columns.
5. Ask the marketing owner to approve one definition for decisions and its display
   label. Until agreement, show both labeled values and the unresolved difference.
   Do not set a tolerance retroactively to make the values look reconciled.

| Comparison record | Value to capture |
|---|---|
| Video and release | Video ID, release run ID, retrieval time |
| Window | Start, exclusive end, UTC and Wistia display timezone |
| Project evidence | Loads, plays, event plays, watch formula, unrounded result |
| Wistia evidence | UI/export value, formula if confirmed, snapshot/window |
| Difference | Absolute difference, relative difference where denominator is nonzero |
| Resolution | Verified cause or unresolved, agreed label, owner and decision date |

The historical 25.8%/14.1% versus 25.6%/14.4% comparison is a useful cross-check,
not evidence that definitions have been fully aligned. Stakeholder sign-off and
source UI evidence are still needed.
