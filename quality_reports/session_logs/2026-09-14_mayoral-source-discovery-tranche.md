# Session log — 2026-09-14

**Session type:** implementation
**Plan reference:** `quality_reports/plans/2026-09-14_mayoral-source-discovery-tranche.md`

## What I did

Implemented the approved source tranche with one agent for persistent limit controls.
The coordinator restored the source evidence, updated the supervisor, and deployed the run on the droplet.
All restored files and portable dependencies reside on the existing data volume.
No source evidence was deleted, and no system configuration was changed.

## What I verified

Both archive hashes matched their saved manifests.
The database passed integrity and expected-count checks.
All 40 manifest-listed file hashes and 29,020 referenced content-file hashes passed.
All 113 relevant tests passed locally and remotely, including actual OCR.
The initial source response passed its hash check and has parsed and text outputs.
The worker and supervisor run with the approved limits and two workers.
The start notification reached the existing helper; Slack delivery is not independently confirmed.

## Decisions

Use a persistent tranche ID to prevent restart from resetting the allowance.
Reserve bytes before dispatch and keep conservative charges after uncertain errors.
Count decoded response body bytes; this is not a cap on TCP or header traffic.
Use two workers because the droplet has less memory than mac2.
Keep at least 8 GB of free space and retain both archives and the original restored code.

## Open threads

The source tranche remains in progress.
Review marginal candidate-year yield after it stops; do not treat parser proposals as human approvals.
Another tranche needs approval.
See `quality_reports/mayoral_source_restart_2026-09-14/report.md` for paths and restart instructions.

## Memory updates

Updated `docs/mayoral_url_discovery.md` with the active run location and limits.
Preserved a concurrent task's new GPT-5.6 review section without edits.
No change to `MEMORY.md` or `KNOWLEDGE_BASE.md`.
