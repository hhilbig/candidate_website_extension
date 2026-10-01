# Plan — Mayoral source-discovery tranche

**Date:** 2026-09-14
**Status:** launched; source tranche remains in progress

## Context

Compare candidate URL leads before and after another bounded pass through previously identified source pages.
The corrected baseline contains 1,093 candidate-years with URL leads among 2,788 active cases.
Hanno approved at most 5,000 additional source attempts and 4,000,000,000 new raw response bytes.

## Recommended approach

Restore the original source evidence and the corrected ledger into a fresh directory on the existing droplet data volume.
Preserve both archives and prior outputs.
Implement persistent attempt and byte limits, including reservations for concurrent work and interrupted requests.
Forward those limits through the supervisor and retain at least 8 GB of free space.
Start the source pass with corrected ownership rules, bounded retries, and project-specific Slack messages.
Verify the first durable result before handing off the long run.

## Critical files

- `scripts/expand_mayoral_saved_sources.py`: persistent tranche limits.
- `scripts/run_mayoral_saved_source_job.py`: launch arguments and notifications.
- `quality_reports/mayoral_ownership_repair_2026-09-10/final/archive_manifest.json`: authoritative restore manifest.
- `quality_reports/mayoral_source_restart_2026-09-14/`: verification and launch records.

## Reused existing functions

Reuse the source parser, corrected ownership attribution, SQLite attempt ledger, and existing Slack helper.
One agent implements limits while the coordinator restores evidence and updates the supervisor.

## Verification

Verify archive hashes, restored database integrity and table counts, code hashes, and saved evidence paths.
Test restart limits, concurrent reservations, interrupted requests, and supervisor argument propagation.
Run the existing source tests and a bounded live smoke within the same approved tranche.
Verify process commands, watcher state, and a durable completed source attempt.

## Non-goals

No paid searches, new storage, automatic human approvals, or repeat campaign-snapshot scrape.
No deletion of restored or archived content.
