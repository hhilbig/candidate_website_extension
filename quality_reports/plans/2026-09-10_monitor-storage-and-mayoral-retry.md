# Plan: Monitor cleanup, storage recovery, and mayoral retry

**Date:** 2026-09-10
**Author:** Codex session
**Status:** done

## Context

The House scrape finished in June, but an old daily monitor still reports it as stopped.
The droplet root disk has 1.5 GB free because system journals and project files use most space.
The mayoral fetch completed all 291,044 queue rows, but 39 rows have retryable request errors.

This task removes the false House alert, restores safe root-disk space, and retries those 39 capture rows.

## Recommended approach

Back up the current cron table and remove only the obsolete House status entry.
Reduce old system journal files to a bounded size without changing journal settings.
Back up the mayoral checkpoint and audit on the attached volume.
Run the existing fetch command against the same queue and checkpoint.
The resume logic skips terminal rows and retries only rows with retryable status.

## Critical files

- Droplet cron table: remove the obsolete `daily_status.sh` entry.
- `/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory/fetch_attempts.csv`: preserve and update the current capture state.
- `/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory/fetch_audit.json`: verify the final queue and status counts.
- `quality_reports/session_logs/2026-09-10_monitor-storage-and-mayoral-retry.md`: record the verified result.

## Reused existing functions

- `run_fetch()` in `scripts/scrape_mayoral_wayback_batch.py`: skips terminal capture keys and retries explicit errors.
- `_read_fetch_checkpoint()`: reads the current checkpoint and any active journal.
- `_compact_fetch_checkpoint()`: writes one current row per capture key after the retry.

## Verification

Confirm that the cron table retains unrelated entries and excludes `daily_status.sh`.
Measure root and attached-volume space before and after journal cleanup.
Verify the checkpoint backup with SHA-256 hashes and row counts.
Run the focused scraper tests before the live retry.
Require 291,044 unique capture keys, no missing or unexpected keys, and no duplicate keys after the retry.
Report every remaining retryable status.

## Non-goals

This task does not restart the completed House queue.
It does not restart the separate German crawl.
It does not delete project data or change the frozen mayoral queue.
