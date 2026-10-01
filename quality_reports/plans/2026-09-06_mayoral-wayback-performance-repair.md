# Plan: Mayoral Wayback performance repair

**Date:** 2026-09-06
**Author:** Codex session
**Status:** done

## Context

The full acquisition queue contains 291,044 archived captures.
The running scraper slows as its attempt table grows.
It also exceeds the memory available on the 2 GB droplet.

This task compares the current scraper with a repaired scraper on the same frozen captures.
The outcome is completed capture fetches per hour with identical status and output records.

## Recommended approach

Keep the append-only journal as the durable record during the run.
Track current rows by capture identifier without copying the full table after each fetch.
Stream pending queue rows instead of storing all pending row dictionaries.
Compact the current-state file at controlled intervals and at clean exit.

Resize the droplet to 4 GB RAM and 2 virtual CPUs.
Use a CPU-and-RAM-only resize so the existing disk remains reversible.

## Critical files

- `scripts/scrape_mayoral_wayback_batch.py` — replace the per-fetch table copy and stream pending queue rows.
- `tests/test_scrape_mayoral_wayback_batch.py` — test resume behavior, replacement precedence, and bounded memory structures.
- `/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory/` — preserve and verify the production checkpoint.

## Reused existing functions

- `_append_csv_row()` — keep each completed attempt durable before state changes.
- `_read_fetch_checkpoint()` — recover the base checkpoint and active journal.
- `_compact_fetch_checkpoint()` — write the canonical current state at a checkpoint.
- `_bounded_fetch_outcomes()` — retain bounded network concurrency.

## Verification

Run the focused test file and the related queue-builder tests.
Run the full offline test suite if the focused tests pass.
Benchmark identical frozen captures with one, two, and four workers.

Before restart, reconcile queue keys with base and journal keys.
After restart, verify new durable rows, status counts, memory use, and recent throughput.

## Non-goals

This task does not change the 291,044-capture acquisition scope.
It does not change replay URLs, retry classifications, text extraction, or stored-byte rules.
It does not change the separate Arm A crawler.
