# Project status and release checklist

Updated October 4, 2026. This file controls the order of remaining work.
The full history to October 4 is in [archive/status_history_2026-10-04.md](archive/status_history_2026-10-04.md).
The persistent execution rules are in [CLAUDE.md](../CLAUDE.md).

## Target

Hanno set the target on October 4, 2026:

1. Finish the mayoral website text collection with the highest possible coverage.
2. Keep the 2026 House/Senate collection running to its endpoint. Only watch it; do not develop it further.

| Track | State | Finish condition |
|---|---|---|
| 1. Historical House/Senate correction | Complete. Public version 3.0 | None |
| 2. Extracted variables | Complete. Harvard Dataverse published version 4.0 on October 2, 2026 (`doi:10.7910/DVN/BZ2JRS`) | None. Open items below are for a later version |
| 3. Mayoral extension | Moved to `mayoral_campaign_websites` on October 24, 2026. Dataverse `doi:10.7910/DVN/IN9WCA` submitted for curator review | Tracked in that repository |
| 4. 2026 House/Senate collection | Collecting each day. Endpoint November 17, 2026 | Collection ends, then final outputs and coverage checks |

## Resume here

**October 24:** the mayoral work moved with its git history to `~/Documents/GitHub/mayoral_campaign_websites` (private GitHub repository `hhilbig/mayoral_campaign_websites`). Its status, including the full mayoral history from this file, is in that repository's `docs/status.md`. The release rebuilt there byte-identical to the deposited files. This repository keeps copies of the six shared files (`src/extract_text.py`, `src/utils.py`, `scripts/icpsr_replicate_coding.py`, `scripts/stage_dataverse_release.py`, `scripts/slack_status.sh`, `config/config.yaml`).

**Progress, October 24 (approximate):** tracks 1 and 2 complete; track 3 is tracked in the mayoral repository (curator review pending). Track 4 (2026 collection) runs to November 17, then final outputs and coverage checks (about 1 agent day). Largest uncertainty: the agent time for the 2026 final outputs.

## 4. 2026 House/Senate collection

The droplet cron `~/run_collect_2026.sh` runs at 09:00 America/Los_Angeles. It is the only scheduler for this collection.
The schedule: a live copy every 30 days, plus the local day before and after each applicable primary or runoff.
Rosters and URLs refresh each week. The rules are in [collection_2026.md](collection_2026.md).
Hanno set the endpoint to November 17, 2026 on September 30 (`collection_end_date` in `config/collection_2026.json`).

Status on October 4, 2026 (`logs/status_20261004.json` on the droplet): 57,904 pages, 22,357 snapshots, 3,689 candidates.
There are 1,964 pending work items and 0 unfinished attempts. The volume has 61 GiB free of 100 GiB.
The daily log `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime/logs/daily.log` shows `rc=0` for October 1 to 4.
These runs came after the September 30 fix for the `Invalid IPv6 URL` stop.

**Strategy review, October 24 (Opus reviewer, major issues only; Hanno decided the same day):**
1. The general election had no targeted copies, so about 95 percent of pairable candidates would have had no live copy after November 3 (next 30-day copy due November 20–24). **Decision: add copies on November 2 and 4** (`live_cadence_due` in `src/collection_2026_outputs.py`; test `test_general_election_gets_copies_on_the_day_before_and_after`).
2. After the end date only `--phase archive` may run, with no roster refresh (`scripts/collect_2026.py`; test `test_after_end_date_only_the_archive_phase_runs`). **Decision: one final archive pass in early December** for the post-general Wayback window (November 4 – December 3). Not scheduled yet; run it by hand, after December 3.
3. The identity check needs the FEC first name, so nickname sites fail (about 206 of 410 rejected homepages name the surname; e.g. Tom Kean, Jerry Nadler). No data loss: unconfirmed sites are copied daily. **Decision: score identity again on stored text after November 17** and report the change before use. No collector change now.
Not issues: the retryable backlog (5,645) is mostly one record per day for about 218 always-failing sites; disk (55 GiB free, about 0.35 GiB a day); time zones for the 50 states. The daily run stops at its 6-hour budget (`--max-run-seconds 21600` in `~/run_collect_2026.sh`) before the archive phase reaches all candidates.
Extra runs (Hanno, October 24): on November 2 and 4 cron also starts the wrapper at 00:05 Pacific (run until 08:20), and the 09:00 run gets 12 hours (ends 05:00 UTC, Eastern midnight); the Monday refresh of November 2 moves to Sunday, November 1. The collector decides what is due at run start, so runs must not cross local midnight. Droplet wrapper `~/run_collect_2026.sh` (tracked copy `scripts/run_collect_2026_droplet.sh`; backups `~/run_collect_2026.sh.bak-20261024`, `~/crontab.bak-20261024`).
Deployment: code for 1 and 2 is deployed to the droplet runtime after the October 24 run ends (backups `*.bak-20261024`); check the November 2 and 4 runs for live copies.

**Action:** check the daily `rc` line and the free space once a week. Act only if a run fails. On November 3 and 5, confirm that the November 2 and 4 runs made live copies for most candidates with a confirmed site.
After November 17, build the final outputs and run the coverage checks; then the identity re-scoring; then the December archive pass.

## Deferred: Wayback scraping skill audit, October 4, 2026

A check of `scripts/scrape_mayoral_wayback_batch.py` against the `wayback-scraping` skill found these gaps. None blocks a release requirement now.

- The fetch journal is deleted at each compaction, rows carry no attempt ID, and no attempt count is kept for each page. On a restart, failed pages are retried in queue order, not least-attempted first.
- The backoff has no jitter. The CDX query has no explicit guard for a result that reaches the 50,000-row limit (the largest so far is 7,005).
- Agent workflows that query the archive through mac2 share no rate limit with each other or with the scraper. Run such workflows at low concurrency and not alongside a scrape.

## 2. Extracted variables: open items for a later version

Version 4.0 is published. These items do not block anything now.

- **Decided October 24 (Hanno): drop the wrong-candidate rows in the next version.** A text check of v4.0 shows only `H0AL02160-2020` (Thomas William Brown Jr., Alabama; 803 page rows of Anthony Brown's Maryland site `anthonybrown.com`) is wrong. The Maryland rows are correct, and both `kateforcongress.com` rows are correct (Kate Schroder, Ohio 2020; Kate Monroe, California 2024; each page names its candidate). Drop only `H0AL02160-2020` from the core files; the extraction tables already exclude it.
- The packager computes `manifest_entries` but does not write them.
- In 17 of 60 sampled endorsement records, the evidence span crosses two list entries.

## Repository housekeeping, October 4, 2026

- All uncommitted work from August 22 to September 27 is committed in groups (commits `5a4b876` to `3e32254`).
  This includes the 2026 collector modules `src/collection_2026_calendar.py` and `src/collection_2026_roster.py`, which `scripts/collect_2026.py` imports.
- 192 old build folders were copied to mac2, `~/backups/candidate_website_extension/build_archive_20261004/` (6,334 files, 3.0 GB, checksum-verified).
  Tests read the small folders, so those were restored locally. Five large folders are now on mac2 only:
  `build/dataverse_v4_0_draft_download`, `build/release_v2_1_candidate`, and in `build/hive_candidate_extraction/` the folders
  `full_segmented_imported`, `v2_2_full_hive_output_20260923` and `v2_1_full_hive_output_20260916p`.
  Only archived tests read `full_segmented_imported`.
- 57 finished endorsement development files moved to `scripts/archive/` and `tests/archive/`. pytest skips `archive/` and `scratch/`.
- Root scratch files moved to the ignored folder `scratch/`. The August 23 validation workbooks moved to `validation/extraction_validation_2026-08-23/`.
- Full test suite on October 4: 1,737 passed. Five tests in `tests/test_merge_municipal_temporal_validation.py` fail under the current pandas string dtype; this failure is older and known.
- `data/deliverable` compressed October 24 (Hanno): `data/deliverable_2026-08-14.tar.gz` (1.1 GB; all four files verified by SHA-256 inside the archive), folder removed. Scripts that read `data/deliverable/` need `tar -xzf` first.

## Progress estimate, October 4, 2026 (approximate)

About 70 percent of the release work is complete. Weights are the remaining steps per track:
Track 1 is 20 of 20, Track 2 is 25 of 25, Track 3 is about 20 of 40, and Track 4 is about 8 of 15.
Remaining agent time is about 15 to 25 hours, almost all on Track 3. Hanno needs less than one hour, for mayoral release decisions and option 3.
The wall-clock floor is November 17 for Track 4, plus one to two days of exports.
The largest uncertainty: how many old URL leads pass the Hive screen and the agent check, which decides the 2010–2021 coverage.

## Rules for the next agent

Follow the target and the steps above. Reuse completed work. Continue after the required checks pass.
Additional work must address a demonstrated blocker to a listed release requirement.
After two unsuccessful repair attempts on the same blocker, report the evidence and the decision needed.
Open blockers with failed repair attempts: none.
