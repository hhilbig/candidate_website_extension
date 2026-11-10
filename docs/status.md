# Project status and release checklist

Updated November 4, 2026. This file controls the order of remaining work.
The full history to October 4 is in [archive/status_history_2026-10-04.md](archive/status_history_2026-10-04.md).
The persistent execution rules are in [CLAUDE.md](../CLAUDE.md).

## Target

Hanno set the target on October 4, 2026:

1. Keep the 2026 House/Senate collection running to its endpoint (November 5, 2026; changed on November 4). Only watch it; do not develop it further.

The mayoral collection (the first target on October 4) moved to its own repository on October 24: `~/Documents/GitHub/mayoral_campaign_websites` (private GitHub `hhilbig/mayoral_campaign_websites`). Mayoral status, data, and notes are there. This repository holds House and Senate work only.

| Track | State | Finish condition |
|---|---|---|
| 1. Historical House/Senate correction | Complete. Public version 3.0 | None |
| 2. Extracted variables | Complete. Harvard Dataverse published version 4.0 on October 2, 2026 (`doi:10.7910/DVN/BZ2JRS`) | None. Open items below are for a later version |
| 4. 2026 House/Senate collection | Collecting each day. Endpoint November 5, 2026 | Collection ends, then final outputs and coverage checks |

## Resume here

**October 24:** the mayoral work moved to `~/Documents/GitHub/mayoral_campaign_websites`. Do not do mayoral work here. This repository keeps copies of six shared files (`src/extract_text.py`, `src/utils.py`, `scripts/icpsr_replicate_coding.py`, `scripts/stage_dataverse_release.py`, `scripts/slack_status.sh`, `config/config.yaml`).

**Progress, October 24 (approximate):** tracks 1 and 2 complete. Track 3 (mayoral) left this repository. Track 4 (2026 collection) runs to November 17, then final outputs and coverage checks (about 1 agent day). Largest uncertainty: the agent time for the 2026 final outputs.

**Progress, November 4 (approximate):** about 90 percent of the release work is complete (track 1 and track 2 about 33 points each, track 4 about 23 of 33). Remaining: about 1 agent day and about 1 hour of Hanno's time; wall clock to after December 3 (collection to November 5, then the archive pass). Largest uncertainty: the agent time for the 2026 final outputs. All remaining work waits on a machine or a date; no decision is open.

**End date moved, November 4 (Hanno):** collection ends on November 5, not November 17. `collection_end_date` is `2026-11-05` in `config/collection_2026.json` and in the droplet runtime (backup `collection_2026.json.bak-20261104`). The last run starts at 09:00 Pacific on November 5 and ends by about 23:00 UTC. The collector stops `run` after that date by UTC date, but the wrapper still runs the Monday `refresh`, so the 09:00 cron line now fires only through November 5 (backup `~/crontab.bak-20261104`). Next action: on November 6, confirm the last `rc` line and that no collector runs.

**November 4 check (droplet, 20:00 UTC):** at this check the end date was still November 17. All runs since October 15 show `rc=0`; 52 GiB free. `logs/status_20261104.json` (16:38 UTC): 185,581 pages, 85,843 snapshots, 3,695 candidates, 0 unfinished attempts. General election, `exports/candidate_event_pairs.csv`, 90 events, 3,651 candidate rows: 1,159 paired, 66 pre_only, 2 post_only, 2,424 uncovered. This export is from before the 09:00 Pacific run of November 4, which runs to about 05:00 UTC on November 5. Next check: November 5, after that run.

**November 9 check (droplet, 00:35 UTC November 10): the collection ended.** The last run finished on November 5 at 23:09 UTC with `rc=0`. No collector runs, and the 09:00 cron line stops after November 5. All runs from October 17 to November 5 show `rc=0`; 52 GiB free. `logs/status_20261105.json`: 188,989 pages (103,599 usable), 87,724 snapshots, 3,695 candidates, 0 unfinished attempts. General election, `exports/candidate_event_pairs.parquet`, 90 events, 3,651 candidate rows (2,460 with a website): 1,171 paired (all live copies), 54 pre_only, 3 post_only, 2,423 uncovered. Post copy missing: 1,191 no website, 728 failed requests, 425 identity unresolved, 131 unusable content. Next action: build the final outputs and run the coverage checks; then the identity re-scoring; then the archive pass after December 3.

**Progress, November 9 (approximate):** about 92 percent of the release work is complete (track 1 and track 2 about 33 points each, track 4 about 26 of 33). Remaining: about 1 agent day and about 1 hour of Hanno's time; wall clock to after December 3 (the archive pass). Largest uncertainty: the agent time for the 2026 final outputs. No decision is open.

**Identity re-scoring, November 9 (report only; no data changed):** `scripts/rescore_identity_2026.py` read the final exports and the stored text on the droplet (folder `candidate_2026/release_prep_20261109/`). Results: `quality_reports/identity_rescore_2026-11-09/`. 636 candidates have a homepage that failed the identity check; 396 of them never passed it. Verdict for each candidate: 187 `given_variant` (family name plus a nickname or short form; the sample is correct), 47 `host_surname` (family name in text and in the host; mostly correct, but the sample has one reused domain), 34 `surname_only` (mixed; the sample has one placeholder site), 368 `no_surname` (parked or lapsed sites, and also real sites that use only the first name). 654 candidate-event rows lack a pair only because of the identity check (424 for the general election). Maximum new pairs: 256 with `given_variant` (152 general); 323 with `given_variant` plus `host_surname` (198 general). **Decided and applied, November 9 (Hanno): nickname rule only.** `identity_match` in `src/collection_2026.py` now also accepts a listed nickname or the first three or more letters of a given name; an ordinary word (will, don, bill) counts only directly before the family name (test `test_identity_accepts_short_forms_of_the_given_name`; 158 tests pass). The rule is in the droplet runtime (backup `src/collection_2026.py.bak-20261109`), so the December archive pass uses it. `rescore_identity_2026.py --apply` changed 12,787 stored homepage rows of 195 candidates to `usable` (field `identity_rule=short_form_rescore_20261109`; list in `release_prep_20261109/identity_rescore_applied_pages.csv`). Database backup: `data/backups/collection_before_identity_rescore_20261109.sqlite` (byte-identical before the change); old exports in `release_prep_20261109/exports_before_rescore/`. New export (`logs/status_20261109_identity_rescore.json`, `rc=0`): pairs 2,286 to 2,533; general election 1,171 to 1,315 paired, 68 pre_only, 3 post_only, 2,265 uncovered; candidates with a usable homepage 1,586 to 1,750. Limit: the collector did not follow links from an unresolved homepage, so these recovered copies have the homepage only.

**Release of the 2026 data (Hanno, November 9): add it to the existing Dataverse dataset as a new version.** No separate dataset. Decisions of November 9:
- Publish all dated copies, live and Wayback, so that users can follow a website over time. The 2026 rows go into `raw_corpus.parquet` and `panel_snapshot_day.parquet`; the pair table is one new file.
- Add a concise rights statement for the live copies.
- Make all derived files for 2026: the ICPSR-compatible measures, the topic shares, and the extracted issues, biography, and endorsements (Hive, same model family and contracts as version 4.0).
- `on_ballot` for 2026: the general-election candidates are known now. The published flag uses the MIT returns (House `10.7910/DVN/IG0UN2`, Senate `10.7910/DVN/PEJ5QU`), which do not have 2026 yet. Open: the source for 2026.

Steps, in order: (1) map the 2026 page corpus to the `raw_corpus.parquet` columns and validate; (2) universe, provenance, crosswalk, and `on_ballot` rows; (3) snapshot-day panel, ICPSR-compatible measures, topic shares; (4) Hive extraction (2026 has about 18,500 distinct usable texts on issue, biography, and endorsement pages, and 6,200 homepages; the version 4.0 run had 23,871 parent documents); (5) README, codebook, rights statement; (6) archive pass after December 3, then refresh steps 1 to 5; (7) first-user review, then publication approval.

**Step 1 complete, November 9 (on the data before the December archive pass).** `scripts/build_2026_release_corpus.py` reads a local copy of the final exports and text (`data/collection_2026/final_20261109/`, not tracked) and writes `build/release_v5_0_2026/raw_corpus_2026.parquet`: 105,839 rows, 1,750 candidates, 70,503 live and 35,336 Wayback rows. The first 28 columns equal the published `raw_corpus.parquet` in name, order, and type. Three new columns: `capture_source` (`live` or `wayback`), `snapshot_id`, `page_id` (the pair table refers to the last two). Choices: only `usable` pages; 10,547 Wayback rows removed because they repeat one archived capture (same candidate, time, URL, text), except rows that a pair uses; `snap_url` is missing for live rows; `bonica_rid` comes from an earlier year of the same FEC ID (missing for 73,630 rows); `n_snap` counts site copies. Checks passed: `page_id` unique, no empty text, `n_char` equals the text length, each text matches its stored hash, all dates from January 7 to November 5, 2026, and all page IDs of the 2,533 pairs are in the file. Known: `on_ballot`, `stage`, `data_source`, `candidate_year_stage` are missing until step 2; one Guam candidate has no normalized district; one candidate (`H8TX02166`) has 1,174 homepage rows because the Wayback Machine holds 735 captures of one day. Next action: step 2, first the source for the 2026 general-election candidate lists.

## 4. 2026 House/Senate collection

The droplet cron `~/run_collect_2026.sh` runs at 09:00 America/Los_Angeles. It is the only scheduler for this collection.
The schedule: a live copy every 30 days, plus the local day before and after each applicable primary or runoff.
Rosters and URLs refresh each week. The rules are in [collection_2026.md](collection_2026.md).
Hanno set the endpoint to November 17, 2026 on September 30, then to November 5, 2026 on November 4 (`collection_end_date` in `config/collection_2026.json`).

Status on October 4, 2026 (`logs/status_20261004.json` on the droplet): 57,904 pages, 22,357 snapshots, 3,689 candidates.
There are 1,964 pending work items and 0 unfinished attempts. The volume has 61 GiB free of 100 GiB.
The daily log `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime/logs/daily.log` shows `rc=0` for October 1 to 4.
These runs came after the September 30 fix for the `Invalid IPv6 URL` stop.

**Strategy review, October 24 (Opus reviewer, major issues only; Hanno decided the same day):**
1. The general election had no targeted copies, so about 95 percent of pairable candidates would have had no live copy after November 3 (next 30-day copy due November 20–24). **Decision: add copies on November 2 and 4** (`live_cadence_due` in `src/collection_2026_outputs.py`; test `test_general_election_gets_copies_on_the_day_before_and_after`).
2. After the end date only `--phase archive` may run, with no roster refresh (`scripts/collect_2026.py`; test `test_after_end_date_only_the_archive_phase_runs`). **Decision: one final archive pass in early December** for the post-general Wayback window (November 4 – December 3). Not scheduled yet; run it by hand, after December 3.
3. The identity check needs the FEC first name, so nickname sites fail (about 206 of 410 rejected homepages name the surname; e.g. Tom Kean, Jerry Nadler). No data loss: unconfirmed sites are copied daily. **Decision: score identity again on stored text after the collection ends** and report the change before use. No collector change now.
Not issues: the retryable backlog (5,645) is mostly one record per day for about 218 always-failing sites; disk (55 GiB free, about 0.35 GiB a day); time zones for the 50 states. The daily run stops at its 6-hour budget (`--max-run-seconds 21600` in `~/run_collect_2026.sh`) before the archive phase reaches all candidates.
Extra runs (Hanno, October 24): on November 2 and 4 cron also starts the wrapper at 00:05 Pacific (run until 08:20), and the 09:00 run gets 12 hours (ends 05:00 UTC, Eastern midnight); the Monday refresh of November 2 moves to Sunday, November 1. The collector decides what is due at run start, so runs must not cross local midnight. Droplet wrapper `~/run_collect_2026.sh` (tracked copy `scripts/run_collect_2026_droplet.sh`; backups `~/run_collect_2026.sh.bak-20261024`, `~/crontab.bak-20261024`).
Deployment: code for 1 and 2 is deployed to the droplet runtime after the October 24 run ends (backups `*.bak-20261024`); check the November 2 and 4 runs for live copies.

**Action:** check the daily `rc` line and the free space once a week. Act only if a run fails. On November 6, confirm that the November 4 run made live copies for most candidates with a confirmed site.
After November 5, build the final outputs and run the coverage checks; then the identity re-scoring; then the December archive pass.

## 2. Extracted variables: open items for a later version

Version 4.0 is published. These items do not block anything now.

- **Decided October 24 (Hanno): drop the wrong-candidate rows in the next version.** A text check of v4.0 shows only `H0AL02160-2020` (Thomas William Brown Jr., Alabama; 803 page rows of Anthony Brown's Maryland site `anthonybrown.com`) is wrong. The Maryland rows are correct, and both `kateforcongress.com` rows are correct (Kate Schroder, Ohio 2020; Kate Monroe, California 2024; each page names its candidate). Drop only `H0AL02160-2020` from the core files; the extraction tables already exclude it.
- The packager computes `manifest_entries` but does not write them.
- In 17 of 60 sampled endorsement records, the evidence span crosses two list entries.
- **Human validation by an RA (Hanno approved October 30).** The v4.0 extraction has only the model-coded check; `human_validation.json` in the bundle is `pending`. The RA codes an Excel workbook. Precision sheets show one released record in a short passage, with the evidence marked. The coder does not see the model's kind, field, or entity type. Recall sheets split passages into sentences or lines, and the coder sees no model output. Builder: `scripts/build_ra_validation_workbook.py` (release `release_20260930_v22_model`, seed 20261030). Pilot output: `validation/ra_validation_pilot_2026-10-30/` (10 rows per precision sheet, 2 passages per recall sheet; the answer key stays with Hanno). October 30: a test reader (an agent in the role of an undergraduate) coded the pilot. The guide now takes its rules from the extraction prompt (`PROMPTS_V2_1`) and the September 27 review questions. Biography Q1 has a new option for a non-biographical fact. Two rules are not in the prompt, and I set them: proposal means a specific plan, and a fact that fits two types gets one type plus a note. October 31: guide shortened to about 440 words. Hanno decided that the RA's coding is the ground truth (no second coder). Next: the RA codes the pilot; then build the full workbook and a scoring script. Then build the full workbook (proposed: 150 issues, 100 biography, 100 endorsements, about 40 passages per recall sheet) and a scoring script.

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

## Rules for the next agent

Follow the target and the steps above. Reuse completed work. Continue after the required checks pass.
Additional work must address a demonstrated blocker to a listed release requirement.
After two unsuccessful repair attempts on the same blocker, report the evidence and the decision needed.
Open blockers with failed repair attempts: none.
