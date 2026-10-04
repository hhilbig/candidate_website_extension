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
| 3. Mayoral extension | Population closed. Website text collection in progress | Collection levers exhausted, coverage documented, deposit approved and published |
| 4. 2026 House/Senate collection | Collecting each day. Endpoint November 17, 2026 | Collection ends, then final outputs and coverage checks |

## Resume here

**The legacy-lead scrape is complete (October 4, 16:18 PDT).** It scraped 231 finalist x domain targets for 215 finalists who have an old URL lead but no downloaded text.
Folder on mac2: `~/candidate_website_extension_stage3/data/municipal_archive_acquisition/mayoral_stage3L_20261004/`, log `runs.log`.
`runs/production/fetch_audit.json` shows stage `fetch_complete` and exact queue-key agreement.
Result: 6,094 of 6,308 pages fetched (219 of them with a timestamp mismatch), 179 of 231 targets with pages, 497 MB raw HTML, 22.3 million text characters.
Not fetched: 136 not available, 69 no content, 7 not HTML, 2 retryable request errors.
Frozen targets in the repository: `data/municipal_archive_acquisition/mayoral_stage3L_20261004/`.

Next steps. Hanno approved all of them on October 4.

1. Done: the fetch audit is complete (see above).
2. On mac2, run `scripts/summarize_mayoral_stage3_pages.py --targets <batch>/candidate_site_targets.csv --run-dir <batch>/runs/production --output <batch>/stage3_saved_pages.csv`.
   Copy the result to `data/municipal_discovery/top100_coverage/inputs/stage3L_saved_pages.csv` with its manifest.
   Add it to `stage3_legacy_pages` in `config/mayoral_top_two_build.json`.
3. On mac2, run `scripts/build_mayoral_legacy_site_items.py`.
   Then submit the Hive job `scripts/hive/run_mayoral_site_attribution.sbatch` with that items file (INPUT and OUTPUT environment variables).
4. Let agents check the Hive "yes" items, as in workflow `wf_5110088a-dd2`.
   Write verdicts keyed by `target_id` with a status. Register them as `legacy_site_attribution`.
5. Rebuild with `scripts/build_mayoral_top_two_coverage.py`, run `tests/test_build_mayoral_top_two_coverage.py`, commit, and report coverage.

After step 5, continue with the next coverage lever in the mayoral section below.

## 3. Mayoral extension

**Population.** The population is the winner and the runner-up of each completed decisive mayoral election.
It covers the 100 largest cities by 2020 Census population, January 2010 to September 14, 2026.
Hanno closed the population step on October 2, 2026: 1,649 of 1,700 city-years assessed.
The step found 411 verified contested elections and 19 uncontested elections, with 822 finalists.

**Coverage on October 4, 2026, before the legacy-lead scrape.** The controlling build is
`data/municipal_discovery/top100_coverage/outputs/manifest.json` (71 tests pass).

| Measure | Finalists of 822 |
|---|---:|
| URL lead (a possible campaign site) | 633 |
| Archive capture in the election window | 419 |
| Downloaded website text | 405 |

| Elections of 411 | Both finalists | Winner | Any finalist |
|---|---:|---:|---:|
| Website found | 64% | 79% | 90% |
| Website text downloaded | 29% | 52% | 69% |

Text for both finalists by period: 2010–2017 20%, 2018–2021 20%, 2022–2026 54%.
The early periods are low because most of their URL leads are old roster leads that the scrapes did not attribute.
The legacy-lead scrape addresses this gap.

**How a URL becomes downloaded text.** Each basis label in the build records how the URL was found and checked:

- Stage 2: an agent search and an independent agent check (`wf_24ab82ba-607`). 155 accepted.
- The domain-pattern sweep on Wayback, followed by one agent check (`wf_c6322dfb-478`). 112 accepted.
- The domain-pattern sweep on Common Crawl, checked by the coordinating agent. 5 accepted.
- The old roster leads, screened by Hive (Qwen3.6-27B-FP8) and then checked by agents (`wf_5110088a-dd2`). 124 accepted.

Only accepted URLs count. Their saved pages count as downloaded text if the pages fall within 365 days before the election.
The stage 3 scrapes run on mac2 with method `mayoral-wayback-site-scrape-v1`.
That method uses a window from 18 months before the election month to 120 days after, and at most 150 pages per target.
The raw pages stay on mac2 in `~/candidate_website_extension_stage3/`. The repository holds the hashed page summaries.

**Remaining coverage levers, in order.**

1. The legacy-lead scrape and its attribution check. In progress, see `Resume here`.
2. Download text for accepted or checked leads that no scrape has covered yet.
   Examples: the 10 `outside_window` items from the legacy check and the 5 shared-host URLs.
   Check each for a valid in-window capture first.
3. Retry the 239 Common Crawl queries that failed because of throttling. The expected yield is low.
4. A wider date window for the 189 finalists with no URL lead ("option 3"). **This needs Hanno's approval.**
   A wider window brings a risk of pages from the wrong election.

**Release steps after collection.**

1. Content checks on the downloaded text: the right candidate, the right election, not a parked or spam page.
2. A frozen build with coverage tables and an explicit list of the remaining gaps.
3. A README, codebook, coverage summary and manifest. Then Hanno's approval, then publication.

Release does not require a website for every finalist or a fixed coverage percentage.
It requires the full population to be assessed, the agreed collection work to finish, and missing evidence to stay explicit.
Detailed queues and limits: [mayoral_url_discovery.md](mayoral_url_discovery.md).

## 4. 2026 House/Senate collection

The droplet cron `~/run_collect_2026.sh` runs at 09:00 America/Los_Angeles. It is the only scheduler for this collection.
The schedule: a live copy every 30 days, plus the local day before and after each applicable primary or runoff.
Rosters and URLs refresh each week. The rules are in [collection_2026.md](collection_2026.md).
Hanno set the endpoint to November 17, 2026 on September 30 (`collection_end_date` in `config/collection_2026.json`).

Status on October 4, 2026 (`logs/status_20261004.json` on the droplet): 57,904 pages, 22,357 snapshots, 3,689 candidates.
There are 1,964 pending work items and 0 unfinished attempts. The volume has 61 GiB free of 100 GiB.
The daily log `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime/logs/daily.log` shows `rc=0` for October 1 to 4.
These runs came after the September 30 fix for the `Invalid IPv6 URL` stop.

**Action:** check the daily `rc` line and the free space once a week. Act only if a run fails.
After November 17, build the final outputs and run the coverage checks.

## 2. Extracted variables: open items for a later version

Version 4.0 is published. These items do not block anything now.

- Not yet decided: drop the rows on `anthonybrown.com` and `kateforcongress.com`, which serve pages under the wrong candidate.
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
- `data/deliverable` (1.7 GB, regenerable in about 8 minutes) stays. Hanno has not chosen between compressing and deleting it.

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
