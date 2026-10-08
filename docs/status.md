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

Updated October 4, 2026, 21:00 PDT. Three mayoral jobs were running at that time. Hanno approved all of them.

**A. Stage 2 repair search (done, commit `7d49bc9`).** Stage 2 recorded 112 finalists as `not_found`, but its notes show that no web search ran or the archive check failed.
Workflow `wf_ca7d0158-af4` (`scripts/workflows_mayoral_url_repair.js`, 72 Sonnet agents, 5.5 million tokens, 3.7 hours) searched again, with Wayback requests through mac2.
Result: 50 accepted, 9 outside_window, 29 not_found, 24 `incomplete`. Files: `url_discovery/repair_20261004/`.
The lead builder applies these as basis `agent_search_repair_wayback_capture_checked`. `incomplete` entries keep their stage 2 status.
Two causes made entries incomplete: Wayback timeouts from mac2 when about 10 agents ran at once, and the shared WebSearch budget (200 calls for each workflow run).
The checkers also flagged weak `not_found` entries with an unchecked domain, for example Jim Gray (Lexington 2014), Sheila Montague (Newark 2022), Richard Berry (`mayorberry.com`, Albuquerque 2013).
Repair attempt count for this blocker: 1. **Next:** one follow-up pass at low concurrency (one or two agents at a time) for the 24 incomplete and the flagged entries. A search fallback that does not use WebSearch is needed. After that pass, the attempt count is 2; stop and report.

**B. Legacy-lead attribution (done, commit `cfcd437`).** Hive job 24455257 screened 139 stage 3L items (113 yes, 26 no).
Workflow `wf_066a43c7-be2` checked the 113 yes items: 104 accepted, 1 not_found, 8 `incomplete` (archive timeouts).
Verdicts: `legacy_site_attribution/agent_verdicts.csv`, registered as `legacy_site_attribution`. The 8 incomplete items belong in the follow-up pass of A.

**C. Stage 3 repair batch (done).** `mayoral_stage3r_20261004`: 63 targets (50 repair accepted, 5 Common Crawl, 8 outside_window), 54 domains.
Pass 1 ended October 5 with 112 retryable pages after archive HTTP 500 errors; the retry pass on October 6 recovered 100.
Result: 2,193 of 2,225 pages fetched, 6 still retryable, 58 targets with pages, 57 with pages in the window. Summary `inputs/stage3r_saved_pages.csv`, registered in `stage3_saved_pages`.

**D. Follow-up pass (done, October 6–7).** Workflow `wf_2007bf4d-b90` (`scripts/workflows_mayoral_followup.js`, 43 Sonnet agents run one at a time, 2.9 million tokens, 8 hours).
It redid 39 repair entries (24 incomplete, 15 flagged by checkers): 9 accepted, 6 outside_window, 6 not_found, 18 still `incomplete`.
It also redid the 8 incomplete legacy attribution items: 7 accepted, 1 not_found.
Most remaining incomplete entries failed because mac2 stopped answering or the archive timed out; WebSearch was refused and DuckDuckGo through mac2 served as the search fallback.
**This was repair attempt 2 on the stage 2 search blocker. The two-attempt limit is reached.** The 18 incomplete entries stay documented as unresolved (`followup/<slug>_url_leads.json`); the build keeps their earlier `not_found` status and reports the count as `repair_incomplete_not_applied`.
Any further attempt on them needs Hanno's decision.

**E. Last scrape batch (done).** `mayoral_stage3f_20261007`: 10 targets, 165 pages fetched, 0 retryable, 8 targets with pages in the window. Summary `inputs/stage3f_saved_pages.csv`, registered.
Shared-host leads outside the window (one weebly.com site) are skipped by `scripts/build_mayoral_stage3_targets.py`.

**F. Repair attempt 3 (done, approved by Hanno October 7).** `wf_8c7b26fc-575` redid the 18 entries still incomplete: 3 accepted, 10 not_found, 5 still incomplete (`followup2/`). The lead builder chains the passes.
The stage 3 target builder now scrapes a site on a shared host (wordpress.com, weebly.com) by its own host name. Batch `mayoral_stage3g_20261007` (4 targets, 29 pages).

**G. Option 3, after-election sweep (done, approved by Hanno October 7).** `scripts/sweep_mayoral_domain_patterns.py --window after` queried the 120 days after each election for the 121 finalists without a URL lead (2,699 domain windows on the droplet, 0 errors; 120 with captures; 38 pages passed the screen).
Workflow `wf_fe9a8fc0-38f` (`scripts/workflows_mayoral_sweep_after_check.js`, three agents at a time) checked 26 finalists: 5 accepted, 8 outside_window, 12 not_found, 1 incomplete.
Basis `domain_sweep_after_election_capture_checked`, applied last. Batch `mayoral_stage3a_20261008` (13 targets, 268 pages, all 13 with pages in the window).
An outside_window site contributes text only through pages within 365 days of the election, here the first 120 days after it.

**H. Every archived page, not only 150 (decided by Hanno October 8, running).** All 598 finalists with text came from capped site scrapes:
467 from the mac2 stage 3 batches and 131 from the August 26 droplet batches (`covered_151_20260826`, `full_discovery_740_20260826`, `openai_residual_expansion_357_20260826`), all with at most 150 pages per site.
`scripts/build_mayoral_uncapped_queue.py` builds, from each batch's existing capture inventory (no new CDX query), a queue of the pages the cap left out: one capture per distinct page, nearest the election.
It fetches only campaign sites the coverage build credits (allowed target lists on mac2 in `uncap_allowed/`; the August batches also hold news sites, google.com and non-finalists, which are excluded), and it drops non-content URLs (assets, platform paths, tokens, crawler loops, calendar and tracking query variants). The drop share is above 20 percent in every batch; the audit in each `runs/uncapped/uncapped_queue_audit.json` gives exact counts, and the inventories stay on disk.
Result: about 9,300 new pages for 44 sites. `scripts/run_mayoral_uncapped_fetch.sh` fetches them on mac2, one batch at a time, log `uncapped_fetch.log`, with a Slack watcher per batch.
Known limit: one stage 3L CDX query reached the 50,000-row limit, so that site's page list is truncated (checked as calendar and token links).
The runner's `date -Is` is not valid on macOS (log lines lack time stamps); fix after the run ends, not while it runs.
**Next:** after the run, fold the `uncapped` run folders into the page summaries and the corpus; the coverage counts do not change, the pages per site do.

**Collection status.** All approved collection levers are done. Unresolved and documented: 5 repair entries and 1 sweep entry still incomplete; 239 failed Common Crawl queries (not retried, low expected yield).
**Next authorized steps:** the release steps in the mayoral section (content checks, frozen build with gap list, README, codebook, manifest).

Final collection coverage, October 8: URL leads 717 of 822 finalists; downloaded text 598 of 822.
Of 411 elections: text for both finalists 57 percent, winner 77 percent, any finalist 89 percent. A known site for both finalists 79 percent, winner 91 percent, any 95 percent.
Text for both finalists by period: 2010–2017 55 percent, 2018–2021 40 percent, 2022–2026 75 percent.

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

## Deferred: Wayback scraping skill audit, October 4, 2026

A check of `scripts/scrape_mayoral_wayback_batch.py` against the `wayback-scraping` skill found these gaps. None blocks a release requirement now.

- The fetch journal is deleted at each compaction, rows carry no attempt ID, and no attempt count is kept for each page. On a restart, failed pages are retried in queue order, not least-attempted first.
- The backoff has no jitter. The CDX query has no explicit guard for a result that reaches the 50,000-row limit (the largest so far is 7,005).
- Agent workflows that query the archive through mac2 share no rate limit with each other or with the scraper. Run such workflows at low concurrency and not alongside a scrape.

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
