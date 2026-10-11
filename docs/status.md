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

**H. Every archived page, not only 150 (decided by Hanno October 8, done October 8).** All 598 finalists with text came from capped site scrapes:
467 from the mac2 stage 3 batches and 131 from the August 26 droplet batches (`covered_151_20260826`, `full_discovery_740_20260826`, `openai_residual_expansion_357_20260826`), all with at most 150 pages per site.
`scripts/build_mayoral_uncapped_queue.py` built, from each batch's existing capture inventory (no new CDX query), a queue of the pages the cap left out: one capture per distinct page, nearest the election.
It fetches only campaign sites the coverage build credits (allowed target lists on mac2 in `uncap_allowed/`; the August batches also hold news sites, google.com and non-finalists, which are excluded), and it drops non-content URLs (assets, platform paths, tokens, crawler loops, calendar and tracking query variants). The drop share is above 20 percent in every batch; the audit in each `runs/uncapped/uncapped_queue_audit.json` gives exact counts, and the inventories stay on disk.
Result: `scripts/run_mayoral_uncapped_fetch.sh` fetched 9,269 of 9,292 queued pages for 44 sites on mac2 (0 retryable; the rest no content or not available), in `runs/uncapped/` of each batch. The August batch folders are now also on mac2 (inventories copied from the droplet; the droplet root disk has under 2 GB free).
Cary Moon (Seattle 2017): all four URL variants had reached the CDX limit of 50,000 rows, mostly event-calendar links. A CDX query without `/events/` gave 117 pages, 27 of them new; fetched in `runs/carymoon_extra/` of stage 3L. Andrew Janz also hit the limit but is not credited.
**Folded in (October 9).** `scripts/summarize_mayoral_stage3_pages.py` now takes several `--run-dir`; the four stage 3 summaries in `inputs/` include the uncapped pages. Coverage counts are unchanged (69 tests pass).
`scripts/build_mayoral_page_manifest.py` lists every saved page of the credited sites across all batches and run folders: mac2 `~/candidate_website_extension_stage3/mayoral_page_manifest.csv.gz` (33,235 pages; 23,943 from the capped runs), per finalist in `outputs/page_manifest_by_finalist.csv`.
The August batch page files were copied from the droplet to mac2 for this. 597 of 598 finalists with text are in the manifest; Ronnie Bastin (2018) has text only in the older droplet corpus `/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory`, which the manifest does not read yet.
Pages within 365 days of the election per finalist: median 19; 37 finalists have 1 page, 58 have 2–5, 209 have 6–20, 256 have 21–150, 37 have more than 150. Small counts mean the archive holds few pages for that site; no cap applies any more.

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

**Release pipeline (decided by Hanno October 9, 2026).**
- Venue: a separate Harvard Dataverse dataset with its own DOI, not a version of `doi:10.7910/DVN/BZ2JRS`.
- Page corpus: every fetched page of a credited site, with days from the election and an in-window flag (365 days).
- Content check: deterministic page flags plus a site-level model check on Hive (open-weight model), calibrated against the agents' earlier verdicts. Failing sites are excluded with a reason code.
- Finalist panel text: every distinct in-window page of the site, one copy per page, in a stable order.

Stages, each with its own check:
1. Freeze inputs: coverage outputs, page manifest and page files with hashes; add Ronnie Bastin's pages from the older droplet corpus.
2. Page corpus: re-extract text from raw HTML with the House/Senate extractor and boilerplate filter; mark duplicate texts within a site.
3. Page flags: empty or app shell, parked or for sale, spam, redirect to another domain, timestamp substitution, other election years named. Parked, spam and off-domain pages are excluded; the rest are flags.
4. Hive site check: root, longest and two random pages per site; own campaign site, this election, usable content.
5. Release files: city-year universe (1,700), elections, finalists (822, with coverage status and reason code), page corpus (parquet), finalist panel.
6. Validator and tests: unique keys, reconciled counts, hash trace for every page, no excluded or out-of-window page in the panel, consistent reason codes, drops above 20 percent reported.
7. README, codebook, coverage summary, gap list, `manifest.json`; then Hanno's approval before publication.

**Release progress, October 9.**
- Stage 1 done: page manifest with all 598 finalists (33,237 pages), including Ronnie Bastin's two pages copied from the older droplet corpus to mac2 as `legacy_all_inventory_20260906`.
- Stages 2 and 3 done: `scripts/build_mayoral_page_corpus.py` wrote mac2 `release_mayoral/mayoral_page_corpus.parquet`.
  Audit `quality_reports/mayoral_release/page_corpus_audit_2026-10-09.json`: 800 pages excluded by rule (449 empty, 331 parked or placeholder, of which 271 are directory listings, 21 spam), 4,093 duplicate texts; 592 of 598 finalists keep usable pages in the window.
  Two rule revisions after inspecting samples: parked phrases count only on pages under 1,200 characters (real campaign pages mention road construction or events "coming soon"), and spam terms count only on pages that do not name the candidate (campaigns discuss casinos; Chinese-language campaign pages are a flag, not spam).
- Stage 4 running: `scripts/build_mayoral_release_site_items.py` made 596 site items for 592 finalists (root page and longest page in the window) plus the 46 known negatives from the October 4 calibration; same classifier `mayoral-site-attribution-v1` as before. Hive jobs 24714339 (high) and 24714340 (low), work dir `/nfs/hive/scratch/hhilbig/mayoral_site_attribution`, output `decisions_release_<partition>.jsonl`.
  Rule: "yes" passes; "no" and "unclear" go to an agent review under the October 4 rule; a site is excluded only if the agent confirms. The Hive model was weak on election timing in the October 4 calibration, so a model "no" alone does not exclude.
- Stages 5 and 6 written and tested on a dry run with all sites passing: `scripts/build_mayoral_release.py`, `scripts/validate_mayoral_release.py` (writes `manifest.json`), `tests/test_mayoral_release.py` (4 pass).
  Dry run: 822 eligible finalists = 592 text, 119 site_not_archived, 100 no_url_lead, 6 no_usable_pages, 5 check_incomplete; 25,682 of 33,237 pages usable; no drop above 20 percent.

**Legacy-lead search (approved by Hanno October 9, running).** 107 finalists had only an old roster URL without an archived copy in the window, and no search had ever run for them (stage 2, the repairs and the sweeps covered only finalists without any URL lead). 92 of them are in 2014–2021, which explains the 2018–2021 dip in coverage.
- Domain-pattern sweep before and after the election on the droplet: `scripts/run_mayoral_legacy_sweep.sh`, folder `url_discovery/legacy_search_20261009/sweep_before|sweep_after`, jobstatus `mayoral-legacy-sweep`.
- Agent search and independent check, three cities at a time: `scripts/workflows_mayoral_legacy_search.js`, run `wf_44cc61ab-187`, outputs `url_discovery/legacy_search_20261009/<slug>_url_leads.json`.
- Agent search done (116 agents, 7.9 million tokens, 10.9 hours): 62 accepted, 23 outside_window, 6 not_found, 16 incomplete; the checkers rejected 2. Many roster URLs belonged to a later or earlier campaign (Tim Keller 2017 used tk4abq.com, the roster held mayortimkeller.com).
- Domain sweep done (droplet): before 2,378 queries, 257 with captures; after 2,378 queries, 165 with captures. Hits that passed the screen for 23 finalists the search left open go to an agent check, `scripts/workflows_mayoral_legacy_sweep_check.js` (run `wf_c56a8985-1e3`).
- Integration: `scripts/build_mayoral_legacy_search_leads.py` writes `linkage/legacy_search_leads.csv` (61 accepted, 21 outside_window, 9 not_found; incomplete ones keep the roster URL), registered as `legacy_search_leads`. The coverage build lets such a lead replace an unarchived roster URL (`replaces_legacy`); the targets builder reads it too. Archive discovery 603 to 661 finalists.
- Batch `mayoral_stage3s_20261010` (82 targets, 78 domains) is frozen; `scripts/run_mayoral_full_site_batch.sh` runs the capped and the uncapped fetch in one go on mac2. It starts after the sweep check, so agents and the scrape do not share the archive.
- Then: the new sites go through the page corpus, the page rules and the site check; then the release build.
The release build waits for this.

**Release candidate v1 built and validated (October 10).** `build/mayoral_release_v1_candidate/` (six data files, `manifest.json`, `README.md`, `CODEBOOK.md`; documents also in `docs/mayoral_release/`). Validator report `quality_reports/mayoral_release/release_v1_candidate_validation_2026-10-10.json`: valid, no drop above 20 percent.
- 650 of 822 eligible finalists have usable text; 272 of 411 elections (66 percent) both finalists, 348 (85 percent) winner, 378 (92 percent) any. 100 no_url_lead, 33 site_not_archived, 16 check_incomplete, 13 no_usable_pages, 10 excluded_site_check.
- Site check: 665 sites, Hive jobs 24714340 and 24731028, agent reviews `wf_5a06b8f6-8cf` and `wf_ce74850f-42c`; 654 pass, 11 excluded; Cyndi Lamm 2019 counts only up to election day (`pages_until`). Final verdicts `release_site_check/site_check.csv`.
- Page corpus rebuilt from `scripts/build_mayoral_target_map.py` (targets matched per batch and domain) on mac2: 35,290 pages, 27,064 usable.
- **Waiting for Hanno:** license and takedown contact (proposed: same terms as the House/Senate deposit), review of README and codebook, then approval to create the separate Dataverse dataset and upload. Nothing is published.

**Pre-publication review and approved rework (October 10).** Three Opus reviews (`quality_reports/mayoral_release/coverage_review_2026-10-10.md`, `future_user_review_2026-10-10.md`, `city_expansion_assessment_2026-10-10.md`).
Demonstrated defects (affected requirement: a correct, documented release), smallest corrections, and the return condition (validator passes on the rebuilt release):
1. `days_from_election` and `in_text_window` use the 15th of the election month for the three August batches (12,549 pages, 131 finalists). Fix: compute from the finalist's election date in the corpus builder.
2. 41 usable pages hold NUL bytes or binary content; bot-challenge, "page not found" and listing pages (tag, author, paging, calendar) count as usable; some addresses repeat. Fix: new page rules and URL-normalized duplicates.
3. `--exclude` in `build_mayoral_stage3_targets.py` drops a finalist instead of a finalist and domain, so three accepted sites were never scraped (Sheila Jackson Lee 2023, Javiera Caballero 2021, Jerry Weiers 2016); Ron Nirenberg 2017 fetch errors were never retried; billbellformayor.com belongs to Bill Bell 2011, not 2013. Fix: key on finalist and domain, scrape and check these sites.
4. Labels: archived URLs rejected as not the campaign site are labelled `site_not_archived`; Chris Adler 2012 has a council-video "lead"; README errors (window after the election is 120 days, "three attempts", calendar links, Honolulu is a CDP). Fix: status `site_rejected`, corrected README.
Approved by Hanno on October 10:
- (a) Reopen the population for the out-of-population elections: Sacramento 2012/2016/2020, Plano 2013/2017/2021, about 21 major races with unverified results (San Diego 2012–2020, Detroit 2013, Atlanta 2013, Baltimore 2011/2024, Tulsa, Memphis 2011 and others), and Tulsa 2024. Verify results, then discovery and scrape for their finalists.
- (b) A fourth search pass on the stage 2 search blocker for about 58 finalists (38 stage-2-only `no_url_lead`, 11 one-attempt `check_incomplete`, 9 excluded without recovery) plus the new finalists from (a), with a working search engine from the start and an archived-Ballotpedia lookup. Repair attempt count on this blocker becomes 4.
- (c) Candidate variables: total votes and vote shares from full official results; incumbency derived from the data (2010 from an outside source or marked missing); a person ID.
- (d) Usability: `phase` column, one `election_id` format, neutral batch and run names, loading example.
- License: same terms as the House/Senate deposit (CC BY 4.0 on the compilation); proposed takedown contact hhilbig@ucdavis.edu, to be confirmed.
- The 20-city expansion pilot is not run now.
Model pilot for (b), October 10 (Hanno asked whether Haiku can replace Sonnet for search): same 20 finalists in Jersey City, Newark, Fort Wayne, Albuquerque and Cleveland, same script (`scripts/workflows_mayoral_haiku_search_pilot.js`), Brave search, two cities at a time; answer key `url_discovery/repair_20261004/`. Haiku 4.5 (run2, `wf_a5f2ae57-ea2`, 357k tokens): 6 of 15 known sites found, 6 known sites called not_found (Baraka x3, Henry x3), 5 incomplete, one Board of Education page accepted without the mayoral check, one result file not written. Sonnet (run3, `wf_ec2095da-c81`, 418k tokens): 14 of 15 known sites found (Gore recorded not_found under the strict rule, URL kept for review), plus two sites the key missed (Berry 2013 mayorberry.com, Didier 2023 tomdidierfortwayne.com), no false acceptance. Hanno chose Sonnet. Outputs `url_discovery/haiku_pilot_20261010/`.
Fourth search pass started October 10: `scripts/build_mayoral_pass4_args.py` selected 99 finalists in 36 cities (41 stage2_only, 11 check_incomplete, 9 excluded, 38 new_finalist); workflow `wf_b4c470bf-1ec` (`scripts/workflows_mayoral_search_pass4.js`), Sonnet, Brave plus archived Ballotpedia, two cities at a time, independent check per city. Outputs `url_discovery/pass4_20261010/`. Next: save the checker verdicts, add the pass as a source in `build_mayoral_url_leads.py` (the new finalists are not in `stage2_args.json`), then scrape the accepted sites with stage3v.
Order: defects 1–4, then (a), then (b), then (c) and (d), then one rebuild, site check, validation and documentation.

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
