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

**October 17, page check (Hive, Qwen3.6-27B-FP8 e89b16eb).** Job 24828287 (low) labeled all 21,361 usable v4 pages with prompt v1: 19,298 this_campaign, 574 unclear, 964 earlier_campaign, 276 not_campaign, 247 other_office, 2 parse errors. Labels: `quality_reports/mayoral_release/page_campaign_check_2026-10-17/labels_round1.jsonl.gz`; table `release_site_check/page_campaign_check.csv`.
Blind Opus check of 60 pages (40 dropped, 20 kept; `sample_key.json`, `opus_part*.json`): all 20 kept pages confirmed; 35 of 40 drops confirmed.
Blocker (repair attempt 1 of 2): v1 labels pages about the primary or first round of the same race as earlier_campaign (Emanuel 2015, Iannarone 2020, Higgins 2025). Correction: prompt v2 (script sha 742c5a96) relabels only the 964 earlier_campaign pages and the 2 parse errors; jobs 24833080 (low) and 24833081 (high), output `labels_v2_*.jsonl` in `/nfs/hive/scratch/hhilbig/mayoral_page_campaign/`. Return condition: a v2 sample check finds no same-race drops.
Attempt 1 result (job 24833080, `labels_v2_low.jsonl`): 110 of the 964 pages returned to this_campaign or unclear, but about 15 pages that the model itself calls an earlier round of the same race stay earlier_campaign (Adler 2014, Richardson 2022, McFarlane 2017). Attempt 1 failed.
Attempt 2 (last under the two-attempt rule): prompt v3 adds race_year; conversion keeps an earlier_campaign page whose race_year is the election year or the year before. If its check still finds same-race drops, stop and report to Hanno.
Attempt 2 result (job 24833305, 986 pages, 0 parse errors; labels `labels_v3.jsonl.gz` in the folder above; merge script `scripts/build_mayoral_page_check_table.py`): the v3 model still labels about 20 same-race pages earlier_campaign (Briley, Higgins, Keller, Hunter, Smith 2012, Richardson 2022, McFarlane 2017). The year rule keeps them, but the rule keeps 71 pages in total, and about 20 of those are other races or other offices (Tishaura Jones for Treasurer, Cantrell and Athas council pages, Ginther 2022, Iannarone 2019 podcasts); about 30 are unclear. Without the rule the filter check passes for races two or more years earlier (17 hits, all correct drops). **Attempt 2 failed; the two-attempt limit is reached. Decision needed from Hanno** (options in the session report of October 17): use v3 with the year rule, v3 without the rule, or a hand check (Opus) of the 71 rule pages with the decisions as overrides.
**Hanno decided October 17: option 3 (repair attempt 3, approved).** v3 labels without the year rule, plus an Opus hand check of the 71 pages the rule would keep; the Opus decisions override those labels. Input `scratchpad/page_check/opus_rule_pages.json`. Open question from Hanno: a primary-period versus general-period flag for pages; ALGED records only the deciding round (Chicago 2015: April only), so first-round dates need a new source.
**Hanno decided October 17: finish v5, then build v6 with the decisive partisan primaries; release only after v6.** v6 adds the top two of the decisive primary (focus: Democratic) in cities with partisan mayoral primaries, plus the `campaign_period` page flag (first-round dates). ALGED has general elections only (New York 2013–2021: November rows only), so the primary population comes from official results. v5 progress (October 17): corpus rebuilt on mac2 with the final labels (`release_mayoral/v5_20261017/`; v1-label build kept in `labels_v1_build/`): 717 finalists with usable in-window pages, 1,506 pages not_this_campaign. Work release `build/mayoral_release_v5b_work/` validates: 710 panel rows, 20,090 usable pages. King 2024 (login screen) and Welch 2021 (county commission page) lose their only pages; both drops are correct. Round 3: 14 newly usable pages, Hive jobs 24837532 (low) and 24837533 (high), watcher `watch_r3.sh`. README and codebook text updated except counts. v6 scoping runs in parallel (agent, output `data/municipal_discovery/top100_coverage/v6_primaries/`).
**v5 done (October 17).** Round 3 labeled 14 new pages (job 24837532, all drops); the next rebuild freed 9 copies with the same text, so the merge script now passes a label to same-text copies of the same finalist (3,605 pages; test `tests/test_mayoral_page_check_table.py`). Final corpus `release_mayoral/v5_20261017/` on mac2 (0 unlabeled usable pages); release `build/mayoral_release_v5_candidate/` valid with manifest: 710 of 860 finalists with text, 306 of 430 elections with both, 20,076 usable pages, 1,644 pages not_this_campaign. README and codebook updated to v5. The first-user review moves to v6, because the release waits for v6.
v6 scoping done (agent; `data/municipal_discovery/top100_coverage/v6_primaries/`): 12 cities with partisan primaries, 50 primaries, 96 candidate rows, 42 not yet finalists. Most counts are from Wikipedia and need a check against official results.
**Hanno decided October 17 (v6):** URL search for the new primary candidates approved (estimate about 3 million Sonnet tokens, 4–6 hours, a few hundred Brave queries); unopposed primaries stay in the primaries file with a flag; the DC Democratic primary of June 2026 is included. Next: check the primary list against official results (and add Charlotte 2022), then the URL search.
Primary list checked (October 17): all 105 rows matched to official results; Tucson added; 13 cities, 56 primaries, 46 candidates to search (7 unopposed rows flagged). 20 cities stay `unverified_assumed_nonpartisan` in `partisan_primary_cities.csv` (no source found). URL search running: workflow `wf_0a7fc1a0-3f4` (`scripts/workflows_mayoral_v6_primary_search.js`, input from `scripts/build_mayoral_v6_primary_search_args.py`), output `/private/tmp/claude-501/v6_primary_search/` (outside ~/Documents, as in the pass 4 rerun).
URL search pass 1 done (October 18, 26 Sonnet agents, 2.1 million tokens, 14.9 hours): of 46 candidates, 24 accepted, 5 outside_window, 6 not_found, 11 incomplete (Brave captcha blocks and archive outages from mac2). Checkers found only minor problems (approximate capture counts). Leads: `url_discovery/v6_primaries_20261017/pass1/`. Rerun of the 11 incomplete (5 cities, Wayback through the droplet): workflow `wf_8ac62e16-4b4`, output `/private/tmp/claude-501/v6_primary_search_rerun/`.
Rerun done (`wf_8ac62e16-4b4`, 0.8 million tokens, 32 minutes): 3 accepted (Tandy, Scanlon, Maginnis), 4 outside_window, 2 not_found, 2 still incomplete (Fort Wayne: Cook 2011, Stevenson 2015; archive offline). This is the second pass on these entries; they stay check_incomplete. Whole search: 27 accepted, 9 outside_window, 8 not_found, 2 incomplete of 46. Merged leads `url_discovery/v6_primaries_20261017/<slug>_url_leads.json` and `linkage/v6_primary_url_leads.csv` (`scripts/build_mayoral_v6_primary_targets.py`).
v6 population (`scripts/build_mayoral_v6_primaries.py`, test `tests/test_build_mayoral_v6_primaries.py`): `outputs/primary_elections.csv` (56 primaries, 13 cities, 7 unopposed) and `outputs/primary_candidates.csv` (105 candidates; 59 linked to their finalist row, 46 searched).
Release code: `build_mayoral_release.py --primaries` writes `mayoral_primary_elections.csv`, `mayoral_primary_candidates.csv`, `mayoral_primary_panel.csv` and a `campaign_period` page column; the validator checks them. Primary panel rule: pages that pass the page rules and the site check, captured in the 365 days up to the primary, one copy per text and address (copy nearest the primary kept). The corpus duplicate rule refers to the general election and had hidden primary-period copies (Mamdani 2025). Smoke build on the v5 corpus validates; 40 of 59 linked finalists have primary-period text. Open: those pages outside the general window have no page-check label; label them on Hive with the new sites.
Download: batch `mayoral_stage3p_20261018` (36 targets, 34 domains), started on mac2 October 18 with `run_mayoral_full_site_batch.sh`, jobstatus `mayoral-scrape-mayoral_stage3p_20261018`.
Open decision (Hanno): first-round dates for the 379 elections in 87 nonpartisan cities. No single ready source; proposal: calendar notes (about 62 dates) plus archived Ballotpedia election pages by script, then a small Sonnet search for the rest.
**Hanno approved October 18:** steps 1 and 2 (script, then a small Sonnet search); undated elections keep an empty flag. Step 1 done: `scripts/collect_mayoral_ballotpedia_round_dates.py` on the droplet read 285 archived Ballotpedia election pages in 99 of 100 cities (Santa Clarita has none; 5 cities failed with CDX 503 and were rerun without errors), output `v6_primaries/ballotpedia_rounds.jsonl`. `scripts/build_mayoral_first_round_dates.py` writes `outputs/first_round_dates.csv` (430 eligible elections): 51 from the primaries file, 62 from calendar notes (Toledo 2015 special set to single_round by hand), 166 from Ballotpedia, 151 open. No conflicts where a note and Ballotpedia both give a date. Step 2 done: 4 Sonnet agents dated all 151 open elections (`v6_primaries/first_round_search/result_*.json`; 115 sources Wikipedia, rest Ballotpedia, official and news pages). My check: every result is consistent with our recorded stage, every non-Tuesday date fits its state (Hawaii and Texas Saturdays, Nashville Thursdays), known races match. Final `first_round_dates.csv`: 227 two_rounds, 203 single_round, 0 unknown. Agent reports: about 20 single_round calls rest on the city's election rules plus our own recorded deciding round, not on a page that states the round structure (Texas May elections in Irving, Lubbock, Garland; November plurality cities Santa Ana, Aurora, Anaheim 2010, Fremont, Irvine, Richmond 2012, Norfolk; Minneapolis 2013; primaries won outright in Riverside, San Jose, Sacramento, Portland, Chula Vista). In each case our verified results already show that the recorded election decided the race, so a later round is excluded; only an unrecorded earlier round could be missed. Accepted; README states the source of each date.
`campaign_period` values (v6): first_round, final_round, single_round, after_primary (primary candidates after their primary), "" (not dated). `mayoral_elections.csv` gets `rounds`, `first_round_date`, `first_round_source`, `first_round_source_url`. Smoke build on the v5 corpus validates.
Download `mayoral_stage3p_20261018` done October 18 13:22 (capped and uncapped runs rc 0). v6a corpus on mac2: `release_mayoral/v6_20261018/build_v6a.sh` (manifest 46,466 pages = v4 44,215 + 2,251 new; candidates `candidate_coverage_v6.csv` = v5 coverage + 105 primary rows; v5 page labels). v6a corpus done (46,466 pages; local copy `build/mayoral_v6a_corpus/`). Site check: 30 of the 36 new targets have usable pages; Hive job 24878961 (high) cancelled, 24878967 (low) done (`release_site_check/hive_decisions_v6.jsonl`): 26 yes; controls 33 no, 6 unclear, 7 yes (same as earlier runs). The 4 no decided in session: pass Fernandez 2023 (old commissioner header, mayoral filing and "Paid for by Fernandez forMayor"), Albanese 2017 (`sal2017.com`), Anthony Hardy Williams 2015 (same person); excluded Reed 2013 (only page is his Aldermen president re-election site). `site_check.csv` now 773 sites. Work release `build/mayoral_release_v6a_work/`: finalists unchanged (710 with text), 21,469 usable pages, 64 of 105 primary candidates with primary-period text.
Page check v6: 2,226 unlabeled pages of 51 candidates (new primary sites and primary-period pages; `build_mayoral_page_check_items.py --primaries --labels`), Hive jobs 24879195 (low) and 24879196 (high), prompt v3, outputs `labels_v6_{low,high}.jsonl` in `/nfs/hive/scratch/hhilbig/mayoral_page_campaign/`. The v5 label table reproduces byte for byte from the scratch label files plus `--corpus`; add the v6 files to that command. Page check v6 done (job 24879195, 2,226 pages, 0 parse errors; 24879196 cancelled): 1,816 this_campaign, 278 earlier_campaign, 67 unclear, 53 other_office, 12 not_campaign. Same rule as v5 (Hanno, October 17): Opus hand check of the 54 earlier_campaign pages dated to the election year or the year before: 48 this_campaign (all 46 Bill Thompson 2013 primary pages), 3 not_campaign, 1 earlier_campaign, 1 other_office, 1 unclear. Checked by hand: Cuomo 2025 other_office pages are old governor pages re-archived in 2025 (correct). Files `quality_reports/mayoral_release/page_campaign_check_2026-10-18/`. Label table `page_campaign_check.csv`: 27,594 pages, 125 hand decisions, 3,711 inherited. Final corpus building on mac2 (`release_mayoral/v6_20261018/build_v6.sh`; v6a build moved to `v6a_build/`). **Release v6 built and valid (October 18):** `build/mayoral_release_v6_candidate/` (manifest written; 0 unlabeled usable pages after one more label, Hive job 24886607). Finalist results equal v5 (710 of 860 with text, 20,076 usable finalist pages, 84 percent before election day). Primaries: 56 elections, 105 candidates, 63 with primary-period text (40 of 59 finalists, 23 of 46 searched); 17 of 49 contested primaries with text for both. campaign_period on usable finalist pages: 3,562 first_round, 10,397 final_round, 6,117 single_round. Validator fix: usable pages of primary candidates are allowed (commit e5c2c29). README and codebook updated (commit 27e7a59). First-user review running: two Opus reviewers on `scratchpad/review_v6/mayoral_release` (primaries researcher, text-as-data researcher). First-user review v6 done (two Opus reviewers). Both reproduced every README count and all manifest checksums; flags and campaign_period recompute with 0 mismatches; first-round dates correct in every race checked. Documentation fixes made (commit 0e895ce). Demonstrated defect for the campaign_period deliverable: the corpus duplicate rule merges copies across periods and keeps the copy nearest the general election, so 568 distinct first-round copies (84 finalists; 354 without a page-check label) are duplicates; Lightfoot and Preckwinkle 2019 have no usable first-round page. Smallest correction: never merge copies from different campaign_period values; label the 354 pages on Hive; rebuild. **Hanno decided October 18: 1(a) and 2(a).** 1(a): `build_mayoral_page_corpus.py --first-rounds` runs the duplicate check separately on each side of the first round (test added; validator now rejects a duplicate_of across the first round). Rebuilt corpus `release_mayoral/v6_20261018/` (build_v6c.sh; earlier build in `v6_build2/`): duplicates 12,746 to 11,802, usable pages 21,274 to 22,115; 670 unlabeled pages of 148 candidates on Hive (jobs 24892444 low, 24892445 high, `labels_v6c_*.jsonl`). 2(a): README states the Indianapolis 2011 exception. Earlier decision list:  (1) that correction, optionally with a targeted download of the last pre-first-round copy of each address; (2) Indianapolis 2011 Democratic primary (not decisive; the Republican primary was unopposed): keep and document, or drop; (3) deferred requests: party for 2022-2026 finalists, near-duplicate and language-mirror flags, a clean and deduplicated text column, a House/Senate-comparable document, a human-validated sample.
Next for v6 (older line): list the partisan-primary cities and their primaries, then a candidate count and cost estimate for Hanno before any collection.
ALGED: OSF returned October 17. `scripts/build_mayoral_ledb_crosswalk.py` links 639 of 647 finalists in 2010-2021 (input `top100_coverage/inputs/ledb_candidatelevel.csv`, sha 86cc4f80); the release build writes `mayoral_ledb_crosswalk.csv`. Commit 1b69e42. Then rebuild the corpus on mac2 (`release_mayoral/v5_20261017/build_v5.sh`), label newly usable pages, build release v5.
Hive SSH from the home network times out since October 17 morning; use the droplet as jump host (`ssh -J root@100.95.38.16 hhilbig@hive.hpc.ucdavis.edu`). On October 22 mac2 also timed out directly; `ssh -J root@100.95.38.16 hannohilbig@100.70.29.103` works.
**Release v6 final build (October 22).** Hive job 24892444 (prompt v3) labeled the 670 pages that the per-side duplicate rule freed: 632 this_campaign, 28 unclear, 9 not_campaign, 1 earlier_campaign (Ginther 2019 page about the 2015 race; race_year 2016, outside the hand-check rule, correct drop). Labels `quality_reports/mayoral_release/page_campaign_check_2026-10-18/labels_v6c.jsonl.gz`. The label table reproduces byte for byte from the Hive files without v6c (`--overrides .../opus_hand_decisions_all.json --corpus <v6c corpus>`), then gains 700 rows with v6c (670 labels, 30 same-text). Corpus rebuilt on mac2 (`release_mayoral/v6_20261018/build_v6d.sh`; v6c build in `v6c_build/`); local copy `build/mayoral_v6d_corpus/`. Release `build/mayoral_release_v6_candidate/` valid with manifest, 0 unlabeled usable pages, 79 mayoral tests pass; the pre-fix build is in `build/mayoral_release_v6_pre_dupfix/`. Finalist text status, coverage and all primary files are unchanged. Usable finalist pages 20,076 to 20,907 (81 percent before election day); first_round 3,562 to 3,733, final_round 10,397 to 11,057. README and codebook updated (duplicate rule per side of the first round, counts, first-round limitation).
Two findings for Hanno: (a) the fix frees 950 copies; 645 of the 837 usable ones are after election day, kept because their only twin is a pre-first-round copy, so the panel `text` repeats some text for about 170 finalists (`text_unique` share 62 to 60 percent; `text_before` unaffected). (b) Lightfoot and Preckwinkle 2019 still have no first-round page: the corpus holds no copy from before February 26, 2019, because the download took the copy nearest the general election. Only the targeted download (option 1, not chosen) would add them. **Next:** Hanno reviews the v6 release and decides on publication.

**October 22 estimate.** About 95 percent of the release work is complete. Tracks 1 and 2 are complete. Track 3 (mayoral) is about 97 percent complete: release v6 is built and valid; Hanno's review, the publication approval and the upload remain. Track 4 (2026) is about 90 percent complete; only the wait to November 17 and the final build remain.
Remaining agent time about 3–4 hours (upload after approval, the 2026 final build after November 17). Hanno's time about 2 hours (review of the v6 README, codebook and files; publication decision). The November 17 endpoint controls the wall-clock finish.
Largest uncertainty: what Hanno's review of v6 changes. The October 4 text below is history.

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
- License: same terms as the House/Senate deposit (CC BY 4.0 on the compilation); takedown contact hhilbig@ucdavis.edu (confirmed by Hanno October 13).
- The 20-city expansion pilot is not run now.
Model pilot for (b), October 10 (Hanno asked whether Haiku can replace Sonnet for search): same 20 finalists in Jersey City, Newark, Fort Wayne, Albuquerque and Cleveland, same script (`scripts/workflows_mayoral_haiku_search_pilot.js`), Brave search, two cities at a time; answer key `url_discovery/repair_20261004/`. Haiku 4.5 (run2, `wf_a5f2ae57-ea2`, 357k tokens): 6 of 15 known sites found, 6 known sites called not_found (Baraka x3, Henry x3), 5 incomplete, one Board of Education page accepted without the mayoral check, one result file not written. Sonnet (run3, `wf_ec2095da-c81`, 418k tokens): 14 of 15 known sites found (Gore recorded not_found under the strict rule, URL kept for review), plus two sites the key missed (Berry 2013 mayorberry.com, Didier 2023 tomdidierfortwayne.com), no false acceptance. Hanno chose Sonnet. Outputs `url_discovery/haiku_pilot_20261010/`.
Fourth search pass started October 10: `scripts/build_mayoral_pass4_args.py` selected 99 finalists in 36 cities (41 stage2_only, 11 check_incomplete, 9 excluded, 38 new_finalist); workflow `wf_b4c470bf-1ec` (`scripts/workflows_mayoral_search_pass4.js`), Sonnet, Brave plus archived Ballotpedia, two cities at a time, independent check per city. Outputs `url_discovery/pass4_20261010/`. Next: save the checker verdicts, add the pass as a source in `build_mayoral_url_leads.py` (the new finalists are not in `stage2_args.json`), then scrape the accepted sites with stage3v.
Order: defects 1–4, then (a), then (b), then (c) and (d), then one rebuild, site check, validation and documentation.
**Fourth pass, result and rerun (October 13).** `wf_b4c470bf-1ec` (6.4 hours, 4.6 million tokens) wrote results for 20 of 36 cities (42 finalists: 16 accepted, 3 outside_window, 19 not_found, 4 incomplete). Checker verdicts: `pass4_checker_verdicts.json`.
Defect: for the other 16 cities (57 finalists), macOS refused the agents access to `~/Documents` ("Operation not permitted"). No search ran for them and no file exists.
Hanno decided on October 13 that the rerun of these 16 cities completes the approved fourth pass; it is not a fifth repair attempt.
Rerun `wf_51ccf5e6-dc5` (same script, `args.out` set to a folder outside `~/Documents`, in the session scratchpad `pass4_rerun/`), jobstatus `mayoral_search_pass4_rerun`. Wayback from mac2 answered only part of the requests at the start.
Next: copy the 16 files into `pass4_20261010/`, merge the checker verdicts, rebuild the leads, then run the `stage3v` scrape from the droplet (the archive does not answer the home network).
Rerun result (October 14, 18.6 hours, 2.4 million tokens): 57 finalists, 38 accepted, 3 outside_window, 16 incomplete. Brave (HTTP 429) and the archive (HTTP 503) blocked mac2 from about 07:00 on October 14, which caused most incomplete entries.
Integrated: 16 files copied, verdicts merged (one downgrade: Pulido 2018). The coordinating agent replaced two Tulsa capture links that had only a date (Bynum 2016 and 2020) with real in-window captures. Whole pass: 53 accepted, 7 outside_window, 19 not_found, 20 incomplete (not applied).
`pass4_url_leads` is registered in `config/mayoral_top_two_build.json`. The coverage build ignores pages from a rejected domain when a pass 4 lead replaces it (Healy 2013, Beutler 2011, Fraim 2014, Pulido 2012).
Coverage of 860 finalists: URL lead 728 to 769, archive capture 678 to 725, downloaded text 673 to 669 (the four rejected sites). 56 tests pass.
Hanno decided on October 14 to search the 20 incomplete entries again as part of pass 4. Run `wf_4320f46a-cdc` (13 cities, outputs in the session scratchpad `pass4_incomplete/`, jobstatus `mayoral_search_pass4_incomplete`). The script now takes `args.wayback_host`; this run sends archive requests through the droplet, so it does not share mac2's archive access with the stage3w scrape. Brave stays on mac2.
Integration rule: each new result replaces the incomplete entry in the city file of `pass4_20261010/`, with a note naming the run; checker problems are added to `pass4_checker_verdicts.json`.
Result (1 hour, 1.7 million tokens): 5 accepted, 2 outside_window, 6 not_found, 7 still incomplete (Brave block pages through mac2 for Riverside and others; droplet archive refusals for Plano). The checker found coxformayor.com for Cheryl Cox 2010; the coordinating agent read capture 20100125192621 and accepted it. Merged into `pass4_20261010/`.
Whole pass 4: 58 accepted, 9 outside_window, 25 not_found, 7 incomplete (not applied). Coverage of 860 finalists: URL lead 775, archive capture 730; downloaded text waits for the scrapes. 56 tests pass.
Batch `mayoral_stage3x_20261014` (6 targets: Cox, Cashell, Mosby, Vazquez, Lane 2016, Jones 2012) is frozen and copied to mac2; a waiter starts it when the stage3w process (PID 44037) ends.
The 7 incomplete entries stay unresolved: pass 4 is complete. A further try needs Hanno's decision.
**Scrapes done (October 14).** stage3w: 3,000 pages fetched (2,138 capped, 862 uncapped), 54 of 57 targets with pages in the window, 1 retryable. stage3x: 72 pages, 4 of 6 targets with pages in the window. stage3v: 44 pages, 4 of 4.
Summaries `inputs/stage3{v,w,x}_saved_pages.csv` (made on mac2, all files hash-verified) are registered. The coverage build now keys page summaries on finalist and domain and uses the one on the checked lead's domain (7 finalists have pages on two domains); test added, 57 pass.
Coverage of 860 finalists in 430 elections: URL lead 775, archive capture 734, downloaded text 731 (was 669). Both finalists with text: 323 elections (75 percent).
Next authorized step: release rebuild — page manifest with the new batches, page corpus and page rules, Hive site check for the new sites, release build, validation, README and codebook update.
**Release rebuild v2 (started October 14).** Work folder mac2 `~/candidate_website_extension_stage3/release_mayoral/v2_20261014/` (current corpus script copied to mac2; the October 10 v1 corpus predated the defect fixes).
- Target map and page manifest: 38,333 pages, 731 finalists, all with pages in the window.
- Page corpus `mayoral_page_corpus.parquet`: 7,964 pages excluded by rule (21 percent, above the 20 percent line; 6,732 are listing pages under the approved defect 2 rule, 685 empty, 474 parked, 112 error or challenge, 41 binary, 21 spam), 10,048 duplicates; 716 of 731 finalists keep usable pages in the window.
- Site check: 57 new sites plus 46 negative controls. Hive job 24794739 (low, Qwen3.6-27B-FP8 `e89b16eb`) finished October 14 at 19:02; job 24794736 was cancelled. The local watcher missed the finish and lost about 1.5 days. New sites: 46 yes, 11 no. Controls: 33 no, 6 unclear, 7 yes, the same as both earlier runs. Agent review `wf_8c813751-617` (input `release_site_check/review_args_3.json`, verdicts `agent_review_verdicts_3.json`): 8 pass, 3 excluded (Pulido 2016 and 2018, Shannon Wright 2024; content only from another election). `site_check.csv` now has 722 sites: 708 pass, 14 excluded. Hive output `hive_decisions_3.jsonl`.
- Build fix: `build_mayoral_release.py` did not read pass 4, so its incomplete entries were not labelled `check_incomplete`. It now takes the pass 4 status as the latest for its 99 finalists; test added, 63 tests pass.
- **Release candidate v2 built and validated (October 16).** `build/mayoral_release_v2_candidate/` (six data files, `manifest.json`, `README.md`, `CODEBOOK.md`; documents also in `docs/mayoral_release/`). Validator report `quality_reports/mayoral_release/release_v2_candidate_validation_2026-10-16.json`: valid, no errors.
- 704 of 860 eligible finalists have usable text (v1: 650 of 822); 302 of 430 elections (70 percent) both finalists, 377 (88 percent) winner, 402 (93 percent) any. 75 no_url_lead, 37 site_not_archived, 15 no_usable_pages, 13 excluded_site_check, 11 check_incomplete, 5 site_rejected. 38,333 pages, 21,040 usable.
- Two page drops are above 20 percent: page rules 20.8 percent (6,732 listing pages, approved defect 2 rule) and duplicates 26.2 percent (10,048). All duplicates are within one finalist and 99 percent are at a different address: the approved URL-normalized duplicate rule and the uncapped runs cause them. No decision needed.
- README and codebook updated (loading example in Python and R added and run, approved item (d)): v2 counts, the new page rules, the duplicate rule, `site_rejected`, and the columns from approved items (c) and (d) (`person_id`, `incumbent`, `top_two_vote_share`, `total_votes`, `vote_share`, `phase`, `n_pages_before_election`). Terms of use filled in (CC BY 4.0, takedown hhilbig@ucdavis.edu). `total_votes` and `vote_share` exist for only 38 eligible finalists, because few official results record a total. `incumbent` is empty for 198 (a city's first election in the data).
- README and codebook rewritten for concision and plain language (commits `06c5a19`, `4c38b9b`, `c6831cc`): README 1,500 to about 900 words, no process history, each fact stated once. Pangram before the cut: README 80 percent AI, codebook 88 percent (`quality_reports/mayoral_release/pangram_check_2026-10-16.md`). Hanno approved both documents on October 16.
- **First-user review (October 16).** Two Opus agents, acting as new users, reviewed the release folder only. No count or key errors. Main finding: the panel joins pre- and post-election pages (37 percent of usable pages are after the election; 89 finalists have none before). Hanno approved: (1) fetch pre-election copies; (2) flag `also_in_election`; (3) drop `total_votes` and `vote_share`; (4) collect party, city partisanship, county FIPS and incumbency for each city's first election where possible; (5) keep `text_clean` as the ICPSR rule. Documentation fixes and page rules for non-pages need no decision.
- **Pre-election refetch running (October 16).** `scripts/build_mayoral_prefetch_queue.py` picked, from the existing capture inventories, the latest capture in the 365 days before the election for every page whose copy is from election day or later: 5,925 copies in 15 batches (`runs/prefetch/` in each batch, audit `release_mayoral/v2_20261014/prefetch_queue_audit.json` on mac2). `scripts/run_mayoral_prefetch.sh` on mac2, jobstatus `mayoral-prefetch-pre-election`, log `release_mayoral/prefetch.log`, about 1 page per second. Next: page manifest (run `prefetch` added), page corpus (pre-election copies now win the duplicate check; new rule `not_a_page`), release build v3, validation, documents.
- **Refetch done; v3 build (October 16).** `prefetch.log` ends "all done fail=0"; about 5,900 of 5,925 copies fetched. The v3 page manifest (`release_mayoral/v3_20261016/` on mac2) has 44,189 pages (v2: 38,333); 43 refetch copies that an earlier run already held are skipped (`build_mayoral_page_manifest.py`, test in `tests/test_build_mayoral_page_corpus.py`). The first corpus run stopped: 8 raw HTML files in `mayoral_stage3_20261002/runs/production/raw_html/` were gone (folder changed October 16 01:38 PDT; no session, cron or launch agent explains it; text files intact). Repair 1: refetched the 8 into `runs/repair_raw8/`, all 8 SHA-256 equal to the original record, copied back with `cp -n`. Corpus rebuilt.
- **v3 release candidate (October 16).** `build/mayoral_release_v3_candidate/`, validator: valid, 0 errors. 703 of 860 finalists have text (v2: 704). Usable pages from before the election: 17,640 of 20,857 (85 percent; v2 61 percent). Panel finalists with no pre-election page: 27 (v2: 89). Changes in `text_status`: Lincoln 2019 Cyndi Lamm gains text (her post-election domain copy is an opposition page); Chula Vista 2014 Mary Salas (only a Bluehost placeholder in the window; her real pages are 382 days before) and Corpus Christi 2014 Nelda Martinez (only `robots.txt`) lose it. Next: documentation fixes from the first-user review with the v3 counts.
- **Second first-user review and v3b (October 16).** Two Opus reviewers on v3 reproduced every README count and checksum. Defect found: `duplicate_of` could point to an excluded or out-of-window copy (370 distinct pages, 47 finalists, about 2 million characters lost). Fix in `build_mayoral_page_corpus.py`: only a page that passes the rules inside the window becomes the kept copy; validator now requires this. `not_a_page` extended (theme, plugin, `cdn-cgi`, CGI, `.js.php`, SiteGround demo). v3b corpus `release_mayoral/v3b_20261016/` on mac2, release `build/mayoral_release_v3b_candidate/`: valid, 704 finalists with text, 21,220 usable pages, 84 percent before the election, 28 panel finalists without a pre-election page. Open: one site never site-checked now has one kept page (Milwaukee 2024, David D. King, `kingformayor.com`); include it in the next site check. Decisions put to Hanno: copy election variables into the panel; a text column with repeated segments removed; README note on nominal runners-up; no 180-day window or language-version rule; a hand-checked sample of 50 sites.
- **Hanno's decisions and Opus site audit (October 16).** Approved 1–4 (panel gets `stage`, `election_type`, `top_two_vote_share`, `population_2020`, `text_unique`; README notes). Hand check replaced by an Opus model audit at Hanno's request: `quality_reports/mayoral_release/opus_site_audit_2026-10-16/`. All 36 sampled Qwen passes confirmed; 6 of 82 Sonnet decisions changed in `site_check.csv`. Release `build/mayoral_release_v3b_candidate/` valid: 707 finalists with text, 305 elections with both, 21,344 usable pages. Possible further work (not started, needs Hanno): fetch Lamm's real site `cyndiforthecity.com` and Fraim's 2014 pages; site-check David King (Milwaukee 2024); a page-level check for reused sites (62 finalists have more than half their text on `other_year_only` pages).
- **Hanno: "do as much as we can before release" (October 16).** Done: 24 downloaded but never credited sites checked by Opus (9 pass) and credited through `release_site_check/extra_target_map.json` (manifest `--extra-map`); Lamm's own site fetched (batch `mayoral_stage3y_20261016`, target `stage3y_ec4606b5d9f702bc370e436d`; stage 3 IDs repeat across batches, so this one includes the domain); King passed. v4 corpus `release_mayoral/v4_20261016/` on mac2, release `build/mayoral_release_v4_candidate/`: valid, 712 finalists with text. Running: Hive page check (Qwen3.6-27B, `scripts/hive/classify_mayoral_page_campaign.py`) on 21,361 usable pages, jobs 24828284 (high) and 24828287 (low, writes `labels_low.jsonl`); watcher cancels the one that does not start. Next: labels to `release_site_check/page_campaign_check.csv`, rebuild the corpus with `--page-check` (rule `not_this_campaign`), label any newly usable pages, release v5, documents.
- **Covariates (October 16).** OSF (ALGED, mv5e6) is in planned maintenance until October 19 09:00 EDT. Dataverse doi:10.7910/DVN/TEPRY2 (`elecs_mayors_allDVs_230919.rds`) matches 578 of 630 eligible finalists in 2010–2021 on city, year and last name. Hanno chose a crosswalk to ALGED IDs (October 16). After OSF returns: build the crosswalk from the full candidate-level file, check its license, then size the agent search for the gaps (about 320 finalists, mainly 2022–2026) and ask for cost approval. County parts input: `inputs/city_county_parts_2020.csv` (not yet in the build).
- **Waiting for Hanno:** approval to create the separate Dataverse dataset and upload. At upload, remove the "Draft for review" line; add the citation once the DOI exists. Nothing is published.
Batch `mayoral_stage3v_20261010` (4 sites from defect 3) is complete on mac2: 44 of 44 pages fetched, no new uncapped pages. Its jobstatus entry "failed" is out of date. Its page summary is not yet folded into `inputs/`.
Batch `mayoral_stage3w_20261014`: `build_mayoral_stage3_targets.py` now reads `pass4_url_leads.csv` (a pass 4 lead replaces the earlier lead). 57 targets (52 accepted, 5 outside_window), 47 domains, all from pass 4. Started on mac2 October 14 with `run_mayoral_full_site_batch.sh`, jobstatus `mayoral-scrape-mayoral_stage3w_20261014`; mac2 reached the archive again.
Next: when stage3w ends, summarize the stage3v and stage3w pages into `inputs/`, rebuild coverage, then the page corpus, page rules, site check, release build and validation.


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
