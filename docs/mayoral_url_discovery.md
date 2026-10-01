# Mayoral campaign website URL discovery

## Top-two coverage audit: partial implementation, September 14, 2026

The separate coverage build uses the national 2020 Census city list and completed decisive elections from January 2010 through September 14, 2026.
It currently contains 25 verified elections and 50 eligible candidate-election records. Calendar checks are complete for 96 of 1,700 city-years. The full election universe remains incomplete.
Do not use this subset's percentages as overall coverage or compare them directly with the earlier provisional 56.9 percent.

The build imports official winner and runner-up records. It preserves unresolved stages, certification, final pairs, and city-year calendars.
It separates URL leads, attributed period captures, and hash-verified saved text. Metadata success does not require successful replay.
The 365-day window uses the verified election date, not an assumed day or month.

Run `PYTHONPATH=. .venv/bin/python scripts/build_mayoral_top_two_coverage.py` for an offline rebuild.
Use `data/municipal_discovery/top100_coverage/outputs/manifest.json` for controlling input and output hashes.
See [the coverage report](../quality_reports/mayoral_top_two_coverage_2026-09-14.md) for definitions, queues, tests, and unfinished work.
The earlier discovery universe and reviewed URL table remain separate inputs; this build does not replace them.
The active goal in this task covers election calendars, official results, coverage, and missing-website queues. The separate website discovery and archive collection task owns attribution reviews and collection. Its reviewed outputs will be imported without repeating collection work. The explicit source manifest is `config/mayoral_top_two_build.json`.

## Latest verified working outputs

The full completion goal is active and incomplete. The user approved 214 captures and 19 exact-URL metadata inventories. Preflight checks and runner verification precede launch. The capture batch has a 128 MiB decoded-response cap; metadata has a 32 MiB cap and 300-request limit. Both retain at least 8 GB of free volume space. Raw pages and text stay on the droplet volume. The jobs run sequentially with one worker. The requirement check is in `quality_reports/plans/2026-09-14_mayoral-outstanding-work.md`. Later source and OCR batches still need separate approval.

The approved 19-query inventory completed with all 19 keys marked `COMPLETE`. It returned 1,421 index rows through 20 requests, including one continuation page. Every saved metadata response passed hash and parser checks. The job used 160,323 decoded bytes; its run files occupied 467,642 bytes at verification. Both processes stopped before the capture job began. Compact audits are in `quality_reports/mayoral_inventory_19_2026-09-14/results/`. Raw metadata and the database remain under `inventory_19_20260915/run/` on the volume. The database SHA256 is `c2f95defa91548b25a8dd82945e860e13436986fd69e593045d34ef2af8dac8f`. These results cover the recorded exact-URL queries and windows, not whole-host history or successful page retrieval.

The saved-evidence comparison preserves all 1,421 new index rows. It identifies 467 saved-response matches and 954 unmatched rows within the inspected stores. The next proposal contains 439 distinct captures across 11 candidate-years: 284 HTML-success records and 155 revisits. The reduction from 1,421 rows to 439 proposed captures is 69.1 percent. It follows the approved selection rule: exclude 467 saved rows, retain 514 redirect/error rows separately, and consolidate one duplicate eligible row. No upstream row is deleted. The proposal has zero exact-key overlap with the approved 214-capture batch, but 63 keys overlap the 1,958 deferred acquisition leads. Do not add those counts without deduplication. This proposal requires separate approval; no requests followed from it. Its queue SHA256 is `9a8cfa723d8aeaa665382b8ee4ac44eb7e4e2c54e0aef7fd56c6610761f60aaa`. Local results are in `reconciliation_all1421/` and `capture_proposal_all1421/` under the 19-query report. All six files have matching remote hashes. The local inventory task occupies 2,239,227 bytes, with no raw pages or extracted text.

The approved 214-capture job started at 03:46 UTC on September 15. It resumes a verified one-capture smoke test and its existing byte charge. The separate indexed contract preserves 107 exact URL relations and 107 URL variants. It requires the requested indexed URL and timestamp for a successful capture. The original proposal remains unchanged. The deployed runner SHA256 is `7a8f5b0a37fe957d1c461144b053d061873e1af1f086941bc3d570a9edc7bea6`. Raw pages, text, and the database stay under `indexed_capture_20260915/run/` on the volume. A host-local watcher records state without external notifications. The frozen approval and launch records are in `quality_reports/mayoral_inventory_22_2026-09-14/acquisition_approved_2026-09-15/`.

The six saved-HTML cases now have complete response reviews. All 18 response hashes and their candidate, capture, URL, timestamp, and byte-count bindings match the source audit. Eight responses are parking pages, two need cookies, four are redirects, one is a shell, and three contain empty HTML. None provides recoverable static campaign text. The review is `quality_reports/mayoral_terminal_audit_2026-09-14/saved_html_no_text_ai_review.json`; the coordinator checks are in `saved_html_review_verification.json`. These findings do not establish historical website absence. Alternative period-matched captures remain necessary where the project needs campaign text. The broader parser and export regression suite passed 181 tests, in addition to 99 recent-work tests.

The current capture-review table is `quality_reports/mayoral_inventory_22_2026-09-14/reviewed_inventory_rows_v2.csv`. It preserves all 359 index rows. Of 37 reviewed rows, 32 support mayoral text, three retain caveats, and two have wrong-office exclusions. The exclusions concern Honeman's February 2011 Assembly-campaign capture and Simmons's October 2020 state-representative capture. Romero's metadata-only capture retains a caveat, not an ownership rejection. The 322 unmatched rows remain unreviewed. Nonempty text, content support, and date support remain separate fields; only their supported intersection contributes to the new text count.

Integration caught and corrected a shared-content timestamp error before publication of the reviewed table. Identical Ackerley bytes occurred in two captures. The corrected review preserves November 18 and December 10, 2019 as separate timestamps. Every review now has an exact row-level capture binding. The table SHA256 is `6a9f7a790628d5e20499846dc684cfe3039c856092d9368a3bd66d03679176eb`; it matches the durable copy in `inventory_22_20260914/run/` on the volume. The supporting review is `saved_capture_reviews_v3.json` in the same local report directory. All 98 relevant tests pass. No candidate URL or website approval changed.

The latest working website table is now `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/period_review_complete/candidate_urls_with_source_period_reviews_v2.csv`. Its SHA256 is `dd0975776cb68681343c1e8c7ff6fb5fb7b66fea7c24c886ee93df45812449fd`. It preserves all 1,385 rows, all original fields, and the 41 AI-accepted websites. Thirty source reviews now identify 21 supported discovery leads and nine leads that need further period evidence. Todd Gloria's HTTPS lead gained period support from a verified March 29, 2020 Wayback response. Both its raw and text hashes matched the acquisition log; the text identifies his San Diego mayoral campaign. This validates the representative capture, not every capture or a new website approval. The table and review JSON match their remote hashes under `source_discovery_20260914/work/production/period_review_complete/`. The previous attribution-review table remains the frozen input.

The current deferred-URL result is `quality_reports/mayoral_deferred_scope_review_2026-09-14/v4/`. It preserves all 1,974 input rows: 1,958 acquisition leads, one unresolved India Walton template path, and 15 technical endpoints. Earlier review promoted 289 uncertain rows because campaign-host listings, embedded views, and identifiable pagination remain useful acquisition leads. Two hash-verified saved Yee campaign pages then resolved the unusual query-route scope for three further capture keys. `leland_saved_route_evidence.json` records the exact evidence and limits. Those three requested timestamps remain unacquired; no URL or timestamp substitution occurred. The labels describe acquisition eligibility, not content approval for unacquired captures. No download follows automatically from them.

Hall's supplemental response now has independently verified acquisition provenance. The original request journal at line 522 links its hash to the November 15, 2019 capture. The 365-day boundary still uses an election-month reference rather than a verified election date. `quality_reports/mayoral_ai_review_2026-09-14/hall_alternative_provenance_check.json` records the check. It does not expand the date window or change website approval.

The approved all-timestamp metadata batch covers 19 exact accepted URLs across 18 candidate-years. It preserves each existing inventory window. Earlier inventories collapsed repeated content and cannot establish every timestamp. `quality_reports/mayoral_inventory_19_2026-09-14/proposal/` records the queue, input hashes, and 32 MiB metadata and 300-request limits. This batch precedes the approved capture batch.

The approved acquisition batch contains 214 distinct indexed captures across 18 candidate-years: 194 HTML-success captures and 20 revisit captures. Half use indexed URL variants. The proposal preserves those variants and exact timestamps. It excludes 37 already-saved rows, retains 107 redirect/error rows for separate review, and consolidates one duplicate. Thus, 359 index rows produce 214 proposed downloads, a 40.4 percent reduction; no upstream rows are deleted. The limits are one worker, 2 MiB per response, 128 MiB per batch, and at least 8 GB of volume headroom. Actual response size is unknown; the limit can stop the batch before all captures finish. The frozen proposal is `quality_reports/mayoral_inventory_22_2026-09-14/acquisition_proposal/`. A fresh scan reproduced all 214 proposed keys and found no new saved matches. It checked 36 response files without hash errors. The scan remains limited to the documented saved stores. Its compact results are in `preflight_saved_2026-09-15/` under the same report directory.

The saved-text check covers the 37 matched index rows and their 36 distinct responses. It saved 34 nonempty text files on the volume. All raw hashes, output text bytes, and row bindings passed verification. Compressed text occupies 40,891 bytes remotely; the compact local audit occupies 45,193 bytes. The local and remote audit SHA256 is `ca651d456b04daf907ad8d24a3296a08bebde1629bd7e0b913aa6f9fb88b3ca4`. Use `quality_reports/mayoral_inventory_22_2026-09-14/saved_text_audit.json`; remote text is under `inventory_22_20260914/run/saved_text/`. Nonempty static text does not establish campaign relevance or date validity.

Two responses need separate treatment. Regina Romero's July 9, 2019 response identifies her mayoral campaign in metadata but provides no static body text. Caroline Simmons's October 20, 2020 response identifies her state-representative campaign in its title, description, and hidden footer text. That response must not count as mayoral campaign support. This is a capture-specific decision, not a rejection of her domain. `quality_reports/mayoral_inventory_22_2026-09-14/empty_text_followup.json` records both findings. Integration must apply the Simmons capture exclusion; other dated captures remain eligible for review. All 81 relevant offline tests pass. No new archive requests accompanied these text checks.

The all-work completion goal is active. The latest saved-evidence reconciliation compares the 359 new index rows with 18 existing fetch ledgers, response journals, validation bindings, and acquisition databases. It preserves all 359 rows and their one duplicate. Thirty-seven rows have hash-verified exact Wayback responses; 322 rows have none in the inspected stores. These unmatched rows represent 194 distinct HTTP-200 HTML captures, 20 revisit records, and 107 redirects or errors. Revisit records refer to previously stored content. No extracted-text files were verified for the matched rows. Uncatalogued files and other ledger formats remain outside this check, so unmatched does not establish global absence.

The controlling result is `quality_reports/mayoral_inventory_22_2026-09-14/saved_reconciliation_v2/`, generated by `scripts/reconcile_mayoral_inventory_saved.py`. Its row-file SHA256 is `31d48ab28cf3f83f854c0a57371924ab7209c3274b26ad47d31d50fc39def61c`. Both output hashes match the durable copy under `/mnt/mayoral_wayback_full_snapshots/inventory_22_20260914/run/saved_reconciliation_v2/`. Local outputs occupy 369,474 bytes. The scan made no archive requests and copied no raw pages. All 76 reconciliation, inventory, acquisition, proposal, and URL-screening tests pass. Acquisition of the unmatched captures requires a separately approved bounded batch.

GPT-5.6-authored deterministic URL rules screened all 1,974 deferred capture keys against the accepted campaign hosts. The current result is `quality_reports/mayoral_deferred_scope_review_2026-09-14/v2/`: 1,666 acquisition leads, 293 scope-review cases, and 15 technical endpoints, preserving every original input field. The 15 endpoints are administration or CRM API routes, not removals from the evidence ledger. Review corrected 52 embedded-view routes from exclusion to uncertainty because such views may contain campaign content. This is URL-rule screening, not individual semantic or ownership review. Archive, taxonomy, query, pagination, template, and embedded-view routes still require adjudication; the deferred-capture task is not complete.

The approved metadata-only inventory for 22 URLs is complete. Twenty-one URLs returned records and one returned an empty result within its recorded date window. All 359 CDX index rows are preserved, including one duplicate; 232 rows report HTTP 200 and HTML. These are index records, not newly downloaded pages. The run used exact CDX URL matching with no digest collapse or status/MIME filter. It did not inventory every path on each host or dates outside the recorded windows.

The run is saved at `/mnt/mayoral_wayback_full_snapshots/inventory_22_20260914`. Independent verification checked all 22 raw-response hashes, every stored index row against the raw response, query keys, page sequences, and the frozen byte/request limits. All responses fit on one page, with at most 118 rows per query; pagination and interrupted-query handling passed offline tests. The run made 22 metadata requests, received 40,858 decoded bytes, and downloaded no replay pages. All batch files occupy 329,360 bytes on the volume. Both worker and watcher are stopped. The compact local result is `quality_reports/mayoral_inventory_22_2026-09-14/verification.json`, with per-URL counts and windows. All 20 inventory and verifier tests passed.

Chris Collins is the empty query. His previously AI-reviewed capture is dated April 5, 2012, seven days after the requested inventory window ended on March 29, 2012. This empty result does not contradict that saved capture and does not revoke its review. It also does not establish archive absence outside the query window. Next compare the new index records with all saved evidence before proposing further downloads. Earlier statements below that inventory approval is pending or that these 22 URLs lack queried inventories are historical.

The corrected discovery table contains 1,385 candidate URLs, including 41 unchanged AI-accepted URLs. All 460 queued saved captures have AI reviews; five wrong-candidate URLs are excluded and preserved as reference evidence. Capture review alone does not approve a website or establish historical completeness.

Use `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/candidate_urls_with_capture_reviews.csv` for the latest verified working table. Its SHA256 is `a6e59bd0aaa1972eee9ff80cf52292c14dc6f57988154ce06c861ccd8959ea51`, rechecked on September 14. The synchronized roster and claims are on the droplet in `source_discovery_20260914/work/production/combined_ai_reviewed` under the attached volume. This is an internal working result, not a published release. Earlier tables and progress statements below describe preceding stages.

The September 10 repair report records removal of the original 2.2 GB temporary restore after verification. The repaired archive still exists on the volume. The September 14 storage check found 20 GiB available on the volume and 3.2 GiB on the droplet root disk. Large claim files remain remote; this handoff creates no data copies.

The user approved the prepared 259 exact-URL capture batch on September 14, capped at 128 MiB of decoded responses on the volume. Launch preparation included a fresh saved-key check, persistent byte reservations, restart tests, and a bounded live smoke. Approval does not cover new inventories, URL variants, other paths, or another source tranche. New inventories still require a choice between all timestamps and digest-collapsed captures.

The approved batch launched under `/mnt/mayoral_wayback_full_snapshots/exact_capture_20260914` after 23 offline tests and a successful one-capture smoke. The input queue hash is `8956c6a5f36b32e5e5a2fb9089b8893ab6a12fb425844887ce5bcc76085213c8`; the deployed runner hash is `51712b7d6ae05bf7424a1f7c072c84f7bf62a218ee73e53d94ed655e0eb2a8b2`. The fresh preflight scanned 15 fetch ledgers and found no existing attempts for these 259 keys. Raw and extracted-text hashes, requested/effective URL, and timestamp matched in the smoke. The full run resumes that same SQLite ledger, preserving its byte charge and completed key.

The run used one worker, at most three attempts per capture, a 2 MiB response ceiling, persistent reservations within the 128 MiB total ceiling, and at least 8 GB of volume headroom. Interrupted requests retain their full reservation. A response may be conservatively truncated up to 8,191 bytes below its ceiling; truncated responses are not counted as fetched. A separate host-local watcher wrote `run/watcher.json` without sending external notifications. `run/audit.json` and `run/ledger.sqlite3` hold final state.

All 259 requested captures finished in 259 HTTP requests, with no retries, redirects, timestamp substitutions, or truncated responses. Independent verification checked every raw and text hash, the exact input-key set, and every requested/effective URL and timestamp. The five candidate-years contribute 248, four, three, two, and two captures. Decoded responses total 131,886,343 bytes, below the 134,217,728-byte cap. Raw pages and text occupy 15,074,317 bytes; the run including its database and audits occupied 19,259,495 bytes before the compact verification record was added. Both worker and watcher are stopped. All 28 runner, proposal, and capture-audit tests passed. Only the compact verification JSON was copied to the Mac: `quality_reports/mayoral_capture_completeness_2026-09-14/proposal/acquisition_verification.json`.

This completes the approved exact-capture batch, not the full historical collection. The 1,974 deferred keys and 22 URLs needing new inventories remain outstanding. These downloads do not add AI or human website approvals; their contents still require any analysis-specific review.

The current remaining-capture list is `quality_reports/mayoral_capture_completeness_2026-09-14/after_exact_acquisition/targeted_capture_retry.csv`. It supersedes the original retry list for planning further acquisition, while the original remains frozen proposal evidence. Reconciliation removed exactly the 259 verified keys from 2,233 prior gaps, leaving 1,974 unchanged rows: 69 scheme or www variants and 1,905 other paths or hosts. All 22 inventory-request rows are unchanged. The audit adapter verifies the completed SQLite ledger and saved bytes before recognizing the new captures; future live scans include this batch. Independent readback checked the exact key subtraction, every retained row, inventory rows, and output hashes. All 42 relevant tests passed. The compact refresh outputs occupy 523,246 bytes locally; no raw pages were copied.

## Outstanding work (checked 2026-09-14)

The active task is to list outstanding mayoral collection work and begin the saved-evidence checks.
The first priority is historical capture completeness for the 41 AI-accepted URLs.
The following groups overlap; do not sum them into a single number of missing websites or downloads.

| Work | Current evidence and next action | State |
|---|---|---|
| Capture completeness for 41 AI-accepted URLs | The approved 259 exact captures are downloaded and hash-verified. The 22 new inventories contain 359 rows: 37 match verified saved responses and 322 do not in the inspected stores. Of 1,974 deferred keys, URL screening proposes 1,666 leads, leaves 293 for review, and identifies 15 technical endpoints. | New-index reconciliation complete within documented stores; acquisition and scope adjudication open |
| Two unsure campaign websites | Hall has a substantive alternative at the 365-calendar-day boundary, recorded in a supplemental AI review. Jamison's second saved page is also sparse. Resolve the supplemental evidence and remaining capture options. | Partly complete |
| Thirty supported but flagged proposals | GPT-5.6 review supports all 30 dated captures, with hashes verified. Six later domain changes, three archive errors, and 21 incomplete-evidence flags remain. | Capture review complete; historical checks open |
| Forty-seven proposals without sufficient historical evidence | Triage complete: 28 availability-negative cases need inventory; eight need period-specific evidence. AI review of the other 11 gives five caveated supports, five rejected captures, and one unresolved case. | Saved-content review complete; remaining checks open |
| Saved evidence for 521 acquisition rows | Sixty-one captures overlap existing validation. All 460 separate captures are reviewed: 214 supported, 125 caveated, 72 uncertain, 44 capture-specific rejections, and five wrong-candidate attributions. All five attribution exclusions are applied; 455 remaining reviews are attached to the working discovery table. | Saved review and attachment complete; evidence caveats retained |
| Priority group of 107 URLs | File audit covers the 19 previous fetch reports: one has matching bytes from the wrong election year; 18 lack matching bytes in inspected stores. Retain metadata for 17 other URLs and review failures for the remaining 71, including one malformed URL. | File audit complete; period evidence open |
| Empty-inventory or terminal-result group of 215 URLs | Audit resolves 209 as empty within recorded query scope. All 18 responses for the other six URLs are reviewed; none contains recoverable static campaign text. Existing Suarez text belongs to 2017, not the requested 2021 cycle. | Saved-response review complete; alternative period evidence remains a collection gap |
| Bounded source discovery | The job exited normally at 21:04 UTC after 2,418 attempts and the byte-budget stop. Attribution v7 removes 21 of the initial 51 new leads, leaving 30. Follow-up saved evidence supports 21; nine still need exact-target period evidence. The new labels are attached without website-approval changes. | Stopped; reattribution complete; nine evidence gaps remain |
| Remaining source queue | The saved completion audit records 44,883 never-attempted sources and 637 retryable sources. Subsequent tranches require their own limits and approval. | Beyond current tranche |
| Deferred source content and access | The saved completion audit records 7,380 unprocessed OCR pages and sources deferred for access or permission. Diagnose these separately; do not bypass access restrictions. | Open |
| Derived outputs after source completion | V7 exports contain 1,390 URLs before review. Prior AI decisions and capture reviews are attached. Five exclusions leave 1,385 URLs and 1,096 active candidate-years with leads. Synchronized roster and claim outputs are verified on the volume; the exact 41 accepted URLs are unchanged. | Rebuild and synchronization complete; promotion pending |
| Storage and reproducibility | Check root and volume free space; retain original archives. Verify durable outputs before proposing removal of temporary restores. | Ongoing constraint |

The source check at 18:23 UTC recorded 264 new attempts started and 100,298,962 new decoded response bytes.
The approved limits are 5,000 attempts, 4 GB of decoded responses, and at least 8 GB free on the data volume.
At that check, root had 3.35 GB free and the attached volume had 23.41 GB free.
No capture downloads, new paid service, additional source tranche, or release publication follows automatically from this list.
The current source database controls live counts; the integrated local table remains the latest verified website table with AI labels.

The saved-capture review compares each queued candidate, office, city, and election year with the corresponding hash-verified HTML.
The first 30 reviews are stored in `quality_reports/mayoral_variant_content_review_2026-09-14/review_*_five.json`.
GPT-5.6 Sol reviewed the remaining records in the numbered review files through `review_351_460.json` in that directory, except the targeted Patricia Watkins/Walls follow-up reviewed by the coordinator.
All 460 queue keys now have exactly one review. This covers the saved-evidence queue, not a representative sample of all project URLs.
Every reviewed key, candidate-year, hash, and timestamp matches the 460-row queue.
David Lee's San Francisco supervisor campaign is incorrectly attributed to Guy Mike Lee in Berkeley in a baseline discovery row.
That URL is not AI accepted; its attribution rejection is applied in the four-exclusion working output below.
The second agent also found Carol Moseley Braun's campaign attributed to William Dock Walls; this mismatch is included in those exclusions.
Officeholder-only pages for Michael Tubbs and Steven Reed remain uncertain, rather than supported campaigns.
The coordinator checked all 90 distinct keys and paired evidence metadata, and corrected those two labels to match the recorded evidence.
The Paul Leon parking page and Scott Griggs council-campaign page are capture-specific rejections, not evidence that those domains never hosted mayoral campaigns.
The [Molly Skold timestamp investigation](../quality_reports/mayoral_variant_content_review_2026-09-14/molly_timestamp_investigation.json) confirms a literal 2022 footer in HTML recorded as an April 2021 capture.
Its acquisition hash matches, but the content date remains unresolved. Do not use it as verified contemporaneous evidence.
These reviews change no acceptance fields and establish neither exact-URL equivalence nor complete historical coverage.

The rebuilt working table with prior AI decisions is `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/ai_review/candidate_urls.csv`.
It is not yet the authoritative published or promoted table: reviewed attribution exclusions remain to be applied.
The integration preserves every v7 source cell and every previous AI field on the 1,360 baseline rows; all 30 new rows remain unaccepted.
The output SHA256 is `d9f12e1f235d2db114654abe437b2eb10a7e6fbe0543163ee8702cf8425281c7`.
An independent readback verified these invariants and the output hash; eight integration tests passed.

Three verified wrong-candidate URLs are now excluded from a subsequent working discovery table at `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review/candidate_urls.csv`.
They concern David Lee assigned to Guy Mike Lee, and Carol Moseley Braun and Patricia Watkins assigned to William Dock Walls.
The 1,390 input rows partition into 1,387 retained leads and three rows in `rejected_reference_urls.csv`, which preserves original fields and review provenance.
Distinct candidate-years remain 1,136, including inactive rows, because both affected candidates retain other leads. The 41 AI-accepted URLs remain unchanged.
The script `scripts/apply_mayoral_attribution_rejections.py` applies only explicit candidate-attribution rejections, not parked-page or wrong-office capture rejections.
It checks queue hashes, candidate keys, paired evidence hashes and dates, and conflicts with prior acceptance. Nineteen exclusion and integration tests passed.
Independent readback verified the exact partition, original cells, accepted count, and output hashes. Full review integration and promotion remain pending.
The exclusion code now also checks that the reviewed target URL still matches the source table, preventing stale-key application to a changed URL.
All 141 parser, exporter, summarizer, integration, and exclusion tests passed; the strengthened checks reproduce the saved outputs exactly.
Review positions 91–120 identify another wrong-candidate URL: David Hernandez's 2017 campaign assigned to David Saltsburg.
This is Saltsburg's only working-table lead and is not AI accepted.
The four verified attribution exclusions are applied in `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_four_exclusions/`.
The current reviewed working output retains 1,386 of 1,390 URL rows; four rejected rows remain in its reference-evidence CSV.
Independent readback verified every original cell, the exact input partition, and both output hashes.
Using the frozen roster's `active_2010_2021` field, provisional active candidate-year coverage falls from 1,098 to 1,097: only `70148__2017` loses its last lead.
Total candidate-years, including inactive rows, fall from 1,136 to 1,135. All 41 AI-accepted URLs remain unchanged.
The previous three-exclusion directory is an intermediate output; promotion and the remaining capture reviews are still pending.
The consolidated `review_summary_151.json` in the variant-review directory separates 13 capture-specific rejections from four candidate-attribution rejections.
The generating script, `scripts/summarize_mayoral_variant_reviews.py`, checks unique review keys, queue and evidence bindings, supported decision labels, and reviewer provenance.
It lists every unreviewed key and retains each reviewed decision, reason, and flag. It does not confer acceptance or recheck raw bytes itself.
The summary and attribution-exclusion modules passed 23 tests.
The final `review_summary_complete.json` supersedes that partial summary and requires complete queue coverage.
It contains 214 supported captures, 125 caveated captures, 72 uncertain captures, 44 capture-specific rejections, and five candidate-attribution rejections.
The coordinator verified all 460 keys, paired evidence metadata, review-file hashes, and count totals. Eleven summary tests passed, including rejection of incomplete coverage.
The fifth wrong-candidate attribution assigns Denise Gutierrez-Homer's 2021 San Antonio campaign to Justin Macaluso.
All five are applied in `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/`, which supersedes the intermediate exclusion outputs.
Its discovery table contains 1,385 URL rows, down from 1,390, and preserves the five rejected rows separately with original fields and review provenance.
Active candidate-years with provisional leads fall from 1,098 to 1,096; total candidate-years fall from 1,136 to 1,134.
Saltsburg and Macaluso lose their only leads. The exact set of 41 accepted URLs is unchanged.
Independent readback verified the exact partition, all source cells, accepted keys, roster-based active counts, and both output hashes.
Review completion does not establish every requested page, substantive text for every capture, campaign-period consistency, or historical completeness. It adds no website acceptances.
Use [the row-level capture review table](../quality_reports/mayoral_variant_content_review_2026-09-14/capture_reviews.csv) to inspect all 460 decisions beside the original evidence paths, hashes, timestamps, and candidate information.
Its separate `ai_capture_*` fields contain the review decision, reason, reviewer, model, date, source file, and remaining flags. They are not website-acceptance fields.
The table is 455,165 bytes. Independent readback checked every original queue field and every attached decision, reason, and flag; 12 summary and row-export tests passed.
The generating command uses `scripts/summarize_mayoral_variant_reviews.py --require-complete --rows-output`; `capture_reviews_summary.json` records input, script, and output hashes.
The corrected working table with capture reviews attached is `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/candidate_urls_with_capture_reviews.csv`.
It preserves all 1,385 retained rows and every existing field, including the 41 accepted decisions. Separate capture-review fields are populated for 455 rows; the other five reviewed URLs are in the rejection reference file.
The attachment script rejects unmatched identities, overlapping reference rows, changed target URLs, field overwrites, and unapplied candidate-attribution rejections.
Independent readback verified every original cell and all attached review fields; 31 attachment, summary, and exclusion tests passed.
The combined parser, exporter, summarizer, review, exclusion, attachment, and queue checks passed 184 tests after integration.
The final integrated CSV and all its recorded input hashes match the attachment audit.
The synchronized roster and claims are saved on the droplet under `/mnt/mayoral_wayback_full_snapshots/source_discovery_20260914/work/production/combined_ai_reviewed`.
All 93,776 source claims retain their keys and original cell values except the summary disposition, eligibility, and review flag on the five rejected baseline claims.
The five rejected claims now have `summary_eligible_url_lead=False` and explicit exclusion provenance. Original eligibility fields remain input history, not current eligibility.
All 4,817 roster rows and their URL counts are verified. The two candidates losing their only lead now have `discovery_status=unresolved`, with their prior status retained separately.
All remote output hashes match the saved audit. Only the compact audit was copied to the Mac; large claim files remain on the volume.
See `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/synchronization_verification.json` for verification details. All 191 combined tests passed.

The capture audit identified an additional scope limit: the original inventory uses digest-collapsed prefix queries with a 50,000-row limit and no pagination.
Saved-inventory completion therefore does not establish every historical capture timestamp.
The audit will report truncation and unqueried targets before any download proposal.
Earlier validation code also kept response bodies in memory while it wrote hashes to CSV.
A historical fetched status alone therefore does not prove that its raw response was persisted.
The 19 priority fetch reports require an actual file or a verified copy elsewhere in the corpus.

The [capture completeness report](../quality_reports/mayoral_capture_completeness_2026-09-14/report.md) documents 2,233 never-queued capture keys.
These comprise 259 exact URLs, 69 scheme/www/default-port variants, and 1,905 other paths or hosts.
The exact-URL subset has an estimated 12.49 MB raw size, or 2.65 MB compressed, excluding text and metadata.
All estimates require a bounded launch before new requests. The 22 new inventories have unknown capture counts and byte needs.
The saved inventories collapse content digests; an all-timestamp inventory is a separate scope decision.
The combined checks passed 26 tests.

The [exact-URL acquisition proposal](../quality_reports/mayoral_capture_completeness_2026-09-14/proposal/README.md) preserves all 2,233 upstream gaps.
Its first batch selects 259 exact captures and defers 1,974 variants or other-path captures for target review.
The proposed decoded-response ceiling is 128 MiB; one worker and an 8 GB free-space reserve would limit the run.
These are proposed limits, not a launched job or a guarantee that every capture fits.
Fourteen proposal and audit tests passed.
At the 18:40 UTC check, the source supervisor and worker remained live, with 474 tranche attempts started.
Root had 3.35 GB free and the data volume had 22.56 GB free.

The [priority-file audit](../quality_reports/mayoral_priority_files_2026-09-14/report.md) located a saved 2019 Bill Saffo response behind a 2011 lead.
That match does not establish 2011 campaign evidence. The other 18 reported fetch URLs lack matching bytes in the inspected stores.
All 19 URL keys and historical reports remain preserved for period-specific follow-up.

The [47-proposal follow-up](../quality_reports/mayoral_unresolved_followup_2026-09-14/report.md) records the remaining inventory, timing, and content checks.
The saved William Bell page concerns Birmingham rather than Durham. The saved Sunshine Grund page remains a personal resume.
Five other saved pages provide campaign support with explicit caveats; those findings do not automatically change the accepted table.

The [Hill URL origin audit](../quality_reports/mayoral_priority_files_2026-09-14/hill_url_origin_audit.json) compares the malformed lead with its original saved model response.
The response placed a campaign email in the website field; its saved hash matches the recorded hash.
The voter-guide source attempt returned HTTP 403 with no saved body. A local content search found no matching copy.
This check establishes the extraction error, but it does not verify the original voter-guide text or a replacement website.
Keep the email-like URL outside acquisition. Do not remove `info@` and treat the remaining domain as a verified campaign website.

At the 19:12 UTC process check, source supervisor 1307203 and worker 1307204 remained live.
The latest progress record, from 19:11 UTC, reported 979 attempts and 1,676,564,863 charged bytes against the approved 4 GB limit.
Charged bytes include retained reservations; they are not the actual downloaded byte count.
The data volume had 23.08 GB free, above the 8 GB reserve. The progress record reported no paid requests.

## Integrated AI review (2026-09-14)

Supplemental GPT-5.6 review compares each of the 30 flagged proposals with its saved primary or supplemental capture.
All 30 captures support the named candidate's mayoral campaign. All decoded hashes match the saved evidence records.
The coordinator verified the 30 unique candidate keys, source-table hash, evidence hash/date pairs, and retention of every original flag.
See [review A](../quality_reports/mayoral_ai_review_2026-09-14/flagged_capture_review_a.json) and [review B](../quality_reports/mayoral_ai_review_2026-09-14/flagged_capture_review_b.json).
These supplemental decisions do not update the main table's acceptance fields or establish full historical coverage.
The current main table therefore still marks 41 URLs as AI-accepted. Later domain changes do not invalidate an earlier supported capture.
Use [the validation table with capture reviews](../quality_reports/mayoral_ai_review_2026-09-14/validation_with_capture_reviews.csv) to inspect these decisions beside the original validation fields.
It preserves all 120 rows and all 186 original columns. The separate `ai_capture_reviewed` field identifies the 30 supplemental decisions.
Blank capture-review fields mean that this supplemental review did not cover the row; existing AI decisions remain in their original fields.
The [attachment script](../scripts/attach_mayoral_capture_reviews.py) verifies keys, paired evidence hashes and dates, and retention of source flags.
The coordinator independently checked every original cell and the output hash; 27 attachment and integration tests passed.

Use `data/municipal_discovery/exhaustive_source_expansion_20260908/ai_review_20260914/candidate_urls.csv` as the current website table with AI review status.
The table retains all 1,360 URL rows and every original field from the frozen corrected table.
It identifies 41 AI-accepted URLs across 40 candidate-years and preserves two unsure decisions.
These URLs already existed as discovery leads, so this step adds no URL rows or candidate coverage.
The added fields record the model, decision, reason, capture timestamp, remote evidence path, and file hash.
The original candidate roster and source claims remain the inputs for universe and source provenance.
Use `ai_accepted_website=True` for this accepted subset; do not interpret the original human-approval fields as AI decisions.
The generating script is `scripts/integrate_mayoral_ai_review.py`; rerun it after a future source rebuild to reattach these decisions.
All 41 accepted URLs have saved, hash-verified campaign HTML with extractable text.
No new download is needed for at least one usable page per accepted URL.
Separate persisted text files and the complete historical inventory were not audited.
See [the integration report](../quality_reports/mayoral_ai_review_2026-09-14/integration_report.md) for evidence and reproduction details.

## GPT-5.6 website review (2026-09-14)

The user authorized AI review of the 43 automatically cleared proposals.
Two GPT-5.6 Sol agents accepted 41 and marked Matt Hall and Greg Jamison unsure because their saved pages lack substantive text.
All 43 saved evidence hashes matched.
The new [validation table](../quality_reports/mayoral_ai_review_2026-09-14/validation.csv) adds explicit AI review fields to all 120 original rows.
It preserves every prior field and records no human approvals.
Use `ai_reviewed` with `ai_review_decision` to distinguish review completion from acceptance.
The checks passed 103 tests. See [the report](../quality_reports/mayoral_ai_review_2026-09-14/report.md) for provenance and limits.

## Source discovery resumed (2026-09-14)

The tranche finished at 21:04 UTC with exit code zero. Both process IDs are absent at the completion check.
The database passed its quick integrity check. Its tranche record contains 2,418 attempts and no outstanding reservations.
New decoded downloads total 703,592,260 bytes. Charged bytes total 3,987,177,476 because uncertain transport errors retain their full charges.
The runner stopped with `tranche_byte_limit`; this is not source-queue completion.
The final export contains 1,411 URL rows, compared with 1,360 in the corrected September 10 baseline: 51 additions and no removals.
Reported active candidate-years with leads rise from 1,093 to 1,101, but these additions remain unreviewed.
Some additions are government and reference pages. Do not promote this export or interpret the reported increase as verified campaign coverage.
Only compact audit and URL files were copied into `quality_reports/mayoral_source_restart_2026-09-14/completion/`.
Both copied URL hashes match the remote export manifest. Raw pages and large claim tables remain on the droplet.
The current AI-labelled website table remains authoritative until the new ownership checks pass.

The local attribution version `mayoral-source-attribution-v6` addresses publisher-labelled websites, press-release paths, and government or officeholder context.
A read-only check of the 315 previously eligible saved claims retains 296 and excludes 19.
The exclusions comprise six government or officeholder claims, two C-SPAN reference claims, and eleven press-release claims.
These are claim counts, not unique websites or candidate-years. All source claims remain preserved.
An initial rule also excluded three valid campaign links; candidate-name labels now preserve those links, with regression tests.
All 114 parser, source-runner, and summarizer tests pass locally.
The completed droplet ledger still uses the prior rules. Reattribution and export verification remain required before promotion.

Reattribution v6 subsequently completed for all 88,412 claims across 5,383 sources.
The backup comparison found no changes to attempts, current acquisition records, sources, contexts, candidates, or claim keys.
The database passed its quick integrity check. All 32 unmatched claims were already unresolved before this repair.
The repaired export contains 1,394 URLs, down from 1,411 before this repair.
It removes 17 of the tranche's 51 new leads (33.3%); each removed URL is a government or reference page.
All 1,360 September 10 baseline URLs remain. The other 34 new leads still require evidence review.
Reported active candidate-years with leads are 1,100, compared with the frozen 1,093; this is discovery coverage, not new acceptance.
The copied repaired URL file matches the export hash. See `quality_reports/mayoral_source_restart_2026-09-14/completion/v6/`.
Retain the database backup until the remaining output and evidence checks finish. No acquisition restarted during this repair.

Review of the 34 surviving new leads found another defect: linked campaign labels named other offices, but the check examined only page titles.
Attribution v7 checks the linked label for Congress, Senate, state representative, and state assembly campaigns, and recognizes explicit government-website context.
The pre-deployment check retains 288 of the 296 eligible claims and excludes eight: five other-office claims and three government-page claims.
All 120 related tests pass, including a check that unrelated office mentions elsewhere on a page do not invalidate a campaign link.
Saved-ledger reattribution v7 is in progress. The original pre-v6 backup remains available; no new downloads are involved.

V7 subsequently finished with exit code zero. The complete acquisition-record and claim-key comparison matches the pre-repair backup.
The repaired table contains 1,390 URLs: all 1,360 baseline URLs plus 30 new leads.
The two repairs exclude 21 of the tranche's original 51 additions (41.2%), for government, reference-page, or wrong-office evidence.
Eligible source claims decrease from 315 to 288. Reported active discovery coverage is 1,098 candidate-years, compared with 1,093 in the frozen baseline.
The [preservation check](../quality_reports/mayoral_source_restart_2026-09-14/completion/v7/preservation_check.json) records counts and hashes.
The new-lead review must retain uncertainty about campaign year, office, and historical capture dates; these are not archive-validated acceptances.

The approved source tranche started on the droplet at 18:05 UTC.
It permits at most 5,000 new source attempts and 4 GB of new decoded response bodies.
Persistent reservations preserve both limits across restarts.
Uncertain errors can cause an early stop because the runner retains their byte reservations.
The runner uses two workers and preserves at least 8 GB of free disk space.
No paid search or storage purchase is authorized.

The first new fetched source passed its content-hash check and has saved parsed and text files.
Both child and supervisor processes were active at the launch check.
The Slack helper received the start message. The watcher reports stalls and completion for Mayoral URL discovery.
Helper invocation does not confirm delivery to Slack.

The current remote run is
`/mnt/mayoral_wayback_full_snapshots/source_discovery_20260914/work/production` on the droplet.
Its database and progress files control live counts. Local September 10 files remain the frozen baseline.
The restore passed database integrity and 29,020 saved-content hash checks.
All 113 source tests passed locally and remotely, including actual PDF and OCR checks.
See [the launch report](../quality_reports/mayoral_source_restart_2026-09-14/report.md) for limits and restart instructions.

## Saved-evidence follow-up (2026-09-13)

Saved files resolve some apparent acquisition gaps without new archive requests.
Among 521 reviewed URL rows, 26 have an exact effective response and 422 have same-path scheme or host variants.
The other 73 have different paths, queries, or hosts.
The audit verified 802 selected file hashes; 434 rows have same-path responses within the 365-day window.
Syntactic variants do not establish URL equivalence or campaign ownership.

Among 107 priority URLs, 36 have earlier capture metadata and 19 have historical fetch reports.
Those 19 still need checks for file presence and effective target.
One malformed email-like URL requires review before any request.
The 215 empty-inventory or terminal-response cases remain a separate assessment group.
All original rows remain preserved. Discovery coverage remains 1,093 of 2,788 active candidate-years.

The combined checks passed 121 tests.
See [the follow-up report](../quality_reports/mayoral_saved_evidence_followup_2026-09-13/report.md).
The source crawl remains stopped.
Its [restart proposal](../quality_reports/mayoral_saved_evidence_followup_2026-09-13/source_restart_proposal.md) requests at most 5,000 additional source attempts and 4 GB of new downloads.
No paid search or storage purchase is proposed. The new tranche requires approval.

## Agent review and acquisition reconciliation (2026-09-13)

The agent review recommends 41 URLs across 40 candidate-years, including 17 candidate-years without a URL in the frozen discovery baseline.
Two other proposals have campaign metadata but sparse Wix page bodies.
The coordinator inspected the main campaign evidence for 30 flagged proposals and retained their flags.
These advisory results add no human approvals.

The corrected roster still contains leads for 1,093 of 2,788 active candidate-years.
The completed full-snapshot corpus and four earlier batches provide exact-URL responses for 427 active candidate-years.
They provide responses on the associated candidate/domain for 801 active candidate-years.
These counts overlap and do not establish campaign content for every candidate.
The completed corpus has exact agreement across 291,044 capture keys and no missing saved raw or text paths.

The acquisition review queue contains 843 URL rows. Most need checks against saved pages or existing metadata.
The priority group contains 107 URL rows across 97 candidate-years for further evidence checks before archive inventory.
Source discovery remains a separate, paused queue. A new download ceiling requires approval before resumption.
The combined test run passed 91 tests.
See [the review and reconciliation report](../quality_reports/mayoral_review_reconciliation_2026-09-13/report.md) for evidence and limitations.

## Current discovery state (2026-09-10)

The ownership repair retains 120 of 225 source-expansion website proposals.
They add leads for 55 candidate-years, bringing the active total to 1,093.
All remain discovery leads pending review. The repair preserves all source
claims and acquisition records. The source crawl remains stopped and incomplete.

Use the corrected compact outputs in
`data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/`.
The report is `quality_reports/mayoral_ownership_repair_2026-09-10.md`.
The production and September 9 completion directories contain earlier results.

Historical validation of all 120 repaired proposals now supports campaign
content for 73 URLs across 70 candidate-years. Forty-three clear all programmed
checks; 30 retain review flags. The remaining 47 lack
sufficient evidence. Twelve live availability false negatives were corrected
using verified saved captures, and one résumé false positive was downgraded.
All rows and original discovery counts remain saved; human approvals remain
zero for these proposals. Use
`quality_reports/mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv`
and the accompanying report for the final historical assessment. Verification
passes 92 tests. The follow-up used saved evidence and added about 0.8 MB of
compact local audit outputs. See `quality_reports/mayoral_validation_flags_2026-09-10.md`.
It separated six genuine domain changes, three archive service errors and
insufficient surrounding evidence. Human review remains pending.

## Historical baseline and launch (2026-09-08)

The reconciled roster contains URL leads for 1,038 of 2,788 active 2010–2021
candidate-years. Reconciliation recovered 31 omitted cases and suppressed one
previously rejected URL, a net gain of 30 over the previous combined list.
All 4,817 candidate-years and 5,364 independent source claims remain saved.

Reprocessing 414 saved source pages, parsing 630 saved search snippets, and
testing 50 live source URLs adds five more candidate-years with source-attributed
URL leads. The working total is 1,043. These additions remain unreviewed
discovery proposals. They do not add human approvals or archive-validation
successes. Existing archive availability and downloaded-content fields are
separate outcomes.

The initial source queue contains 52,472 distinct URLs linked to 76,130
candidate-source records. The source pass follows candidate references,
preserves exact URLs and source context, and records failed or deferred work.
It uses saved search results and public source requests; paid search requests
remain zero. Search snippets alone cannot establish URL ownership because they
can combine unrelated passages. Mixed mayor/council lists, later campaigns for
other offices, and previously rejected URLs require separate review.

The authoritative local artifacts for this phase are under
`data/municipal_discovery/exhaustive_source_expansion_20260908/production/`.
`roster/` is the frozen reconciled baseline. `combined/` holds the current
candidate list and source claims. `sources.sqlite` is the resumable source
ledger. The parent directory's earlier source-expansion outputs are diagnostic
smoke results and must not supply discovery counts.

The source pass is running on mac2, launched at 02:23:31 UTC on September 9
(September 8 locally). The isolated deployment is under
`runs/mayoral_sources_20260908/` inside the repository. Its source ledger controls
live progress after launch; local outputs are the prelaunch snapshot until
resynchronized. `job_state.json`, `progress.json`, `source_pass.log`, and
`audit.json` are inside its `data/municipal_discovery/exhaustive_source_expansion_20260908/production/`
directory. A detached supervisor uses the existing Slack helper and names this
project explicitly in completion, failure, and stalled-progress notifications.

The runner uses eight workers and one active request per host, including
redirects. It honors Retry-After and saves every attempt before requesting a
page. It stops at 8 GB of newly downloaded raw bytes or 5 GB of remaining disk
space. Documents exceeding 25 MB, unfinished OCR pages, deeper follow-up links,
and access restrictions remain explicit residuals. Repeated empty HTTP 202 or
HTTP 403 responses defer that host. The live test found seven empty Ballotpedia
responses and deferred 831 additional URLs for access review. It also retained
41 unfinished OCR pages from two large PDFs.

Verification: 122 relevant tests passed. The live-test SQLite database passed
its integrity check, and all 1,091 saved response bodies matched their recorded
SHA-256 hashes. The full roster and source-disposition exports have exact key
agreement. The approved plan and detailed execution notes are in
`quality_reports/plans/2026-09-08_mayoral-discovery-roster-and-source-expansion.md`.

## Historical channel-screen results

The first 2010-2021 source-channel screen is complete, and it does not support
full search acquisition. The legacy baseline contains 4,817 candidate-years
and 118 source-backed candidate-years. Under the new evidence rules, those 118
cases remain a frozen legacy benchmark rather than automatically becoming
grade A or B evidence.

Archive-It is the first implemented new channel. Its two fixtures produce 33
claims for 30 candidate-years. Hanno accepted all 33 identity, timing, and URL
matches. Fourteen grade-A custom domains count as strict new candidate-years;
19 accepted grade-C claims remain non-strict.

A first promotion batch added Birmingham 2017, Grand Rapids 2019, and Cleveland
2021. Hanno accepted all five grade-A custom-domain claims. Three increase
coverage; the two Cleveland candidates were already in the legacy baseline.

A second, 20-collection batch produced six new strict candidate-years under a
prespecified sampled-audit rule. Hanno accepted all three sampled grade-A
proposals, for 100 percent observed precision and strict confirmation. Coverage
therefore rises from 138 to 144 of 4,817 candidate-years (2.99 percent), and the
four-through-nine gate calls for one more small Archive-It batch.

The final small Archive-It batch tested 20 unused collections after the strongest
catalog entries had been processed. Only one collection produced a candidate
URL. Hanno approved Sharon Weston Broome's 2016 Baton Rouge campaign domain as
grade A. Coverage rises from 144 to 145 candidate-years (3.01 percent). One
addition falls below the prespecified stop threshold, so broad Archive-It
acquisition is complete. The next active channel is official municipal records.

The first official municipal-record pass is complete. It tested 34 source
targets for the 20 highest-priority 2000 through 2007 cities, saved 76 unique
official files, and produced 103 terminal contest-source attempts. After all 41
PDFs were parsed, it produced no candidate-linked URL claims and no strict
additions. The posted-file channel therefore fails the continuation gate. A
pre-2010 public-record-request test was prepared, approved, and saved as three
Gmail drafts, but none was sent. That test is now paused because a returned
domain cannot count as strict evidence when the Wayback Machine lacks
election-period campaign content. Active acquisition now begins in 2010.

The search pilot is complete and does not justify more search-credit spending.
It recovers three strict URLs among 120 sampled candidate-years, a 2.5 percent
raw yield. Post-stratification raises the estimate to 2.90 percent, or 136
projected additions among the 4,699 unresolved candidate-years, but the 90
percent bootstrap interval spans 0.24 to 6.33 percent. Hanno accepted six of
ten proposed URL leads, for 60 percent lead precision, and confirmed all three
negative audits as negative.

The bounded 2010-2021 search expansion used exactly one Serper query for each
of the 480 sampled candidates. It added six accepted grade-A custom domains to
coverage. Three belong to candidates in the fixed screen; three are contest
spillovers discovered while searching for another sampled candidate. The
blinded candidate-screen yield is therefore 3 of 480 (0.63 percent), not 6 of
480. The weighted estimate is 0.49 percent among the 2,228 candidate-years
represented by the screen, or 10.9 projected additions, with a one-sided 90
percent lower bound of 4.7. The early-period yield is 0 of 160. The channel
meets the prespecified drop rule of no more than three on-sample strict URLs.
No fallback query wave will run.

## Question and comparison

This phase asks which historical sources can recover election-specific campaign
websites for mayoral candidates at high precision and reasonable cost.

The completed initial screen used the same blinded sample of 480 unresolved
candidate-years for every source channel. It draws 120 cases from each of four
eras. The revised next screen will draw 160 cases from each of 2010-2013,
2014-2017, and 2018-2021. A separate revised sample will contain ten complete
contest-years per period. The source evaluator measures marginal and cumulative
strict yield, precision, analyst time, license cost, and cost per accepted URL.

## Scope and assumptions

The current design assumes:

1. The universe is ALGEL mayoral records from 2000 through 2021.
2. A record enters the discovery scope when the mayor is directly elected, the
   contest has at least two recorded candidates, and the city had at least
   100,000 residents in 2020.
3. The candidate-year is the coverage unit. All recorded election rounds remain
   in discovery, while reports distinguish verified general elections.
4. The election month comes from the authoritative universe. Sixty-five of
   4,817 candidate-years have no recorded month. The pipeline retains those
   values as missing and never substitutes November.
5. A source must attribute an HTTP or HTTPS URL to the named candidate and
   election. Search-result guesses are leads, not evidence.
6. No external message is sent without approval of the exact recipient and
   text. No paid source is used without a sample, terms review, cost estimate,
   and approval.
7. The bounded search amendment used exactly 480 Serper credits. It authorized
   no fallback query wave and no further search spending.
8. New acquisition targets elections from 2010 onward. Pre-2010 evidence already
   found remains in coverage, but new pre-2010 discovery is a sensitivity
   exercise unless the source also supplies contemporaneous archived campaign
   content. The project has not estimated a national pre-2010 archive rate.

These rules produce 11,020 mayoral candidate records before the discovery
restrictions and 4,817 in-scope candidate-years in 1,504 contests and 286
cities.

## Evidence and coverage rules

The claim table preserves every independent source claim. It stores a stable
claim ID and source ID, raw URL, case-preserving exact normalized URL, separate
site root, registrable domain, URL type, source date, parser version, candidate
and contest identifiers, election month, evidence grade, and review status.

The grades are:

- **A:** An election-period capture of the exact URL identifies the candidate,
  mayoral campaign, and city.
- **B:** A contemporaneous official source links the URL, and archived campaign
  content confirms the campaign.
- **C:** A source supports the URL, but no qualifying campaign capture exists.
- **D:** Domain shape, current content, WHOIS, DNS, or similar evidence provides
  only a lead.

Only human-accepted grade A or B claims enter the unified claim table as strict
coverage. The primary measure counts candidate-owned custom domains. The
secondary measure also counts candidate subdomains and candidate paths on
shared hosts. Social, fundraising, and officeholder profiles remain leads and
count in neither measure.

Collection-batch evaluations additionally report coverage under a prespecified
sampled-audit rule. When at least 95 percent of sampled machine-grade-A claims
are accepted and remain strict, the evaluation applies that rule to unsampled
grade-A claims in the same frozen batch. The disposition file labels those
claims as inferred rather than human-coded; they do not receive
`human_accepted=true` in the unified claim table.

The headline temporal window is 365 days from the recorded election month. A
sensitivity window starts 18 months before the election and ends 120 days after
it. The Archive-It validator also inspects two years on either side for domain
reuse.

## Implemented pipeline

### Frozen baseline and explicit build

The pre-repair state manifest records 18 inputs, outputs, and pilot files with
byte counts, row and column counts, and SHA-256 hashes. The production combine
step now defaults to
[`mayoral_discovery_build_manifest.json`](../config/mayoral_discovery_build_manifest.json).
It does not discover input shards from ambient file patterns. The
[`mayoral_url_source_registry.json`](../config/mayoral_url_source_registry.json)
controls enabled adapters, inputs, outputs, years, jurisdictions, and attempt
tracking.

The clean manifest build reproduces all baseline denominators and the 118
legacy source-backed candidate-years. The repaired evidence file adds contest
and election-month context without changing the baseline coverage result.

The version-four legacy archive rebuild has exactly 124 candidate-URL keys,
matching the current evidence key set with no duplicate, missing, or unexpected
rows. It finds a capture within 365 days for 101 keys and no capture in the
queried window for three. Public Wayback failures leave 20 keys unresolved.
Forty-three rows are fully checked and 61 retain a partial-query error; both the
61 partial rows and 20 error rows remain retryable rather than being recorded
as completed negatives.

### Attempt ledger

The contest-source universe and source-attempt ledger contain 2,041 exact
targets and 2,041 terminal attempt rows. They have no duplicate or missing
keys. Outcomes are 56 `found`, 1,983 `no_result`, and 2 `request_error`.
Thirty-four target rows have no recorded election month and retain a missing
value.

Every scope filter is written to an audit file. Sixteen of 20 filter stages remove
more than 20 percent because sources are intentionally limited by time,
jurisdiction, or profile availability. The audit records the before and after
counts rather than hiding those reductions.

### Search-pilot repair

The repaired search queue joins direct-domain, embedded-domain, and expanded
source-page leads into one evidence queue. It retains all 16 date-conflict rows
for review. It carries candidate-year, contest, and election-month context and
makes no new search request.

The handcoding importer refuses incomplete results. After review it writes an
adapter-ready claim file and estimates raw yield, post-stratified yield across
the original six sampling cells, projected additions, and a 90 percent
bootstrap interval.

The completed review finds three strict grade-A custom domains, three accepted
grade-C leads, four rejected temporally mismatched domains, and no missed URL in
the three negative audits. The low 60 percent precision among proposed leads
rules out further Serper spending in this phase.

### Archive-It campaign collections

The Archive-It adapter ingests public Partner API seed metadata and unwraps
Archive-It, UCLA, and Wayback container URLs before normalization.

- UCLA collection `5903` contains 55 auditable mayoral seed records covering
  Los Angeles elections from 2001 through 2017. It yields 27 matched claims for
  24 candidate-years.
- Birmingham collection `16766` contains six campaign domains matched to six
  of eight candidates in 2021.

Together they yield 33 claims for 30 candidate-years. Collection-local capture
validation proposed grade A for 14 custom domains, including five Birmingham
claims, and grade C for 19 claims. Hanno accepted the identity, timing, and URL
match for all 33 claims. The 14 grade-A custom domains count as strict new
candidate-years; the 19 grade-C claims remain non-strict. None of the 14 strict
candidate-years overlaps the 118-case legacy baseline.

The first promoted collection batch adds five accepted claims: William Bell and
Randall Woodfin in Birmingham 2017, Rosalynn Bliss in Grand Rapids 2019, and
Kevin Kelley and Justin Bibb in Cleveland 2021. All five exact domains have an
election-period capture that confirms the candidate, mayoral campaign, and
city. Bell, Woodfin, and Bliss are new covered candidate-years. Kelley and Bibb
already had legacy source-backed URLs, so their Archive-It claims strengthen
the evidence without increasing candidate-year coverage.

The collection-catalog enumerator scans the 14,150 public collections once and
applies 297 local city, mayoral, election, topic, and subject queries. It finds
753 unique high-recall collection leads, including 751 beyond the two fixtures.
The one-row-per-collection priority table preserves every matched query and the
raw catalog metadata for promotion review.
Examples include Birmingham 2017, Cleveland 2021, Los Angeles 2007, District of
Columbia election collections for 2018 and 2020, and Grand Rapids 2019. These
are collection leads, not URL additions. Metadata ambiguity and sparse catalog
records require seed extraction and candidate-level temporal validation before
promotion.

The next promotion test froze 20 public collections selected for uncovered
city-years. Seven collections produced 13 claims for 12 candidate-years, and
five collections produced six new grade-A proposals. The run made no Serper
request and used no paid service. A six-item audit sampled three grade-A
proposals, included both ambiguous campaign-content cases, and sampled one
remaining non-strict case. Hanno accepted all three sampled grade-A claims,
accepted two grade-C claims as non-strict, and rejected one officeholder site.
The frozen files and final evaluation are documented
in [`batch20_report.md`](../quality_reports/mayoral_url_discovery_2026-08-25/batch20_report.md).

The final catalog-exhaustion batch targeted 20 unused collections and 66
collection-candidate rows. Only collection `7977`, *Mayoral Campaign 2016*,
produced a candidate match. The election-period page for Sharon Weston Broome
identifies her 2016 mayor-president campaign and Baton Rouge. Hanno approved the
custom domain as grade A. The batch adds one strict candidate-year, so the final
decision is `drop_archiveit_no_more_than_three_additions`. The frozen inputs,
audit, and decision are documented in
[`batch20_round2_report.md`](../quality_reports/mayoral_url_discovery_2026-08-25/batch20_round2_report.md).

### Official municipal records

The early-period priority table selects the 20 cities with the largest number
of unresolved 2000 through 2007 candidate-years. The source inventory records
34 official election, filing, voter-guide, and campaign-finance sources for
those cities.

The municipal adapter reads saved HTML, CSV, TSV, Excel, JSON, and PDF files.
It parses each source once for all candidates in a contest. PDF extraction
includes hyperlink annotations as well as visible text. The adapter emits
stable claims and one terminal status for each contest-source target. No
public-record request has been sent. A city-specific approval packet records
the election years, candidate counts, draft text, and candidate recipient
pages.

The first live pass saved 85 file instances (76 unique files) from 28 of 34
source targets. Seventeen of the 20 cities have at least one saved official
file. Clarksville, Pomona, and Wichita have none. The explicit manifest expands
these sources to 103 source-year targets, and the terminal attempt ledger has
exactly 103 unique contest-source keys: 72 `match_unresolved`, 16 `no_result`,
9 `source_unavailable`, and 6 `request_error`. The evidence queue is empty.
The detailed audit and decision are recorded in
[`municipal_records_phase_report.md`](../quality_reports/mayoral_url_discovery_2026-08-25/municipal_records_phase_report.md).

The three approved requests for Columbus, Austin, and Pomona were saved as
Gmail drafts and paused before sending. They target elections before 2010 and
therefore do not fit the revised acquisition frame. The next municipal priority
table will rank unresolved 2010-2021 contests by archive availability and by
whether official records contain campaign-website fields.

### Historical civic-data providers

The masked provider sample contains 100 candidate-years from 2018 through
2021. Drafts are ready for BallotReady/CivicEngine, Ballotpedia, Democracy
Works, Vote Smart, and LWV/DNet/VOTE411. A usable response must contain the
candidate, office, jurisdiction, election date, round, historical URL,
observation date, and provenance. Current-profile-only data fail the test.

No provider has been contacted and no data have been purchased.

## Screening and decision gates

The completed 480-case screen has exactly 120 candidate-years per era and excludes the
120-case search pilot. Its weights recover 4,579 eligible unresolved
candidate-years. The separate complete-contest sample contains 32 contests and
86 candidate-years. Inclusion probabilities and population weights are stored
with each candidate.

The revised screen contains 480 unresolved candidate-years from 2010 through
2021, with 160 in each four-year period. It excludes all earlier pilot and
screening cases. A separate sample contains 30 complete contest-years, ten per
period. The candidate-screen weights represent 2,228 eligible candidate-years
after the prespecified exclusions.

A channel continues when it produces at least ten new strict URLs among 480
cases or at least five among the 160 cases from 2010-2013. It must also reach at
least 95 percent adjudicated precision and cost less than $100 per accepted
URL. It drops with no more than three strict additions, precision below 90
percent, or cost above $250 per accepted URL.

Adjudicated precision is the share of reviewed claims that Hanno accepts as the
candidate's election-period target URL. Evidence grade is separate: an accepted
grade-C URL improves precision but does not count toward strict yield.

The existing independent confirmation sample is retained as a historical
artifact but will not govern the revised acquisition decision. A replacement
confirmation sample will draw 800 cases from 2010-2021, with 320 from 2010-2013
and 240 from each later period. It will exclude all earlier pilots, screens, and
complete-contest samples. Full acquisition starts
only if projected primary coverage reaches 15 percent, its one-sided 90 percent
lower bound reaches 13 percent, precision remains at least 95 percent, and the
approved cost ceiling is met.

The blinded Archive-It screen result is `hold`. Nine claims fall in the
480-case screen, all nine are accepted target URLs, and five are strict grade-A
custom domains. Adjudicated precision is therefore 100 percent. The weighted
screen projects 17.5 strict additions, with a one-sided 90 percent lower bound
of 6.5. Archive-It is above the drop threshold of three strict URLs but below
the continuation threshold of ten, and it has no strict early-period case. The
subsequent targeted collection batches provide the operative acquisition
decision: the final batch added one strict candidate-year and closed the
Archive-It channel.

The bounded search expansion result is `drop`. Three accepted strict URLs
belong to sampled candidate-years, none is from 2010-2013, and adjudicated
precision is 100 percent. Three additional accepted URLs belong to candidates
outside the sample and remain valid coverage additions, but they do not enter
the blinded yield or weighted projection. The nominal credit value was $0.48,
or $0.16 per on-sample strict URL. Human review time was not instrumented. The
search stack therefore does not proceed to the independent 800-case screen or
full acquisition.

## Current outputs

The baseline files remain under `data/municipal_discovery/final/`. The main new
files are:

- `mayoral_contest_universe.csv`
- `mayoral_source_attempt_ledger.csv`
- `mayoral_source_scope_filter_audit.csv`
- `mayoral_url_claims.csv`

The current claim table has 201 rows: 140 frozen legacy claims and 61 reviewed
new-source claims. The new-source rows contain 32 accepted grade-A claims, 24
accepted grade-C claims, and five rejected grade-D claims. Two of the 32 strict
claims cover candidate-years already present in the legacy baseline. Directly
adjudicated combined coverage is therefore 148 of 4,817 candidate-years, or
3.07 percent. The sampled-audit rule adds three separately labeled inferred
claims, producing combined coverage of 151 candidate-years (3.13 percent).
The unresolved 2010-2021 frame now contains 2,663 candidate-years.
The frozen 118-case legacy benchmark has not been re-adjudicated under the
current grading rules.

The review apps are in
`quality_reports/mayoral_url_discovery_2026-08-24/handcode/` and
`quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit/`. The
20-collection audit, disposition file, and final gate are in
`quality_reports/mayoral_url_discovery_2026-08-25/`.

## Full-snapshot acquisition performance

The production acquisition downloads every inventoried Wayback snapshot for
the frozen candidate-domain targets. The original fetch loop rewrote and
sorted the complete `fetch_attempts.csv` after every page. By 34,000 attempt
rows, this meant rewriting about 27 MB for each new result. A direct benchmark
measured 1.50 seconds for one such rewrite, while the live scraper's median
interval between recent completions was 4.36 seconds.

The replacement writes each completed attempt to a durable append-only
journal. It atomically compacts the journal into the canonical CSV every 250
requests and when the process exits normally. On restart, it reads the compact
CSV and journal together and keeps the latest row for each capture ID. Tests
cover recovery from an un-compacted journal, replacement of a retryable row by
a later terminal row, exact queue-key agreement, and parallel fetching.

A same-URL benchmark compared 60 captures under production-equivalent retry
and timeout settings. One worker took 88.35 seconds, two workers took 62.46
seconds, and four workers took 62.80 seconds. Every run produced the same 56
fetched and four no-content results, with no retryable rows and exact 60-row
key agreement. The production setting is therefore two workers with one
request per second across both workers. Four workers add no measured speed at
that aggregate rate.

The controlled restart preserved 34,270 unique attempt rows. The production
scraper and its project-labeled Slack watcher restarted on August 30, 2026.
The benchmark establishes short-run throughput, not the final completion
date: Wayback refusal bursts and unusually slow pages can still reduce the
long-run rate.

## Reproduction

The baseline and local artifacts rebuild without a paid service:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/validate_mayoral_source_registry.py
PYTHONPATH=. .venv-release/bin/python scripts/combine_mayoral_discovery.py
PYTHONPATH=. .venv-release/bin/python scripts/build_mayoral_source_attempt_ledger.py \
  --universe data/municipal_discovery/final/mayoral_candidate_universe.csv \
  --source-registry config/mayoral_url_source_registry.json \
  --manifest config/mayoral_discovery_build_manifest.json \
  --output-dir data/municipal_discovery/final
PYTHONPATH=. .venv-release/bin/python scripts/build_mayoral_discovery_screen.py
PYTHONPATH=. .venv-release/bin/python scripts/build_mayoral_confirmation_sample.py
PYTHONPATH=. .venv-release/bin/python scripts/validate_mayoral_url_archives.py \
  --workers 4 \
  --audit-output quality_reports/mayoral_url_discovery_2026-08-24/legacy_v4_archive_validation_audit.json
```

Archive-It ingestion and validation use free public endpoints:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/discover_archiveit_mayoral_urls.py \
  --fetch-live
PYTHONPATH=. .venv-release/bin/python scripts/enumerate_archiveit_mayoral_collections.py \
  --fetch-live
PYTHONPATH=. .venv-release/bin/python scripts/validate_archiveit_mayoral_claims.py \
  --attempts 5 --delay-seconds 3
PYTHONPATH=. .venv-release/bin/python scripts/build_mayoral_evidence_claims.py \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit/adjudicated_claims.csv \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit_expansion/adjudicated_claims.csv \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-24/handcode/adjudicated_claims.csv \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-25/batch20_audit/adjudicated_claims.csv \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-25/batch20_round2_audit/adjudicated_claims.csv \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_adjudication/adjudicated_claims.csv \
  --audit-output quality_reports/mayoral_url_discovery_2026-08-25/mayoral_url_claims_audit.json
PYTHONPATH=. .venv-release/bin/python scripts/evaluate_mayoral_source_channels.py \
  --claims quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit/adjudicated_claims.csv \
  --costs config/mayoral_source_channel_evaluation_archiveit.json \
  --output-dir quality_reports/mayoral_url_discovery_2026-08-24/channel_evaluation
```

The bounded search expansion rebuilds from its frozen provider responses and
validation artifacts without spending more credits:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/apply_search_lead_adjudication.py \
  --validation data/municipal_discovery/screening_2010_2021/search_source_expansion/temporal_validation/merged/search_lead_temporal_validation.csv \
  --decisions quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_adjudication/decisions.csv \
  --output quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_adjudication/adjudicated_claims.csv \
  --audit-output quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_adjudication/audit.json
PYTHONPATH=. .venv-release/bin/python scripts/evaluate_mayoral_source_channels.py \
  --screen data/municipal_discovery/screening_2010_2021/candidate_screening_sample_2010_2021.csv \
  --claims quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_adjudication/adjudicated_claims.csv \
  --costs config/mayoral_source_channel_evaluation_search_2010_2021.json \
  --output-dir quality_reports/mayoral_url_discovery_2026-08-25/search_source_expansion_evaluation \
  --expected-screen-size 480 --expected-early-size 160 \
  --early-year-start 2010 --early-year-end 2013
```

The search runner reads its 145-coverage pre-search roster from
`search_source_expansion/frozen_inputs/`. The current roster in the parent
directory has 151 covered candidate-years. Keeping the two paths separate
prevents a coverage rebuild from changing the frozen search contract.

The Archive-It validation is resumable. Completed current-method rows are
retained, retryable errors run again, and stale keys are removed.

## Remaining human decisions

1. Do not send the approved Columbus, Austin, or Pomona drafts unless the
   pre-2010 sensitivity exercise is explicitly reactivated.
2. Approve an exact recipient and message before any 2010-2021 city request is
   sent.
3. Approve a provider sample, terms, quote, and projected cost before any
   purchase.

## Glossary

- **Candidate-year:** one named candidate in one election year.
- **Claim:** one independent source attribution of one exact URL to a
  candidate-year.
- **Strict coverage:** a human-accepted grade A or B claim with an eligible
  campaign URL type.
- **Legacy baseline:** the 118 source-backed candidate-years identified before
  the current A through D grading and human-review rules.
- **Attempt target:** one contest-source pair that an enabled exhaustive adapter
  is expected to process.
