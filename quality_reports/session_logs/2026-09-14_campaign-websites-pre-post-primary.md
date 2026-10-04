# Session log — 2026-09-14

**Session type:** implementation
**Plan:** `quality_reports/plans/2026-09-14_campaign-websites-pre-post-primary.md`

**Final decision, September 15:** Live copies are monthly (every 30 days), plus the local day before and after each primary/runoff. This supersedes weekly and daily live collection. Base forecast: 27.78 GiB; separate 2× allowance: 55.56 GiB. The authoritative forecast is `data/collection_2026/monthly_cadence_20260915/pilot_forecast.json`. Full mode remains disabled; no new storage is approved.

## Work and decisions

The new collection compares each 2026 candidate's website before and after each primary or runoff. It combines archived pages with direct live copies. It preserves the whole approved FEC population, including missing URLs and unknown ballot participation.

The new workflow uses separate modules for the roster, election calendar, acquisition, and outputs. It does not call the old quarterly selection or URL-dropping roster routine. It reuses name parsing and text extraction. Each logical work item has a stable key. Every network attempt has a separate identifier. Raw response bytes and extracted text use compressed, content-addressed files. Repeated observations retain separate records even when their bytes match.

The 30-day windows use jurisdiction timezones. A page must lie on the same side of the election in every applicable timezone. Election-day pages remain in the corpus but do not enter the main pairs. Main pairs require a usable homepage with a candidate name match. Subpages inherit that identity only within the same site and snapshot. Unresolved identity remains explicit. This automated name check is evidence, not a manual ownership adjudication.

Live acquisition follows relevant campaign links through two levels. The operational limit is 60 attempted pages per snapshot and 10 MB per response. The snapshot manifest records omitted links. These limits control storage and requests; they are not completeness thresholds. HTML extraction uses no paid model. A separate browser context handles live JavaScript pages; archived scripts are not executed.

Successful archive inventories are reused for seven days. Failed inventories retain their original request window and retry status. A later successful inventory may supersede an earlier failure only when it covers the earlier window. Homepage and subpage timestamps are checked after redirects. Completed archived captures can be reused without another fetch.

## Deployment

The isolated runtime and data root reside below the existing attached volume's `candidate_2026` directory on the droplet. Python and browser dependencies are installed in that runtime. No existing collection process or system configuration was changed.

## Pilot results

The pilot completed an initial pass and a recovery pass on September 14. It contains ten candidates in each office/party group, including sourced primary-loss and runoff cases. The refreshed roster contains 3,674 same-year House and Senate Democratic and Republican records. The FEC download contained 8,557 records: the approved filters excluded 4,196 other election years, 29 other offices, and 658 other parties. No candidate was excluded for lacking a website. Source files, hashes, roster versions, and lookup outcomes are saved under the remote data root's `rosters/` directory.

| Pilot observation | Count |
|---|---:|
| Candidates | 40 |
| Candidates with a URL lead | 39 |
| Page observations | 693 |
| Usable pages | 500 |
| CDX capture inventory rows | 153,623 |
| Logical request attempts | 845 |
| Candidate-event rows | 101 |
| Paired candidate-event rows | 23 |
| Pre-only candidate-event rows | 3 |
| Post-only candidate-event rows | 1 |
| Uncovered candidate-event rows | 74 |

These are collection counts, not confirmed ballot-participation counts. Participation is unknown for 93 candidate-event rows. The coverage table includes forthcoming events and possible later rounds. Two pilot candidates had successfully completed empty archive inventories; one had no URL. The pilot also observed HTTP redirects and a live page recovered by the isolated browser.

All 227 queued work items have explicit outcomes: 171 complete, 43 empty, two blocked for missing URLs, and 11 retryable. Every request attempt has a finished record. Retryable work remains eligible for future attempts and is never treated as evidence of absent archives.

## Verification and corrections

The saved-file audit verified 681 HTML hashes, 658 extracted-text hashes and lengths, 284 usable archived-page event windows, and 287 selected page/event windows. No artifact or date-integrity assertion failed. The request and candidate-event identifiers are unique. CSV and Parquet outputs exist and retain the full pilot denominator.

Tests cover window edges, election-day exclusion, multiple rounds, redirected timestamps, Memento headers, missing URLs, parser failures, exhausted requests, interrupted crawls, CDX continuation, repeated identical content, URL history, and output equivalence. The CDX pilot exposed an embedded JSON pagination format; the parser now handles it and saves continuation keys between requests. Reporting now distinguishes request failures from unresolved identity.

The forecast originally attributed archive-index costs to live requests. It now allocates each table and index to the records that create it. A measured request failure can support a pilot estimate without being marked successful. Unfinished requests, parser errors, missing files, or an absent size sample still prevent estimation. There is no minimum coverage requirement.

The droplet has about 4 GiB of RAM. Production exports therefore stream database rows and process one candidate's metadata at a time. The live schedule holds only the latest usable homepage date per candidate. The run limit is 20 hours, with live copies first and older live attempts before newer ones. The measured live-request duration extrapolates to about 11 hours for one full-roster pass.

## Historical storage estimate (withdrawn)

The September 15 forecast is about 349 GiB for the remaining full collection, compared with about 19 GiB free. It includes compressed HTML and text, weekly archive inventories, database indexes, exports, roster versions, and a factor of two for growth. It assumes every candidate obtains a URL and takes no credit for identical-content deduplication. The growth allowance is a planning assumption, not a statistical bound. The large archive inventory accounts for much of the storage estimate.

The pilot extrapolates to about 77,600 initial logical requests and 59 hours of serial request time. Remaining scheduled collection is about 1.98 million logical requests. Browser subrequests and future server delays can increase the workload. Most races stop on November 17; the current calendar extends affected later rounds through December 26.

Full collection is disabled because the forecast exceeds capacity. An additional 400 GiB volume would cost $40 per month at [DigitalOcean's published rate](https://docs.digitalocean.com/products/volumes/details/pricing/). No storage has been purchased or resized. The user's approved plan requires a cost estimate and explicit approval before this expense.

Small review copies are in `data/collection_2026/pilot_review_20260914/`. The compressed corpus and complete inventory stay on the droplet. The final coverage report, pilot verification, and forecast in that directory control the measured counts if a later recovery pass changes them.

## Daily operation

The active Codex heartbeat is `collect-2026-campaign-websites`, scheduled daily at 09:00 America/Los_Angeles. It checks the exact running process before launching work, uses the saved environment file without exposing credentials, and stays quiet unless there is a meaningful completion, a failure requiring attention, or a decision. It uses pilot mode until `full_enabled.json` exists. It checks calendar sources weekly and pauses after final collection and export verification.

The runtime is `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime` on `root@droplet`; data are in `../data`. Both September 14 pilot processes finished. Final code deployment, streaming exports, and artifact verification passed on September 15. The deployed output-module SHA-256 matches the local file: `63071b0a8261a9f01aa71fb017f513993fe5ac71c808317433d4e8041855e952`. All 130 focused collection and adjacent snapshot/release tests passed. Review copies of the coverage, pairs, corpus metadata, verification, and forecast have been refreshed locally.

The September 15 due-work pass was launched in pilot mode as PID 1339730, logging to `../data/due_20260915.log`; inspect that exact process and durable status before any further launch. Full mode remains disabled. After storage approval and availability, rerun `forecast`, confirm `storage_fits`, run `enable-full`, and launch the due work detached. Do not overlap another run, purchase storage without approval, or change the agreed population, collection cadence, or snapshot windows to fit existing capacity.

## September 15 daily-pass completion and historical estimate (withdrawn)

PID 1339730 finished; the process is absent and the run lock is free. Its last request finished at 15:27:42 UTC. The daily pass increased the corpus to 881 page observations, including 623 usable pages. There are now 25 paired, four pre-only, one post-only, and 71 uncovered candidate-event rows. Eight work items remain retryable. Every queued item has an explicit outcome and every attempt has finished. Today's due-work pass has already run; do not repeatedly relaunch the same failures during additional same-day checks.

The saved-file audit passed for 857 HTML responses, 821 text files, and 387 selected page/event windows. New review files are preserved separately under `data/collection_2026/pilot_review_20260915/`.

Successful retries increased the full archive inventory from 153,623 to 373,066 records. The updated forecast is 751,068,470,041 bytes, about 700 GiB, versus about 19 GiB free. About 393 GiB is projected capture-table/index storage and 166 GiB is the full inventory CSV export, including the existing factor-of-two growth allowance. This is the estimate for the current storage layout; it is not a statistical bound. The earlier 400 GiB/$40-per-month proposal is superseded and is insufficient for this revised estimate. An additional 800 GiB volume would cost $80 per month at the previously cited DigitalOcean rate. No new storage is approved or purchased. Full collection remains disabled pending a storage decision.

The hostname-based follow-up inspection did not return promptly. A separate connection to the documented Tailscale IP succeeded and independently verified the saved forecast and artifacts. No collection process was inferred to have failed from that connection delay, and no overlapping collection was launched.

## September 15 HTML-only storage correction (authoritative)

The user confirmed that images and other assets must not be saved and requested a corrected estimate. The pilot audit found zero saved non-HTML page payloads. The correction therefore addresses repeated archive metadata and exports as well as enforcing the HTML-only response rule before body retrieval.

The droplet working capture table fell from 373,066 to 23,148 rows: 194,600 known asset URL records and 132,170 other non-HTML metadata records were excluded, and 23,148 repeated eligible rows were merged. This is a 93.8% reduction in working metadata, with no candidate or page observations removed. The migration preserved 881 pages, 261 work records, and 1,082 request attempts. A consistent pre-migration SQLite backup and every compressed raw index response remain on the existing volume. Possible HTML, unknown/revisit MIME records, and non-asset redirects remain eligible. Broad raw index queries continue because MIME-only server filters can hide redirects.

The SQLite file is now 25,624,576 bytes. The verified gzip inventory contains all 23,148 working records in 1,773,080 bytes. Its obsolete 95,046,302-byte uncompressed CSV was removed after verification. New weekly inventories reuse candidate/timestamp/URL records and retain first and latest completed inventory provenance. Repeated page observations remain separate even when content is identical.

The corrected base estimate is 77,470,827,020 bytes (72.15 GiB). Components are 42.25 GiB live HTML/text, 4.80 GiB archived HTML/text, 14.59 GiB broad compressed archive-index responses, 10.48 GiB database/exports, and 0.03 GiB roster versions. The separate 2× planning allowance is 154,941,654,040 bytes (144.30 GiB). The estimate assumes the approved collection cadence and gives no credit for future identical-content savings. The earlier roughly 700 GiB forecast and both paid storage proposals are withdrawn. Full mode remains disabled; no paid service or new storage was used.

All 163 collection and adjacent tests passed. The deployed source hashes match local files. The database integrity check passed. Remote verification checked 857 HTML references, 821 text references, 384 usable archived-page windows, and 387 selected page/event windows. Coverage remains 25 paired, four pre-only, one post-only, and 71 uncovered candidate-event rows. Every queued item has an explicit outcome; eight remain retryable. No further same-day collection was launched.

Small authoritative review files are in `data/collection_2026/storage_correction_20260915/`: `pilot_forecast.json`, `storage_migration.json`, `pilot_verification.json`, and `coverage_report.json`. The daily automation should use this corrected state and the current remote forecast; it must not repeat the withdrawn 400/800 GiB requests.

## September 15 user-approved cadence reduction

Implemented weekly live collection plus targeted local-day copies at primary/runoff date minus one and plus one. Confirmed nonparticipants receive weekly copies only. Same-day successful copies satisfy the targeted request. Forecast and acquisition share the cadence function. The daily task checks what is due and does not imply daily website copying. All 164 focused and adjacent tests passed, including targeted dates, election-day behavior, repeat suppression, and weekly cadence. Runtime, configuration, plan, and automation were updated.

The remaining forecast is 37,646 live site snapshots (about 10 per candidate), down from 130,110. Base storage is 37.56 GiB; the separate 2× allowance is 75.11 GiB. The new forecast is estimated, but storage_fits remains false. No additional collection or purchase was launched.

## Final monthly decision recorded and implemented

The user approved monthly live collection plus one copy immediately before and after each primary/runoff. Monthly is implemented as 30 days since the latest successful live snapshot; targeted copies can satisfy and reset that interval. The local target dates are election date minus one and plus one. The existing Wayback comparison windows, weekly roster/URL refreshes, and daily due-work check continue. Code, configuration, approved plan, and automation reflect this decision. All 164 focused and adjacent tests passed. No stored observations were removed and no additional collection or storage purchase was launched.
