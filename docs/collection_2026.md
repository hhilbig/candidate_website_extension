# 2026 campaign website collection

This collection compares each 2026 House and Senate candidate's campaign website before and after each primary or runoff. It retains all same-year FEC Democratic and Republican candidates, including candidates without websites. FEC registration does not establish ballot participation.

## Commands

Use a separate output directory. The current release remains a separate artifact.

```sh
python scripts/collect_2026.py refresh --root data/collection_2026 --lookup-limit 0
python scripts/collect_2026.py pilot --root data/collection_2026 --lookup-limit 40
python scripts/collect_2026.py run --root data/collection_2026 --pilot --phase all
python scripts/collect_2026.py forecast --root data/collection_2026
python scripts/collect_2026.py enable-full --root data/collection_2026
python scripts/collect_2026.py run --root data/collection_2026 --phase all
```

The `pilot` command freezes 40 candidate IDs before URL requests. It rejects replacement of an existing pilot. `enable-full` requires measured pilot outcomes, actual stored files, and a storage forecast that fits the available disk. Exhausted request failures count as observed outcomes and remain retryable; they do not establish absent archives. Pending work, parser errors, interrupted unfinished requests, missing artifacts, or absent size measurements block the estimate. There is no minimum coverage requirement. The command does not approve new expenses.

The `run` command defaults to the pilot until `full_enabled.json` exists. It copies live sites first, refreshes stale URLs in saved batches, then inventories and retrieves archived pages. A file lock prevents overlapping runs. `--phase live` and `--phase archive` isolate those stages. The default invocation limit is 20 hours; `--max-run-seconds` overrides it and unfinished work remains retryable. The pilot's measured live request time exceeds four hours when extrapolated to the full roster, so the longer limit is needed for full-roster passes. An idle `status` or `export` command writes current CSV and Parquet outputs; the capture inventory uses compressed CSV only.

The runtime requires requests, Beautiful Soup, lxml, pandas, pyarrow, python-dotenv, PyYAML, and Playwright with Chromium. `COLLECTION_2026_ENV_FILE` can identify an existing environment file. `PLAYWRIGHT_BROWSERS_PATH` can place browser binaries on the data volume. Never print environment secrets.

## Dates and coverage

The sourced calendar distinguishes primary, runoff, special, nonpartisan, convention, and general-election events. It records uncertain or cancelled events separately. The [FEC calendar](https://www.fec.gov/help-candidates-and-committees/dates-and-deadlines/2026-reporting-dates/congressional-pre-election-reporting-dates-2026/) and state sources appear in individual event records.

Each main pair uses the nearest usable, complete homepage observation within 30 days before and after the event. It excludes election day in every applicable jurisdiction timezone. Each selected subpage must independently satisfy the same side and window. Outputs retain actual timestamps, local date ranges, distance from the event, and source. Mixed Wayback/live pairs are labelled. Unknown participation remains unknown; jurisdiction matches do not prove participation.

A page is usable when it contains extracted text, passes the response checks, and has candidate identity evidence. The automated homepage check requires family-name and given-name tokens. Subpages may inherit that evidence within the same site. Unresolved identity, empty pages, HTTP errors, transport errors, blocked redirects, oversized responses, and timestamp mismatches remain in the corpus with separate statuses. The name check does not replace manual identity review.

Live copies occur monthly (every 30 days), plus one targeted copy on the local day before and one on the local day after each applicable primary or runoff. A copy already taken on that target day satisfies it. Collection continues through 14 days after the general election and later applicable runoffs. **Superseded September 30, 2026:** Hanno set a fixed end date of November 17, 2026 (14 days after the general election). Later runoffs no longer extend collection, and `collect_2026.py run` exits without collecting after that date (`collection_end_date` in `config/collection_2026.json`). Confirmed nonparticipants do not receive targeted copies; their monthly collection continues. **Added October 24, 2026 (Hanno):** the general election now also receives targeted copies on the local day before and after (November 2 and 4), because with the 30-day cadence alone most candidates' next copy after their late-October copy fell after the November 17 end date. After the end date only the Wayback archive phase may run (`collect_2026.py run --phase archive`); one final archive pass in early December covers the post-general window. Calendar entries for possible runoffs remain explicit when actual participation is unresolved.

The crawler follows relevant internal links through two levels. The default limits are 60 attempted pages per snapshot and 10 MB per response. Snapshot manifests record links excluded by depth, page limits, or absent eligible archive captures. These are operational limits, not completeness claims. JavaScript rendering applies only to live pages in a fresh browser context.

## Saved artifacts

| Artifact | Contents |
|---|---|
| `rosters/` | Versioned FEC files, all candidate rows, prior URLs, and source lookup attempts |
| `pilot_roster.json`, `pilot_manifest.json` | Frozen pilot and input hashes |
| `collection.sqlite` | Work states, every request attempt, capture inventory, and page observations |
| `html/`, `text/` | Compressed responses and extracted text, addressed by their stored-content hashes |
| `snapshots/` | Page membership and omitted links for each crawl |
| `run_manifests/` | Candidate and calendar inputs, configuration, source hashes, and code copies |
| `exports/` | Candidate roster, election calendar, page corpus, event pairs, coverage, work states, and full archive inventory |
| `pilot_forecast.json` | Measured size and request estimates, unresolved work, and storage decision |

The HTTP client decodes Content-Encoding before storage and hashing. The Wayback `id_` URL requests unrewritten replay content; saved bytes are not described as original WARC records. Attempt records preserve requested URLs, redirect history, headers, effective URLs, and stored hashes. Rendered live HTML is distinguished from its initial HTTP response.

The page client saves HTML and extracted text only. It rejects known non-HTML response types before reading their bodies and checks untyped responses before saving them. Browser rendering blocks images, media, and fonts; any scripts or styles needed for rendering are transient. Compressed raw Wayback index responses remain as request evidence.

The working capture index excludes known asset URLs and non-HTML responses, preserves possible HTML and redirects, and stores each candidate/timestamp/URL once across weekly inventories. It retains first and latest completed inventory provenance. The full raw index responses preserve query history and other metadata. The inventory export is `capture_inventory.csv.gz`; duplicate uncompressed inventory exports are obsolete.

Identical response bytes share a file, but observations at different times remain separate. Interrupted live crawls retain their observations. A retry creates a new crawl. Failed archive requests never establish an empty archive. Completed inventories are reused for seven days. CDX continuation keys are saved after each completed page so interrupted inventories can resume. Completed archived captures may be reused across event comparisons without another fetch.

## Verification and production

Run the focused tests:

```sh
.venv/bin/python -m pytest -q tests/test_collection_2026*.py
```

Inspect decompressed HTML and text, verify their hashes, and check that all 40 pilot candidates receive explicit work outcomes. Report paired, pre-only, post-only, and uncovered candidate-event records. Separate incomplete work from absent archives. Parquet support is required in production.

Production exports stream database rows and write Parquet in batches. Pair selection holds one candidate's metadata at a time. The live schedule reads only the latest usable homepage date per candidate and attempts older work first. These choices keep the accumulated corpus out of Python memory on the droplet.

The deployed runtime is `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime` on the existing droplet. Its data root is `../data`. A daily Codex task, `collect-2026-campaign-websites`, checks work due at 09:00 America/Los_Angeles. It checks the active process before any launch, resolves routine failures, and requests a decision when the measured storage projection exceeds capacity. The approved workflow authorizes no paid APIs or new storage. Full collection starts only after the pilot forecast permits it. See the [session log](../quality_reports/session_logs/2026-09-14_campaign-websites-pre-post-primary.md) for measured results and the current capacity constraint.

## Storage estimate before cadence reduction (superseded), September 15

The corrected base estimate is 72.15 GiB for the remaining full collection: 42.25 GiB live HTML/text, 4.80 GiB archived HTML/text, 14.59 GiB compressed archive-index audit responses, 10.48 GiB database and exports, and 0.03 GiB roster versions. A separate factor-of-two planning allowance raises the total to 144.30 GiB. It is a planning assumption, not a statistical bound, and takes no credit for future identical-content savings. Weekly raw index costs remain in the estimate because broad queries preserve redirects with misleading MIME types.

The earlier roughly 700 GiB forecast and the 400/800 GiB storage proposals are withdrawn. The pilot contained no saved non-HTML page payloads; repeated index rows and an uncompressed inventory export caused most of the inflated projection. The deployed correction preserves all 881 page observations and the 25 paired, four pre-only, one post-only, and 71 uncovered candidate-event rows. All 163 focused and adjacent tests passed, followed by remote hash, timestamp, queue-outcome, database-integrity, and compressed-export checks.

The authoritative corrected forecast and migration report are in [`storage_correction_20260915`](../data/collection_2026/storage_correction_20260915/). About 19 GiB remains free, so full collection is still disabled. No storage purchase is approved.

## Weekly cadence, September 15 (superseded)

The user approved reducing collection to weekly live copies plus the day before and after each primary or runoff. This replaces the daily 14-day window. A failed targeted copy remains recorded; subsequent weekly copies provide recovery opportunities within the existing comparison window. Saved observations remain intact.

The revised forecast projects 37,646 live site snapshots across 3,674 candidates for the remaining season, about 10 per candidate on average. The base estimate is 37.56 GiB, or 75.11 GiB with the separate 2× planning allowance. The authoritative forecast is [`reduced_cadence_20260915/pilot_forecast.json`](../data/collection_2026/reduced_cadence_20260915/pilot_forecast.json). All 164 focused and adjacent tests passed. Code and configuration are deployed, and the daily due-work automation now specifies the reduced cadence. Full mode remains disabled because available capacity is insufficient.

## Final cadence decision, September 15 (current)

Collect live HTML/text monthly (every 30 days), plus one targeted copy on the local day before and one on the local day after each primary or runoff. A successful targeted copy also resets the regular interval. This is the user-approved decision and supersedes weekly or daily live copying. Roster and URL refreshes remain weekly, and the daily automation checks due work. Wayback still supplies historical pre/post captures.

The schedule and automation are updated; 164 focused and adjacent tests passed. The current forecast is `data/collection_2026/monthly_cadence_20260915/pilot_forecast.json`.
