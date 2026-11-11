# Draft for version 5.0: codebook rows for the 2026 data

Status: draft. These rows go into `docs/deliverable_codebook.md` when Hanno approves
`docs/release_v5_0_2026_readme_draft.md`. The values are from the corpus of November 10, 2026.

## `raw_corpus.parquet`: new fields

| Field | Definition |
|---|---|
| `capture_source` | `wayback` for an Internet Archive copy; `live` for a copy that the project made from the campaign server. All rows before 2026 are `wayback` |
| `snapshot_id` | One copy of one website (one crawl). Missing before 2026, where `date` identifies the copy |
| `page_id` | One page of one copy; `event_pairs_2026.csv` refers to it. Missing before 2026 |
| `page_type_context` | 2026 only. Page type that the link to the page implies: `homepage`, `issues`, `biography`, `news`, or `endorsements`. `page_type` uses the URL only |
| `extraction_route` | 2026 only. Route of the page in the extracted-variable files: `issues`, `biography`, `endorsements`, or missing (page not read) |

## `raw_corpus.parquet`: fields with a different meaning in 2026

| Field | 2026 definition |
|---|---|
| `date` | Time of the page copy (UTC, `YYYYMMDDhhmmss`). Pages of one `snapshot_id` have different times |
| `snap_url` | Internet Archive URL for a `wayback` row; missing for a `live` row |
| `urlkey` | URL of the page |
| `n_snap` | Number of distinct `snapshot_id` values of the candidate |
| `on_ballot` | Candidate is in the general-election candidate list of the English Wikipedia state page (source file and revision IDs: see README) |
| `data_source` | `general_2026_collection` if `on_ballot`, else `election_year_2026_collection` |
| `bonica_rid` | Carried from an earlier year of the same `cand_id`; missing for a new candidate |

`general_votes` is missing for 2026 in each file.

## `panel_candidate_year.csv`

| Field | Definition |
|---|---|
| `sel_snapshot_id` | 2026 only. `snapshot_id` of the selected copy (the copy with the most text). `sel_date` is the earliest page time of that copy |

## `candidate_year_universe.csv` and `candidate_year_provenance.csv`

| Field | 2026 values |
|---|---|
| `capture_status` | Adds `captured_under_other_fec_id`: the person has two FEC IDs in 2026, and the text is under the other ID |
| `observability_status` | Adds `no_url_found`, `valid_url_no_usable_capture`, and `captured_under_other_fec_id` |
| `collection_failure_reason` | For `valid_url_no_usable_capture`: most frequent result of the homepage copies. `identity_unresolved` (text does not name the candidate), `blocked` (the server refused automatic requests), `request_error`, `http_error`, `no_content`. Else `not_applicable` |
| `collection_method_version` | `collection_2026_live_and_wayback` |
| `provenance_evidence_scope` | `released_usable_pages_and_2026_collection_roster` |

## New file: `event_pairs_2026.csv`

One row for each 2026 candidate and election event of the candidate's state and office
(primary, runoff, convention, special election, general election). Key: `cand_id`, `event_id`.
The file names the nearest complete website copy within 30 days before the event (`pre_*`)
and after it (`post_*`). Election day is excluded in each time zone of the jurisdiction.

| Field | Definition |
|---|---|
| `cand_id`, `candidate_cycle_id` | Keys to the other files |
| `event_id`, `event_date`, `event_type`, `event_round`, `event_status` | The event. `event_status` is `held`, `scheduled`, or `conditional` (a runoff that occurs only if required) at the time of the collection |
| `event_source_url`, `event_verified_source_url`, `event_verification_status` | Source of the event date, and whether a state, local, or party source confirms the FEC date |
| `participation_status`, `participation_source_url` | `confirmed` if a source shows that the candidate took part in the event; else `unknown` |
| `coverage_status` | `paired`, `pre_only`, `post_only`, or `uncovered` |
| `pre_available`, `post_available` | A copy exists on that side |
| `pre_missing_reason`, `post_missing_reason` | Why no copy exists: `missing_website`, `failed_requests`, `no_eligible_capture`, `identity_unresolved`, `unusable_content`, `missing_archives`, `timestamp_mismatch`, `incomplete_snapshot`, `captured_under_other_fec_id` |
| `pre_snapshot_id`, `post_snapshot_id` | `snapshot_id` of the copy in `raw_corpus.parquet` |
| `pre_homepage_page_id`, `post_homepage_page_id` | `page_id` of the homepage of the copy |
| `pre_page_ids`, `post_page_ids` | JSON list of the `page_id` values of the copy |
| `pre_n_pages`, `post_n_pages` | Length of that list |
| `pre_captured_at`, `post_captured_at` | Time of the homepage copy (UTC) |
| `pre_distance_days`, `post_distance_days` | Local calendar days between the copy and the event |
| `pre_source`, `post_source` | `live` or `wayback` |
| `mixed_source_pair` | The two sides have different sources |
| `pair_homepage_span_days` | Days between the two homepage copies |

The remaining fields (`*_local_date_min`, `*_local_date_max`, `*_page_types`, `*_page_sources`,
`*_page_time_min`, `*_page_time_max`, `*_page_span_hours`, `*_snapshot_complete`, `timezones`,
`date_policy`, `homepage_required`, `comparison_eligible`, and the candidate fields) describe
the copy and the selection rule.
