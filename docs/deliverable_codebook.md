# Codebook: U.S. House (2018–2024) and Senate (2002–2024) candidate websites

This codebook describes the corrected and extended local staging build. It does
not claim that an existing Dataverse version or DOI has been updated.

## File relationships, keys, and missing values

The release contains a full population of 15,041 candidate-years and observed
website text for 7,348 of them.

`candidate_cycle_id` links all eight data files. It combines the canonical FEC
candidate ID and election year. It identifies a candidacy-year, not a person
across years or offices. Within-file keys are:

| File | Unique key |
|---|---|
| `candidate_year_universe.csv` | `candidate_cycle_id` |
| `release_roster.csv` | `candidate_cycle_id` |
| `candidate_year_provenance.csv` | `candidate_cycle_id` |
| `raw_corpus.parquet` | No single row ID; rows are observed archived pages |
| `panel_snapshot_day.parquet` | `candidate_cycle_id`, `snap_day`, `page_scope` |
| `panel_candidate_year.csv` | `candidate_cycle_id` |
| `panel_icpsr_compat.csv` | `candidate_cycle_id` |
| `candidate_crosswalk.csv` | `candidate_cycle_id` |

Empty CSV cells and Parquet nulls denote missing values. An empty
`text_snap_content` string means that extraction returned no text for that raw
page. Missing text-derived values do not mean zero; the snapshot and provenance
status fields explain whether text or a requested scope was observed.

Party fields retain file-level backward compatibility. `raw_corpus.parquet`,
`panel_snapshot_day.parquet`, and `panel_icpsr_compat.csv` use `democrat` and
`republican`. `panel_candidate_year.csv`, `candidate_crosswalk.csv`,
`candidate_year_universe.csv`, `release_roster.csv`, and
`candidate_year_provenance.csv` use `D` and `R`. Minnesota
Democratic-Farmer-Labor FEC records map to `D`. The crosswalk also supplies
`party_full`.

Dates and timestamps are UTC Wayback values. A day is an eight-digit integer in
`YYYYMMDD` form. A full timestamp is normally a fourteen-character
`YYYYMMDDHHMMSS` string. The raw validator accepts an observed Wayback value
with 8 through 14 digits and derives the day from its first eight digits.

## Population and shared contest concepts

The population begins with same-election-year Democratic, Republican, and
Minnesota Democratic-Farmer-Labor FEC records. It covers House elections in
2018–2024 and Senate elections in 2002–2024. Senate rows are restricted to
state-years with a general election. Reviewed ballot exceptions are added, and
reviewed state legislative records are removed. FEC candidate status is
descriptive and does not filter the population.

The following concepts recur across files; no single file necessarily contains
every field in this table:

| Field | Definition |
|---|---|
| `cand_id` | Reviewed canonical FEC candidate identifier |
| `source_cand_id` | FEC identifier attached to the source record or capture before canonical review; it can differ from `cand_id` |
| `candidate_cycle_id` | Canonical `cand_id` plus election year; the public candidate-year key |
| `candidate` | Cleaned display name based on the FEC record |
| `candidate_icpsr` | Display name, normally in `Last, First` form, used for ICPSR compatibility |
| `name_key` | Lowercase normalized name retained for compatibility; it is not used as a person identifier |
| `state` | Two-letter postal abbreviation |
| `office` | `house` or `senate` |
| `year` | Target election year |
| `district` | Normalized House district; `0` is at-large or territorial and Senate is missing |
| `district_id` | State plus two-digit House district; Senate is missing |
| `cand_election_yr` | Candidate election year recorded in the FEC candidate file |
| `cand_status` | FEC candidate status code; metadata only |
| `universe_source` | `same_year_fec` or reviewed `ballot_override` |
| `on_ballot` | Match to a Democratic or Republican official general-election return |
| `general_votes` | Candidate votes in the matched general-election return; missing when `on_ballot` is false |
| `stage` | `2` for matched general-election candidates; missing otherwise |
| `data_source` | `general_wayback` for matched general-election candidates and `election_year_wayback` otherwise |
| `candidate_year_stage` | ICPSR-compatible candidate/year/stage key; populated only for matched general-election candidates |

## `candidate_year_universe.csv` and `release_roster.csv`

Each file has 15,041 rows and 29 columns. One row is one candidate-year in the
full population, including candidates without a historical URL record or a
released capture. `release_roster.csv` is an identical compatibility alias of
`candidate_year_universe.csv`; new work should prefer the explicit universe
name.

### Identity, contest, and geography fields

| Field | Definition |
|---|---|
| `candidate_cycle_id`, `cand_id`, `source_cand_id` | Public candidate-year key and canonical/source FEC identifiers defined above |
| `cand_election_yr`, `cand_status`, `cand_ici` | FEC election year, candidate status, and incumbent/challenger/open-seat code |
| `candidate`, `fec_raw_name` | Cleaned display name and unmodified FEC source name |
| `state`, `office`, `year`, `party` | Contest state, federal office, election year, and `D`/`R` party |
| `source_district` | District exactly as supplied by the source FEC record |
| `district` | Normalized House district; missing for Senate and for invalid or missing House source districts |
| `geography_status` | `valid_house_district`, `senate_not_applicable`, `missing_house_district`, or `invalid_house_district` |
| `universe_source` | `same_year_fec` or `ballot_override` |
| `on_ballot`, `general_votes` | General-election match and vote total defined above |

A valid House district is `0` or an integer from 1 through 53. An invalid
source value remains visible in `source_district`; the build does not invent a
normalized federal district.

### URL and capture fields

| Field | Definition |
|---|---|
| `historical_attempt_record` | Whether the candidate-year appears in the prior URL-bearing roster; false means the staged evidence has no such record, not that a search returned no URL |
| `website_url` | Historical URL exactly as stored; missing when there is no prior record |
| `normalized_website_url` | Accepted URL with lowercase scheme and host, leading `www` label removed, fragment removed, and non-root trailing slash removed |
| `website_domain` | Normalized host, not an inferred registrable domain |
| `website_cycle_id` | Normalized host plus year; a website-year key, not a person identifier |
| `url_discovery_status` | Evidence state: `legacy_no_url_record`, `legacy_blank_url_record`, or `legacy_url_recorded` |
| `url_validation_status` | `not_evaluated_no_url_record`, `missing_or_blank`, `invalid_or_placeholder`, `valid`, or `valid_manual_override` |
| `url_source` | Historical URL source when known; `legacy_unknown` means a URL is recorded but its original source was not preserved |
| `has_url` | True exactly when `url_validation_status` is `valid` or `valid_manual_override` |
| `captured` | Whether corrected substantive raw pages exist for the candidate-year |
| `capture_status` | `captured`, `not_captured`, or `invalid_capture_excluded` |

`invalid_capture_excluded` means that a reviewed historical capture belongs to
the wrong site or contains unrelated material. The row remains in the
population denominator but is absent from the corpus and captured panels.

## `candidate_year_provenance.csv`

The file has 15,041 rows and 68 columns. It starts with all 29 universe fields
defined above and adds evidence about observed pages, selection, adjudication,
and the availability of derived measures.

### Observed capture evidence

| Field | Definition |
|---|---|
| `n_observed_snapshot_timestamps` | Number of distinct retained Wayback timestamps |
| `n_observed_snapshot_days` | Number of distinct retained calendar days; several timestamps can occur on one day |
| `first_capture_timestamp`, `last_capture_timestamp` | Earliest and latest retained Wayback timestamp |
| `first_capture_day`, `last_capture_day` | Earliest and latest retained calendar day |
| `capture_span_days` | Calendar days between `first_capture_day` and `last_capture_day` |
| `n_raw_page_rows` | Number of rows in `raw_corpus.parquet` |
| `n_distinct_page_urls` | Number of distinct normalized observed page URLs |
| `n_distinct_page_texts` | Number of distinct SHA-256 hashes of exact raw text strings |
| `page_text_digest_sha256` | SHA-256 digest of the sorted page-text hashes, including duplicate page occurrences |
| `n_page_types_observed`, `page_types_observed` | Count and semicolon-separated names of observed page types |
| `n_source_hosts`, `source_hosts` | Count and semicolon-separated normalized hosts observed in archived page URLs |
| `max_raw_pages_one_day` | Largest raw page count on any retained day |
| `n_runaway_days` | Number of retained days with more than 100 raw pages |
| `share_raw_pages_on_runaway_days` | Share of candidate-year raw rows that occur on days with more than 100 pages; missing when no raw page exists |
| `n_homepage_raw_page_rows` | Number of raw rows classified as `homepage` |
| `n_preelection_raw_page_rows` | Number of raw rows on or before the federal Election Day |

Counts are zero for candidate-years without substantive raw pages. Their
timestamps, dates, span, digest, and runaway share are missing where the
concept requires observed pages.

### Panel selection and adjudication

| Field | Definition |
|---|---|
| `selected_panel_timestamp` | Timestamp selected in `panel_candidate_year.csv` |
| `selected_text_quality` | `usable`, `thin`, or `empty` for the selected extraction |
| `selected_panel_rule` | `longest_text_snapshot_latest_timestamp_tie` for a selected row; missing without a selected row |
| `capture_adjudication_status` | `not_reviewed`, `retained_after_review`, or `excluded_invalid_capture` |
| `capture_adjudication_reason` | Human-readable reason from the reviewed capture decision; missing when not reviewed |
| `capture_date_adjudication_status` | `not_reviewed` or `retained_approved_date_range` |
| `capture_date_min_snap_day`, `capture_date_max_snap_day` | Inclusive approved day boundaries; missing when no date-range review applies |
| `capture_date_adjudication_reason` | Human-readable reason for the approved date restriction |

### Observability and measure-family status

| Field | Definition |
|---|---|
| `observability_status` | Candidate-level evidence state defined below |
| `all_text_measure_status` | Availability of all-page ICPSR-compatible measures |
| `homepage_measure_status` | Availability of homepage-only measures |
| `preelection_measure_status` | Availability of measures using pages on or before Election Day |
| `topic_measure_status` | Availability of candidate-year topic shares |

`observability_status` takes these values:

| Value | Meaning |
|---|---|
| `captured_usable_text` | Raw pages exist and the selected extraction has at least 1,500 characters |
| `captured_thin_text` | Raw pages exist and the selected extraction is under 1,500 characters but is not classified as empty |
| `captured_empty_text` | Raw pages exist but the selected extraction meets an empty or placeholder rule |
| `valid_url_no_capture_unknown_reason` | A valid historical URL is recorded, but the released evidence does not identify why no substantive page exists |
| `invalid_or_placeholder_url` | No accepted URL supports collection in the historical evidence |
| `legacy_no_url_record` | The staged inputs contain no prior URL-roster record; this is not a documented failed search |
| `invalid_capture_excluded` | A reviewed non-campaign or unrelated capture was excluded |

Each measure-family status is one of:

| Value | Meaning |
|---|---|
| `observed` | A supported nonzero derived value is present |
| `observed_zero` | A supported derived value is present and equals zero |
| `insufficient_clean_text` | The requested page scope exists but no text survives the ICPSR cleaning requirement |
| `scope_not_observed` | No page in the requested homepage or pre-election scope was observed |
| `not_computed` | The measure was not computed for that row, including rows without substantive captures |

### Evidence-bounded collection fields

| Field | Staged value and interpretation |
|---|---|
| `collection_method_version` | `legacy_mixed_unknown`: the released pages combine historical collection methods that cannot be assigned exactly by candidate-year |
| `cdx_availability_status` | `unknown`: successful released pages do not reconstruct the complete CDX response |
| `collection_failure_reason` | `unknown`: the staged evidence cannot distinguish no archive, request failure, or another cause |
| `snapshot_cap_binding_status` | `unknown`: the historical inputs do not show whether a cap bound for that candidate-year |
| `provenance_evidence_scope` | `released_successful_pages_and_legacy_roster` |

The current crawler uses three-month URL-specific deduplication and a
temporally stratified 200-snapshot cap. The `legacy_mixed_unknown` value warns
users not to assume that every released candidate-year was collected under
that current policy.

## `raw_corpus.parquet`

The file has 797,554 rows and 28 columns. One row is one extracted archived
page at one retained timestamp. It is the only data file that stores page text.

### Identity and population fields

| Field | Definition |
|---|---|
| `candidate_cycle_id`, `cand_id` | Canonical candidate-year key and FEC identifier |
| `candidate_icpsr`, `name_key` | ICPSR-compatible display and normalized names |
| `bonica_rid` | DIME recipient identifier; missing when unmatched |
| `state`, `district`, `district_id`, `office`, `year` | Normalized contest fields; Senate district fields are missing |
| `party` | `democrat` or `republican` |
| `cand_election_yr`, `cand_status`, `universe_source` | FEC and population metadata defined above |
| `on_ballot`, `stage`, `data_source`, `candidate_year_stage` | General-election match and compatibility fields defined above |

### Page, timestamp, and text fields

| Field | Definition |
|---|---|
| `date` | Retained Wayback snapshot timestamp |
| `urlkey` | Archived page URL or normalized source URL carried by the historical corpus |
| `snap_url` | Internet Archive snapshot URL for source inspection |
| `page_type` | One of `homepage`, `issues`, `biography`, `news`, `endorsements`, `constituent_services`, `action`, or `other` |
| `n_char` | Exact Python string length of `text_snap_content` |
| `n_words` | Number of whitespace-separated strings in `text_snap_content`; this differs from the ICPSR `\\w+` word tokenizer |
| `n_tags` | Number of `#+#`-delimited text components after preliminary ICPSR character cleaning and before the segment filter |
| `n_clean_tags` | Number of those components that pass the ICPSR segment filter |
| `n_snap` | Distinct full Wayback timestamps retained for the candidate-year after all release and date adjudications; repeated on every page row |
| `text_snap_content` | Extracted page text, with `#+#` separating visible text components |

`n_snap` counts full timestamps, not calendar days. Use
`n_observed_snapshot_days` in the provenance file or `snap_day` in the daily
panel for day-level counts.

## `panel_snapshot_day.parquet`

The file has 299,185 rows and 84 columns. Its unique key is
`candidate_cycle_id × snap_day × page_scope`. It contains 74,355 `all` rows,
one for every observed candidate-day, and 224,830 detail rows for page types
observed on those days. It does not duplicate page text.

`page_scope` is `all` or one of the eight page types. The `all` row aggregates
every observed page on the day. A page-type row aggregates only pages assigned
to that type. Absent page types do not receive structural rows.

### Carried candidate fields

| Field | Definition |
|---|---|
| `candidate_cycle_id`, `cand_id`, `candidate`, `candidate_icpsr`, `name_key` | Candidate-year and display identifiers; `candidate` aliases the available ICPSR display label when the raw corpus lacks a separate candidate field |
| `cand_election_yr`, `cand_status`, `state`, `district`, `district_id`, `office`, `year` | FEC and contest metadata |
| `party` | `democrat` or `republican` |
| `universe_source`, `on_ballot`, `stage`, `data_source`, `candidate_year_stage` | Population and general-election fields defined above |

### Timing fields

| Field | Definition |
|---|---|
| `snap_day` | Calendar day formed from the first eight digits of the raw Wayback timestamp |
| `page_scope` | `all` or an observed page type |
| `first_capture_timestamp`, `last_capture_timestamp` | Earliest and latest timestamp contributing to the day and scope |
| `n_capture_timestamps` | Distinct full timestamps contributing to the day and scope |
| `election_day` | Federal general Election Day in `YYYYMMDD` form |
| `days_to_election` | `election_day - snap_day` in calendar days: positive before, zero on, and negative after the election |
| `pre_election` | True on or before Election Day |
| `post_election` | True after Election Day |

### Coverage, URL, and digest fields

| Field | Definition |
|---|---|
| `n_pages_observed` | Raw page rows in this day and scope |
| `n_pages_all_scopes` | Total raw page rows across all page types that day; repeated on detail rows |
| `n_clean_valid_pages` | Pages in the scope with positive character count after ICPSR cleaning |
| `n_distinct_urls` | Distinct unwrapped page URLs in the scope, falling back to `urlkey` |
| `n_distinct_page_hashes` | Distinct SHA-256 hashes of exact raw text strings |
| `n_page_types` | Number of page types represented in the scope |
| `page_types_present` | Ordered list of page types represented in the scope |
| `source_hosts` | Sorted list of hosts extracted from archived page URLs, falling back to `urlkey` |
| `page_hashes_sha256` | Sorted list containing one exact-text SHA-256 hash per raw page row; duplicate hashes are retained |
| `day_digest_sha256` | SHA-256 digest of sorted row fingerprints built from timestamp, page type, page URL, `urlkey`, exact-text hash, and raw character and word counts |
| `runaway_day` | True when the all-scope day contains more than 100 raw page rows; the same value is repeated on detail rows |

The digest is invariant to input-row order. It supports change detection but
does not replace the source URL or archived text as provenance.

### Text totals, daily means, and statuses

| Field | Definition |
|---|---|
| `raw_n_char_total`, `raw_n_words_total` | Sums of the raw page character and whitespace-word counts in the scope |
| `clean_n_char_total`, `clean_n_words_total` | Sums of cleaned ICPSR character and `\\w+` word counts |
| `clean_n_tags_total`, `clean_n_clean_tags_total` | Sums of component counts before and after the ICPSR segment filter |
| `icpsr_n_char`, `icpsr_n_words` | Mean cleaned page character and word counts among clean-valid pages in the day and scope |
| `icpsr_n_tags`, `icpsr_n_clean_tags` | Mean component counts among clean-valid pages |
| `icpsr_ttr_approx`, `icpsr_mattr_approx`, `icpsr_entropy_approx` | Mean page-level lexical diversity and entropy measures among clean-valid pages |
| `measure_status` | `observed` when at least one page survives cleaning; otherwise `insufficient_clean_text` |
| `topic_status` | `observed`, `insufficient_clean_text`, or `not_computed` |
| `day_topic_<topic>` | One of 31 daily topic probabilities; supported rows sum to one |

The daily ICPSR columns are page means within one day and scope. Averaging the
`all` rows across days reproduces the candidate-year two-level aggregation for
the corresponding non-topic measures, subject to ordinary floating-point
precision.

Daily topics use a different operation. The builder orders and joins all
clean-valid page text within the day and scope, then scores that document once.
The `all` document is built directly from all pages, not from page-type topic
probabilities. Daily topic probabilities should not be averaged to reconstruct
`icpsr_topic_*`.

## `panel_candidate_year.csv`

The file has 7,348 rows and 25 columns. One row is one captured candidate-year
at one selected full timestamp. `party` uses `D` and `R`.

It carries `candidate`, `state`, `district`, `office`, `year`, `party`,
`source_cand_id`, `cand_id`, `candidate_cycle_id`, `cand_election_yr`,
`cand_status`, `universe_source`, `on_ballot`, `stage`, `data_source`, and
`candidate_year_stage` as defined above. Its remaining fields are:

| Field | Definition |
|---|---|
| `sel_date` | Retained full timestamp whose nonempty pages have the largest total `n_char`; the later timestamp wins an exact tie |
| `n_snapshots_available` | Distinct retained full timestamps containing at least one nonempty page after adjudication |
| `n_pages` | Nonempty raw page rows at `sel_date` |
| `page_types` | Semicolon-separated page types represented at `sel_date` |
| `urlkey` | URL key from the first selected page in the fixed concatenation order |
| `text` | Selected page texts joined with `#+#`; pages are ordered by page type and then decreasing page length |
| `n_char` | Sum of selected page `n_char` values; inserted separators are not counted |
| `n_words` | Sum of selected raw whitespace-word counts |
| `text_quality` | `usable`, `thin`, or `empty` under the exact rule below |

`text_quality` measures extraction length and placeholder content. It does not
validate candidate identity. The rule is:

1. `empty` if `n_char < 100`;
2. `empty` if `n_char < 1,500` and the first 2,000 characters contain a
   case-insensitive placeholder or error marker;
3. `thin` if `n_char < 1,500` and neither empty rule applies; and
4. `usable` otherwise.

The placeholder and error markers are: `loading`, `coming soon`,
`under construction`, `site is being built`, `site is being updated`,
`check back`, `domain for sale`, `domain is for sale`, `buy this domain`,
`godaddy`, `namecheap`, `parked`, `enable javascript`,
`requires javascript`, `page not found`, `404`, `403 forbidden`,
`account suspended`, and `website coming`.

## ICPSR text cleaning and two-level aggregation

The ICPSR-compatible candidate and daily measures use the same implemented
page-level cleaning rule:

1. remove URL-like strings;
2. replace `&amp;` with `and` and normalize two stored apostrophe encodings;
3. retain the implemented ASCII letter, punctuation, and `#+#` separator set,
   dropping digits and other characters;
4. split the result at `#+#`; and
5. retain a segment only if it has at least 10 `\\w+` tokens, contains at least
   one sentence-ending `?`, `!`, or `.`, and contains at least one uppercase
   letter.

The builder joins retained segments with spaces and collapses repeated
whitespace. A page is clean-valid when the resulting cleaned string has at
least one character.

For the non-topic candidate-year measures, the estimator is:

1. compute the measure on each clean-valid page;
2. average pages within `candidate_cycle_id × snap_day`; and
3. average the daily means within `candidate_cycle_id`.

This gives every observed day equal weight regardless of how many pages the
crawler produced that day. The homepage and pre-election families repeat the
same calculation after restricting pages. Election Day belongs to the
pre-election family.

`icpsr_n_char` is cleaned string length. `icpsr_n_words` uses lowercase
`\\w+` tokens. `icpsr_ttr_approx` is the type-token ratio from the replicated
lexical tokenizer. `icpsr_mattr_approx` is moving-average type-token ratio with
a 200-token window and falls back to TTR for pages with 200 or fewer tokens.
`icpsr_entropy_approx` is the token-frequency-weighted mean of Google Books
per-word entropy among tokens with entropy data.

## Frozen topic classifier

Both topic products use the same locally stored, frozen classifier inputs:

- U.S. Manifesto Project version `2021-1`, core release `MPDS2021a`;
- the frozen 31-class consolidated topic mapping;
- a TF-IDF vectorizer followed by an RBF support vector classifier with
  `C = 20`, probability estimates enabled, and random seed `2163`; and
- the replicated lowercasing, punctuation removal, English stopword removal,
  and Snowball stemming applied before prediction.

The classifier was trained locally on Manifesto quasi-sentences. No released
topic value depends on a commercial language-model API. Topic values are
probabilities that sum to one when supported. They measure textual emphasis,
not policy position.

The candidate-year and daily document rules differ. `icpsr_topic_*` scores one
document made by joining every cleaned page in a candidate-year; the `_home`
family scores one joined homepage document. `day_topic_*` scores one document
per day and scope. Neither topic family uses the page-then-day mean applied to
the non-topic measures.

## `panel_icpsr_compat.csv`

The file has 7,348 rows and 114 columns. One row is one captured candidate-year.
`party` uses `democrat` and `republican`; `icpsr_compatible` is identical to
`on_ballot`.

### Identity and population fields

| Field | Definition |
|---|---|
| `candidate_icpsr`, `name_key`, `state`, `district_id`, `office`, `year`, `party` | ICPSR-compatible display, contest, and full-label party fields |
| `source_cand_id`, `cand_id`, `candidate_cycle_id` | Source and canonical FEC identifiers and candidate-year key |
| `cand_election_yr`, `cand_status`, `universe_source` | FEC and population metadata |
| `on_ballot`, `stage`, `data_source`, `candidate_year_stage` | General-election and compatibility fields |
| `general_votes` | Official general-election votes; missing off ballot |
| `icpsr_compatible` | Same boolean as `on_ballot` |

### Derived measure families

| Field or pattern | Definition |
|---|---|
| `icpsr_n_char*`, `icpsr_n_words*` | Two-level means of cleaned page character and word counts |
| `icpsr_n_tags*`, `icpsr_n_clean_tags*` | Two-level means of component counts before and after the segment filter |
| `icpsr_ttr_approx*`, `icpsr_mattr_approx*`, `icpsr_entropy_approx*` | Two-level means of the approximate lexical measures defined above |
| `icpsr_n_valid_snap*` | Distinct input days containing a raw-nonempty page in the stated scope |
| `icpsr_n_valid_pages*` | Input raw page count after the raw-nonempty gate in the stated scope |
| fields ending `_home` | Homepage-only version of the base field |
| fields ending `_preelec` | Version using pages captured on or before Election Day |
| `icpsr_first_snap_day`, `icpsr_last_snap_day` | First and last input day after the raw-nonempty page gate |
| `icpsr_snap_span_days` | Calendar days between first and last input day |
| `icpsr_max_pages_1day` | Largest raw-nonempty page count on one candidate-day |
| `icpsr_share_pages_runaway` | Share of candidate raw-nonempty pages occurring on days with more than 100 pages |
| `icpsr_runaway_flag` | True when `icpsr_max_pages_1day > 100` |
| `icpsr_topic_<topic>` | Probability from one cleaned all-page candidate-year document |
| `icpsr_topic_<topic>_home` | Probability from one cleaned homepage-only candidate-year document |

The fields named `n_valid_snap` and `n_valid_pages` preserve the established
output names. Operationally, their gate is positive raw page length before the
ICPSR cleaner; a page can enter those counts and still contribute no derived
measure if no text survives cleaning. Use the provenance family statuses or
daily `n_clean_valid_pages` when that distinction matters.

The 31 topic names are:

`Agriculture and Farmers`, `Centralisation`, `Civic Mindedness`,
`Constitutionalism`, `Culture`, `Democracy`, `Economic Planning`, `Education`,
`Equality`, `Foreign Special Relationships`, `Free Market Economy`,
`Freedom and Human Rights`, `Governmental and Administrative Efficiency`,
`Incentives`, `Internationalism`, `Labour Groups`, `Law and Order`,
`Market Regulation`, `Military`, `Multiculturalism`, `National Way of Life`,
`Other`, `Peace`, `Political Authority`, `Political Corruption`,
`Protectionism`, `Social Groups`, `Sustainability`,
`Technology and Infrastructure`, `Traditional Morality`, and `Welfare State`.

## `candidate_crosswalk.csv`

The file has 7,348 rows and 36 columns. One row is one captured candidate-year.
It carries the shared candidate, contest, ballot, stage, source, and canonical
identifier fields. `party` uses `D` and `R`; `party_full` supplies `democrat` or
`republican`.

| Field | Definition |
|---|---|
| `candidate`, `candidate_icpsr`, `name_key` | FEC-derived display name, ICPSR-compatible display name, and normalized compatibility name |
| `state`, `district`, `district_i`, `district_id`, `office`, `year` | Contest fields and numeric/string district variants used in matching |
| `party`, `party_full` | Short and full party labels |
| `cand_id`, `source_cand_id`, `candidate_cycle_id` | Canonical/source FEC identifiers and candidate-year key |
| `fec_raw_name`, `cand_election_yr`, `cand_status`, `cand_ici` | Original FEC name and FEC metadata |
| `universe_source` | `same_year_fec` or `ballot_override` |
| `on_ballot`, `general_votes`, `stage`, `data_source`, `candidate_year_stage` | General-election and compatibility fields defined above |
| `bonica_rid` | DIME recipient identifier |
| `ICPSR`, `NID` | DIME/ICPSR identity fields; missing when unmatched |
| `cfscore`, `cfscore_dyn` | Static and election-cycle DIME campaign-finance scores |
| `dwnom1`, `dwnom2` | DW-NOMINATE dimensions linked through DIME; missing when unavailable |
| `seat`, `dime_district`, `dime_ico` | DIME seat, district, and incumbency fields |
| `lname`, `fname` | DIME normalized surname and given-name components |

External identity and score fields are missing when no reviewed or source
crosswalk supports the match.
