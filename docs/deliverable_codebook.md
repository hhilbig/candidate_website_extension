# Codebook: U.S. candidate campaign websites, 2002–2024

## Keys and missing values

`candidate_cycle_id` is the primary key in every candidate-year file and the
foreign key in the raw corpus. It combines the canonical FEC candidate ID with
the election year. A `cand_id` can therefore appear in more than one year.
Empty CSV cells and Parquet nulls denote missing values. An empty string in an
archived text field means that extraction returned no text; it is not an
imputed value.

## `raw_corpus.parquet`

One row is one archived page at one snapshot date. The file has 799,058 rows.

| Field | Definition |
|---|---|
| `candidate_cycle_id` | Canonical candidate-year identifier |
| `cand_id` | Canonical FEC candidate identifier |
| `candidate_icpsr` | Display name in `Last, First` form |
| `name_key` | Lowercase normalized name used for compatibility only |
| `bonica_rid` | DIME recipient identifier; missing when unmatched |
| `state` | Two-letter postal abbreviation |
| `district` | House district; missing for Senate |
| `district_id` | Two-letter state plus two-digit House district; missing for Senate |
| `office` | `house` or `senate` |
| `year` | Election year |
| `party` | `democrat` or `republican` |
| `cand_election_yr` | Election year recorded in the FEC candidate master file |
| `cand_status` | FEC candidate-status code; descriptive, not a release filter |
| `universe_source` | `same_year_fec` or `ballot_override` |
| `on_ballot` | Whether the candidate matches a major-party general-election return |
| `stage` | `2` for general-election candidates; missing otherwise |
| `data_source` | `general_wayback` for ballot candidates; `election_year_wayback` otherwise |
| `candidate_year_stage` | ICPSR-compatible candidate/year/stage key; missing off ballot |
| `date` | Internet Archive snapshot timestamp |
| `urlkey` | Archived page URL or normalized source URL |
| `snap_url` | Internet Archive snapshot URL |
| `page_type` | Page classification such as homepage, issues, biography, or other |
| `n_char` | Extracted character count for the page |
| `n_words` | Extracted word count for the page |
| `n_tags` | Number of `#+#`-delimited visible-text components before the ICPSR-compatible text filter |
| `n_clean_tags` | Number of those components retained by the ICPSR-compatible text filter |
| `n_snap` | Number of distinct snapshot dates available for the candidate-year |
| `text_snap_content` | Extracted page text |

## `panel_candidate_year.csv`

One row is one captured candidate-year. The file has 7,353 rows and selects the
snapshot date with the greatest total extracted text.

Identity and population fields are `candidate_cycle_id`, `source_cand_id`,
`cand_id`, `cand_election_yr`, `cand_status`, `universe_source`, `candidate`,
`state`, `district`, `office`, `year`, `party`, `on_ballot`, `stage`,
`data_source`, and `candidate_year_stage`. `source_cand_id` records the FEC ID
attached to the original capture when a reviewed mapping changed the canonical
ID.

| Field | Definition |
|---|---|
| `sel_date` | Selected Internet Archive snapshot timestamp |
| `n_snapshots_available` | Distinct snapshot dates observed for the candidate-year |
| `n_pages` | Pages included from the selected snapshot |
| `page_types` | Semicolon-separated page types present |
| `urlkey` | Campaign-site URL represented by the selected snapshot |
| `text` | Selected pages concatenated with `#+#` separators |
| `n_char` | Total extracted characters in `text` |
| `n_words` | Total extracted words in `text` |
| `text_quality` | `usable`, `thin`, or `empty`, based on documented length and placeholder rules |

## `panel_icpsr_compat.csv`

One row is one captured candidate-year. The file has 7,353 rows. Identity and
population fields have the same meanings as in `panel_candidate_year.csv`.
`icpsr_compatible` is identical to `on_ballot`; it identifies candidates
matched to an official major-party general-election return.

The remaining fields form repeated measurement families:

| Field or prefix | Definition |
|---|---|
| `candidate_icpsr` | Candidate name in ICPSR-compatible display form |
| `name_key` | ICPSR-compatible normalized name |
| `district_id` | State and two-digit House district; missing for Senate |
| `general_votes` | Candidate votes in the matched general-election return; missing off ballot |
| `icpsr_n_char*`, `icpsr_n_words*` | Mean extracted character and word counts |
| `icpsr_n_tags*`, `icpsr_n_clean_tags*` | Mean visible-text component counts before and after the ICPSR-compatible text filter |
| `icpsr_ttr_approx*`, `icpsr_mattr_approx*`, `icpsr_entropy_approx*` | Approximate lexical-diversity measures |
| `icpsr_n_valid_snap*`, `icpsr_n_valid_pages*` | Valid snapshot and page counts |
| `icpsr_max_pages_1day`, `icpsr_share_pages_runaway`, `icpsr_runaway_flag` | Diagnostics for unusually dense single-day crawls |
| `icpsr_topic_<topic>` | Share assigned to the named topic across all valid pages |
| `icpsr_topic_<topic>_home` | Homepage-only share assigned to the named topic |
| fields ending `_home` | Homepage-only version of the base measure |
| fields ending `_preelec` | Version restricted to snapshots on or before Election Day |
| `icpsr_first_snap_day`, `icpsr_last_snap_day` | First and last valid snapshot dates |
| `icpsr_snap_span_days` | Days between first and last valid snapshots |

Topic shares are proportions and are missing when no valid text supports the
measure. Counts are nonnegative. Lexical-diversity measures are missing when
the required text is absent.

## `candidate_crosswalk.csv`

One row is one captured candidate-year. The file has 7,353 rows. It contains
the shared identity and population fields plus the following links:

| Field | Definition |
|---|---|
| `district_i` | Numeric House district used during matching |
| `fec_raw_name` | Candidate name as recorded in the source FEC file |
| `cand_ici` | FEC incumbent/challenger/open-seat code |
| `bonica_rid` | DIME recipient identifier |
| `ICPSR`, `NID` | DIME/ICPSR identity fields; missing when unmatched |
| `cfscore`, `cfscore_dyn` | Static and cycle-specific DIME campaign-finance scores |
| `dwnom1`, `dwnom2` | DIME-linked DW-NOMINATE dimensions; missing for non-officeholders |
| `seat`, `dime_district`, `dime_ico` | DIME seat, district, and incumbent-status fields |
| `lname`, `fname` | DIME normalized name components |
| `candidate_icpsr`, `name_key`, `party_full`, `district_id` | ICPSR-compatible display fields |
| `general_votes` | Votes in the matched general-election return; missing off ballot |

## `release_roster.csv`

One row is one in-scope candidate-year with a URL attempt. The file has 9,848
rows and is the denominator for collection coverage.

| Field | Definition |
|---|---|
| `candidate_cycle_id`, `cand_id`, `cand_election_yr`, `cand_status` | Canonical FEC identity and metadata |
| `candidate`, `state`, `district`, `office`, `year`, `party` | Candidate and contest fields |
| `universe_source` | Same-year FEC record or reviewed ballot override |
| `website_url` | URL recorded for the collection attempt |
| `url_status` | `valid`, `valid_manual_override`, `missing_or_invalid`, or `invalid_capture_source` |
| `has_url` | Whether the current URL cleaner accepts the recorded URL |
| `captured` | Whether the candidate-year appears in the released corpus |
| `capture_status` | `captured`, `not_captured`, or `invalid_capture_excluded` |
| `on_ballot` | General-election ballot match |
| `general_votes` | General-election votes; missing off ballot |
| `stage`, `data_source` | Population labels defined above |

## Integrity checks

The three candidate-year products contain the same 7,353 unique
`candidate_cycle_id` values. All are present in the roster and raw corpus. The
roster marks exactly 7,353 rows as captured. The raw file contains no candidate
outside that set. Every ordinary row has `cand_election_yr == year`; reviewed
ballot identity exceptions are explicitly labeled. No candidate-status code is
used to select the population.
