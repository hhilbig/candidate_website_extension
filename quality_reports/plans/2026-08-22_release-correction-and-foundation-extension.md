# Plan — Correct the release and build the first extension layer

**Date:** 2026-08-22  
**Author:** Codex session  
**Status:** implemented and validated locally; Dataverse draft/publication pending

The separately requested CLARA-style Hive extraction extension is recorded in
`quality_reports/plans/2026-08-22_hive-candidate-page-extraction.md`. Its revised
technical pilot passes, but substantive human validation and the production run
remain pending; no extracted field is part of the staged release.

## Baseline when drafted

The version 2.0 local mirror was internally consistent, but the audit found false
captures, inconsistent geography, public-file packaging errors, and missing
provenance. The release also collapses most of its within-year time variation:
6,726 of 7,353 captured candidacies have more than one observed snapshot date,
but the plain-text panel retains only one date.

This plan covers four workstreams:

1. correct the released data and Dataverse package;
2. build a snapshot-day panel from the corrected raw corpus;
3. replace the URL-bearing roster with a rectangular candidate universe;
4. add observability and provenance fields modeled on CLARA.

The outcome is a corrected local staging build with a defensible denominator and
a new longitudinal data product. This plan does not authorize publication or any
sample-changing adjudication. Those actions have explicit approval gates below.

## Local implementation result

The completed build is staged at `build/release_v2_1_candidate/`. The independent
validator passes with 7,348 captured candidate-years, 797,554 raw pages, 3,032
captured ballot candidates, a 15,041-row rectangular universe, and 299,185
snapshot-day-scope rows. The manifest inventories eight data files. The roster
and universe are identical 29-column views; provenance has 15,041 rows and 68
columns.

The post-correction capture audit reports 836 review-only findings across 785
candidacies and zero hard errors. It makes no automatic exclusions. All 63
offline tests pass; 29 live-network tests are explicitly deselected. No
Dataverse draft was created and nothing was published.

## Recommended approach

Workstream 1 is a hard prerequisite. After the corrected corpus is frozen,
workstreams 2 and 3 can proceed independently. Workstream 4 joins their outputs
and defines missingness consistently across the release.

```text
1. Correct release
       ├── 2. Snapshot-day panel ──┐
       └── 3. Candidate universe ──┤
                                   └── 4. Observability and provenance
```

This plan assumes:

1. The population remains same-election-year Democratic, Republican, and DFL
   House candidates in 2018–2024 and Senate candidates in 2002–2024, restricted
   to state-years with a Senate election, plus reviewed ballot exceptions.
2. `candidate_cycle_id` continues to identify an FEC candidacy-year. It is not
   redefined as a person-year, because no complete stable person identifier is
   available.
3. The corrected release documents the mixed legacy collection history. It does
   not claim that one snapshot-cap rule generated every office-year.
4. The first rectangular universe records historical URL evidence without
   rerunning current APIs. Live recovery of missing historical URLs and new
   Wayback collection require a separate plan after this foundation is released.

## Workstream 1 — Correct the released data

### Why

Users currently receive at least three unrelated captures as usable campaign
text. Two apparent U.S. House rows are state-legislative campaigns, one Senate
candidate-year changes to a House site within the year, and the Dataverse
inventory does not match its README or manifest. Derived measures and the new
snapshot panel must not inherit these errors.

### 1A. Create a complete capture-validity review queue

Add `scripts/audit_capture_validity.py`. It reads a staged release and writes an
internal review table; it never excludes rows automatically. Each flag records
`candidate_cycle_id`, the rule, the triggering metric, a short evidence excerpt,
the proposed action, and review status.

The screen covers:

- extreme text, page, and snapshot counts;
- gambling, parked-domain, generic-platform, and high-CJK content;
- candidate, state, office, and government-domain contradictions;
- normalized domains assigned to distinct candidate IDs in the same year;
- House districts outside 0–53 and nonmissing Senate districts;
- campaign domains whose content changes identity or office within the year.

Freeze one domain-normalization function before review. Two exploratory screens
found 51 and 52 shared year-domain groups under slightly different rules; the
final queue must reconcile that difference.

Every high-severity flag receives a manual decision before rebuilding. Decisions
about bad captured content go in `config/capture_adjudication.csv`. Decisions
that a candidate is outside the federal-candidate universe go in a new
`config/candidate_universe_adjudication.csv`. Keeping these decisions separate
prevents a false website capture from being confused with an ineligible
candidate.

### 1B. Present sample-changing decisions for approval

The first review sheet must include these known cases:

| Candidate-cycle | Evidence | Recommended action |
|---|---|---|
| `H4OR06013-2024` | Indonesian economics site with a Cape Town contact | Exclude capture |
| `S4MN00023-2024` | Ballotpedia text mixed with gambling spam | Exclude capture |
| `H0MI07135-2020` | New Jersey Representative Chris Smith's site assigned to a Michigan candidate | Exclude capture |
| `H2TN92014-2022` | Text describes a Tennessee State House district 92 campaign | Exclude from federal universe |
| `H4TN57013-2024` | Text describes a Tennessee State House district 57 campaign | Exclude from federal universe |
| `S4IA00103-2014` | Early captures describe David Young's Senate campaign; the selected May capture describes his IA-3 House campaign | Retain only the valid Senate-period dates and recompute measures |

No row changes until this sheet is approved. Any additional proposed exclusions
from the full screen join the same review.

### 1C. Repair geography and public definitions

Create one geography-normalization helper and use it in the raw corpus, panels,
crosswalk, roster, and universe. Senate district must be missing. House district
0 remains valid for at-large and territorial seats; other House districts must
fall between 1 and 53. Impossible source values are adjudicated, not silently
changed.

Keep `D` and `R` in the main release products to avoid a data schema break.
Correct the codebook, which currently says those files contain `democrat` and
`republican`. Document that `text_quality` measures text length and placeholder
content, not capture validity. Add the exact length thresholds, text-cleaning
rule, two-level ICPSR aggregation formula, and topic-classifier provenance.

Reconcile the collection specification and tests with the current production
policy. Record that legacy office-years were collected under more than one rule;
do not infer a candidate-level cap from the observed corpus. Mark network tests
as live tests and remove assertions that contradict the selected production
policy.

### 1D. Rebuild into a clean staging directory

Make `scripts/rebuild_release.py` refuse a nonempty output directory unless an
explicit overwrite flag is supplied. Build the correction into a new staging
directory rather than modifying `build/release_candidate/`.

Whole-candidate exclusions may filter the existing coded panel only if every
retained coded value is proven unchanged. Any date-level repair, including the
recommended David Young truncation, requires regenerating
`panel_icpsr_compat.csv` from the corrected raw corpus with
`scripts/icpsr_replicate_coding.py`.

The approved correction yields 7,348 captured candidate-years and 797,554 raw
rows. This includes the three whole-capture exclusions, the two universe
exclusions, and removal of 357 David Young page rows outside the approved
2014-01-05 capture day.

### Workstream 1 gate

This gate passed for the six listed adjudications: three capture exclusions, two
federal-universe exclusions, and the David Young date restriction. Any
additional sample exclusion still requires approval. The corrected local
package passes, but separate approval is still required before creating or
publishing a Dataverse draft.

## Workstream 2 — Build the snapshot-day panel

### Why

The raw corpus records pages and capture timestamps, while the current text
panel selects one longest snapshot per candidate-year. A snapshot-day panel
makes within-campaign change usable without repeating the raw page text.

### Unit and output

Add `panel_snapshot_day.parquet`, built only from the corrected staged
`raw_corpus.parquet`. Its unique key is:

`candidate_cycle_id × snap_day × page_scope`

`snap_day` is the UTC date in the first eight digits of the Wayback timestamp.
`page_scope` takes `all` plus each of the eight existing page types. The `all`
row represents the full site on that day; the page-type rows let users separate
homepage, issues, biography, news, endorsements, constituent services, action,
and other pages. The file does not copy text. Users join its key to the raw
corpus when they need page text.

### Fields and calculations

Carry candidate, contest, ballot, and source identifiers. Add:

- timing: first and last capture timestamp, number of timestamps, election day,
  days to election, and pre/post-election indicators;
- coverage: pages, clean-valid pages, distinct URLs, distinct page types,
  source hosts, and a `>100`-pages runaway-day flag;
- content provenance: exact-page hashes and a day digest that is invariant to
  input row order;
- raw totals: characters and words;
- cleaned totals and within-day page means for ICPSR-compatible characters,
  words, tags, clean tags, TTR, MATTR, and entropy;
- 31 `day_topic_*` shares, produced by applying the frozen classifier once to
  the cleaned text for that day and scope.

Call the daily topics `day_topic_*`, not `icpsr_topic_*`. The existing ICPSR
topic is a nonlinear classification of one candidate-year document, so an
average of daily topic probabilities does not reproduce it.

Retain day/scope rows with no surviving cleaned text. Their substantive measures
are missing and their status is `insufficient_clean_text`; they are not zero.
Build the `all` row directly from its pages rather than averaging page-type
rows. Reuse `icpsr_clean_text()`, `page_measures()`, and the frozen Manifesto/SVM
classifier in `scripts/icpsr_replicate_coding.py`.

The corrected output contains 299,185 rows: 74,355 all-page candidate-days plus
224,830 candidate-day-page-type rows. It has 84 columns.

### Workstream 2 validation

- The output key is unique and every page-type row has an `all` row.
- All-scope page and character totals reconstruct exactly from raw pages and
  equal the sum across disjoint page-type scopes.
- Means of the all-scope daily compatibility measures reproduce the base coded
  panel within floating-point tolerance. Homepage and pre-election subsets
  reproduce the corresponding variants.
- Topic vectors sum to one when supported and are entirely missing otherwise.
- Election timing identities and hashes are exact and deterministic.
- No invalid capture contributes substantive measures.

## Workstream 3 — Publish a rectangular candidate universe

### Why

The current 9,848-row roster begins with candidates who retained a URL record.
It omits 5,195 otherwise eligible FEC candidate-years, so it cannot measure URL
discovery or capture selection against the full target population.

### Population and output

Build `release_roster.csv` from FEC records first, then left-join historical URL,
capture, ballot, and adjudication outcomes by canonical FEC ID. Do not use names
as the primary join. Preserve the reviewed 2020 Arizona ballot exception unless
the user chooses a strict FEC-only universe.

Before workstream-1 universe adjudications, the rectangular baseline was 15,043
rows: 15,042 qualifying FEC candidate-years plus one reviewed ballot exception.
The approved universe contains 15,041 rows after excluding the two Tennessee
state-legislative records. Relative to the 9,848-row version 2.0 roster, the new
roster adds 5,193 rows net: 5,195 `legacy_no_url_record` rows enter and the two
approved universe exclusions leave. This is an expansion of the denominator,
not a sample drop. `release_roster.csv` and `candidate_year_universe.csv` are
identical 29-column compatibility views.

Fields must separate three stages:

- population: identity, office, geography, FEC metadata, ballot match, and
  votes;
- URL evidence: recorded URL, normalized URL, whether a historical attempt row
  exists, validation result, and source when known;
- collection: captured, excluded invalid capture, or not captured.

Historical absent rows receive `url_discovery_status = legacy_no_url_record`.
Do not label them `not_found`, because old source failures and negative results
were not retained separately. Existing URLs with unknown provenance receive
`url_source = legacy_unknown`.

Retain `candidate_cycle_id` as the candidacy-year key. Add a normalized
`website_cycle_id` for identifying the 51–52 year-domain groups shared across
FEC IDs. Do not fabricate a stable person ID from names or domains. The codebook
must warn that one person or website can appear under multiple candidacy IDs.

Build a separate URL-valid scrape queue from the universe rather than removing
URL-free rows from the universe. Modify future URL-source code so each source can
record `found`, `no_result`, `source_unavailable`, `request_error`, and
`ambiguous_match`; do not retroactively assign those statuses to legacy runs.

### Workstream 3 validation

- The baseline equals the qualifying FEC ID set plus approved exceptions.
- No House years before 2018 and no Senate rows outside election state-years.
- `candidate_cycle_id` is unique and every join is one-to-one.
- URL status never changes population membership.
- Every captured candidate appears exactly once in the universe.
- Existing URLs remain byte-for-byte unchanged except approved corrections.
- Ballot candidates have vote totals and off-ballot candidates do not.
- `has_url` agrees exactly with accepted URL-validation states.

## Workstream 4 — Add observability and provenance

### Why

Missing website measures currently conflate no URL, no capture, insufficient
text, unsupported homepage or pre-election measures, and excluded content. A
user can mistake an archive or processing gap for a substantive zero.

### Candidate-year provenance product

Add `candidate_year_provenance.csv`, with one row for every row in the
rectangular universe. It joins URL and capture outcomes with source statistics:

- observed snapshot timestamps and days, first/last capture, and span;
- raw page rows, distinct page URLs, page-text digests, page types, and hosts;
- maximum pages in one day and runaway-page share;
- selected panel date and the longest-text/latest-tie selection rule;
- capture-adjudication status and reason;
- known collection-method version, or `legacy_mixed_unknown` where the evidence
  does not support a more precise claim.

The released raw corpus shows successful observed pages. It cannot establish all
CDX availability, failed attempts, or whether a cap bound for every legacy
office-year. Use `unknown` rather than infer those fields. Where retained progress
or logs prove a more specific state, record the evidence source.

The implemented provenance file has 15,041 rows and 68 columns. Evidence that
cannot be reconstructed from successful released pages remains explicitly
`unknown`; the collection method is `legacy_mixed_unknown` where batch-specific
rules cannot be proven.

### Status taxonomy

Use one candidate-year `observability_status`:

- `captured_usable_text`
- `captured_thin_text`
- `captured_empty_text`
- `valid_url_no_capture_unknown_reason`
- `invalid_or_placeholder_url`
- `legacy_no_url_record`
- `invalid_capture_excluded`
- `universe_excluded` (kept only in the internal review ledger, not the public
  universe)

Add family-level statuses rather than one companion field for every derived
column:

- `all_text_measure_status`
- `homepage_measure_status`
- `preelection_measure_status`
- `topic_measure_status`

Each uses `observed`, `observed_zero`, `insufficient_clean_text`,
`scope_not_observed`, or `not_computed`. A numeric zero is permitted only when
the measure was actually evaluated and zero is meaningful.

### Workstream 4 validation

- Every universe row has exactly one observability status.
- Every missing derived value is explained by its family status.
- `observed` and `observed_zero` are the only statuses permitting substantive
  values; unsupported statuses require missing values.
- Candidate-year provenance counts reconstruct from raw and snapshot-day files.
- First/last/span and selected-date identities agree across products.
- Invalid captures remain visible in the universe/provenance but absent from
  substantive data products.

## Critical files

- `config/capture_adjudication.csv` — capture-level decisions.
- `config/candidate_universe_adjudication.csv` — new federal-universe decisions.
- `scripts/audit_capture_validity.py` — new deterministic review queue.
- `src/release_universe.py` — authoritative population and adjudication logic.
- `src/release_adjudication.py` — capture, universe, and date-decision loaders.
- `src/release_geography.py` — shared office/district normalization.
- `scripts/rebuild_release.py` — clean staging build and all new products.
- `scripts/build_snapshot_day_panel.py` — new daily/scoped panel builder.
- `scripts/build_universe_provenance.py` — rectangular universe and provenance.
- `scripts/icpsr_replicate_coding.py` — reused cleaning and measurement helpers.
- `scripts/coverage_vs_returns.py` — ballot matching over the full universe.
- `scripts/validate_release.py` — expanded cross-product and status validation.
- `src/build_candidate_roster.py` — separate full universe from scrape queues.
- `SPEC.md`, `README.md`, `docs/RELEASE_README.md`, and
  `docs/deliverable_codebook.md` — methods, inventory, population, and status
  definitions.
- `tests/test_release_universe.py`, `tests/test_release_repairs.py`,
  `tests/test_capture_validity.py`, `tests/test_snapshot_day_panel.py`,
  `tests/test_universe_provenance.py`, `tests/test_release_adjudication.py`, and
  `tests/test_release_geography.py` — offline regression tests.

## End-to-end verification

- [x] Run the offline unit suite. Result: 63 passed; 29 live-network tests were
  deselected.
- [x] Rebuild into a new empty staging directory.
- [x] Run `scripts/validate_release.py` against that directory. It passes all
  frozen counts and cross-product checks.
- [x] Compare the corrected release with version 2.0 and investigate all
  candidate, raw-page, ballot, schema, and support changes.
- [x] Confirm that snapshot-day and provenance products reconstruct from raw and
  that the roster equals the approved universe.
- [x] Verify the README, codebook, and eight-file manifest against staged bytes.
- [ ] With separate approval, create and verify an unpublished Dataverse draft
  with tabular ingest disabled for CSVs.
- [ ] With explicit publication approval, publish the corrective version and
  rerun the download-byte checks against the DOI version.

## Approval gates

1. **Adjudication gate — passed:** the approved decisions include the three
   invalid captures, two Tennessee rows, and David Young date rule.
2. **Schema gate — passed:** `release_roster.csv` expands to the approved 15,041
   rows, with snapshot-day, universe, and provenance products added.
3. **Dataverse draft gate — pending:** no authorization to create or replace
   files in a draft has been given.
4. **Publication gate — pending:** no authorization to publish has been given.

## Saved publication and announcement sequence

The next release step is an unpublished Dataverse draft, not immediate
publication.

1. Upload the eight data files, README, codebook, and manifest with tabular CSV
   conversion disabled.
2. Download every draft file by its Dataverse data-file ID and verify filename,
   MIME type, byte size, SHA-256, rows, and columns.
3. Regenerate the manifest from those downloaded bytes, replace the draft
   manifest, and repeat the download check.
4. Present the verified final inventory for explicit publication approval.
5. Publish only after that approval, verify the live DOI version, and then post
   the announcement.

Saved announcement draft:

> Updated Candidate Campaign Websites data: corrected capture errors, a
> rectangular 15,041-row candidacy-year universe, a new 299,185-row
> snapshot-day panel, and clearer provenance. The release covers 797,554
> archived pages for 7,348 candidacies. [DOI]

Creating the draft does not authorize publication or posting the announcement.

## Non-goals

- No live URL recovery or new Wayback scraping in this plan.
- No third-party, independent, presidential, gubernatorial, state-legislative,
  or special-election population extension.
- No stable person identifier inferred from names, domains, or incomplete DIME
  links.
- No new model-coded candidate-presentation or campaign-infrastructure fields;
  those follow after observability is fixed.
- No paid API use. The daily topic classifier is the existing local SVM, not an
  LLM service.
- No publication without explicit approval.

## Glossary

**Candidacy-year:** one FEC candidate identifier in one election year. A person
can have more than one candidacy-year row in the same year when filing for more
than one office.

**Snapshot day:** a UTC calendar day with one or more observed Wayback capture
timestamps for a candidacy. The panel key also includes `page_scope`.

**Capture validity:** whether archived content belongs to the assigned federal
campaign. It is separate from text length and extraction success.

**Observability status:** the recorded reason a candidate-year or measure is
available, missing, or excluded.
