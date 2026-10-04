# Released data and extension audit

**Date:** 2026-08-22  
**Audit baseline:** Harvard Dataverse version 2.0, published 2026-08-21,
DOI [10.7910/DVN/BZ2JRS](https://doi.org/10.7910/DVN/BZ2JRS)  
**Baseline local data checked:** `build/release_candidate/`; at audit time its
five data-file hashes matched the manifest deposited with version 2.0  
**Corrected local staging build:** `build/release_v2_1_candidate/`; validated
locally and not published  
**Verdict at audit time:** repair the false captures and the Dataverse packaging before
expanding collection. The existing data are structurally consistent, but at
least three released candidate-years contain clearly unrelated content and are
labeled usable. The extension should first expose the time variation already in
the corpus and adopt CLARA-style observability metadata.

## Local implementation result

The corrected staging build now passes the independent release validator. This
resolves the local data, schema, documentation, manifest, universe, snapshot,
and provenance work. It does not resolve the Dataverse packaging problem because
no draft was created and nothing was published.

| Product or gate | Validated local result |
|---|---:|
| Captured candidate-years | 7,348 |
| Raw page rows | 797,554 |
| Captured candidates matched to the general-election ballot | 3,032 |
| Universe, roster, and provenance rows | 15,041 each |
| Snapshot-day-scope rows | 299,185 |
| All-page snapshot-day rows | 74,355 |
| Page-type snapshot-day rows | 224,830 |
| Data files in the manifest | 8 |

The build removes the three confirmed invalid captures and excludes the two
approved Tennessee state-legislative records from the federal universe. It
retains David Young's 2014 Senate capture only on the approved day, 2014-01-05.
Five raw/crosswalk district inconsistencies were repaired from canonical
candidate metadata without excluding candidates. Six uncaptured FEC records
with invalid House district codes remain in the rectangular universe: their
source values are preserved, normalized districts are missing, and
`geography_status` records the problem.

The post-correction capture screen produces 836 review-only findings across 785
candidacies and zero hard errors. These findings remain pending human review;
the screen applies no automatic exclusion and does not alter an adjudication
ledger.

## What this audit asks

The data audit asks whether the released files contain internally inconsistent
or substantively implausible values, and whether a user can understand the
population, units, missing values, and measurements from the deposit alone. The
extension audit asks what additional collection or derived variables would add
the most research value.

The checks compare candidate-years in the release roster with captured
candidate-years in the text and coded panels. They inspect raw page records,
reconstruct the panel with the existing validator, test identity and timestamp
rules, inspect extreme records, and compare the public README and codebook with
the actual Dataverse file inventory. The extension comparison uses CLARA version
3.0, DOI [10.7910/DVN/2KJSCO](https://doi.org/10.7910/DVN/2KJSCO), as the
reference design.

## 1. Released-data audit of version 2.0

### Bottom line

The release has two critical problems. First, at least three candidate-years
contain text from the wrong site or from a domain that no longer hosted the
candidate, but all three have `text_quality = usable`. Second, Dataverse ingested
three promised CSV files as tab-delimited `.tab` files while the README and
manifest still name and checksum the original CSV files. A user cannot reproduce
the manifest against the files Dataverse presents.

The core mechanical checks pass on the complete local staging directory. The
7,353 captured candidate-year keys agree across the panel, coded panel, and
crosswalk; all keys are unique; timestamps fall in the assigned election year;
page character and word counts reconstruct from text; topic shares sum to one;
and the candidate-year panel reconstructs exactly from the raw corpus. These
checks establish cross-file consistency, not content validity.

### Findings

| Priority | Motivation | Test | Verdict |
|---|---|---|---|
| Critical | Unrelated pages can enter as campaign text after a domain changes hands or a URL is assigned to the wrong person. | Inspect extreme text records and compare their contents with candidate, office, state, and URL. | At least three released rows are false captures and all are labeled usable. Remove them and rerun derived measures. |
| Critical | The public inventory and checksums must describe the files users download. | Compare Dataverse version 2.0's file list with its README and manifest. | Dataverse exposes `candidate_crosswalk.tab`, `panel_icpsr_compat.tab`, and `release_roster.tab`, while the documentation and manifest promise `.csv`. Their deposited byte sizes therefore also differ from the manifest. Replace the ingested files without tabular conversion or update the inventory and manifest to the actual downloads. |
| High | A `usable` label should not certify content relevance. | Compare `text_quality` with manual content checks. | `text_quality` measures length/placeholders, not whether the page belongs to the candidate. Rename or document it as extraction-length quality and add a separate capture-validity field. |
| High | Contest geography should be valid and have consistent missing-value semantics. | Check office against district values and plausible House district ranges. | Senate district is `0.0` in 1,543 rows, missing in 18, and `3.0` in one row, although the codebook says it is missing for Senate. Two House rows have impossible districts 57 and 92. Standardize Senate district to missing and adjudicate the two House records. |
| High | One person can file under multiple FEC office IDs in the same year. Users need to know whether these are distinct candidacies or duplicated websites. | Group candidate-year rows by year and normalized campaign URL. | The frozen normalization finds 52 shared year-domain groups (104 findings). Retain `candidate_cycle_id` as the candidacy-year key and add `website_cycle_id`; do not infer a stable person ID from incomplete name or domain evidence. |
| Medium | Public category definitions must match values. | Compare codebook categories with released values. | The codebook defines `party` as `democrat` or `republican`, but the files contain `D` and `R`. Correct the codebook or recode the files. |
| Medium | Users need an explicit population denominator. | Compare the FEC target universe with the release roster. | The target office-years contain 15,042 same-year major-party FEC records after Senate-race filtering; only 9,847 correspond to roster rows, leaving 5,195 without a roster URL-attempt row. The README states the roster excludes these records but does not quantify them. Publish a full-universe roster with URL-discovery status. |
| Medium | Very large selected sites can dominate text analyses. | Inspect the upper tail of panel length and page counts. | The largest panel row has 8.39 million characters and 219 pages; other false captures have 3.97 and 3.09 million characters. The coded panel has runaway-page diagnostics, but the plain text panel has no upper-tail or relevance flag. Add domain/content adjudication and expose page concentration in the main panel. |
| Medium | The collection method should have one current specification. | Compare `SPEC.md`, configuration, production code, and tests. | They conflict. `SPEC.md`, config, and production code apply quarterly deduplication and a 200-snapshot cap; `tests/test_no_caps.py` says no caps is the intended ICPSR method; live end-to-end tests require monthly deduplication. Resolve the intended method, remove obsolete tests/configuration, and record which rule generated each release year. |
| Medium | A user should be able to interpret derived variables exactly. | Compare codebook descriptions with generating logic. | The codebook describes repeated families but does not give the two-level aggregation formula (mean over pages within snapshot-day, then mean over days), the exact text-cleaning rule, topic model/training provenance, or the thresholds behind `text_quality`. Add these definitions or link a deposited methods note. |
| Low | Territorial candidates may surprise users expecting only voting House districts. | Inspect state codes. | The release includes AS, GU, MP, PR, and VI. This can be defensible, but “U.S. House candidates” should state explicitly that delegates and the resident commissioner are included and how district 0 is encoded. |

### Confirmed false captures

1. `H4OR06013-2024`, Denyc Boles, Oregon House: the selected text begins with a
   Cape Town contact and Indonesian-language economics content. The row contains
   about 3.97 million characters and is labeled usable.
2. `S4MN00023-2024`, Ole Savior, Minnesota Senate: the selected text contains
   Ballotpedia material mixed with mojibake and gambling spam. The row contains
   about 3.09 million characters and is labeled usable.
3. `H0MI07135-2020`, Christopher Bartholomew Smith, Michigan House: the selected
   URL is `chrissmith.house.gov` and the text identifies the officeholder as the
   representative for New Jersey's 4th district. The row contains about 1.10
   million characters and is labeled usable.

These examples came from a targeted extreme-value and identity screen, not an
exhaustive manual review. They imply that the full set needs a systematic audit
for candidate-name, state, office, domain ownership, and generic/spam content.

### Checks that passed on the version 2.0 baseline

- The complete local staging directory passes `scripts/validate_release.py`:
  7,353 candidate-years, 799,058 raw rows, and 9,848 roster rows.
- Candidate-year keys are unique and agree across the three captured-candidate
  products. The roster contains every captured key.
- All raw and selected timestamps fall in their assigned election year.
- Raw `n_char` and `n_words` equal counts reconstructed from preserved text.
- The selected candidate-year panel reconstructs exactly from the raw corpus.
- Ballot, vote, stage, and data-source semantics agree across release products.
- All supported 31-topic vectors sum to one and the topic measures stay in
  `[0, 1]`.
- No malformed FEC IDs or candidate-cycle ID mismatches were found.

### Repair status

1. **Complete locally:** run the capture-validity screen and record the six
   approved capture, universe, and date adjudications.
2. **Complete locally:** rebuild eight data files and all affected derived
   measures from the corrected raw corpus.
3. **Pending separate approval:** create a Dataverse draft without tabular CSV
   conversion, verify the downloaded bytes, and obtain explicit publication
   approval.
4. **Complete locally:** correct party/geography documentation and define the
   cleaning, aggregation, quality, topic, and status rules.
5. **Complete locally:** publish the 15,041-row rectangular universe in the
   staging package, including 5,195 `legacy_no_url_record` rows.

## 2. Extension audit based on version 2.0

### Bottom line

Do not start by collecting more election-year homepage snapshots. The corpus
already contains two or more snapshot dates for 6,726 of 7,353 candidate-years,
and 5,084 span at least 180 days. The median candidate-year spans 255 days. The
highest-return extension is to turn those existing captures into a documented
snapshot-day panel, then improve URL/page discovery for missing candidates and
structural pages. More crawling should target identifiable coverage gaps rather
than add density to already dense candidate-years.

### Recommended sequence

#### Phase 1: expose and validate the existing longitudinal data

Build a snapshot-day panel with one row per candidate, date, and page type. It
should contain content/count measures, topic shares, capture validity, days to
election, pre/post-election status, page counts, and selected-domain metadata.
The current release collapses the plain-text panel to one longest snapshot even
though 91.5% of candidate-years have multiple dates. The coded panel averages
over dates, which discards within-year campaign dynamics.

This phase should also add CLARA-style provenance:

- first and last capture timestamps;
- available snapshots and unique content digests;
- fetched pages and pages by type;
- extraction attempts and successes;
- snapshot/page selection mode and whether a cap bound;
- capture-validity decision and reason;
- a status field for every substantive derived measure, distinguishing observed
  zero, observed positive, not extracted, insufficient coverage, and invalid
  capture.

#### Phase 2: expand candidate and URL coverage

The release roster is not the full FEC candidate universe. Within the current
office-years, 5,195 of 15,042 same-year major-party FEC records do not have a
roster URL-attempt row. Create a rectangular universe first, then run additional
URL discovery only for missing cases. Record each discovery source and failure
reason.

Priority sources and methods are:

1. archived campaign/FEC directories and archived search results;
2. committee websites and linked social profiles;
3. deterministic domain probes based on candidate names and slogans;
4. search-assisted matching with retained evidence and manual review;
5. archive-index URL discovery for structural subpages that are absent from the
   captured homepage navigation.

After major-party coverage is rectangular, add general-election independents
and third-party candidates before adding every minor FEC filer. Across all
2002–2024 House and Senate FEC records, 7,780 same-year records use affiliations
other than DEM/REP, including 2,091 Libertarian, 1,722 independent, and 717
Green records. A ballot-based extension gives a clear denominator and avoids a
large set of nominal filings with little public campaign activity.

House 2002–2016 should remain an ICPSR merge rather than a recollection unless a
specific harmonization problem requires raw pages. Other defensible later
extensions are special elections outside the even-year windows, presidential
campaigns, gubernatorial campaigns, and state legislative candidates. Each is a
new population and should be released as a separate module, not silently folded
into this panel.

#### Phase 3: add CLARA-style derived variables

The most transferable variables do not require personal-data extraction:

| Family | Candidate-website variables | Why it adds value |
|---|---|---|
| Campaign infrastructure | donation link, volunteer link, newsletter signup, event calendar, contact form, office address, join/support link | Measures campaign professionalization and mobilization capacity. |
| Social presence | candidate-specific Facebook, Instagram, X, YouTube, TikTok, and Telegram links; count of platforms | Measures cross-platform campaign infrastructure; exclude share buttons and national-party accounts. |
| Site structure | total pages, distinct page types, issue pages, news pages, biography pages, endorsements, action pages | Provides transparent controls for crawl density and campaign information supply. |
| Issue breadth | number of distinct topic families, dominant topic, topic diversity, issue-block count | Complements current topic shares with breadth and concentration. Keep salience separate from position. |
| News dynamics | deduplicated article count; topic, function, and geographic-focus shares by date | Creates a genuinely time-varying communication outcome rather than relying on static issue pages. |
| Candidate presentation | biography present, endorsements, officeholding/incumbency claims, local-root references, personal/family background, candidate photo density | Captures personalization and candidate-centered campaigning. These require validation before release. |
| Electoral appeals | donation/volunteer/event calls, get-out-the-vote language, opponent mentions, partisan/national versus district focus | Links site functions to campaign strategy. Label definitions need a fixed taxonomy and human agreement checks. |
| Provenance/observability | per-feature status, unique digests, capture bounds, selection mode, cap hit, extraction success | Prevents archive and processing gaps from being coded as substantive zeros. This is the highest-priority CLARA transfer. |

Model-coded extraction should use the same open-weight model family on Hive for
all released values, with a frozen prompt/schema, evidence spans, deterministic
post-processing, and a stratified human-validation sample. No model-coded field
should be released without a status variable and validation summary.

### What not to prioritize

- More snapshots for candidate-years that already have dense within-year
  coverage. Nineteen candidate-years already have at least 200 observed dates.
- A Playwright fallback for archived JavaScript shells. The project has already
  established that Wayback often lacks the backend responses needed to recover
  their content.
- Running the existing paid `classify_pages_llm.py` as written. It uses a paid
  API, has not been run, and conflicts with the project's Hive provenance rule.
- A single all-purpose “website quality” score. Keep capture validity, text
  quantity, page diversity, and extraction observability separate.
- Position or ideology inference from the existing 31 topic shares. They measure
  issue attention only.

## Implementation decisions and remaining choices

1. **Decided:** the current production policy uses quarterly deduplication plus
   a 200-snapshot cap. Older batches retain `legacy_mixed_unknown` provenance
   where the surviving evidence cannot identify their exact rule.
2. **Decided:** `candidate_cycle_id` identifies a FEC candidacy-year;
   `website_cycle_id` identifies normalized year-domain reuse. No person ID is
   inferred.
3. **Decided for this build:** the rectangular population is the approved
   major-party House/Senate universe plus the reviewed Arizona exception.
4. **Open:** choose and validate the next derived-variable family. Campaign
   infrastructure plus social presence remains the recommended first batch.
5. **Open and approval-gated:** decide whether to create a corrective Dataverse
   draft and, separately, whether to publish it.

## Glossary

**Capture validity:** whether archived content actually belongs to the assigned
candidate and campaign period. This is distinct from text length or parsing
success.

**Candidate-year:** one FEC candidacy identifier in one election year. It is not
always a unique person-year because a person can have more than one FEC office
identifier.

**Snapshot-day panel:** one row per candidacy, archived UTC capture day, and
page scope, with pages aggregated within that day and scope.

**Observability status:** a categorical reason that states whether a feature was
measured and, if not, whether the failure came from the archive, crawling,
extraction, or capture adjudication. It uses `unknown` where the released
evidence cannot distinguish those causes.
