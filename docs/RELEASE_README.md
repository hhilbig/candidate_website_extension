# U.S. House (2018–2024) and Senate (2002–2024) candidate websites

This release contains archived campaign website text for Democratic and
Republican U.S. House candidates from 2018 through 2024 and U.S. Senate
candidates from 2002 through 2024. It extends the House candidate data in ICPSR
226001 through 2024 and adds a Senate series. Version 4.0 adds six files of
issue statements, biography facts, and endorsements extracted from the same
pages; the core files are unchanged from version 3.0.

## Files

The release separates the full candidate denominator, observed website text,
candidate-level measures, snapshot-day measures, and evidence about why a
value is or is not observed.

| File | Rows | Columns | One row represents |
|---|---:|---:|---|
| `candidate_year_universe.csv` | 15,041 | 29 | One candidate in one election year, whether or not a URL or capture was recorded |
| `release_roster.csv` | 15,041 | 29 | An identical compatibility alias of `candidate_year_universe.csv` |
| `candidate_year_provenance.csv` | 15,041 | 68 | One universe candidate-year with collection, capture, adjudication, and measure-availability evidence |
| `raw_corpus.parquet` | 797,554 | 28 | One archived page at one snapshot timestamp |
| `panel_snapshot_day.parquet` | 299,185 | 84 | One candidate-day and page scope, without page text |
| `panel_candidate_year.csv` | 7,348 | 25 | One captured candidate-year at its selected snapshot timestamp, with concatenated text |
| `panel_icpsr_compat.csv` | 7,348 | 114 | One captured candidate-year with ICPSR-compatible derived measures |
| `candidate_crosswalk.csv` | 7,348 | 36 | One captured candidate-year with FEC, ballot, DIME, and ICPSR-compatible identifiers |

The snapshot-day panel consists of 74,355 `all`-scope candidate-days and
224,830 rows for observed page-type scopes. The captured files contain 3,032
candidate-years matched to Democratic or Republican general-election returns.
The other 4,316 captured candidate-years are same-year federal FEC candidates
that were not matched to those returns.

The release also contains this README, the codebook, and `manifest.json`. The
manifest records each of these eight data files' row and column count, byte
size, and SHA-256 checksum.

Version 4.0 adds six files of extracted variables, described in
`candidate_extraction_codebook.md` and inventoried in
`candidate_extraction_manifest.json`.

| File | Rows | Columns | One row represents |
|---|---:|---:|---|
| `candidate_extraction_status.csv` | 45,123 | 9 | One candidate-year and route (issues, biography, endorsements) |
| `candidate_extraction_documents.parquet` | 23,200 | 47 | One distinct page text for a candidate-year and route |
| `candidate_extraction_segments.parquet` | 34,346 | 76 | One model input window within a page |
| `candidate_issue_statements.parquet` | 287,427 | 26 | One issue statement on one page |
| `candidate_biography_facts.parquet` | 104,331 | 20 | One biography fact on one page |
| `candidate_endorsements.parquet` | 153,108 | 45 | One endorser named on one page |

Every extracted record quotes the page it comes from. The records were checked
by a model coder, not by hand, and about half of all rows repeat a record from
another version of the same page; see the extraction codebook before counting
records per candidate.

## Population

The population starts from Federal Election Commission (FEC) candidate files,
not from successful URL or Wayback records. It includes:

1. Democratic, Republican, and Minnesota Democratic-Farmer-Labor records whose
   FEC candidate election year equals the file year;
2. House records in 2018, 2020, 2022, and 2024;
3. Senate records in even years from 2002 through 2024, limited to states that
   held a Senate general election that year; and
4. reviewed general-election candidates whose canonical FEC identity is absent
   from the same-year FEC population.

FEC candidate status is metadata, not a population filter. Two reviewed FEC
records for Tennessee state legislative campaigns are excluded because they
are not federal House candidacies. A House district of `0` denotes an at-large
or territorial seat; Senate district is missing by definition. The universe
retains an invalid or missing source district as `source_district`, sets the
normalized `district` to missing, and explains the case in `geography_status`.

Use `candidate_cycle_id` as the candidate-year key. It combines the canonical
FEC candidate ID and election year. It is not a stable person identifier: the
same person can have a different key in another year or office. The
`website_cycle_id` field combines a normalized website host and year. It is a
website-year key, not a person key.

`on_ballot` identifies candidates matched to official Democratic or Republican
general-election returns. In the captured text, coded panel, and crosswalk
files, these rows have `stage = 2` and `data_source = general_wayback`; other
captured rows have missing `stage` and `data_source = election_year_wayback`.
Use `on_ballot`, not FEC candidate status, to select the general-election
sample. The universe, roster, and provenance files expose `on_ballot` directly
and do not duplicate `stage` or `data_source`.

## What was observed

The full population contains 15,041 candidate-years. The corpus contains
substantive archived page rows for 7,348 of them. Absence from the corpus does
not by itself show that the campaign lacked a website, that the Internet
Archive had no captures, or that a collection attempt failed. The historical
source only preserves URL evidence for part of the population.

The universe and provenance files distinguish these cases. In particular,
`legacy_no_url_record` means that the staged inputs contain no historical URL
record for that candidate-year. It does not mean that a search was attempted
and returned no URL. `valid_url_no_capture_unknown_reason` means a valid URL is
recorded but the released evidence does not establish why no page appears.

Reviewed captures unrelated to the assigned campaign remain in the universe
with `capture_status = invalid_capture_excluded`, but they do not appear in the
substantive corpus or captured panels. Reviewed date adjudication can retain an
inclusive part of a candidate-year's capture history when a website changes to
a different campaign during the year. The provenance file records both forms
of adjudication.

## Construction and derived measures

Campaign URLs came from historical FEC and Wikidata collection records. The
crawler queried election-year Wayback records, retrieved the homepage, and
followed same-domain links one level deep. The eight page types are `homepage`,
`issues`, `biography`, `news`, `endorsements`, `constituent_services`, `action`,
and `other`.

The historical corpus combines collection runs made under different snapshot
selection rules. The current crawler keeps the latest record for each
normalized URL within a three-month bucket, then applies a temporally
stratified cap of 200 selected snapshots per candidate-year. These current
settings must not be projected backward onto every released row. The release
does not contain the complete CDX query responses needed to determine whether
a historical cap bound or why an attempted collection failed.

`panel_candidate_year.csv` selects the snapshot timestamp whose pages contain
the most extracted characters. It uses the later timestamp when totals tie and
then concatenates pages in a fixed page-type order. `text_quality` describes
the length and placeholder content of that selected extraction. It is not a
judgment about whether the website belongs to the candidate.

The ICPSR-compatible measures first apply the replicated ICPSR text cleaning
rule to each page. They drop pages with no surviving cleaned text, average page
measures within a snapshot day, and then average the daily values within a
candidate-year. Homepage and pre-election variants repeat this calculation for
their stated scopes. Election Day is included in the pre-election period.

The 31 candidate-year topic shares use a frozen supervised classifier trained
on U.S. Manifesto Project version 2021-1 quasi-sentences. The model is a
TF-IDF plus radial-basis SVM with fixed class mapping and random seed. It scores
one concatenated cleaned candidate-year document, with a separate homepage
document. No released topic value depends on a commercial language-model API.

`panel_snapshot_day.parquet` exposes the daily intermediate data. Every
candidate-day has one `all` row and one row for each observed page type. Its
ICPSR-compatible text measures are page means within that day and scope. Its
`day_topic_*` shares instead score one cleaned document per day and scope.
Consequently, averaging `day_topic_*` values does not reproduce the
candidate-year `icpsr_topic_*` values.

## Use

Use the files according to the question:

- Use `candidate_year_universe.csv` or its identical `release_roster.csv` alias
  as the denominator for population, URL, and capture analyses.
- Use `candidate_year_provenance.csv` to distinguish absence of evidence from
  an observed zero, an unsupported text measure, or an adjudicated exclusion.
- Use `raw_corpus.parquet` when page text, URLs, or exact capture timestamps are
  required.
- Use `panel_snapshot_day.parquet` for timing analyses and for diagnostics that
  should not load or duplicate the text.
- Use `panel_candidate_year.csv` for one selected text observation per
  candidate-year.
- Use `panel_icpsr_compat.csv` for measures designed to append to ICPSR 226001.
- Use `candidate_crosswalk.csv` for identity and external-score fields.
- Use the six extraction files, described in `candidate_extraction_codebook.md`,
  for issue statements, biography facts, and endorsements quoted from the
  pages. Keep one row per `candidate_record_key` before counting.

For the ICPSR append, restrict this release to captured House candidate-years
in 2018 through 2024 with `icpsr_compatible = TRUE`. A compact R function is
available from the [project website](https://www.hannohilbig.com/candidatewebsites/merge_with_icpsr.R).

Party coding differs by file for backward compatibility. The raw corpus,
snapshot-day panel, and ICPSR-compatible panel use `democrat` and `republican`.
The selected panel, crosswalk, universe, roster, and provenance files use `D`
and `R`; the crosswalk also includes `party_full`.

## Limitations

- Wayback coverage is selective. Missing pages and dates need not be random
  across candidates, years, parties, or types of website.
- The released data record successful pages and a partial historical
  URL roster. They do not contain complete CDX availability, failure, retry, or
  legacy cap-binding logs.
- The collection covers the election year. A page captured after Election Day
  can reflect a changed or expired campaign domain. Use the pre-election
  measures or the snapshot timing fields when that distinction matters.
- Linked pages can come from nearby archive timestamps. A snapshot-day row is
  therefore a day-level grouping of observed pages, not proof of one perfectly
  synchronized live website.
- `text_quality` evaluates extraction length and placeholder markers only.
  Capture identity is handled separately through adjudication.
- `icpsr_ttr_approx`, `icpsr_mattr_approx`, and `icpsr_entropy_approx` are close
  replications rather than bit-for-bit reproductions of the original software.
- Topic shares measure issue emphasis, not a policy position or the truth of a
  campaign claim.

## Terms of use

The compilation, derived variables, and documentation are released under the
Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright
in the underlying campaign material remains with its authors. Each text row
records `snap_url`, the archived source URL. Each extracted record links through
`parent_document_id` to its page in `candidate_extraction_documents.parquet`,
which lists the archived URLs in `wayback_urls`.

Rights holders may request removal through the deposit owner's contact
channel.

## Sources

- FEC candidate master files: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison data: DOI `10.3886/E226001V1`
