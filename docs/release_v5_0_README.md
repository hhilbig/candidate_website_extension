# U.S. House (2018–2026) and Senate (2002–2026) candidate websites

This release contains campaign website text for Democratic and Republican U.S.
House candidates from 2018 through 2026 and U.S. Senate candidates from 2002
through 2026. It extends the House candidate data in ICPSR 226001 and adds a
Senate series. Six files hold issue statements, biography facts, and
endorsements extracted from the same pages.

Version 5.0 adds the 2026 election to each file. The rows for 2002 through 2024
are unchanged from version 4.0, with one exception: version 5.0 corrects the
general-election flag `on_ballot` for 113 candidate-years (see "Population").
The 2026 rows differ from the earlier years in
their source, their frequency, and their search record; see "2026 collection"
below. The 2026 data are a first version, collected through November 5, 2026.

## Files

The release separates the full candidate denominator, observed website text,
candidate-level measures, snapshot-day measures, and evidence about why a
value is or is not observed.

| File | Rows | Columns | One row represents |
|---|---:|---:|---|
| `candidate_year_universe.csv` | 18,697 | 29 | One candidate in one election year, whether or not a URL or capture was recorded |
| `release_roster.csv` | 18,697 | 29 | An identical compatibility alias of `candidate_year_universe.csv` |
| `candidate_year_provenance.csv` | 18,697 | 68 | One universe candidate-year with collection, capture, adjudication, and measure-availability evidence |
| `raw_corpus.parquet` | 898,565 | 33 | One page at one copy time |
| `panel_snapshot_day.parquet` | 354,195 | 84 | One candidate-day and page scope, without page text |
| `panel_candidate_year.csv` | 9,081 | 26 | One captured candidate-year at its selected snapshot timestamp, with concatenated text |
| `panel_icpsr_compat.csv` | 9,081 | 114 | One captured candidate-year with ICPSR-compatible derived measures |
| `candidate_crosswalk.csv` | 9,081 | 36 | One captured candidate-year with FEC, ballot, DIME, and ICPSR-compatible identifiers |
| `event_pairs_2026.csv` | 8,611 | 60 | One 2026 candidate and one election event of the candidate's state and office, with the nearest website copy before and after the event |
| `on_ballot_2026_sources.csv` | 92 | 6 | One Wikipedia page and revision used for `on_ballot` in 2026 |

The snapshot-day panel consists of 91,974 `all`-scope candidate-days and
262,221 rows for observed page-type scopes. The captured files contain 3,779
candidate-years of general-election candidates (`on_ballot`). The other 5,302
captured candidate-years are same-year federal FEC candidates without that
match.

The release also contains this README, the codebook, and `manifest.json`. The
manifest records each of these ten data files' row and column count, byte
size, and SHA-256 checksum.

Six files hold the extracted variables. They are described in
`candidate_extraction_codebook.md` and inventoried in
`candidate_extraction_manifest.json`.

| File | Rows | Columns | One row represents |
|---|---:|---:|---|
| `candidate_extraction_status.csv` | 56,091 | 9 | One candidate-year and route (issues, biography, endorsements) |
| `candidate_extraction_documents.parquet` | 38,492 | 48 | One distinct page text for a candidate-year and route |
| `candidate_extraction_segments.parquet` | 55,077 | 76 | One model input window within a page |
| `candidate_issue_statements.parquet` | 460,957 | 26 | One issue statement on one page |
| `candidate_biography_facts.parquet` | 138,650 | 20 | One biography fact on one page |
| `candidate_endorsements.parquet` | 213,956 | 45 | One endorser named on one page |

Every extracted record quotes the page it comes from. The records were checked
by a model coder, not by hand, and about half of all rows repeat a record from
another copy of the same page; see the extraction codebook before counting
records per candidate.

## Population

The population starts from Federal Election Commission (FEC) candidate files,
not from successful URL or Wayback records. It includes:

1. Democratic, Republican, and Minnesota Democratic-Farmer-Labor records whose
   FEC candidate election year equals the file year;
2. House records in 2018, 2020, 2022, 2024, and 2026;
3. Senate records in even years from 2002 through 2026, limited to states that
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

For 2002 through 2024, `on_ballot` identifies candidates matched to official
Democratic or Republican general-election returns. In the captured text, coded
panel, and crosswalk files, these rows have `stage = 2` and
`data_source = general_wayback`; other captured rows have missing `stage` and
`data_source = election_year_wayback`.
Use `on_ballot`, not FEC candidate status, to select the general-election
sample. The universe, roster, and provenance files expose `on_ballot` directly
and do not duplicate `stage` or `data_source`.

Version 5.0 corrects this flag for 2002 through 2024. It adds 60 candidates
whom the earlier match did not find (most at-large House seats, and the
Minnesota and North Dakota party labels in the returns). It removes the flag
from 7 candidates who had the votes of a namesake, and from 46 further FEC IDs
where one returns row had flagged two or three IDs; the flag stays with one ID.
The returns do not cover the territorial delegates (the District of Columbia
in part), so most of these contests have no flagged candidate before 2026. A
contest can have more than one flagged candidate of one party (Louisiana,
California and Washington top-two contests, Alaska).

For 2026, official returns are not yet published. `on_ballot` comes from the
general-election candidate lists of the English Wikipedia state pages;
`on_ballot_2026_sources.csv` lists each page and its revision. Treat the 2026
flag as provisional. `general_votes` is missing for 2026, and `data_source` is
`general_2026_collection` or `election_year_2026_collection`. A later version
replaces this source with official returns. Five 2026 general-election
candidates whose FEC record is outside the population rule enter as reviewed
additions. Fifteen persons have two FEC IDs in 2026; the text is under one ID,
and the other ID has `capture_status = captured_under_other_fec_id`. The rule
requires the same name, state, office, party, and website. A person who has
IDs for two states or two offices, or two spellings of the name, keeps text
under each ID.

## What was observed

The full population contains 18,697 candidate-years. The corpus contains
substantive page rows for 9,081 of them. Absence from the corpus does
not by itself show that the campaign lacked a website, that the Internet
Archive had no captures, or that a collection attempt failed. The historical
source only preserves URL evidence for part of the population.

The universe and provenance files distinguish these cases. In particular,
`legacy_no_url_record` means that the staged inputs contain no historical URL
record for that candidate-year. It does not mean that a search was attempted
and returned no URL. `valid_url_no_capture_unknown_reason` means a valid URL is
recorded but the released evidence does not establish why no page appears.
These two values apply to 2002 through 2024 only.

Reviewed captures unrelated to the assigned campaign remain in the universe
with `capture_status = invalid_capture_excluded`, but they do not appear in the
substantive corpus or captured panels. Reviewed date adjudication can retain an
inclusive part of a candidate-year's capture history when a website changes to
a different campaign during the year. The provenance file records both forms
of adjudication.

## 2026 collection

The 2026 population has 3,656 candidate-years. 2,459 have a website URL, and
1,733 have page text in the corpus (101,011 page rows; 1,689 of them have
`text_quality = usable` in the candidate-year panel). Of the 954
general-election candidates, 705 have page text. The 2026 rows differ from the earlier years
in three ways.

**Source.** For 2002 through 2024, all text comes from the Internet Archive.
For 2026, the project also copied each campaign website directly, from
September 14, 2026 through November 5, 2026 (69,017 page rows). Archived copies
of 2026 start in January 2026 (31,994 page rows). `capture_source` is `live`
for a direct copy and `wayback` for an archived copy. `snap_url` is missing for
a live row, because no archived copy exists for it; `urlkey` gives the page
address.

**Frequency.** From September 14, each 2026 website has a direct copy about
every 30 days, and a copy on the day before and the day after an election of
the candidate's state. In practice these are the days around the general
election of November 3. Copies around the primaries come from the Internet
Archive, at the dates the archive holds; all primaries but one (Delaware,
September 15) took place before September 14. A candidate has several dated
copies (median 6). For 147 candidates the project also copied the homepage
alone on most days, until the name check passed; count copies with more than
one page if the number of copies matters.
`snapshot_id` identifies one copy of one website, and `date` is the time of
each page. `panel_snapshot_day.parquet` holds the measures for each copy day.
`panel_candidate_year.csv` keeps the copy with the most text, as in the
earlier years; `sel_snapshot_id` names that copy. `event_pairs_2026.csv` names,
for each candidate and election event, the nearest complete copy within 30
days before and after the event. Of the 2,371 candidate-events with a copy on
both sides, 1,270 are general-election pairs (almost all direct copies), 710
are primary pairs (almost all archived copies), and 391 are pairs for another
event type (runoff, nonpartisan primary, convention, special election).

**Search record.** For 2026, the project searched for a website for each
candidate and recorded each failed copy. The provenance file therefore uses
three status values that do not occur before 2026: `no_url_found`,
`valid_url_no_usable_capture` (with the cause in `collection_failure_reason`),
and `captured_under_other_fec_id`.

A 2026 copy is in the corpus only if its homepage text names the candidate
(family name and given name, or a listed short form of the given name). The
other pages of the copy are not checked one by one. The corpus omits
three kinds of pages: pages on a host that is not a campaign website (a news
article, a social profile, or an office page that the FEC record names),
archived addresses with a query string that the collection stored as a
homepage, and two pages of one site that held injected link text.

`page_type` follows the same URL rule as the earlier years. For 2026,
`page_type_context` adds the type that the link to the page implies (a page
reached from the menu entry "Issues" is `issues`). The extracted issue and
biography records use both; `extraction_route` shows the route of each page.

## Construction and derived measures

The rest of this section describes 2002 through 2024. Campaign URLs came from
historical FEC and Wikidata collection records. The
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
- Use `event_pairs_2026.csv` to compare a 2026 website before and after a
  primary, a runoff, or the general election. Join `pre_snapshot_id` and
  `post_snapshot_id` to `snapshot_id` in `raw_corpus.parquet`.
- Use the six extraction files, described in `candidate_extraction_codebook.md`,
  for issue statements, biography facts, and endorsements quoted from the
  pages. Keep one row per `candidate_record_key` before counting.

For the ICPSR append, restrict this release to captured House candidate-years
in 2018 through 2026 with `icpsr_compatible = TRUE`. A compact R function is
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
- 2026: a direct copy can fail when a website blocks automatic requests;
  `collection_failure_reason` records this. The 2026 corpus omits 242 archived
  pages whose stored body was compressed data, not text.
- 2026: the archive continues to add copies of 2026 pages. A later version adds
  them, replaces the `on_ballot` source, and can change 2026 counts.
- 2026: `bonica_rid` is carried from an earlier year of the same FEC ID and is
  missing for new candidates. The other DIME fields of the crosswalk (`cfscore`,
  `cfscore_dyn`, `dwnom1`, `dwnom2`, `ICPSR`, `NID`, `seat`, `dime_district`,
  `dime_ico`, `lname`, `fname`) are missing for each 2026 row.
- 2026: a direct copy holds more pages than an archived copy. Text length and
  page counts are therefore not comparable between 2026 and the earlier years
  without a restriction on the pages.
- 2026: the pages of one archived copy can have different archive dates. In
  `event_pairs_2026.csv`, 627 of 5,489 distinct copies span more than 24 hours;
  `pre_page_span_hours` and `post_page_span_hours` give the span.
- 2026: the selected copy in `panel_candidate_year.csv` is from after Election
  Day for 46 percent of the 2026 rows (37 percent in 2024).
- 2026: the extracted records were checked by a model coder, as in version 4.0.
  Two coded fields are less accurate in 2026 than in the earlier years; see the
  extraction codebook.

## Terms of use

The compilation, derived variables, and documentation are released under the
Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright
in the underlying campaign material remains with its authors. Each text row
records `snap_url`, the archived source URL. Each extracted record links through
`parent_document_id` to its page in `candidate_extraction_documents.parquet`,
which lists the archived URLs in `wayback_urls`.

The 2026 rows with `capture_source = live` are copies that the project made
from public campaign websites; `urlkey` gives the address of each page and
`date` the time of the copy. No archived URL exists for these rows. Copyright
in this material remains with its authors, and the same removal procedure
applies.

Rights holders may request removal through the deposit owner's contact
channel.

## Sources

- FEC candidate master files: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison data: DOI `10.3886/E226001V1`
- 2026 general-election candidate lists: English Wikipedia, pages and revisions
  in `on_ballot_2026_sources.csv`
