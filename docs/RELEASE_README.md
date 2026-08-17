# U.S. candidate campaign websites, 2002–2024

This release contains archived campaign-website text for Democratic and
Republican candidates for the U.S. House and Senate. The candidate population
is defined by the election year recorded in the FEC candidate master file.
Verified general-election candidates with a superseded or missing same-year FEC
identifier are retained through a reviewed identity crosswalk.

The repository is not part of the deposit. It is linked only as supporting
code. The deposit consists of the five data files below, this README, the
codebook, and the checksum manifest.

## Files to be released

| File | Rows | Columns | Description |
|---|---:|---:|---|
| `raw_corpus.parquet` | 799,058 | 28 | One row per archived page and snapshot |
| `panel_candidate_year.csv` | 7,353 | 25 | One representative website snapshot per captured candidate-year, with text |
| `panel_icpsr_compat.csv` | 7,353 | 114 | Candidate-year text measures and topic shares in ICPSR-compatible form |
| `candidate_crosswalk.csv` | 7,353 | 36 | Candidate identifiers, election metadata, and DIME links |
| `release_roster.csv` | 9,848 | 20 | All in-scope candidate-years for which the collection had a URL attempt, including failures |

`manifest.json` records the exact byte size and SHA-256 checksum of each data
file. The codebook defines every field or field family and its missing-value
rules.

## Population and unit of observation

The broad population is major-party House and Senate candidates whose FEC
recorded election year equals the target year. Candidate status is retained as
metadata and is not used as a filter. Senate candidate-years are included only
when that state held a Senate election in the target year.

`release_roster.csv` is the denominator for collection coverage. It contains
9,848 candidate-years with a recorded URL attempt. Of these, 7,353 have a valid
capture in the released corpus. The release does not claim that the roster is a
complete list of every FEC filer without a campaign URL.

General-election status is a separate attribute. The released panel contains
3,032 candidates matched to an official general-election ballot and 4,321
other same-year FEC candidates. For ballot candidates, `stage` is 2 and
`data_source` is `general_wayback`. For other same-year candidates, `stage` is
missing and `data_source` is `election_year_wayback`.

## Construction

The source websites were archived by the Internet Archive. Each candidate-year
panel row selects the snapshot date with the largest total amount of extracted
text; ties use the later date. Pages within that snapshot are ordered with the
homepage first, followed by substantive page types and then other pages. The
raw Parquet file preserves page-level text and metadata.

Candidate identities use FEC candidate IDs. The release filters the FEC cycle
files on `cand_election_yr == year`, retains `cand_status`, and applies the
reviewed mappings in `config/candidate_identity_overrides.csv`. It does not
match or deduplicate people solely by cleaned name. General-election flags and
vote totals come from the MIT Election Data and Science Lab House and Senate
returns.

## Capture-quality decisions

The release removes captures that were demonstrably unrelated to the assigned
campaign. These included generic AOL, Yahoo, Facebook, and Google pages, one
law-firm site, an error page, and an expired campaign domain serving movie
piracy spam. The affected attempted candidates remain in `release_roster.csv`
with `capture_status = invalid_capture_excluded`. Two malformed stored URLs
that lead to genuine campaign content are retained through manual overrides.
The complete decision table is `config/capture_adjudication.csv` in the linked
repository.

## Recommended use

Use `candidate_cycle_id` as the cross-file key. Use `on_ballot` or
`icpsr_compatible` when an analysis requires general-election candidates. Use
`release_roster.csv` to study capture selection; do not calculate collection
coverage from the captured panel alone.

The corpus contains public website text, not statements verified by the
researchers. Archived pages may be incomplete, and some campaign sites changed
ownership after an election. Text quality and archive coverage vary by
candidate and year. The release reports raw length and capture fields so users
can impose alternative thresholds.

## Sources and reproduction

- FEC candidate master files and field definitions: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT Election Data and Science Lab House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Election Data and Science Lab Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison dataset: DOI `10.3886/E226001V1`

The linked repository contains the collection and release-building code. Create
an environment from `requirements-release.txt`, place the documented source
archives and public source files at their configured paths, and run:

```bash
python scripts/rebuild_release.py --out-dir build/release_candidate
python scripts/validate_release.py build/release_candidate
```

The command writes the five data files and `manifest.json` to a separate
staging directory. It does not overwrite the current release files.
