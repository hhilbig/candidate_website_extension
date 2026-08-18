# U.S. candidate campaign websites, 2002–2024

This release contains archived campaign-website text for Democratic and
Republican U.S. House and Senate candidates. It covers House elections from
2018 through 2024 and Senate elections from 2002 through 2024.

## Files

| File | Rows | Columns | Unit |
|---|---:|---:|---|
| `raw_corpus.parquet` | 799,058 | 28 | Archived page and snapshot |
| `panel_candidate_year.csv` | 7,353 | 25 | Captured candidate-year, with text |
| `panel_icpsr_compat.csv` | 7,353 | 114 | Captured candidate-year, with ICPSR-compatible measures |
| `candidate_crosswalk.csv` | 7,353 | 36 | Captured candidate-year and identifiers |
| `release_roster.csv` | 9,848 | 20 | Candidate-year with a recorded URL attempt |

The deposit also contains this README, the codebook, and `manifest.json`.
The manifest records each data file's byte size and SHA-256 checksum.

## Population

The roster includes major-party candidates whose election year in the FEC
candidate master file equals the target year and for whom the collection
recorded a URL attempt. Reviewed general-election candidates with a missing or
superseded same-year FEC identifier are retained through an identity crosswalk.
Candidate status is metadata, not a filter. Senate candidates are included only
when their state held a Senate election that year.

Of 9,848 attempted candidate-years, 7,353 have a valid capture. The roster is
the denominator for collection coverage; it is not a list of every FEC filer
without a campaign URL. Of the captured candidates, 3,032 match an official
major-party general-election return and 4,321 do not. Ballot candidates have
`stage = 2` and `data_source = general_wayback`; other same-year candidates
have a missing `stage` and `data_source = election_year_wayback`.

## Construction and quality control

Each panel row uses the snapshot date with the most extracted text, breaking
ties in favor of the later date. Pages are ordered with the homepage first,
followed by substantive page types. The raw Parquet file retains the page-level
text and metadata.

Candidate identities use FEC IDs and the reviewed mappings in
`config/candidate_identity_overrides.csv`; cleaned names alone are never used
to identify people. Ballot flags and votes come from MIT Election Data and
Science Lab returns.

The release excludes captures demonstrably unrelated to the assigned campaign,
while retaining those candidates in the roster with
`capture_status = invalid_capture_excluded`. Genuine campaign pages with
malformed stored URLs are retained through manual overrides. The decisions are
recorded in `config/capture_adjudication.csv`.

## Use and limitations

Use `candidate_cycle_id` as the cross-file key. Use `on_ballot` when an analysis
requires general-election candidates; `icpsr_compatible` has the same meaning
in the ICPSR-compatible panel. Use the roster, not the captured panel, to study
capture selection.

Archived pages may be incomplete or may reflect a domain that changed ownership
after an election. The corpus reports what campaigns published, not statements
verified by the researchers. Text quality and coverage vary by candidate and
year; raw length and quality fields support alternative thresholds.

## Terms of use

The compilation, derived variables, and documentation are released under the
Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright
in the underlying campaign material remains with its authors. Each text row
records `snap_url`, its archived source URL.

Rights-holders may request removal through the Contact Owner control on the
Dataverse record. We will act on substantiated requests.

## Sources

- FEC candidate master files: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison data: DOI `10.3886/E226001V1`
