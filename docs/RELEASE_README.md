# U.S. House (2018–2024) and Senate (2002–2024) candidate websites

This release contains archived campaign website text for Democratic and
Republican U.S. House candidates from 2018 through 2024 and Senate candidates
from 2002 through 2024. The House panel extends ICPSR 226001 through 2024.

## Files

| File | Rows | Columns | Unit |
|---|---:|---:|---|
| `raw_corpus.parquet` | 799,058 | 28 | Text from each archived page and snapshot |
| `panel_candidate_year.csv` | 7,353 | 25 | Panel by captured candidate and year, with text |
| `panel_icpsr_compat.csv` | 7,353 | 114 | Panel by captured candidate and year, with measures compatible with ICPSR 226001 |
| `candidate_crosswalk.csv` | 7,353 | 36 | Captured candidate-year and identifiers |
| `release_roster.csv` | 9,848 | 20 | Candidate-year with a recorded URL attempt |

The deposit also contains this README, the codebook, and a manifest with each
data file's byte size and SHA-256 checksum.

## Population

The roster includes Democratic and Republican candidates with a recorded URL
attempt whose FEC election year equals the target year. An identity crosswalk
retains reviewed candidates in general elections with missing or superseded FEC
identifiers. FEC candidate status is metadata, not a filter. Senate candidates
enter the roster only when their state held a Senate election that year.

Of 9,848 URL attempts, 7,353 yielded a valid capture. Of these, 3,032 are
Democratic or Republican candidates in official general election returns; 4,321
are not. Ballot candidates have `stage = 2` and
`data_source = general_wayback`; other same-year candidates have a missing
`stage` and `data_source = election_year_wayback`. Use the roster as the
coverage denominator. It excludes FEC filers without a recorded URL attempt.

## Construction and quality control

Campaign websites were identified from FEC records and Wikidata. The crawler
followed homepage links one level deep in the Wayback Machine.

Each panel row selects the snapshot date with the most extracted text, breaking
ties in favor of the later date. The raw Parquet file retains every page and
snapshot used to make that selection.

Candidate identities use FEC IDs and reviewed mappings; cleaned names alone
never identify people. Ballot flags and votes come from MIT Election Data and
Science Lab returns.

The release excludes captures unrelated to the assigned campaign and labels
them `invalid_capture_excluded` in the roster. Manual overrides retain verified
campaign pages with malformed stored URLs.

## Use and limitations

Use `candidate_cycle_id` to merge files and `on_ballot` to select candidates in
general elections. Use the roster to study capture selection.
`icpsr_compatible` is identical to `on_ballot`.

The compatible House panel for 2018–2024 can be appended to the 5,267
candidate-years in ICPSR 226001. A compact R function is available from the
[project website](https://www.hannohilbig.com/candidatewebsites/merge_with_icpsr.R).

Eleven percent of records contain little or no text; `text_quality` identifies
them. Pages captured after the election may reflect changed domains, and linked
pages can come from nearby dates. Measures based on full sites therefore have
`_preelec` versions when applicable. The median record spans 255 days, so pooled
comparisons are more reliable than changes within candidates.

Topic shares measure emphasis, not policy position. The corpus records campaign
text; it does not verify the claims in that text.

## Terms of use

The compilation, derived variables, and documentation are released under the
Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright
in the underlying campaign material remains with its authors. Each text row
records `snap_url`, its archived source URL.

Rights holders may request removal through Dataverse's Contact Owner control.

## Sources

- FEC candidate master files: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison data: DOI `10.3886/E226001V1`
