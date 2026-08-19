# U.S. candidate campaign websites, 2002–2024

This release contains archived campaign-website text for Democratic and
Republican U.S. House candidates from 2018 through 2024 and Senate candidates
from 2002 through 2024.

## Files

| File | Rows | Columns | Unit |
|---|---:|---:|---|
| `raw_corpus.parquet` | 799,058 | 28 | Archived page and snapshot |
| `panel_candidate_year.csv` | 7,353 | 25 | Captured candidate-year, with text |
| `panel_icpsr_compat.csv` | 7,353 | 114 | Captured candidate-year, with ICPSR-compatible measures |
| `candidate_crosswalk.csv` | 7,353 | 36 | Captured candidate-year and identifiers |
| `release_roster.csv` | 9,848 | 20 | Candidate-year with a recorded URL attempt |

The deposit also contains this README, the codebook, and a manifest with each
data file's byte size and SHA-256 checksum.

## Population

The roster includes major-party candidates whose FEC election year equals the
target year and for whom the collection recorded a URL attempt. An identity
crosswalk retains reviewed general-election candidates with missing or
superseded FEC identifiers. FEC
candidate status is metadata, not a filter. Senate candidates enter the roster
only when their state held a Senate election that year.

Of 9,848 URL attempts, 7,353 yielded a valid capture. Among the captured
candidates, 3,032 match an official major-party general-election return and
4,321 do not. Ballot candidates have `stage = 2` and
`data_source = general_wayback`; other same-year candidates have a missing
`stage` and `data_source = election_year_wayback`. Use the roster as the
coverage denominator. It excludes FEC filers without a recorded URL attempt.

## Construction and quality control

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

Use `candidate_cycle_id` to merge files, `on_ballot` to select general-election
candidates, and the roster to study capture selection. `icpsr_compatible` is
identical to `on_ballot`.

Archived pages may be incomplete or may reflect a domain that changed ownership
after an election. The corpus records campaign text; it does not verify the
claims in that text. Coverage and text quality vary by candidate and year.

## Terms of use

The compilation, derived variables, and documentation are released under the
Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright
in the underlying campaign material remains with its authors. Each text row
records `snap_url`, its archived source URL.

Rights-holders may request removal through Dataverse's Contact Owner control.

## Sources

- FEC candidate master files: <https://www.fec.gov/campaign-finance-data/candidate-master-file-description/>
- MIT House returns: DOI `10.7910/DVN/IG0UN2`
- MIT Senate returns: DOI `10.7910/DVN/PEJ5QU`
- ICPSR comparison data: DOI `10.3886/E226001V1`
