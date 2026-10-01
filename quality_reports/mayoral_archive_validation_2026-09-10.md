# Historical validation of repaired mayoral URL proposals

The first historical pass supports 73 of the 120 repaired URL proposals,
covering 70 candidate-years. Of these, 37 clear all programmed checks and 36
retain review flags. The other 47 lack sufficient evidence. All 120 rows remain
in the output; no human approvals were added.

The follow-up raises the cleared count to 43 and leaves 30 review flags.
It also identifies three archive errors hidden in HTTP-200 responses.
See [the follow-up report](mayoral_validation_flags_2026-09-10.md).
The authoritative assessment is now
[the updated validation table](mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv).
The remainder of this report records the first pass.
Use `final_validation_status` for the final disposition and
`historical_campaign_content_supported` for the narrower content finding.
The `automated_*` columns preserve the original machine assessment. Original
discovery columns remain provenance, not newly granted validation flags.

## What was checked

This check compares each repaired candidate-year and proposed URL with archived
pages near that candidate's election. The input is the ownership repair's
`combined/new_candidate_urls.csv`: 120 unique URL keys, with no missing election
months. Its SHA-256 is
`3408dbcdd182661f2a38b522f23126eca7c01e9dfd45e06c0fd8a368497d0634`.

The existing validator uses the fifteenth of the election month as its reference
date and a 365-day window in either direction. It checks candidate identity,
mayoral campaign context and city. Four additional availability probes cover
the surrounding two years; up to two surrounding captures are inspected for
reuse. These are bounded checks, not an exhaustive domain history. Effective
replay dates control timing. Scheme, www and default-port equivalents preserve
the full path and query; an empty root path is equivalent to `/`.

| Final assessment | URL proposals |
|---|---:|
| Campaign evidence and all programmed checks passed, pending review | 37 |
| Campaign evidence recovered from saved captures; reuse unchecked | 12 |
| Campaign evidence confirmed by text review; machine warnings retained | 24 |
| No capture found by this bounded check or saved-corpus comparison | 28 |
| Captures outside the 365-day window | 8 |
| Identity, office or city insufficiently established | 10 |
| Personal résumé incorrectly classified as a campaign page | 1 |
| Total | 120 |

The 73 supported proposals comprise 26 new-candidate URLs, seven additional
sites and 40 alternative URLs on known sites. These are URL proposals, not 73
newly covered candidates. Discovery-lead coverage remains a separate measure.

## Corrections and limitations

The availability service returned no capture for 40 proposals. Comparing the
existing corpus recovered campaign evidence for 12 of them, leaving 28 without
a capture found. The recovered cases are Curt Skoog, Jules Bailey, Kate Gallego,
Tim Seay, Keith Summey, David Briley, Sherrie Hall Everett, Keith Amemiya, Rick
Blangiardi, Barbara Bry, Mark Leno and Gery Chico. An empty availability response
therefore does not prove that a URL was never archived.

The saved-corpus scan checked 291,044 ledger rows and verified 37 selected HTML
hashes: 25 literal URLs and 12 scheme/host/root equivalents. No raw corpus files
were copied. These supplemental captures cannot independently clear the reuse
checks.

Content inspection produced 26 keyed reviews: 25 support campaign content and
one overturns an automatic positive. The positive reviews include wording such
as “for Tucson Mayor,” Saint Paul versus St. Paul, and Ken versus Kenneth Dudley.
They also identify genuine campaign pages where surrounding-year content raises
a reuse warning. Those warnings remain in the table.

Sunshine Grund's selected capture is a personal résumé listing past campaigns;
its automatic positive was downgraded. Date failures include a later secretary
of state campaign for Diane Harris, a 2024 officeholder site for Larry Agran's
2012 rows, and an Assembly campaign for Len Augustine. Dennis Herrera's selected
page contains unrelated insurance advice. These records remain available as
unresolved or unsupported evidence.

Greg Jamison and Matt Hall have thin visible pages whose titles or metadata
identify the campaign. Their attribution evidence does not establish useful
substantive campaign text. More generally, a capture within 365 days does not
by itself prove that every passage concerns the exact election cycle. Automated
eligibility and Codex text review do not constitute human acceptance or release
approval.

## Verification and storage

All 120 candidate keys, election contexts and source-claim references match the
input. All 629 newly saved response bodies pass their hashes, as do the 37
selected existing captures. The run recorded 735 request records and no
unresolved request errors. All 58 related tests pass; the scripts compile.
An early checkpoint error on a missing value was fixed and regression-tested;
the final journal contains exactly one completed result for each input key.

The [summary](mayoral_archive_validation_2026-09-10/final/summary.json) records
the input, output, response-journal and review hashes. The copied validation
table matches its recorded SHA-256.

The droplet run is saved at
`/mnt/mayoral_wayback_full_snapshots/ownership_validation_20260910/`.
It uses 5,742,192 bytes, including 3,382,189 bytes of compressed response evidence.
Compact local audit outputs use 577,340 bytes before this report. The volume
still has approximately 31 GiB free. There was no archive extraction, bulk
download to the Mac, paid-service use or restart of the unfinished source crawl.

The scripts are `scripts/validate_repaired_mayoral_leads.py`,
`scripts/supplement_repaired_mayoral_validation.py` and
`scripts/finalize_repaired_mayoral_validation.py`. The first uses the existing
`validate_search_lead_wayback_available_content.py` rules. Reproducing the final
assessment also requires the saved corpus and the
[hash-bound content reviews](mayoral_archive_validation_2026-09-10/content_reviews.json).
