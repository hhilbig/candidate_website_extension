# Follow-up on the 36 flagged mayoral proposals

Six proposals now pass all programmed checks. The total rises from 37 to 43.
The other 30 flags have explicit causes. Historical campaign evidence still
supports 73 URLs, and all 120 original rows remain saved.

Use [the updated table](mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv).
The input was the previous final table, with SHA-256
`45066bf7b91e1ade77fb12fbc853d977033a92fbf6b890dc0b7df402ea81bd16`.
The [audit](mayoral_archive_validation_2026-09-10/reuse_resolution/audit.json)
records each main capture hash and each surrounding capture assessment.

## Results

This follow-up compares each flagged candidate URL with its saved election-period
page and saved pages from the surrounding two years.

| Cause or result | Proposals | Implication |
|---|---:|---|
| Text-rule errors corrected; existing checks now pass | 6 | Ready for human review |
| Consistent candidate identity in saved surrounding pages | 11 | Archive inventory remains incomplete |
| No saved surrounding page for the exact path | 1 | Reuse check remains incomplete |
| Later unrelated commercial content confirmed | 6 | Retain the domain-reuse warning |
| Blank, thin or failed historical pages | 9 | Identity cannot be established for those pages |
| Archive service rate-limit pages | 3 | Replay errors require a later retry |
| Total | 36 | All target keys assessed |

The six cleared proposals concern Pat Harris, Ted Wheeler, Regina Romero,
Lydia Ortega, Kenneth Dudley and Michelle Kaufusi. The revised rules recognize
supplied-city campaign phrases, bounded name variants and city variants.
The rules also recognize candidate-bound “4 Mayor” wording. A résumé structure
without affirmative campaign context cannot establish a campaign page.

The six later domain changes concern Billie Redmond, Fernando Herrera, Annetta
Catchings, Sam Abed, Greg Harland and Ken Houston. Their later pages contain
unrelated commercial content. Their earlier campaign pages remain valid evidence
for the selected dates. The two-year reuse warnings remain active.

Two Dai Thao URL proposals and David Chiu's proposal contain archive rate-limit
responses among their surrounding-page checks. The service returned HTTP 200,
so the earlier parser treated these responses as historical pages. The parser
now detects this response and records a retryable archive error. This corrects
the previous report's statement that no unresolved request errors remained.

The saved corpus supplies 19 surrounding captures for 11 supplemental proposals.
Eight proposals have captures on both sides; three have only earlier captures.
Sherrie Hall Everett's exact `/home` path has none. The selected pages show the
same candidates, including earlier campaigns for different offices. These
positive observations do not prove that the full archive inventory is complete.
They therefore do not clear the original availability checks.

## Verification and storage

All 92 related tests pass, and the scripts compile. The revised parser rechecked
92 saved main pages. All 37 previously cleared proposals still pass its content
checks. The follow-up verified 64 surrounding-capture references against saved
hashes. It preserved candidate keys, source references and human-approval fields.
All original fields in the 84 untargeted rows remain unchanged.

The follow-up made zero archive requests and copied no raw HTML. New local
audit outputs use 767,741 bytes. The remote result directory uses 667,063 bytes,
plus compact code and review records. The droplet volume retains about 31 GiB
of free space. No temporary archive restore required removal.

Remote results are under
`/mnt/mayoral_wayback_full_snapshots/ownership_validation_20260910/reuse_resolution/`.
The [summary](mayoral_archive_validation_2026-09-10/reuse_resolution/summary.json)
records code, input, review and output hashes. The previous parser remains in
the attribution repair's isolated code directory, with its original verified hash.

The scripts are `scripts/resolve_mayoral_validation_flags.py` and
`scripts/inspect_saved_mayoral_reuse.py`. The shared content parser is
`scripts/validate_search_lead_wayback_available_content.py`, with content-signal
version `mayoral_content_signals_v2`.

Next, human review can start with the 43 cleared proposals. The 30 supported
but flagged proposals require the case-specific checks above. The other 47
proposals remain outside validated coverage. No human approvals were added.
