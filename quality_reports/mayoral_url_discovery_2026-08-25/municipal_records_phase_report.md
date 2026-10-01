# Official municipal-record discovery: first 20 cities

## Decision

The first public-file pass adds no strict mayoral campaign URLs. The channel
should not be scaled from the files currently posted on municipal websites.
The project will not proceed with public-record requests for these pre-2010
contests. Even a returned domain often cannot satisfy the strict evidence rule
when the Wayback Machine lacks election-period campaign content. No request has
been sent.

## Question

This pass asks whether official election, candidate-filing, voter-guide, and
campaign-finance sources can recover campaign websites for the 335 unresolved
candidate-years in the 20 highest-priority cities from 2000 through 2007. The
pipeline parses each saved physical file once and then matches its URL records
to every candidate in the relevant contest.

## Inputs and assumptions

1. The city universe is the 20-row priority table in
   `data/municipal_discovery/screening/city_priority_2000_2007.csv`. It contains
   335 unresolved candidate-years in 60 contest-years.
2. The source universe is the explicit 34-row inventory in
   `quality_reports/mayoral_url_discovery_2026-08-24/official_municipal_source_inventory.csv`.
3. The crawler follows only same-host links that mention a target election year
   or a target candidate's first and last name. It stops after two link levels
   and 12 files per source.
4. The parser reads saved HTML and PDF files. PDF parsing covers visible URLs
   and hyperlink annotations. It does not infer a website from an email address.
5. A candidate URL remains a pending lead until temporal ownership validation
   and human adjudication. Only accepted grade A or B evidence counts as strict
   coverage.

Pomona's 2004 election month is missing in the authoritative universe. The
pipeline retains the case with a missing month and does not impute November.

## What was recovered

The fetch pass saved 85 source-file instances representing 76 unique files:
41 PDFs and 35 HTML files, totaling 54,359,720 bytes. Twenty-eight of 34 source
targets returned at least one file. Three ended in `source_unavailable` and
three in `request_error` after the bounded retry and curl fallback.

Seventeen of 20 cities have at least one saved official file. Clarksville,
Pomona, and Wichita have none. Columbus and New Haven each have a failed source
target but remain represented through another official source. The city-level
file gap is therefore 3/20 (15 percent), while the source-target failure rate is
6/34 (17.6 percent). Neither exceeds the 20 percent reporting threshold, but
both are retained explicitly in the attempt ledger.

The explicit extraction manifest contains 103 source-year targets and 76
content-addressed local files. Its attempt ledger contains exactly 103 rows,
one per manifest source and contest target, with no duplicate keys:

| Terminal status | Targets |
|---|---:|
| `match_unresolved` | 72 |
| `no_result` | 16 |
| `source_unavailable` | 9 |
| `request_error` | 6 |
| Total | 103 |

`match_unresolved` means that a source contains web links, but none appears in
context that uniquely identifies one mayoral candidate. The links are mainly
government navigation and document references. All 41 PDFs were reprocessed
with `pypdf`; no visible URL occurs within a one-line window of a target
candidate name, and the PDFs contain no hyperlink annotations. The zero yield
is therefore not caused by the initial missing-PDF-library error or by a
candidate-to-link ambiguity inside these files.

## Yield and implication

The public-file yield is 0/335 unresolved candidate-years (0 percent), with
zero proposed claims and zero strict additions. Precision and cost per accepted
URL are undefined because there are no positives. Under the prespecified gate,
the currently posted municipal-file channel is a drop: it produces no more
than three strict additions.

This result does not test native historical files that agencies no longer post.
Those files may identify domains, but the project now treats sparse pre-2010
Wayback coverage as a binding validation constraint. Pre-2010 records requests
are therefore paused. They can return as a sensitivity exercise only when a
source can also supply contemporaneous archived campaign content. The
city-specific drafts are retained in
`quality_reports/mayoral_url_discovery_2026-08-24/municipal_public_record_request_packet.md`.
The first three exact recipient-message pairs are in
`quality_reports/mayoral_url_discovery_2026-08-25/municipal_request_batch_1.md`.
Hanno approved those three pairs, and Gmail drafts were created, but the drafts
remain unsent under the revised plan. The active municipal frame begins in 2010.

## Reproducible artifacts

- Fetch ledger: `data/municipal_discovery/municipal_records/download_manifest.csv`
- Fetch attempts: `data/municipal_discovery/municipal_records/source_fetch_attempts.csv`
- Explicit extraction manifest: `config/official_municipal_extraction_manifest.json`
- Contest-source attempt ledger: `data/municipal_discovery/municipal_records/mayoral_source_attempts_municipal.csv`
- Evidence queue: `data/municipal_discovery/municipal_records/mayoral_url_evidence_municipal.csv`
- Frozen hashes: `quality_reports/mayoral_url_discovery_2026-08-25/municipal_records_state_manifest.json`
