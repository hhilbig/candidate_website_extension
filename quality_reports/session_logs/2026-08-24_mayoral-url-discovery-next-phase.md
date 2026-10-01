# Mayoral URL discovery next phase

## Purpose

This session implemented the approved next phase of the mayoral campaign
website discovery project. The work asks whether contest-level archives,
official municipal records, and historical civic-data providers can raise URL
coverage without spending more search credits.

The source test compares unresolved ALGEL mayoral candidate-years with
election-specific historical source records. A new URL counts as strict only
after a human accepts grade A or B evidence for the named candidate, mayoral
contest, city, and election period.

## Work completed

The session froze 18 pre-repair inputs and outputs with row counts and SHA-256
hashes. It replaced the production combine default with an explicit build
manifest and made the source registry control enabled adapters, files, years,
jurisdictions, and attempt tracking.

The repaired pipeline now unwraps archive containers before normalization,
preserves exact URL paths and case, stores the site root separately, retains
independent source claims, carries contest and election-month context, retries
transient errors, prunes stale validation rows, retains date conflicts, and
joins all search lead types into one review queue.

The contest-source universe contains 2,041 targets. The attempt ledger contains
exactly 2,041 terminal rows with 56 `found`, 1,983 `no_result`, and 2
`request_error` outcomes. Sixteen large scope reductions are all recorded in the
filter audit.

The new claim table contains 188 independent claims. It preserves 140 legacy
claims without regrading them and adds 38 Archive-It claims plus ten search
claims. Hanno accepted 44 new-source claims and rejected four search claims.
Twenty-two grade-A custom-domain claims count as strict under the current
rules; 22 accepted grade-C claims remain non-strict. Two strict claims overlap
candidate-years in the legacy baseline, leaving 138 unique source-backed
candidate-years.

## Source channels

Archive-It collections `5903` and `16766` contain 61 auditable mayoral seed
records. Candidate matching yields 33 claims for 30 candidate-years. The
resumable temporal validator initially hit 13 HTTP 429 errors. Exponential and
`Retry-After` backoff recovered all 13. The completed run proposes grade A for
14 custom domains and grade C for 19 claims.

The public collection enumerator scanned 14,150 Archive-It collections once
and ran 297 deterministic local queries. It retained 753 unique high-recall
collection leads, 751 beyond the two fixtures. These results are not URL
coverage. They require seed extraction, candidate matching, and temporal
ownership validation.

The official-source inventory covers the 20 highest-priority early-period
cities with 34 municipal source pages. The saved-file adapter reads HTML, CSV,
TSV, Excel, JSON, and PDF. Its PDF parser extracts visible URLs and hyperlink
annotations. No public-record request was sent.

The masked provider sample contains 100 candidate-years from 2018 through
2021. Outreach drafts are ready for five provider families. No provider was
contacted and no data were purchased.

## Screening and review

The source screen contains 480 candidate-years with 120 per era. Its weights
recover the 4,579 eligible cases after excluding the original 120-case search
pilot. The separate contest sample contains 32 complete contests and 86
candidate-years.

The independent confirmation sample contains 800 candidate-years with an era
split of 320, 160, 160, and 160. It has zero overlap with the search pilot, the
480-case screen, or the 32 complete-contest sample.

The search handcoding app contains ten URL leads and three negative audits. The
Archive-It app contains all 33 claims, including the 14 grade-A proposals. Both
importers refuse incomplete results and write adapter-ready reviewed claim
files. The search importer also reports raw yield, post-stratified yield, and a
90 percent bootstrap interval.

The channel evaluator counts only accepted grade A or B claims with positive
identity and temporal matches. It reports marginal and cumulative primary and
exact-URL yield, precision, time, cost, and source gates. Its full-acquisition
gate also checks 15 percent projected primary coverage, a 13 percent one-sided
90 percent lower bound, 95 percent precision, and the approved production-cost
ceiling.

## Errors found and fixed

1. The freeze reader failed on the Latin-1 ALGEL file. The reader now tries
   UTF-8 and then Latin-1, and the freeze test passes.
2. The requested five-way screening cells exceeded the 120-case quota in one
   era. The sampler now uses positive-probability four-way strata and systematic
   ordering on contest size while preserving all five balance dimensions.
3. The first hand-built city-source inventory used a different tie order than
   the deterministic priority table. The inventory was corrected to match the
   generated top 20 exactly.
4. Archive-It rate limiting left 13 rows retryable and only eight grade-A
   proposals in the first pass. Backoff and resumable retries eliminated all
   request errors and raised the complete proposal count to 14.
5. The confirmation sample overlapped the separate complete-contest sample by
   15 candidate-years. The builder now excludes all 86 complete-contest rows;
   the regenerated 800-case sample has zero overlap.
6. The channel evaluator ignored negative identity and temporal fields when it
   recomputed strict coverage. It now requires explicit positive matches when
   those fields exist.
7. The municipal adapter wrote `acceptance=pending`, which the claim builder
   could not parse. Pending municipal acceptance is now blank, and an
   adapter-to-claim integration test passes.
8. The full-acquisition thresholds existed only in documentation. They are now
   a coded gate in the evaluator.
9. The version-four Wayback validator was too slow in serial execution. It now
   supports bounded, resumable parallel validation with exact-key persistence.
10. The first post-adjudication channel run treated accepted grade-C URLs as
    false positives. Precision now measures accepted identity, timing, and
    target-URL matches, while strict yield remains limited to grade A or B. A
    regression test fixes this distinction.
11. The search handoff preserved one embedded domain without an HTTP scheme.
    The importer now preserves the raw value, adds `http://` to the exact
    normalized URL, and tests the adapter-to-claim integration.

The bounded retry and offline checkpoint merge retain exactly 124 current
candidate-URL keys, with no duplicates, missing keys, or unexpected keys.
Forty-three rows are fully checked, 61 have a usable result with at least one
query error, and 20 remain unresolved request errors. The current results place
101 captures within 365 days and find no capture for three keys. The 61 partial
rows and 20 error rows remain explicitly retryable.

## Verification

The final scoped suite passes all 197 mayoral and municipal tests after both
handcoding imports. Before adjudication, a fresh temporary build reproduced 22
production data and report artifacts byte for byte. The source-registry report
also matched, for 23 exact file matches. After adjudication, the claim table and
three channel-evaluation outputs were rebuilt directly from the canonical
Archive-It export and matched the production files byte for byte. The pilot
import's adjudication, metrics, reviewed claims, and combined claim table also
match fresh rebuilds byte for byte.

The repository-wide suite is not a stable verification target during this
session because separate untracked candidate-extraction files are being edited
concurrently. The latest unrelated run stopped during collection on two frozen
semantic-validation hash errors. Those files are outside this task and were not
changed here.

## Current implication

Archive-It is in the pre-specified `hold` region. Nine of its claims fall in the
480-case screen; all nine are accepted target URLs and five are strict. This is
100 percent adjudicated precision, but it falls below the ten-URL continuation
threshold and above the three-URL drop threshold. The completed search pilot
finds three strict URLs among 120 cases, a 2.5 percent raw yield and 2.90 percent
post-stratified yield. Six of ten proposed leads are accepted, so search lead
precision is only 60 percent. The remaining Serper credits should stay unused,
and the 800-case confirmation sample must not be activated for source
acquisition.

## Required user actions

1. Approve an exact recipient and request before any municipal outreach.
2. Review a provider sample, quote, terms, and projected cost before any paid
   acquisition.
