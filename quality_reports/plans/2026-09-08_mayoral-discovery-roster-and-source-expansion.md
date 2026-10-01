# Plan — Mayoral discovery roster and source expansion

**Date:** 2026-09-08
**Status:** step 1 complete; step 2 stopped at the byte limit, audited, archived, and removed from mac2

## Context

This work compares mayoral candidate-years with a campaign URL before and after
the extraction of URLs from previously identified sources. Active discovery
covers 2010–2021. The prior combined list covers 1,008 of 2,788 active
candidate-years. It leaves 1,780 unresolved, including 340 outside the full
search wave. Reconciliation may change these counts.

The full wave saved 16,904 Serper results and 32,232 distinct OpenAI source URLs.
It did not run the source-page expansion stage. The approved work now completes
the roster reconciliation and processes these sources.

## Recommended approach

1. Reconcile prior claims and review decisions using explicit input manifests.
   Preserve the full 4,817-row universe and identify the active 2010–2021 rows.
   Retain every independent claim and all unresolved or rejected records.
2. Build a deduplicated source queue from saved search responses, source links,
   prior evidence pages, and saved source files. Record a disposition for every
   source-to-candidate relation. Parse each physical document once.
3. Extract HTML hyperlinks, visible URLs, PDF annotations, and scanned text.
   Attribute URLs using the local candidate context. Match all candidates in
   each relevant contest. Save uncertain matches for review.
4. Fetch relevant public source pages with bounded retries and host-level rate
   limits. Persist attempts and parsed outputs transactionally. Follow relevant
   candidate profiles and campaign links; retain a queue for deeper references.
5. Export new claims, source dispositions, and updated discovery counts. Report
   pending fetches, blocked sources, parse failures, and review cases separately.

## Definitions and assumptions

- A candidate-year is one recorded candidacy in one election year.
- A URL lead is a proposed website with traceable source provenance.
- A source-attributed URL requires a passage or link associating the candidate
  with that website. A domain-shaped guess alone does not meet this definition.
- An archive metadata hit records a capture’s existence. Replay failure does
  not erase that discovery result. Saved content remains a separate outcome.
- Historical campaign ownership remains unverified unless the source supplies
  sufficient identity and election context. New extraction does not invent a
  human approval or promote claims to evidence grades A or B.
- Exact URL paths and case, source dates, source passages, and election months
  remain in the output. Missing dates stay missing.

## Critical files

- `scripts/reconcile_mayoral_discovery.py`: rebuilds the full discovery roster.
- `config/mayoral_discovery_reconciliation_20260908.json`: lists reconciliation inputs.
- `src/mayoral_source_expansion.py`: parses documents and attributes website links.
- `scripts/expand_mayoral_saved_sources.py`: prepares, runs, resumes, and exports the source pass.
- `data/municipal_discovery/exhaustive_source_expansion_20260908/`: run outputs.

The current coverage roster and the last full-wave candidate list control the
initial denominators. The new input manifest records the exact authoritative
files and their hashes before the revised roster becomes authoritative.

## Reused existing functions

Reuse the project’s exact-URL normalizer, Wayback wrapper handler, URL-type
classifier, and document parsers where they preserve the required provenance.
Reuse saved source responses before making another public-page request.

## Verification

Test key reconciliation, rejected-claim precedence, exact paths and URL case,
multi-candidate attribution, PDF annotations, OCR, resume behavior, and retries.
Run the revised roster build and verify exact universe agreement. Test the
fetch runner against local fixtures, then run a bounded public-page smoke.
Check source dispositions, raw-file hashes, parsed records, and first durable
results before the full source pass. Report all filters exceeding 20 percent.

## Execution

Two agents handle independent modules: roster reconciliation and document
parsing. The coordinator implements the fetch pass, tests integration, and
reviews actual claims. Long source downloads run on mac2 with a host-local
completion notification that identifies the project and batch.

## Deferred work

New paid searches, provider purchases, new outreach, broad archive enumeration,
and additional campaign-snapshot acquisition belong to later plan steps.

## Execution record (2026-09-08)

The frozen reconciled baseline is `data/municipal_discovery/exhaustive_source_expansion_20260908/production/roster/`.
It preserves 4,817 candidate-years, 2,788 active cases, and 5,364 independent
source claims. Its 1,038 active candidate-years with URL leads comprise the
previous 1,008, plus 31 recovered omissions, minus one previously rejected URL.
No previous review was promoted or reversed by source expansion.

The source manifest freezes 52,472 original fetch URLs and 76,130
source-candidate relations. Restricting active work from 4,817 to 2,788 cases
excludes 42.1 percent from new fetching under the approved year restriction;
all remain in the roster. URL deduplication reduces fetches by 31.1 percent
relative to source-candidate relations, while preserving every relationship.

The local verification pass processed 414 cached pages and 630 saved snippets,
then 50 live source URLs. Reattribution with version four produces five newly
represented candidate-years and seven additional exact-URL strings. The total
is 1,043 active candidate-years with a URL lead. The additional claims remain
grade-D discovery proposals. Source-reference links and uncertain matches
remain in the evidence output, rather than contributing to coverage.

Repairs demonstrated by actual sources:

- Preserve archive transport URLs separately from unwrapped exact targets.
- Use the saved final URL when resolving relative links in cached pages.
- Separate HTML metadata and article links from campaign website ownership.
- Retain PDF annotations and recognize OnBase's literal document-download redirect.
- Respect MIME type and file signatures when parsing snippets attached to PDF URLs.
- Use publication dates and both document and search-result titles for review flags.
- Prevent council candidates absent from the roster from inheriting a mayoral identity.
- Keep snippet-only claims unresolved until a page corroborates their ownership.
- Preserve raw bytes, immutable parse artifacts, retries, and retired follow-up edges.
- Treat empty HTTP202 responses as unavailable documents and defer repeatedly restricted hosts.

The 50-source live test recorded 38 fully parsed pages, two partially parsed
PDFs, two HTTP errors, one request error, and seven empty HTTP202 responses.
It retained 41 OCR pages for follow-up. A host-level access restriction deferred
831 unattempted Ballotpedia URLs. These are recorded limitations of this pass,
not evidence that the underlying campaign URLs do not exist.

All 122 relevant tests passed. SQLite integrity and 1,091 saved-body hashes
passed verification. Roster and source-disposition keys agree exactly. A clean
temporary roster rebuild reproduced the frozen outputs. The parser test suite
covers actual hyperlink annotations and OCR, and the supervisor's message
format is tested without sending a test Slack message.

The isolated mac2 deployment is
`/Users/hannohilbig/Documents/GitHub/candidate_website_extension/runs/mayoral_sources_20260908/`.
It uses the existing repository virtual environment. The source run is
`data/municipal_discovery/exhaustive_source_expansion_20260908/production/`
relative to that deployment. A host-local supervisor records process state and
uses the existing Slack helper for completion, failure, or stalled progress.
Live progress on mac2 is authoritative after launch. Local files remain a
prelaunch snapshot until synchronized back.

Launch verification found supervisor PID 1851 and source-runner PID 1853. The
source log already contains new successful fetches after deployment. The three
runtime-file hashes and prelaunch SQLite hash matched between the local
machine and mac2. The supervisor's process-state file reports `running`.

Resume with `scripts/expand_mayoral_saved_sources.py fetch --run-dir <run>`.
Use `reattribute` for matching-rule repairs without downloading or reparsing
the documents. Use `reparse` only when document extraction changes. Exports can
be reconstructed with `export`. Do not run concurrent writers or rerun paid
searches. The current Wayback acquisition process is separate.

## Final status (2026-09-10)

The source pass stopped at its byte limit after processing 11,900 of 58,463
sources. It left 43,961 sources unattempted and 415 retryable. The finished run
is preserved as a verified archive on the attached droplet volume. The mac2
source copy was deleted after archive verification and user approval.

A deterministic 60-row audit found that the 225 proposed exact URLs should not
be treated as validated coverage. The change-type-weighted exact-URL precision
estimate is 44.8 percent. Most false positives are news or reference pages that
mention the candidate. See
`quality_reports/mayoral_source_expansion_url_audit_2026-09-10.md`.
