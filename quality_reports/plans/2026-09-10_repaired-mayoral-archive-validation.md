# Validate repaired mayoral URL leads

Validate the 120 ownership-supported proposals against archived content near
their election dates. The input is the ownership repair's
`combined/new_candidate_urls.csv`, frozen by SHA-256 before requests.

Use the existing availability/content validator with its 365-day headline
window and bounded checks for domain reuse within two years. Preserve the
effective replay timestamp, content hash, source claim IDs, and unresolved
request errors. An automated positive remains pending review.

Run serially on the droplet volume. Save compressed response evidence there,
with a 2 MB response limit and 64 MB total compressed-body limit. Copy compact
validation tables and audit summaries to the Mac. Checkpoint after every lead.

Test input keys, missing election months, archive-only redirects, and storage
limits, then run a three-lead live check before processing all 120. Inspect
positive content signals and report unresolved cases separately from archive
absence. Document exact completion and storage counts in the project status.

Completed: all 120 rows assessed; 73 have historical campaign evidence, including
37 that clear all programmed checks. Saved-corpus comparisons corrected 12
false absence results. Content review downgraded one résumé false positive.
All 58 tests pass. See `quality_reports/mayoral_archive_validation_2026-09-10.md`.
