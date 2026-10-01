# Plan — Mayoral saved-evidence follow-up

**Date:** 2026-09-13
**Status:** complete; the separate download tranche remains proposed

## Context

Compare current 2010–2021 campaign URL leads with saved responses before requesting more archive pages.
The September 13 reconciliation queue contains 843 URL rows, not 843 required downloads.
The September 10 corrected roster and September 13 reconciliation control this work.

## Recommended approach

1. Check 521 rows with saved domain pages or validation evidence for exact-path URL variants.
2. Check the 107 priority rows against other saved evidence.
3. Preserve the 215 rows with empty inventories or terminal responses for separate assessment.
4. Record URL equivalence, capture existence, saved responses, and campaign ownership separately.
5. Estimate a bounded source-discovery restart using the saved queue and actual storage constraints.

Two agents perform independent saved-evidence checks. The coordinator verifies their outputs and prepares the restart proposal.

## Critical files

- `quality_reports/mayoral_review_reconciliation_2026-09-13/reconciliation/`: frozen audit inputs.
- `quality_reports/mayoral_saved_evidence_followup_2026-09-13/`: new outputs.
- `docs/mayoral_url_discovery.md`: current status.

## Reused existing functions

Inspect the existing exact-URL comparison and validation helpers before introducing another URL rule.
Use saved metadata and remote ledgers without new archive requests.

## Verification

Require unique input and output URL keys and exact agreement within each assigned group.
Test scheme, host, path-case, query, candidate-year, and missing-evidence cases.
Recheck source hashes and inspect reported examples.

## Non-goals

No paid search requests, archive downloads, human approvals, or dataset promotions.
Do not launch source discovery before approval of a new download ceiling.
Do not remove or overwrite prior evidence.
