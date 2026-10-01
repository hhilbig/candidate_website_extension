# Mayoral website review and scrape reconciliation

**Date:** 2026-09-13
**Status:** complete; advisory recommendations and acquisition reconciliation verified

## Scope

Hanno approved agent review of the recovered websites and reconciliation of the completed scrape before discovery resumes.
Compare each proposed website with its saved campaign evidence and each current candidate URL with the frozen acquisition records.

## Work

Two agents inspect separate subsets of the 43 automatically cleared proposals.
They read saved content, verify its hash, and check candidate identity, office, city, URL, and capture date.
The coordinator reviews the remaining flags and checks the combined recommendations.
A third agent reconciles the corrected roster against the completed capture inventory and fetch records.
Keep exact-URL matches separate from domain matches and saved responses separate from confirmed campaign content.

## Inputs and outputs

The September 10 ownership repair controls the candidate roster.
The September 10 reuse-resolution validation table controls the 120 recovered proposals.
The droplet production capture records control completed acquisition.
Write separate advisory outputs under `quality_reports/mayoral_review_reconciliation_2026-09-13/`.
Agent recommendations are not human approvals and do not replace existing research-data fields.
The user explicitly requested agents for this advisory review; this does not change the default for future batch research-data labels.

## Verification

Hash the input files. Require one recommendation per assigned proposal, with no duplicate or unexpected keys.
Verify saved evidence against its recorded hash and preserve effective capture dates.
Check reconciliation keys and all reported denominators. Test any new join or aggregation code.
Use existing saved evidence. New discovery, archive requests, and a new download ceiling require a separate decision.

## Result

Agents recommend 41 URLs across 40 candidate-years. Two saved Wix pages need content warnings.
The coordinator retained the existing flags on 30 additional campaign-supported proposals.
Across the completed and four earlier acquisition batches, 427 active candidate-years have exact-URL responses and 801 have same-domain responses.
The remaining acquisition review contains 843 URL rows; 107 rows across 97 candidate-years need priority evidence checks before inventory.
All 291,044 completed capture keys agree. All saved raw and text paths exist.
The combined tests passed 91 cases.
See `quality_reports/mayoral_review_reconciliation_2026-09-13/report.md` for definitions and limits.
