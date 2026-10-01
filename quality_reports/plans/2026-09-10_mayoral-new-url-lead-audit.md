# Plan: Audit new mayoral URL leads

**Date:** 2026-09-10
**Author:** Codex session
**Status:** done

## Current state

The source-expansion summary proposes 225 new exact URL rows for 87 candidate-years. Every row is a grade-D discovery lead pending review. None is a human-approved or strict historical campaign URL.

The authoritative lead file is the hash-verified `combined/new_candidate_urls.csv` in the local mac2 completion bundle. The matching claim evidence remains inside the verified droplet archive.

## Audit design

The unit is one candidate-year and exact URL pair. The audit compares the named candidate, mayoral contest, city, and election year with the claimed URL and its saved source evidence.

The audit has two parts:

1. Run structural checks on all 225 rows. Check unique keys, exact claim linkage, candidate and campaign terms, article-shaped paths, other-office terms, cross-candidate URL reuse, source timing, and source-evidence coverage.
2. Select 60 rows before judging outcomes. Rank rows by a SHA-256 hash of the candidate URL identifier and select 20 from each change type: new candidate lead, additional site for a known candidate, and alternative exact URL for a known site.

The sample estimates precision within each change type and overall after applying the observed change-type shares. No numerical pass threshold is imposed. Structural warnings identify records for later review but do not determine acceptance by themselves.

## Outputs

- A compact evidence table with one row per proposed URL.
- The frozen 60-row audit sample and its SHA-256 hash.
- Row-level audit decisions with reasons and evidence grades.
- A report that states the qualitative verdict first, then precision estimates and failure patterns.
- Corrected status fields in the September 6 and September 8 run plans.

## Verification

- Require 225 unique candidate-year and exact-URL keys.
- Require every source-expansion claim identifier to match exactly one archived evidence row.
- Preserve source URLs and evidence text in the compact output.
- Test the audit builder on duplicate keys, missing claims, sample allocation, and deterministic output.
- Confirm that the compact audit outputs do not materially increase local disk use.

## Limits

The audit does not approve URLs, rebuild the release, resume the source crawl, or make new paid requests. Live and archive checks remain evidence for audit decisions, not acquisition output.
