# Session log: New mayoral URL lead audit

**Date:** 2026-09-10
**Status:** done

## Result

The 225 proposed source-expansion URLs are not reliable enough to count as validated campaign websites. A frozen 60-row audit estimates exact-URL precision at 44.8 percent, with an approximate 95 percent interval from 33.8 to 55.7 percent.

The alternative-URL stratum performed well: 20 of 20 sampled rows were accepted as grade-C claims. The new-candidate and additional-site strata performed poorly: 6 of 20 and 5 of 20 exact URLs were accepted. One additional-site row contains a recoverable candidate-site root, and one new-candidate row remains unresolved.

## What was done

The authoritative 225-row input was matched to source evidence streamed from the verified droplet archive. All 267 expected source claim identifiers matched exactly once. No full archive table was stored locally.

The audit ran structural checks on all 225 rows. It selected 20 rows from each change type by SHA-256 rank before decisions. Saved evidence supported most decisions. Four ambiguous cases received targeted public-source checks. No paid service was used.

## Verification

Four focused tests pass for unique keys, exact claim linkage, deterministic stratified selection, and exact decision-sample agreement. The audit directory uses 484 KB. The audit did not modify the combined roster or release.

## Error and fix

The first failure-pattern cross-tab collided with blank decision columns already present in the frozen sample. The audit results were unaffected. Dropping those placeholders before the merge produced the verified cross-tab.

## Next action

Repair the source-expansion eligibility rule before rebuilding the combined URL list. The repair must reject news and reference targets even when their anchor text names the candidate. Preserve the current 225 rows as discovery leads and do not resume the large source crawl until storage is planned.
