# Mayoral website ownership repair

The repaired list contains 120 website proposals, down from 225. The repair excludes 105 proposals (46.7 percent) and preserves all 65,683 source claims. The proposals remain unreviewed discovery evidence.

## Changes and results

This repair compares candidate-year and URL pairs before and after explicit target-ownership checks. The verified September 9 archive supplies the ledger, parsed documents, and frozen roster.

The old rule treated the publication label “Web site:” as a campaign-website label. The revised rule distinguishes citations, party sites, blog indexes, and candidate websites. It checks account routes and domains for other offices. It recognizes election years separated from “mayoral election” by a city name. It preserves legitimate Google Sites paths.

Claims now include `candidate_site_ownership_supported` and `candidate_site_ownership_reason`. Both the exporter and summarizer require this support. Older claims without ownership fields go to review. The offline repair cannot promote a previously unresolved identity.

| Measure | Before | After |
|---|---:|---:|
| New exact URL proposals | 225 | 120 |
| New active candidate-years with leads | 87 | 55 |
| Active candidate-years with any lead | 1,125 | 1,093 |
| Source-expansion claims preserved | 65,683 | 65,683 |

The 120 proposals comprise 58 new-candidate URLs, 13 additional sites, and 49 alternatives on known sites. The decrease in new candidate-years is 32 of 87 (36.8 percent). No candidate-year leaves the roster.

Of the 105 excluded proposals, 95 are news, government, bibliographic, party, or blog references. Seven have office or election-year conflicts. Three need ownership, personal-site, or account-route review. [The complete comparison](mayoral_ownership_repair_2026-09-10/final/proposal_changes.csv) records each reason. The lower count reflects stricter evidence requirements, not lost source data.

## Verification

All 4,817 roster keys and 65,683 claim keys agree with the inputs. The combined source table retains 71,047 claims, including 5,364 baseline claims. It contains no duplicate claim keys or URL keys. Thirty-two claims no longer reproduce an identity match under the revised rules. They remain available for review.

Acquisition table hashes match the original ledger. This covers 58,463 sources, 85,049 source relations, 14,042 attempts, 11,900 current records, and 4,817 candidates. The source pass remains incomplete, with 43,961 never-attempted sources and 415 retryable sources.

All 27 rejects from the original audit leave the proposal list. Thirty of its 31 accepts remain. Ricco Wright and the Joe Tuman login route remain excluded pending review.

One original acceptance needs correction. Andrew Jones's proposed 2017 URL comes from a **2021 St. Louis mayoral election** page. The source mentions his 2017 nomination but does not establish the website for that year. The repair retains the claim in date review. This evidence-based exception replaces the plan's expectation that all 31 accepts would remain eligible.

The first additional 30-row sample found three reference or wrong-office targets and two election-year conflicts. Those findings informed the final rules. The final deterministic sample contains 28 surviving proposals outside the original 60: ten new-candidate URLs, eight additional sites, and ten alternatives. Their saved evidence supports all 28 as discovery leads. Twenty-five appeared in the earlier additional sample; three are replacement rows. This is a verification exercise, not an unbiased precision estimate. [Final decisions](mayoral_ownership_repair_2026-09-10/final/fresh_sample_decisions.csv) explain each judgment.

The focused suite passes 108 tests. It covers citations, campaign domains, shared hosting, date and office conflicts, missing ownership fields, fixed claim keys, and preserved acquisition records. No audit decision creates a human approval or strict historical-coverage flag. Election-period page content remains unverified for these proposals.

## Files and storage

The compact corrected outputs are in `data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/`. The original completion bundle remains a historical record.

The rebuild used the droplet's attached volume. Raw source bodies remained compressed in the original archive. The corrected archive is `/mnt/mayoral_wayback_full_snapshots/attribution_repair_20260910/repaired_20260910.tar.zst`. It contains the repaired ledger, exports, audit evidence, and required code. Its size is 167,647,002 bytes. All 41 archive members passed SHA-256 verification. The [archive manifest](mayoral_ownership_repair_2026-09-10/final/archive_manifest.json) records each file hash and the original archive dependency.

The 2.2 GB temporary restore was removed after verification. The original archive remains intact. The compact Mac bundle uses 7,523,070 bytes; local audit files use about 0.7 MB. Recovery uses the original archive for parsed or raw source evidence and the repair archive for corrected claims and exports.

The implementation uses `src/mayoral_source_expansion.py`, the source exporter and summarizer, and `scripts/verify_mayoral_ownership_repair.py`. Attribution version is `mayoral-source-attribution-v5`; summary version is `mayoral-source-expansion-summary-v2`.
