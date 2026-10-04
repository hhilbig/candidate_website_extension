# Session log: Codex endorsement review, 2026-09-14

Session type: implementation and analysis.
Plan: `quality_reports/plans/candidate_endorsement_codex_ai_review_2026-09-14.md`.

## What I did

Three Codex agents reviewed the current 40-page sample. One agent reviewed only the complete source text. Another agent checked the existing AI draft against each complete source. A third agent checked seven difficult contexts independently. The primary agent inspected disagreements and recorded decisions for 22 pages.

The authoritative input was `build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json`. Its SHA-256 was `c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea`.

I added `scripts/review_candidate_endorsement_codex.py` and its tests. The script prepares source-only inputs, validates reviews, compares the two full reviews, and assembles the separate AI reference.

The final result is `build/private_candidate_endorsement_codex_ai_review_20260914a/ai_reviewed_results_v2.json`. The report is `ai_review_report_v2.json` in the same directory. Per-page review files preserve the source hashes, exact name spans, support spans, uncertainty, and reviewer identities. Earlier versions remain available.

## What I verified

All 40 parent identities, metadata, and source hashes matched the authoritative input. All 1,960 records retained exact source spans. Every name fit inside its support. No exact duplicate records or overlapping organization names remained. I verified 131 review-file hashes. The report matched the result without its responses. The result contained 927,049 bytes.

The relevant test suite passed: 72 tests in 36.71 seconds. This suite covered round-four extraction, development inputs, sample selection, regression checks, the review app, and the new review script. Python compilation and `git diff --check` passed.

Two agents audited the first assembled result independently. Their audits identified an omitted standalone PACE reference and overly broad uncertainty for HPR Ammunition and Arizona Republic. I corrected these findings in version two. I then checked the corrected statuses and spans directly.

The independent final audit also passed for version two. It verified all counts, source identities, spans, flags, and 131 hashes. The result SHA-256 is `56abd8a8f34f2ab3f13498b838792f714784430747a7405d08810e6ca4887704`.

## What I found

The source-only review proposed 1,630 records. The draft audit proposed 1,972 records. They agreed on 1,614 exact name-coordinate and type identities. Disagreements or uncertainty required decisions on 22 pages.

The final result contains 1,960 proposed records. AI review accepts 1,093 records and marks 867 records uncertain. Missing endorsement headings and repeated profile identities explain much of the uncertainty. These labels provide a provisional reference, not human-validated accuracy.

The original draft contained 1,877 records. The final result retains 1,794 exact original identities, adds 166, and removes 83. Boundary corrections can count as both additions and removals. These counts do not measure endorsement precision or recall.

## Decisions

Every record remains `human_reviewed=false`. The result states `review_status=ai_reviewed`. Human-truth, production, full-expansion, and release flags remain false. No pages were removed.

The approved plan records the durable decisions about repeated organization names, public-support lists, explicit corporate endorsements, and uncertain parent dispositions.

The user selected strong Codex agents. The agents inherited the primary Codex model. The tool did not expose its exact deployment identifier. This review used the existing Codex allowance. It did not call a separately billed API, purchase credits, or redeem usage resets.

## Open threads

A human must resolve the uncertain records and exclusions before the reference can support human-validated accuracy claims. This review did not establish temporal alignment between each endorsement and the sampled election. It did not integrate labels into production or update the webapp.

## Memory updates

No memory or knowledge-base update was required. The approved plan records the review-specific decisions.
