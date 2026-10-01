# Plan: Mayoral URL discovery next phase

**Date:** 2026-08-24  
**Status:** implemented through the first municipal pass; revised 2026-08-25

## Current state

The mayoral universe contains 4,817 candidate-years. The accepted baseline has
118 source-backed campaign URLs. The remaining 4,699 candidate-years have no
accepted URL.

The frozen legacy roster has 2,695 unresolved candidate-years from 2010 through
2021 and 2,004 before 2010. Accepted Archive-It additions postdate this frozen
roster, so the pipeline must rebuild the unresolved roster before drawing the
next sample.

The search pilot contains 120 unresolved candidate-years and 235 completed
Serper requests. It has six direct or embedded URL proposals and four additional
source-page leads. The repository has not accepted these ten records. The first
task will repair the handoff and submit the records for human review.

## Objective

The next phase will discover URLs from contest-level sources. It will test
Archive-It collections, official municipal records, and historical civic-data
providers. It will not spend the remaining Serper credits.

The project will report two coverage measures. The primary measure counts strict
candidate-owned custom domains. The secondary measure also counts strict
candidate subdomains and shared-host paths.

## 2026-08-25 scope revision

The main acquisition target is now elections from 2010 through 2021. A URL is
useful for the headline measure only when archived campaign content can support
grade A or B evidence. Sparse Wayback coverage before 2010 makes many otherwise
credible historical domains impossible to validate under that rule. The project
will retain pre-2010 URLs already found, but it will not spend the next round of
discovery or records-request effort on that period.

This is a feasibility decision, not a claim that every pre-2010 URL lacks an
archive. The project has not estimated a national pre-2010 Wayback capture rate.
Pre-2010 discovery can return as a sensitivity exercise if a source supplies
contemporaneous archived content, not only a domain name.

## Work order

1. Freeze the current inputs and outputs with row counts and SHA-256 hashes.
2. Repair URL normalization, source provenance, retry logic, and archive handoffs.
3. Make an explicit build manifest control all enabled source outputs.
4. Add a contest universe and a unified source-attempt ledger.
5. Build a local handcoding app for the ten current URL leads and sampled negatives.
6. Ingest Archive-It collections 5903 and 16766, then enumerate similar collections.
7. Build the early-period city priority table and municipal source registry.
8. Build the 480-case screen, 32 complete contests, and masked provider sample.
9. Prepare provider and public-record outreach drafts. Do not send them without approval.
10. Run an independent 800-case confirmation only after the screening gates pass.

Steps 1 through 9 are complete. The early municipal public-file pass produced
zero URL claims among 335 candidate-years. Three approved pre-2010 records
requests were saved as Gmail drafts but paused before sending after the scope
revision.

The revised work order is:

1. Rebuild the unresolved roster after all accepted Archive-It claims.
2. Restrict the active acquisition frame to 2010 through 2021.
3. Draw a new 480-case screen with 160 cases in 2010-2013, 2014-2017, and
   2018-2021. Exclude all cases used in the search pilot and earlier screens.
4. Draw 30 separate complete contest-years, with ten in each period.
5. Re-rank municipal and curated archives by unresolved 2010-2021 candidates,
   archive availability, and whether the source records campaign websites.
6. Test the same source stack on the new sample. Report strict yield both within
   the 2010-2021 acquisition frame and against the full 4,817-case denominator.
7. Confirm any source stack that passes on an independent 800-case sample,
   allocating 320 cases to 2010-2013 and 240 to each later period.

## Evidence rules

- Grade A requires an election-period capture of the exact URL. The capture must
  identify the candidate, mayoral campaign, and city.
- Grade B requires a contemporaneous official source and archived campaign content.
- Grade C has source support but no qualifying campaign capture.
- Grade D is a generated or forensic lead without campaign-content proof.

Only grades A and B count as strict coverage. The headline window is 365 days
from the election. A sensitivity check uses 18 months before the election through
120 days after it. A two-year check on each side detects domain reuse.

## Screening gates

The revised screen contains 480 candidate-years, with 160 cases in each of three
periods from 2010 through 2021. It excludes the original search pilot and all
earlier screening cases. A separate sample contains 30 complete contest-years,
with ten contests in each period.

A source continues when it produces at least ten new strict URLs in 480 cases,
or five new strict URLs among the 160 cases from 2010-2013. Precision must reach 95%.
Projected cost must remain below $100 per accepted URL.

A source stops when it produces no more than three strict additions, precision
falls below 90%, or cost exceeds $250 per accepted URL. Full acquisition starts
only when projected primary coverage reaches 15% and its one-sided 90% lower
bound reaches 13%.

## Approval boundaries

No external message will be sent without approval of the exact recipient and
text. No paid API or data purchase will occur without a sample, cost estimate,
terms review, and explicit approval. Existing user changes will remain intact.

Approval of a pre-2010 request does not override the revised feasibility rule.
The Columbus, Austin, and Pomona drafts remain unsent unless Hanno explicitly
reactivates the pre-2010 sensitivity exercise and says to send them.
