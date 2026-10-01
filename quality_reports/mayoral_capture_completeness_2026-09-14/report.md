# Mayoral capture completeness and outstanding work

The saved inventories do not yet establish complete historical acquisition for the 41 AI-accepted URLs.
Twenty-two URLs have no matching targets in the five inspected batch inventories.
Thirteen have every inventoried response saved and verified. Six have captures that earlier selection rules never queued.
All 41 still have at least one usable campaign page from the separate website-review evidence.

## Inventory and acquisition

The audit compares the 41 accepted URL records with frozen targets, raw query responses, inventories, queues, and saved response files.
It matches candidate-year and domain, then distinguishes exact URLs from syntactic variants and other paths or hosts.
The capture key retains candidate-year, requested timestamp, and exact original URL. It removes duplicate keys across batches.

The matched inventories contain 4,432 keys. Earlier selection queued 2,199 and left 2,233 unqueued, a 50.4 percent reduction.
These are previously selected acquisition subsets, not new deletions from the candidate sample.
Every queued key has a verified saved response somewhere in the inspected batches.
All 2,773 checked physical response files passed their recorded SHA-256 checks.
Ten saved responses have no usable extracted content and require content review rather than automatic redownload.

| Pending acquisition scope | Capture keys | Estimated raw bytes | Estimated compressed bytes |
|---|---:|---:|---:|
| Exact accepted URLs | 259 | 12.49 MB | 2.65 MB |
| Same-path scheme, www, or default-port variants | 69 | 3.33 MB | 0.71 MB |
| Other paths or hosts on the matched domain | 1,905 | 91.86 MB | 19.49 MB |
| All known unqueued captures | 2,233 | 107.68 MB | 22.84 MB |

The estimates use observed mean saved response sizes. They exclude extracted text and metadata and are not upper bounds.
The 22 missing target inventories have unknown capture counts and download sizes.
Exact-URL gaps concern India Walton (248 captures), Bevan Dufty (four), Tony Hall (three), Courtney Ruby (two), and Leland Yee (two).
Variants and other paths still require target and content checks before an acquisition batch.
Default-port normalization moved 32 records from the initial other-path group into the variant group; the total did not change.

All 72 matched query records have terminal filtered statuses and verified raw-response hashes.
No matched query reached its 50,000-row limit; the largest returned 1,430 rows.
However, the original queries collapsed content digests and lacked pagination.
They cannot establish completeness across every historical timestamp.
Any future inventory must state whether the target is every timestamp or distinct archived page content.

## Other completed checks

The [215-row terminal-result audit](../mayoral_terminal_audit_2026-09-14/summary.json) resolves 209 URLs as empty within their recorded query filters and dates.
It found 18 saved HTTP 200 HTML responses without extracted text across the other six URLs.
All 746 distinct query responses and all 18 HTML files passed their hash checks.
No query errors or missing query variants appeared in this group.
The saved HTML includes parking references, cookie/reload scripts, or image and redirect templates.
These records do not support a new automatic retry batch or an archive-wide absence claim.

The [Hall supplement](../mayoral_ai_review_2026-09-14/hall_alternative_review.json) identifies substantive campaign text in a saved November 15, 2019 capture.
That date is exactly 365 calendar days after the recorded election reference, at the existing window boundary.
The supplement records AI support with a date caveat; the frozen acceptance table remains at 41.
The coordinator matched this capture's URL and hash to the saved request journal.
Jamison's second saved May 2014 page contains the same sparse text as his first capture.
His uncertainty remains unresolved.

## Files and next actions

The [outstanding-work list](../../docs/mayoral_url_discovery.md) tracks the complete mayoral backlog, including the active source tranche.
The [URL audit](url_capture_audit.csv) accounts for all 41 accepted URLs.
The [capture queue](targeted_capture_retry.csv) contains the 2,233 never-queued keys; it is a proposal, not a launch instruction.
The [inventory queue](targeted_inventory_queue.csv) identifies the 22 unqueried targets.
The [no-content review](saved_no_content_review.csv) preserves the ten saved responses separately.
The [summary](summary.json) records hashes, size estimates, and limitations.

Prepare a bounded inventory proposal for the 22 targets and an exact-URL acquisition proposal for the 259 known captures.
Review the 69 variants and 1,905 other-path records separately before expanding that acquisition scope.
The current work used saved evidence only and made no archive requests.

The audit, integration, and related regression checks passed 26 tests. Both new audit scripts compiled successfully.
