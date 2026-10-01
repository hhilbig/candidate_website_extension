# Outstanding mayoral collection work

The user requested a persistent goal, a list of outstanding work, and work on the next steps.
The goal is active and incomplete. The user approved the 214-capture batch and 19-query metadata inventory. The status list is in `docs/mayoral_url_discovery.md`.

## Full-goal check after saved-evidence reviews

The full collection is incomplete. This check compares all eleven requested steps with the current outputs and remaining evidence gaps.
The saved review work does not authorize further acquisition or a final freeze.

| Requirement | Current evidence | Remaining work |
|---|---|---|
| 1. Reconcile 359 inventory rows | `mayoral_inventory_22_2026-09-14/saved_reconciliation_v2/` preserves every row and identifies 37 exact saved-response matches. | Uncatalogued stores remain outside the stated check; unmatched rows are not global-absence findings. |
| 2. Adjudicate 1,974 deferred captures | `mayoral_deferred_scope_review_2026-09-14/v4/` retains 1,958 acquisition leads, 15 technical endpoints, and one uncertain template path. | The template path still needs evidence. Acquisition eligibility is not content review. |
| 3. Complete timestamp inventories and window exceptions | Both approved inventories passed verification: 22 queries returned 359 rows; 19 queries returned 1,421 rows. | Resolve Collins and Hall date-window limitations without silently extending the windows. Preserve duplicate and variant index rows. |
| 4. Acquire missing eligible captures | The approved 259-capture batch is complete. The 214-capture job passed its smoke test and runs within its 128 MiB response cap. | Verify the final batch results. Further deferred acquisition leads require later approved batches. |
| 5. Finish evidence and text checks | The 460-capture review is complete. All 37 newly matched rows and all 18 responses in the six empty-text cases are reviewed. | Period-specific evidence remains missing for uncertain, flagged, availability-negative, priority, and date-conflict cases listed in the status document. |
| 6. Resolve 30 new source leads | Source-review v3 supports 21 leads and leaves nine pending exact-target period evidence. | Obtain the missing period evidence. Do not relabel these gaps as completed human review. |
| 7. Complete source and OCR backlog | A fresh read-only database check confirms 44,883 never-attempted sources, 637 retryable sources, and 7,380 unprocessed OCR pages. | Further source and OCR batches need separate limits and approval. Respect access and permission restrictions. |
| 8. Integrate synchronized outputs | The current website table preserves 1,385 rows and 41 AI-accepted websites. Source-period labels and the 359-row capture-review table have verified remote copies. | Integrate later acquisition and source results before the final synchronized build. |
| 9. Run final audits | Recent work passes 99 tests; the broader parser/export suite passes 181 tests. Keys, hashes, and review bindings were independently checked. | Repeat final provenance, coverage, and residual-error audits after the remaining data work. These tests do not prove full-goal completion. |
| 10. Freeze the authoritative internal dataset | Verified working outputs exist remotely and locally. | Do not call an interim snapshot the final dataset. Finish collection, decisions, and final audits first. |
| 11. Remove verified redundant temporary data | The original repair restore was removed after verification; original archives remain. | Defer final cleanup until the final outputs and required provenance are durably saved. Do not delete retained evidence to make the goal appear complete. |

The current website table is `completion/v7/period_review_complete/candidate_urls_with_source_period_reviews_v2.csv` under the source-restart report.
Its SHA256 is `dd0975776cb68681343c1e8c7ff6fb5fb7b66fea7c24c886ee93df45812449fd`.
The read-only source check found 62,403 sources, 16,460 attempts, 88,412 claims, and zero unfinished attempts.
No matching source, capture, or inventory process was active. All delegated reviews had finished.

The approved next batches are 214 captures, capped at 128 MiB of decoded responses, and 19 metadata queries, capped at 32 MiB and 300 requests.
Both retain at least 8 GB of volume headroom and require a fresh preflight before launch.
Approval for these two batches does not authorize the separate source/OCR backlog or remove later evidence gaps.
Publication remains outside the goal.

## Original execution sequence

First compare the 41 AI-accepted URLs with saved query records, inventories, queues, and response files.
Preserve the distinction between exact URLs, syntactic variants, and other paths on a domain.
Check query truncation and selection rules before claiming historical completeness.
Prepare a targeted download proposal with size estimates; do not fetch captures during the audit.

Independently assess the 215 URL rows previously grouped as empty inventories or terminal fetch results.
Verify saved query responses and identify which results remain retryable or unresolved.
Inspect alternative saved evidence for the two unsure websites without replacing earlier review records.

Read live source-job state and preserve its existing approved limits.
The source job runs separately from archive acquisition.
Later work includes new source-lead review, remaining acquisition evidence, and derived outputs after the tranche completes.
Publication, new paid services, and further source tranches require separate authority.

Store compact audit outputs locally. Keep raw response files on the droplet.
Run relevant tests and inspect all result keys, status counts, and hashes before reporting completion.
