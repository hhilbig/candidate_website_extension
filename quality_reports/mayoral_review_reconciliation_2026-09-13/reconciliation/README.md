# Completed scrape and current URL list

Saved responses cover more candidates than the last scrape alone suggests.
Across five inspected production checkpoints, 427 active candidate-years have an exact requested URL response.
Saved responses share a candidate-year and domain with leads for 801 candidate-years.
Neither measure establishes campaign ownership or usable campaign text.

This comparison joins the September 10 corrected candidate URL list to the completed droplet corpus and four earlier production checkpoints.
The active universe contains 2,788 candidate-years. Its 1,093 candidates with leads have 1,305 exact URL rows.
The full roster retains 4,817 rows; the approved 2010–2021 scope excludes 2,029 rows from active counts.
No input rows were removed.

## Saved responses and review queue

The completed uncapped corpus contains 291,044 capture keys in each inventory, queue, and final checkpoint.
The audit found no duplicate, missing, or unexpected keys and no missing saved raw or text files.
It records 284,481 fetched responses and 473 responses with timestamp substitutions.
The other outcomes are 6,035 no-content responses, 54 unavailable captures, and one permanent redirect loop.
No retryable rows remain in that checkpoint.

Earlier production batches add exact responses for 125 current URL rows.
Together, the five checkpoints supply exact requested URL responses for 462 current active URL rows.
This prevents repeat acquisition of those saved responses.

The [remaining queue](remaining_acquisition_review_queue.csv) contains 843 active URL rows without an exact response in the inspected checkpoints.
It is a review queue, not a download queue.

| Required check | URL rows |
|---|---:|
| Inspect saved domain pages and exact-path differences | 461 |
| Inspect saved validation captures and URL equivalence | 60 |
| Assess completed empty inventories or terminal fetch outcomes | 215 |
| Check other prior evidence, then inventory the exact URL if needed | 107 |

These counts are URL rows, not unique candidates.
The 120-proposal validation table independently records saved capture metadata for 76 active candidate-years.
Its campaign-content flag supports 70 candidate-years. These automated flags do not constitute human approval.
Check the new agent review before drawing conclusions from those flags.

## Join rules and limits

Exact matches use candidate-year plus requested original URL.
They preserve the URL scheme, host, path case, and query; only host case, fragments, and empty root paths are normalized.
The broader join uses candidate-year plus the supplied registrable domain.
Shared hosts, different paths, redirects, and domain reuse can invalidate an ownership inference.
The output records both joins separately.

The full frozen batch contains 783 targets. One target no longer matches a current candidate-domain pair.
It is Ethan Berkowitz's 2015 `ethanforanchorage.com` target, with 71 saved captures.
The [separate table](frozen_targets_absent_from_current_list.csv) preserves that discrepancy.

The remote audit checked every saved raw and text path in the completed corpus.
It did not recompute their content hashes or classify their text.
Earlier checkpoints were inspected for exact URL and domain associations, with saved raw-file existence checks.
Their complete queue histories were not re-audited.
Saved validation metadata remains a separate attribute; the main review verifies its content evidence.

## Reproduction

Run `PYTHONPATH=. .venv/bin/python scripts/reconcile_mayoral_completed_scrape.py` from the repository root.
This command reads remote files and transfers compact aggregates. It makes no archive requests.
Add `--reuse-remote` to rebuild local tables from the saved remote aggregates.
Run `PYTHONPATH=. .venv/bin/python -m pytest -q tests/test_mayoral_completed_scrape_reconciliation.py` for the join tests.

The [summary](summary.json) records counts, input hashes, and the reconciliation check.
The [remote aggregates](remote_aggregates.json) record hashes for the remote inputs.
The [candidate table](candidate_year_reconciliation.csv) preserves the candidate-year denominator.
The [URL table](url_reconciliation.csv) provides each exact URL's acquisition and review status.
