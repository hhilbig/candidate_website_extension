# Mayoral website review and acquisition reconciliation

## Website review

Agents recommend acceptance of 41 proposed URLs across 40 candidate-years.
Seventeen candidate-years lack a URL in the frozen discovery baseline.
The other recommendations concern alternative URLs or additional sites for known candidates.
These counts describe advisory recommendations, not additions to human-approved coverage.

The review compares the 43 automatically cleared proposals with saved election-period HTML.
Two agents reviewed separate subsets of 22 and 21 proposals.
The coordinator checked all review keys, URLs, content hashes, effective timestamps, and temporal distances against the frozen validation table.
All 43 records passed those consistency checks.
The review added no human approvals.

Matt Hall's 2018 Carlsbad website and Greg Jamison's 2014 Sioux Falls website need a content warning.
Their saved metadata identifies the campaign, but their Wix pages contain little visible text.
Retain both as strong website leads. Do not describe these captures as full campaign-text acquisition.

The 43 proposals represent 35.8 percent of the 120 recovered proposals.
This subset follows the approved review priority. All 120 records remain in the input table.
The coordinator also inspected hash-verified main-page titles and campaign excerpts for the 30 supported proposals with flags.
All 30 contain evidence consistent with the named mayoral campaigns.
Six flags concern later domain changes; three concern archive service errors.
The other 21 concern incomplete surrounding evidence or archive inventories.
Preserve these flags for capture-specific use and follow-up.
The remaining 47 proposals retain their prior unresolved status and received no new substantive review.

The reviewers inspected saved first-party evidence without script execution.
They did not independently inspect every third-party discovery source or every surrounding capture.
The temporal checks use the recorded election month and a day-15 reference, not a verified election day.
This review does not estimate population precision or validate the entire downloaded corpus.

## Files

- [Combined agent recommendations](combined_review.json) contain the 43 reviewed proposals.
- [Flagged proposal assessment](flagged_review.json) records the coordinator's 30 assessments and their limits.
- [First reviewer](review_a.json) and [second reviewer](review_b.json) retain independent evidence and provenance.
- The input is `quality_reports/mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv`.
- Its SHA-256 is `ff4574d436694bada058d79437ca10b4f31ae32a9115fa4b3b3ae97699fc17ee`.

## Verification

The combined review, validation, and reconciliation test run passed 91 tests.
The combination rejects duplicate, missing, or unexpected proposal keys and candidate-year mismatches.
The test confirms that advisory recommendations do not change human-approval fields.

## Acquisition reconciliation

The current active roster contains 1,305 URL rows for 1,093 candidate-years.
The active universe contains 2,788 candidate-years; 1,695 have no current URL lead.
The reconciliation compares this roster with the completed full-snapshot corpus and four earlier production checkpoints.

Across those inspected batches, 462 URL rows for 427 candidate-years have saved responses at the exact requested URL.
A total of 801 candidate-years have saved responses on the associated candidate-year/domain pair.
These sets overlap. Domain association does not prove ownership, campaign content, or an exact-path match.
Exact comparisons preserve scheme and host, so HTTP/HTTPS and www variants can fall into the domain-match category.
The 120-proposal historical validation remains a separate evidence source and identifies campaign support for 70 candidate-years.

The completed corpus contains 291,044 unique keys in the inventory, queue, and final checkpoint.
The reconciliation found no missing, duplicate, or unexpected capture keys.
It found 284,481 saved responses at the requested timestamp and 473 timestamp substitutions.
All saved-response rows have their raw and text paths present. The full corpus hashes were not recomputed.
No retryable fetch rows remain in this completed batch.
One frozen target is absent from the current URL list: Ethan Berkowitz's 2015 `ethanforanchorage.com` target.
Its 71 captures remain saved. This reconciliation does not change the roster or discard those captures.

The [remaining acquisition review queue](reconciliation/remaining_acquisition_review_queue.csv) contains 843 URL rows.
This queue is not a list of 843 new downloads.

| Required assessment | URL rows |
|---|---:|
| Inspect saved pages from the same candidate/domain and exact-path differences | 461 |
| Assess existing capture metadata or terminal fetch outcomes | 215 |
| Inspect saved validation captures and URL equivalence | 60 |
| Check other prior evidence, then consider exact-URL inventory | 107 |

The last group represents 97 candidate-years.
These rows are the first acquisition candidates after the saved-evidence checks.
The [candidate-year table](reconciliation/candidate_year_reconciliation.csv) retains all 4,817 roster keys.
The [URL table](reconciliation/url_reconciliation.csv) retains all 1,360 current URL keys, including earlier years.
The [summary](reconciliation/summary.json) records input hashes, join audits, counts, and limitations.

## Next action

Steps 1 and 2 are complete as advisory review and acquisition reconciliation.
Retain the 41 acceptance recommendations separately until Hanno approves their use as human-accepted coverage.
Resolve saved URL equivalents and inspect the 107 priority rows before new archive requests.
Then resume source discovery from its saved checkpoint with corrected ownership rules and a newly approved download ceiling.
The separate source queue retains 43,961 unattempted sources and 415 retryable sources in its latest saved audit.
The source queue and the acquisition review queue have different units and must not be added together.
