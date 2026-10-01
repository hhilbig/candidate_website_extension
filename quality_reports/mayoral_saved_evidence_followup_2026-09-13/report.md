# Saved mayoral website evidence

The audit found saved evidence that the first exact-URL comparison did not match.
It does not add new candidate websites to the discovery roster.
The roster still contains URL leads for 1,093 of 2,788 active candidate-years.

This audit compares 628 URLs from the September 13 review queue with existing response files and earlier validation records.
Two agents checked independent groups of 521 and 107 URL rows.
The remaining 215 queue rows need a separate assessment of empty inventories or terminal fetch results.
All 843 input rows remain preserved; this audit does not drop or approve any record.

## Saved URL variants

Saved responses account for most apparent gaps in the 521-row group.

| Result | URL rows |
|---|---:|
| Saved response at the exact effective URL | 26 |
| Saved response at a scheme or host variant with the same path and query | 422 |
| Saved responses only at a different path, query, or host | 73 |

The second group includes HTTP/HTTPS and www variants.
Syntactic similarity does not prove that these addresses are interchangeable.
The audit found no qualifying request-to-variant redirect proof for that group.
Preserve exact URLs and review saved content before requesting another page.

For 434 rows, a same-path response falls within 365 days of the recorded election reference date.
This date uses the recorded election month and day 15, not a verified election day.
The audit verified hashes for 802 distinct selected files across five production checkpoints and saved validation evidence.
It selected the nearest saved response for each URL relationship, rather than verifying all corpus hashes.
These results establish saved responses, not campaign identity or usable campaign text.

See [the row-level variant audit](variants/saved_url_variant_audit.csv) and [its summary](variants/summary.json).

## The 107 priority URLs

Earlier local records supply archive metadata for 36 requested URLs in this group.
Nineteen have reports of successful fetches and response hashes.
One has a prior campaign-support assessment: Jackie Cherryhomes, Minneapolis, 2013.
These historical reports do not establish that their raw files still exist or that the effective capture matches the requested URL.
Seven report an exact effective target; five report a scheme or www variant; three report a changed target.
These categories can overlap across historical attempts, and some reports lack effective-target metadata.

The other 71 URLs lack matching capture metadata in the inspected tables.
One is malformed: `https://info@hill4neworleans.com` contains an email-like user component.
Review its original claim before correcting it or sending an archive query.
For the other 70, inspect prior failure records and then prepare a bounded inventory if needed.
No result here means that Wayback lacks captures.

The agent froze 26 local validation tables with hashes.
Repeated historical rows remain evidence records, not independent confirmations or new human approvals.
See [the priority audit](priority/priority_audit.csv), [source records](priority/prior_evidence.json), and [summary](priority/summary.json).

## Next action

Use saved content for the 26 exact matches and inspect the 422 URL variants before new downloads.
Locate the 19 historically reported response files and preserve the other capture metadata as discovery evidence.
Keep the 73 path differences, 215 terminal-result cases, and remaining priority URLs as separate review groups.
Do not combine these groups into an automatic download list.

Resume source discovery separately after approval of a bounded tranche.
The [restart proposal](source_restart_proposal.md) specifies at most 5,000 additional source attempts and 4 GB of new raw downloads.
The attached volume has about 33 GB free; no storage purchase is proposed.
The supervisor needs explicit tranche limits before launch.
No new archive request, source request, paid request, or human approval occurred in this audit.

## Verification

The combined audit, reconciliation, and source-runner check passed 121 tests.
The checks cover exact path case, queries, archive wrappers, changed effective targets, candidate-year isolation, and frozen input hashes.
The coordinator confirmed that the corrected roster and URL-table hashes still match their September 10 values.
The audit scripts are `scripts/audit_mayoral_saved_url_variants.py` and `scripts/audit_mayoral_priority_saved_evidence.py`.
Use `--reuse-remote` with the variant script to reproduce local outputs from its frozen cache without another remote scan.
