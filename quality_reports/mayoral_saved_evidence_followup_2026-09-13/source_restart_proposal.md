# Proposed source-discovery restart

Hanno approved this proposal on September 14. The tranche started that day.
The text below records the original preflight proposal.
See `../mayoral_source_restart_2026-09-14/report.md` for the implemented limits and live run location.

Resume a bounded part of the saved source queue after approval of a new download ceiling.
Do not buy new search credits or repeat the completed campaign-snapshot batch.
This proposal compares the remaining source documents with the previous source pass to estimate the next download volume.

## Scope and expected volume

The corrected ledger retains 43,961 never-attempted sources and 415 retryable sources.
It separately records 2,252 host-deferred sources and 7,226 unfinished optical character recognition pages.
These groups can overlap. Do not add them into one workload count.

The prior pass downloaded 8,063,489,603 raw bytes and processed 11,270 source documents, including 1,091 cached documents.
Dividing bytes by 10,179 noncached document outcomes gives about 0.79 MB per document.
This rough ratio includes retry costs and does not predict the size of any particular source.
At that ratio, 5,000 additional documents would require about 4 GB of raw downloads.
All 43,961 never-attempted sources would require about 35 GB, before new follow-up sources.
These are planning estimates, not throughput or yield forecasts.

Proposed first tranche: at most 5,000 additional source attempts and 4 GB of additional raw response bytes.
Use corrected ownership rules and existing host restrictions.
Keep uncertain claims and prior rejections in the evidence table.
Review marginal candidate-year yield before allocating another tranche.

## Storage and cost

A read-only droplet check found 32,993,816,576 bytes free on the attached data volume.
The root filesystem had only 3,494,420,480 bytes free.
Keep restored data, temporary files, and new outputs on the attached volume.
The original archive contains 8,164,812,284 source-file bytes.
The corrected repair archive adds about 2.3 GB when fully restored.
A conservative full restore plus 4 GB of new responses leaves about 18.5 GB before new exports and temporary files.
Retain at least 8 GB of free space and check both filesystems before launch.
No storage purchase or paid API is proposed. Existing hosting charges continue.
This check does not estimate provider transfer overages; stop if deployment requires billable transfer.

## Required launch checks

1. Verify both archive hashes against the saved manifests before extraction.
2. Restore original raw and parsed evidence plus the corrected ledger and exports into a fresh run directory.
3. Verify database integrity, saved evidence paths, source keys, and current candidate-year counts.
4. Preserve the existing ownership attribution version and use a frozen input manifest.
5. Add explicit tranche limits to the supervisor and test their propagation to the child command.
6. Enforce the byte allowance before dispatch and reserve space for concurrent responses.
7. Keep the watcher and Slack messages specific to Mayoral URL discovery in candidate_website_extension.
8. Verify the first durable source outcome and effective command after launch.

The current supervisor does not expose the runner's byte or source limits.
The runner checks cumulative bytes periodically, so its existing limit permits an overshoot.
Do not represent that check as a hard 4 GB tranche ceiling.
An implementation must subtract prior bytes or set the corresponding cumulative threshold and enforce the new allowance.
The prior cumulative value plus 4 GB is 12,063,489,603 bytes.
The current 8 GB default would stop the resumed run almost immediately.

## Evidence and limits

The current corrected audit controls source counts:
`data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/audit.json`.
The original transfer manifest controls restored file sizes:
`/mnt/mayoral_wayback_full_snapshots/backups/mayoral_source_expansion_20260908/TRANSFER_MANIFEST.md` on the droplet.
The repair manifest controls the corrected ledger and its code:
`quality_reports/mayoral_ownership_repair_2026-09-10/final/archive_manifest.json`.
No restore, new source request, archive request, or paid request occurred during this preflight.
The tranche remains proposed until Hanno approves its limits.
