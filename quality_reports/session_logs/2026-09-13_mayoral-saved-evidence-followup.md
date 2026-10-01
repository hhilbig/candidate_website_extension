# Session log — 2026-09-13

**Session type:** analysis
**Plan reference:** `quality_reports/plans/2026-09-13_mayoral-saved-evidence-followup.md`

## What I did

Two agents compared current campaign URLs with saved responses and earlier validation records.
The coordinator reviewed the scripts, requested effective-target and frozen-input checks, and prepared a source-discovery restart proposal.
New artifacts reside in `quality_reports/mayoral_saved_evidence_followup_2026-09-13/`.

## What I verified

The combined test command passed 121 tests across the new audits, prior reconciliation, and source-expansion modules.
The variant audit checked 521 URL rows and verified 802 selected raw-file hashes.
The priority audit checked 107 rows across 26 frozen local tables.
The current roster and URL-table hashes still match the September 10 corrected outputs.
A read-only droplet check found about 33 GB free on the data volume and 3.5 GB free on the root filesystem.

## What I found

The variant group contains 26 exact effective URLs, 422 same-path syntactic variants, and 73 changed paths, queries, or hosts.
The priority group contains 36 URLs with prior capture metadata, including 19 with historical fetch reports.
One email-like URL requires correction from its original evidence before any archive query.
These results do not change the 1,093 active candidate-years with discovery leads.

## Decisions

Preserve source-request and effective-capture URLs separately.
Do not treat syntactic variants as proven interchangeable URLs.
Do not turn historical fetch reports into verified current file availability.
Keep all review groups and human-approval fields intact.

## Open threads

Review saved variant content, locate historically reported files, and assess the 215 terminal-result cases.
The source crawl remains stopped pending approval of 5,000 additional attempts and 4 GB of new raw downloads.
Before launch, implement and test explicit supervisor limits and prevent byte-limit overshoot.

## Memory updates

Updated `docs/mayoral_url_discovery.md` with current findings and the pending download decision.
No change to `MEMORY.md` or `KNOWLEDGE_BASE.md`.
