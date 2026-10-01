# Session log: Mayoral ownership repair

**Session type:** implementation
**Plan:** `quality_reports/plans/2026-09-10_mayoral-ownership-repair.md`

## Decisions

The September 9 archived ledger and parsed evidence control this repair. The original 60 audit decisions serve as regression evidence, not human approvals.

The repair checks ownership for existing source claims. It preserves acquisition tables and claim identifiers. The old reattribution path also revises follow-up relations and retrieval state, so this repair uses an explicit ownership-only path.

Bibliographic entries that begin with “Web site:” do not establish campaign ownership. Candidate domain shape or campaign-site branding can support a citation to a campaign homepage. Account login routes need review of the site root. Undated personal-site references need campaign evidence. Council-only source titles flag an office conflict.

## Verification

The focused suite passes 108 tests. The repaired outputs retain 120 of 225 proposals. New candidate-year leads fall from 87 to 55. Active candidate-years with leads total 1,093. All 65,683 source claims remain saved, with exact key agreement and unchanged acquisition table hashes.

The original audit has one corrected decision. Andrew Jones's proposed 2017 URL comes from a 2021 election page and now needs date review. All 27 original rejects are excluded; 30 original accepts remain. The first fresh sample exposed party sites, blog indexes, wrong-office targets, and election-year conflicts. The final check supports 28 surviving leads. These checks confer no human approval or historical-content validation.

The archive contains 41 verified members and uses 167,647,002 bytes. Its SHA-256 is `01715f51ebd42d7a0368fb73d016c261c0e7d94b386b9c1989ccc1ad7a262571`. It is stored at `/mnt/mayoral_wayback_full_snapshots/attribution_repair_20260910/repaired_20260910.tar.zst`. The 2.2 GB temporary restore was removed. The Mac received 7.52 MB of compact outputs and about 0.7 MB of audit files.

## Open work outside this repair

The source crawl remains stopped and incomplete. The proposals need review and historical archive evidence before validated coverage can increase. The original archive remains available for recovery.
