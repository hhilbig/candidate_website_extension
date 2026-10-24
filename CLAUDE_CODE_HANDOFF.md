# Claude Code handoff

Prepared September 15, 2026. Historical correction is complete.
The remaining releases were extracted variables and the full mayoral extension. On October 24 the mayoral work moved to `~/Documents/GitHub/mayoral_campaign_websites`; this file no longer covers it. The separate 2026 collection uses the approved monthly schedule and awaits a capacity decision.
Read `CLAUDE.md` for persistent execution rules and `docs/status.md` for the controlling release checklist and current work table.
This handoff preserves the task state. It does not transfer a running process or scheduler, or authorize new costs or publication.

## Start Claude with this instruction

```text
Read CLAUDE.md, docs/status.md, and CLAUDE_CODE_HANDOFF.md.
Continue the next already-authorized step for the requested track.
If no track is specified, select the first unblocked, already-authorized
next action in the active-work table.

State the selected step, required output, and completion checks, then work.
Reuse verified outputs. Choose routine implementation details yourself.
After the required checks pass, record the evidence and continue.

Additional work must resolve a demonstrated blocker to a listed requirement.
After two unsuccessful repair attempts on the same blocker, record the
attempts and decision needed. Continue independent authorized work.
Preserve the count across sessions. Do not add optional release requirements.

Before a handoff or context reset, update the active-work table with output
paths, check results, blockers, repair counts, and the next authorized action.
```

## Release order and next outputs

Use the five steps per extension in [the checklist](docs/status.md). Each large step can proceed in bounded batches.

| Track | Sequence | Next concrete output |
|---|---|---|
| Historical correction | Complete | No new work without a demonstrated material error |
| Extracted variables | Finalize implementation → full extraction → six-file package → existing human validation → release | Identify compatible route inputs and the remaining production integration work |
| 2026 collection | Completed storage correction and monthly schedule → capacity decision → full collection → final outputs | Refresh available capacity against the current monthly forecast; enable full mode only when the existing storage check passes |

The documentation request approves these instructions. Existing task approvals still govern execution.
Full-corpus extraction, production integration, new acquisition tranches, costs, and publication require the authority specified in their task records.
Do not ask again for actions already approved. Prepare a concrete proposal before requesting new authority.

## Current evidence and settled decisions

**Historical correction:** Public Dataverse version 3.0 matches the local manifest and all eight data-file checksums.
Hanno closed this track. Use the [release README](docs/RELEASE_README.md) for the population and files.
The old public project page is a follow-up, not a release blocker.

**Extracted variables:** The scope includes issue statements, six biography fields, and endorsement occurrences with exact source evidence.
The [codebook](docs/candidate_extraction_extension_codebook.md) controls the schema; its opening status is stale.
The [14-row adjudication](quality_reports/candidate_endorsement_line_ledger_human_a3_21447139.json) is complete and authorizes its associated Stage B.
The September 14 [development evaluation](quality_reports/plans/candidate_endorsement_pipeline_progress_2026-09-14.md) and [AI-coded review](quality_reports/session_logs/2026-09-14_independent40-ai-coded-review.md) are complete.
Reuse these results. The context-radius experiment was not promoted. AI-coded labels do not establish human accuracy.
The six-file package and existing independent human validation remain unfinished.
The [approved extraction plan](quality_reports/plans/2026-08-24_corrective-release-and-candidate-extraction-v2.md) retains the validation criteria and workload limits.

**2026 collection:** The [approved plan](quality_reports/plans/2026-09-14_campaign-websites-pre-post-primary.md) controls the roster, cadence, date windows, and endpoint.
The final September 15 decision is live HTML/text every 30 days, plus the local day before and after each applicable primary or runoff.
A successful targeted copy resets the regular interval. Confirmed nonparticipants receive regular monthly copies only.
Roster and URL refreshes remain weekly. The daily automation checks due work; the earlier daily election window is superseded.
Wayback comparison windows and the collection endpoint remain as approved.
The [current monthly forecast](data/collection_2026/monthly_cadence_20260915/pilot_forecast.json), generated at 17:04 UTC, estimates 27.78 GiB before the separate 2× allowance.
The total with that allowance is 55.56 GiB, plus the existing 5 GiB headroom requirement for the storage check.
It projects 11,512 remaining live site copies across 3,674 candidates. Full mode remains disabled; no new storage is approved.
The [storage migration](data/collection_2026/storage_correction_20260915/storage_migration.json) is complete and preserves all 881 page observations and raw index responses.
The working index fell from 373,066 to 23,148 rows after non-HTML exclusions and deduplication. The inventory export is compressed.
The earlier 400/800 GiB proposals are withdrawn. Reuse the completed correction and monthly schedule.
The [session log](quality_reports/session_logs/2026-09-14_campaign-websites-pre-post-primary.md) reports deployment and 164 passing focused and adjacent tests.

## Operational state at the last check

The 2026 rows incorporate the later September 15 session log, monthly forecast, and saved automation configuration.
Check current remote state before operational changes.
Read `~/.codex/remote_machines.md` before connecting to any remote host.

| Work | Location or owner | Last observed state |
|---|---|---|
| 2026 runtime on the droplet | `/mnt/mayoral_wayback_full_snapshots/candidate_2026/runtime`; data in `../data` | Session log records PID 1339730 finished; last request 15:27:42 UTC; monthly schedule deployed; full mode disabled |
| 2026 daily checks | Codex heartbeat `collect-2026-campaign-websites`, 09:00 America/Los_Angeles | Active; saved prompt uses monthly copies plus targeted pre/post copies; no Claude scheduler created |

Preserve the existing scheduler until Hanno authorizes its replacement. Establish one owner before another scheduled collector starts.
The September 15 due pass already completed. Its eight retryable items do not justify repeated same-day launches.
Keep large raw files on the droplet. Copy only the necessary review outputs locally.

## Repository transfer and task history

The preparation used the existing `main` checkout at `110ea6d`, with extensive unrelated uncommitted work.
Inspect the current working tree before edits. Preserve unfamiliar files and overlapping work.
The root `CLAUDE.md` is now eligible for version control through a narrow `.gitignore` exception.
These handoff changes are uncommitted. A fresh clone will need a scoped commit or an explicit copy of the required files and artifacts.
A Claude session opened in this working directory can read the prepared files immediately.

| Earlier Codex task | Task ID |
|---|---|
| 2026 candidates | `01a0a10e-0f0e-7662-85da-553487bb25be` |
| Candidate endorsement coding | `01a07294-2a04-7c91-8021-b3ffbd5e49e3` |

Use repository artifacts to resume. Retrieve old conversations only when a needed decision is absent from those records.
The workflow draws on [OpenAI's completion criteria](https://learn.chatgpt.com/guides/best-practices) and [Anthropic's incremental progress guidance](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents).
The two-attempt repair limit is Hanno's adopted project rule, not a vendor-prescribed threshold.
