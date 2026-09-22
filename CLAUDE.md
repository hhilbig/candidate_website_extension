# Candidate Website Extension

Finish the approved data releases using the checklist in `docs/status.md`.
Historical House/Senate correction is complete. Do not reopen it without a demonstrated material error.
Do not recollect House 2002–2016. Governor races remain outside scope.

## Start and resume

1. Read `docs/status.md` for the current step and recorded blockers.
2. Read `CLAUDE_CODE_HANDOFF.md` when taking over this repository or a different project track.
3. Check `git status --short --branch` and inspect the current inputs and outputs for the selected step.
4. Read only the linked definitions, scripts, and prior decisions needed for that step.
5. Before remote work, check the exact running jobs, durable attempt records, and existing scheduler ownership.

The status file controls work order. Current artifacts establish completion.
Older plans retain approved research definitions and resource limits; their historical task lists do not create new work.
Use relevant sections of `KNOWLEDGE_BASE.md` and `MEMORY.md` for substantive decisions and known failure modes.

## Execute the release checklist

- Identify the active step, required output, relevant checks, and authority before substantial work.
- For a large step, choose a bounded batch with a measurable output and the existing resource limits.
- Keep deliverables and scope fixed. Choose routine implementation details autonomously.
- Reuse compatible verified outputs and completed adjudications. Do not repeat development work without evidence of a defect.
- Continue already-authorized work without requesting another plan approval or creating another plan document.
- Mark a step complete only after inspecting its outputs and passing its required checks.
- After those checks pass, record the evidence and proceed to the next authorized step.
- Rerun checks when changes or new evidence invalidate the earlier result.
- Record optional improvements as deferred items. Do not implement them during release work.

## Deviations and failed attempts

Additional work must address a demonstrated blocker to a listed release requirement.
Record the defect, affected requirement, smallest proposed correction, and condition for returning to the checklist in `docs/status.md`.
Do not add variables, model comparisons, review samples, broad refactoring, or new release criteria without approval.

After two unsuccessful repair attempts on the same blocker, stop that repair loop and report the evidence and decision needed.
An attempt consists of a substantive correction followed by its relevant verification.
Keep the attempt count across sessions, compaction, and delegation. A new task name does not reset it.
Existing network retry limits still control individual requests; this rule limits repeated repair strategies.
Continue independent approved work while a blocker awaits a decision.

## Persistent progress

Update the active-work table in `docs/status.md` after a material result and before a handoff or context reset.
Record the step or batch, output paths, checks and results, blocker, failed repair count, and next authorized action.
Preserve source/model versions and exact remote job IDs when relevant.
Keep the current summary concise; link existing detailed reports instead of copying their chronology.
A fresh session must resume from this evidence instead of repeating completed exploration.

## Progress reports to Hanno

Tell Hanno where the work stands against the final goal, not only what the last step did.
Give this report at the end of a work session, after a step or batch completes, and before a handoff.
Also give it when he asks for status.

Report these four items:

1. The share of the overall release work that is complete, as a percent.
2. The share that each open track contributes, so the percent is auditable.
3. The estimated remaining work time, split into agent time and Hanno's own time.
4. The estimated remaining wall-clock time when a queue, a scrape, or a scheduled job controls it.

Rules for the estimate:

- Count the tracks in the `docs/status.md` table. Weight each track by its remaining steps.
- Base the estimate on measured rates from this project, such as minutes per chunk or captures per hour.
- Name the largest single source of uncertainty in one sentence.
- Separate the work that waits on a decision from the work that waits on a machine.
- State the number as approximate. Do not give a false precision such as 63.5 percent.
- Record the same figures in the `Resume here` section of `docs/status.md`.

## Research and operational boundaries

- Preserve approved populations, coding definitions, missingness rules, validation criteria, and human workloads.
- Keep AI-coded labels distinct from human validation. Never relabel failures or missing sources as measured zeros.
- Obtain explicit authority for full-corpus inference and new acquisition tranches where the existing plan requires it.
- Estimate costs and obtain explicit approval before any paid API or cloud purchase.
- Prepare reviewable release files before requesting publication approval. Publish only after explicit approval.
- Never edit paper `.tex` without approval of the exact changes. Never send email without approval of recipients and final text.
- Preserve unfamiliar changes. Stage or commit only the requested, verified scope.
- Read `~/.codex/remote_machines.md` before remote-host work.
- Read the `hive-llm` skill before Hive work. Use the established open-weight model family for research extraction and repairs.
- Read the `wayback-scraping` skill before archive acquisition. Preserve each runner's approved worker count, limits, and retry history.
- Use one worker for the legacy Wayback scraper. Do not launch overlapping collectors for the same queue.
- Never compress active scrape outputs or run overlapping compression commands on the same files.
- Confirm scheduler ownership before replacing a monitor or launching scheduled work. The handoff alone transfers no automation.

## Entry points and checks

### Settled 2026 collection decision

Collect live HTML/text every 30 days, plus the local day before and after each applicable primary or runoff.
A successful targeted copy resets the 30-day interval. Confirmed nonparticipants receive regular copies only.
Refresh rosters and URLs weekly. The daily automation checks due work; it does not collect every website daily.
Preserve the existing 30-day Wayback comparison windows and election-day exclusions.
Use `docs/collection_2026.md` and its current forecast. The storage correction is complete; the 400/800 GiB proposals are withdrawn.
Resume from the monthly decision, not the superseded weekly schedule or daily election window.

### Commands

Run commands from the repository root using the task's established environment.
Check script arguments and current input manifests before any build or collection command.

| Work | Existing entry points |
|---|---|
| Extracted variables | `scripts/build_candidate_extraction_release.py`, `scripts/validate_candidate_extraction_release.py`, `tests/test_candidate_extraction_release.py` |
| Mayoral population | `scripts/build_mayoral_top_two_coverage.py`, `tests/test_build_mayoral_top_two_coverage.py` |
| Mayoral acquisition | `docs/mayoral_url_discovery.md` identifies the current approved runners and queues |
| 2026 collection | `scripts/collect_2026.py`, `tests/test_collection_2026_*.py` |
| Historical reproduction, only if required | `README.md` gives the locked environment, build, and validation commands |

Run the relevant existing tests with `python -m pytest <test files>` in that environment.
Use the real output validator and inspect generated artifacts when tests alone do not establish completion.

## Writing style

Write all responses in this project in **ASD-STE100 Simplified Technical
English**. Apply these rules:

- Write short sentences. Use a maximum of 20 words in an instruction and 25
  words in a descriptive sentence.
- Use the active voice. Name the agent of each action.
- Use one word for one meaning. Do not change the term for the same thing.
- Write one instruction in one sentence.
- Use simple verb tenses: present, past, and future. Do not use the perfect
  or the progressive tense if a simple tense is sufficient.
- Do not use the `-ing` form as a noun.
- Keep the articles `a`, `an`, and `the`. Do not remove them.
- Use a maximum of three nouns together. Break longer noun clusters apart.
- Write a maximum of six sentences in a paragraph.
- Do not use slang, jargon, or idioms.

This rule applies to chat responses, reports, session logs, and code comments.
It does not apply to manuscript prose, which the `hanno-voice` rules control.
