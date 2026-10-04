# Session log: Endorsement pipeline progress, 2026-09-14

The local diagnostic, evaluator correction, context inputs, and human-review app are complete. Hive job 23300739 now runs on one Blackwell GPU. The model loaded successfully. Its effect on extraction remains unknown. The goal remains active.

Session type: implementation, debugging, and analysis.
Plan: `quality_reports/plans/candidate_endorsement_pipeline_progress_2026-09-14.md`.

## What I did

I compared the existing extraction on 40 complete source pages with the separate Codex AI review. I kept AI-accepted and AI-uncertain occurrences separate. Exact comparisons require matching name coordinates and entity type. Normalized comparisons use the existing name normalization and count repeated occurrences within each page.

The authoritative source task remains `build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json`, SHA-256 `c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea`. The reference remains `build/private_candidate_endorsement_codex_ai_review_20260914a/ai_reviewed_results_v2.json`, SHA-256 `56abd8a8f34f2ab3f13498b838792f714784430747a7405d08810e6ca4887704`. Neither file was changed.

I added `scripts/diagnose_candidate_endorsement_codex_review.py` and its tests. The private outputs are `report.json`, `disagreements.json`, and `source_trace.json` in `build/private_candidate_endorsement_codex_diagnostic_20260914a/`. The trace authenticates the original Stage-A line classifications and Stage-B outputs before assigning each omission to a pipeline point.

I corrected `scripts/evaluate_candidate_endorsement_round4_regression.py` to preserve independently valid target slots when another slot in the same batch fails. The evaluator uses the existing app's slot sanitizer and source validation. It records unresolved slots and optional-field nulling. It does not rewrite model names or add endorsements.

Three existing Codex agents handled bounded independent work: the review-app adapter, read-only context implementation, and source-date audit followed by the independent-pool census. I inspected their outputs and ran the integrated checks.

The opt-in context implementation is in `scripts/build_candidate_endorsement_round4_development_inputs.py` and `scripts/hive/extract_candidate_endorsement_round4_development_vllm.py`. Radius zero preserves the old contracts. Radius ten adds adjacent source lines and Stage-A support headings. Names must still originate in their assigned target blocks. Full source text authenticates context projections but is omitted from the model prompt.

The new inputs are `build/hive_candidate_extraction/v2_1_endorsement_recall40_codex_context_stage_b_20260914a/`: 40 pages, 498 batches, 1,941 targets. Manifest SHA-256: `e5fd86f9a5353202d71318439497ee001e00962525ced55be1dda67df4ddbe92`. Generation SHA-256: `92e342e4371d85847f4f43639c0a78d8ec98ee575d013c76a1cadda6d6f51ac7`.

I copied only the required code and new inputs into the isolated Hive directory `/home/hhilbig/endorsement_context_development_20260914a`. Job 23300739 uses one Blackwell GPU, a one-hour limit, the existing frozen runtime, and the same Qwen3.6-27B-FP8 snapshot `e89b16ebf1988b3d6befa7de50abc2d76f26eb09`. Old outputs remain untouched. No separately billed API, credit purchase, or usage reset was used.

## What I verified

| Motivation | Check | Result |
| --- | --- | --- |
| Incorrect reference or lost occurrences | Source identities, spans, status partitions, and original raw-run authentication | All 40 pages reconciled; 1,877 predictions, 1,093 AI-accepted references, 867 uncertain references |
| Valid siblings discarded with a failed batch | Authenticated slot replay plus regression test | 39 records retained from 14 partially failed batches; the corrected evaluator reproduces all 1,877 seed occurrences |
| Context expands extraction ownership | Source projections, line identities, context hashes, forged-ID and context-only-name tests | Passed; context names remain ineligible outside the owned blocks |
| Prompts require truncation | Exact pinned tokenizer applied to all 498 batches | Largest prompt 14,720 tokens; minimum margin 1,664 tokens after reserving 16,384 output tokens; required margin 256 |
| App silently accepts AI seeds as human labels | App startup, pending blocks, export gate, and isolated synthetic export/import | All 1,960 initial rows unconfirmed; all 88 blocks pending; export blocked until explicit review |
| Browser selection and keyboard interactions fail | Existing Playwright checker on the separate local app | Selection, autosave, keyboard confirmation, split, block review, export/import passed; zero console or page errors |
| Regressions across the pipeline | Nine-file relevant pytest suite | 107 tests passed in 40.66 seconds |
| Syntax or whitespace errors | Python compilation, JavaScript syntax check, and `git diff --check` | Passed |

The checker initially failed because it tried to focus the document body while the source panel retained focus, and assumed exactly two children after splitting. I corrected the checker to focus the coding control and compare the split count with its initial count. I did not change the renderer's focus policy. Synthetic confirmations existed only in the isolated browser session. I cleared its local storage, reloaded the app, and inspected `output/playwright/codex-review-next-clean.png` showing the clean pending state.

The app is `build/private_candidate_endorsement_codex_human_review_20260914a/index.html`, served locally at `http://127.0.0.1:8781/`. Its task has 1,960 seeds, 71 AI source issues, and 17 original unresolved blocks. The renderer currently calls all 88 pending blocks “failed model slots”; the instructions explain that the label also includes AI source issues. No human review export was created. These checks establish app mechanics, not completed human coding.

## What I found

The existing extraction agrees with most AI-accepted occurrences, but omissions remain. It matches 1,028 of 1,093 accepted occurrences exactly (94.05%) and 766 uncertain occurrences. Sixty-five accepted and 101 uncertain reference occurrences are unmatched. Eighty-three predictions have no exact AI-reference match. Normalized matching finds 1,031 accepted matches, leaving 62 accepted omissions and 80 unmatched predictions. These are provisional AI agreement rates, not human precision or recall.

The 65 exact accepted omissions trace to 34 rejected Stage-B targets, 17 occurrences without targets at their name coordinates, eight nonrepairable Stage-B targets, four boundary or compound-name differences, and two targets that omitted an occurrence. Read-only context can inform target decisions; it cannot recover occurrences that never received Stage-B targets. The context rerun must be evaluated before claiming improvement.

The source audit recovered metadata for 300 archived URLs covering the 40 pages. Eleven pages have only captures after their cycle's general-election day, ten include captures on both sides, and nineteen have only captures before or on that day. No page was excluded. Capture dates do not establish endorsement dates, so a measurement window remains a substantive decision.

The audit is `build/private_candidate_endorsement_codex_source_audit_20260914a/source_context_date_audit.json`. The audit's programmed election dates were subsequently checked against official FEC sources for [2018](https://www.fec.gov/updates/tip2018-handy-election-day-links/), [2020](https://www.fec.gov/updates/tip2020_election-day-resources/), [2022](https://www.fec.gov/updates/tip-2022-election-day-tuesday-november-8/), and [2024](https://www.fec.gov/updates/ao-2024-12/). The separate `election_calendar_verification.json` preserves that follow-up without changing the audit's original provenance. Repository inventories did not find original HTML for these pages; this finding does not cover other machines.

The remaining independent pool contains 1,622 pages, 1,579 source components, and 772 candidate cycles. It includes 1,013 Democratic and 609 Republican pages. M means a multi-window page with positive legacy extraction; Z means a page with zero legacy extraction. These are legacy classifications, not validated endorsement statuses. M and Z contain 1,403 and 219 pages respectively. Additional exclusions remove 142 of 1,764 baseline pages (8.05%): the current pages, their candidate cycles, and the complete linked-source closure. All prior 802 forbidden source components remain excluded. No forbidden source was reconstructed or emitted, no sample was drawn, and no quotas were imposed. The aggregate result is `build/private_candidate_endorsement_codex_independent_pool_20260914a/pool_census.json`. The next selector needs a revised contract and joint feasibility check before an independent sample can be claimed.

## Decisions

All new diagnostics and seeds remain AI-reviewed, not human truth. Human-truth, production, full-expansion, and release authorizations remain false. I did not alter the 40-page source sample, coding definitions, or uncertain reference statuses.

The evaluator fix addresses counting, not extraction. The context rerun is a bounded development comparison on the existing pages. It is not a full-corpus submission. Missing headings, shared surnames, compound organization names, and temporal exclusions were not resolved by imposing new rules.

## Open threads

Retrieve and authenticate job 23300739 outputs, then compare them with the pinned AI reference and old extraction. Preserve technical failures rather than treating them as zero endorsements. Decide whether further target-construction changes are justified by that evidence.

Revise the independent-sample contract and verify feasible quotas without opening the sealed reserve. Resolve coding-definition and archive-window choices before changing those rules. Complete human review later before human-validated accuracy claims. Full-corpus work, production integration, and release need separate approval.

## Memory updates

No personal memory or knowledge-base files were edited. Durable decisions are recorded in the approved plan and this session log.

## Continuation: Authenticated comparison

The previous goal turn made progress. This continuation confirmed job 23300739 as live, first pending and then running. Slurm and the job log both confirmed its state. No duplicate job was submitted.

I extended `scripts/diagnose_candidate_endorsement_codex_review.py` to evaluate the context run against the same AI reference. The comparison requires a recorded output inventory hash. It authenticates both model stages and converts only valid slots with the existing source checks. It preserves unresolved blocks.

The comparison rejects changes to page identities, source text, source hashes, character counts, or metadata. It also rejects human confirmations. Exact and normalized comparisons retain separate counts for accepted matches, uncertain matches, and unmatched predictions.

I replayed the old authenticated run through the new path. The replay reproduced all 40 pages, 1,877 predictions, and 17 unresolved blocks. Every count difference was zero. The artifact is `build/private_candidate_endorsement_codex_diagnostic_20260914a/paired_comparison_old_run_replay.json`. This artifact validates the comparison path; it is not a result from the context run.

The focused suite passed 20 tests. The integrated nine-file suite passed 116 tests in 37.57 seconds. Python compilation and `git diff --check` passed. The original diagnostic command also executed and retained its existing results.

After the context run finishes, copy its complete outputs into a new local directory. Authenticate its inventory before evaluation. Use the diagnostic's `--context-run-output-dir` and `--expected-output-inventory-sha256` options. The default result directory is `build/private_candidate_endorsement_codex_context_evaluation_20260914a/`. The command writes a paired comparison, private disagreements, and an unconfirmed provisional draft. It does not write human labels or update production.

## Continuation: Partial outputs and visibility audit

The next continuation confirmed job 23300739 as running. The first completed file contains 60 responses. I copied it into `build/hive_candidate_extraction/v2_1_endorsement_recall40_codex_context_stage_b_outputs_20260914a/`.

The initial transfer copied the file but reported a directory-permission error when it tried to preserve remote attributes. Future transfers use content-only options. This error did not change the copied bytes. I replayed every response with the existing runner against the authenticated input chunk and generation configuration. All 60 wrappers matched, and all 60 passed technical checks. The attestation is `build/private_candidate_endorsement_codex_diagnostic_20260914a/context_first_chunk_attestation.json`. Complete metadata and all nine output files remain necessary for final authentication.

I inspected all 17 AI-accepted occurrences that had no Stage-B targets. Seven fall outside the deterministic visibility boundary. Five received Stage-A nonrecord labels. Five received Stage-A continuation labels but no owned target. The uncertain omissions comprise 14 nonrecord occurrences, two next-continuation occurrences, and one occurrence with both continuation directions.

The seven visibility exclusions occur on parent 25, before the recognized “Organizational Endorsements” heading. The source already states “support Joe for Congress” before the congressional list. The existing visibility expression recognizes endorsement and supporter forms, but not this support statement. Government-official categories remain allowed; this is a visibility boundary, not a rule excluding politicians.

The private result is `build/private_candidate_endorsement_codex_diagnostic_20260914a/untargeted_role_audit.json`. It preserves the trace hash, source-task hash, code hash, and separate accepted and uncertain counts. This analysis explains why the current context rerun cannot recover those occurrences. It reuses Stage-A targets and does not change the earlier boundary. I did not broaden visibility or modify labels.

I asked whether the next independent sample may reuse the prior 40-page design with 32 legacy-positive multi-window pages and eight legacy-zero pages. The proposed exclusions preserve prior samples, reserved pages, and current candidate cycles. No new sample or seed was selected while that decision remains pending. The current rerun does not depend on the answer.

## Continuation: Complete context comparison

The context change improved AI-reference agreement slightly but also increased unmatched predictions. We compare the old extraction and context extraction on the same 40 pages against unchanged AI labels. This comparison does not establish human accuracy.

Job 23300739 completed all 498 responses in 17 minutes and 53 seconds. Slurm recorded FAILED with exit code 2. The runner deliberately returns 2 when technical failures remain. The log reports 23 technical failures before engine teardown. All nine output files and complete metadata passed remote authentication. Local evaluation separately authenticated the unchanged Stage-A output and new Stage-B output.

The remote receipt is `build/private_candidate_endorsement_codex_diagnostic_20260914a/context_origin_attestation.json`. The output inventory hash is `1170928ae8b1019dcf9ada0b2dd823d89a960690ec110058fe475b703a2c06d3`. The local comparison verified identical source identities, text, hashes, character counts, and metadata. No failed response was retried or counted as a zero-endorsement page.

The context extraction contains 1,903 provisional occurrences, compared with 1,877 before. Normalized accepted matches rose from 1,031 to 1,042 of 1,093. Unmatched predictions rose from 80 to 90. Exact accepted matches rose from 1,028 to 1,031; unmatched predictions rose from 83 to 105. Both versions retain 17 unresolved blocks. These results do not justify promotion of the context version.

The paired result is `build/private_candidate_endorsement_codex_context_evaluation_20260914a/paired_comparison.json`. The same directory contains the private disagreements, report, and unconfirmed provisional draft. I inspected their counts, conversion issues, and non-trivial sizes. The converter retained 23 failed-batch audit entries, 17 nonrepairable targets, three unclear targets, and 43 unowned continuations. It nullified 14 unsupported optional fields. All outputs remain AI-derived and unconfirmed by a human.

The two relevant comparison test files passed 28 tests. The previous integrated suite passed 116 tests. The current result preserves the failed targets and the original reference. Further corrections require evidence from these disagreements, not repeated generation until a response passes.

I interpreted the user's continuation as approval of the proposed independent sample with 32 legacy-positive pages and eight legacy-zero pages. I stated this interpretation before action. A separate Codex agent prepares the sample under the previous constraints with a fresh seed. It preserves prior exclusions, sealed reserves, and current candidate-cycle and source-component exclusions. No new reserve, changed quota, or full-corpus run is approved.

## Continuation: Independent disagreement audit

An independent Codex audit confirms that the context result remains mixed. We compare individual gains and losses against the same AI reference and inspect the complete source pages. The audit found 24 normalized accepted gains and 13 losses. Gains concentrate on Crow, Bradshaw, and Kunkel. The net improvement does not eliminate genuine omissions.

Eight accepted names use separate spans for ordinary name words in the context output. Four occur on Campbell and four on Cotter Smasal. These representation changes explain the difference between the net normalized gain and net exact gain. They do not establish eight additional recovered entities.

The audit decomposed the 22 additional exact unmatched predictions. Seventeen involve boundary or compound identities, and five have no name-reference match. The number of repeated-coordinate disagreements did not change. The ten additional normalized unmatched predictions also include boundary and definition differences. They are not ten established false endorsements.

I inspected the authoritative Giles and Kamlager source pages to verify three semantic errors. Rachel Jones and Cory McGarr appear in the committee's slate of candidates under the state legislative section. The source does not show them endorsing Giles. The context output also treats “30 for 30 Donors” as an organization, although the source uses it as a donor-list heading. Direction and record-role errors remain relevant even when more context is visible.

The current eight-file pipeline suite passed 107 tests in 12.09 seconds. I reconstructed the human-review task from the pinned inputs and compared its exact bytes with the saved task. Its HTML also matches the current renderer and recorded hash. All 40 pages, 1,960 unconfirmed seeds, and 88 pending blocks remain intact. These checks prove package consistency, not human completion.

The independent sample selector now uses sample round four and seed 20263827. The agent preserves the previous constraints and canonical optimization objectives. Six focused tests passed; the exact assignment still runs at this checkpoint. After it finishes, authenticate the frozen assignment before constructing the selected source package and Stage-A inputs. No model run or new human labels are authorized by an input manifest alone.

## Continuation: Frozen independent assignment

The canonical selector froze a new independent 40-page assignment under the approved previous constraints. The final assignment contains 32 legacy-positive pages and eight legacy-zero pages across 38 source components. It includes 22 Democratic and 18 Republican pages. No candidate cycle contributes more than two pages. All feature minima pass, the optimization gap equals zero, and excluded-source overlap equals zero.

The preliminary feasibility witness contained 39 components. The canonical selector subsequently chose 38 components under the same constraints. The final assignment, not that preliminary witness, controls the source package and future inputs.

The folder is `build/private_candidate_endorsement_independent40_selection_20260914a/`. The assignment is `selection.json`, with SHA-256 `33e81123f6318e2cbcbd3f3f8cd02899111d574de3911a314495073cf45257f6`. The source package is `selected_sources.json`, with SHA-256 `f487dd6137f4235abfeae15333684aa804687d9666f2c8112623fb78ff44cf3e`. The package binds the assignment hash and contains only its 40 selected source inventories. It contains no model labels or human confirmations.

I ran all seven selector tests after the artifacts existed. All passed in 6.85 seconds, including source-package closure and the exclusion-before-source-reconstruction check. The prior sample and sealed reserve remain excluded. The agent now adds explicit sample-round-four metadata support for executable Stage-A inputs. It preserves the old default, prompts, schemas, and source ownership. No new inference started.

## Continuation: Independent input integration

The independent Stage-A artifact contains 292 batches across five files. Its manifest hash is `fd096373714c3188d72892c0a02e550a0dd5cb7eccceac9295935893ceb700b0`. I authenticated all 40 full source inventories against the selected package and original export. The runner accepted sample round four and its frozen parent-to-source binding.

The agents added explicit sample-round-four metadata support. Stage B preserves the authenticated Stage-A round and independent bindings. The review converter also preserves that round. A synthetic Stage-A-to-Stage-B test passed without a model run. The original default remains round three. The new adapter suite passed 17 tests; the integrated ten-file suite passed 142 tests in 34.24 seconds.

Those tests did not prove complete integration. I inspected the actual new manifest and found incompatible provenance fields in the review source loader. A direct call to `_validate_source_manifest` raised `fresh Stage-A input provenance differs`. The new adapter omitted three required source-provenance hashes and emitted a different inventory field. The agents now align those fields and add a test with the actual adapter output.

The first independent input artifact remains unchanged for audit. A corrected artifact will use revision b. The frozen assignment and selected source package remain authoritative. No inference or human confirmation occurred. Complete review conversion remains unverified until the actual-manifest test passes.

## Continuation: Corrected independent inputs and bounded run

Revision b fixes the actual source-provenance mismatch. Its manifest hash is `0a962b41e022a68eebbffe01c3f406cf41bbc55b4710251102ae6ba92f79f1d2`. Both the runner and review source loader accept it. I independently loaded all 40 pages, 174,852 source characters, and 4,764 physical lines. The new integration tests compare all consumed source fields against the separately authenticated selected package. Revision a remains unchanged; all five chunk files are byte-identical across revisions.

The integrated eleven-file suite passed 150 tests in 40.91 seconds. Compilation and whitespace checks passed. The current comparison command authenticated and reproduced the completed context result. The original human-review task and HTML still match their saved exact bytes. These checks do not establish real human confirmations or new independent extraction results.

I added `scripts/check_candidate_endorsement_independent40_token_budget.py` and five tests. All five tests passed. The checker uses only the exact cached tokenizer, never a model or API. On Hive, all 292 prompts passed. The largest prompt contains 10,200 tokens. The minimum margin is 20,520 tokens after the 2,048-token output reservation; the required margin is 256. The local receipt is `build/private_candidate_endorsement_independent40_selection_20260914a/token_budget.json`. Its checker hash matches the local script.

The mixed development result motivates a bounded independent test of generalization. The user approved continued work and the fresh sample under the active goal. This run preserves the sample, coding definitions, frozen prompts, and model family. It does not constitute full-corpus approval. The selection artifact's earlier submission flag records its creation checkpoint; it does not grant authority by itself.

I checked the live Hive queue, GPU types, drains, reservations, and scheduler feasibility. The scheduler accepted one Blackwell GPU, two CPUs, 96 GB memory, and a 45-minute limit. No reservation blocks the run. Two unrelated account jobs remain untouched. The new run uses isolated paths, existing free resources, an offline locked runtime, and the pinned Qwen snapshot. No paid API or cloud service is involved.

Slurm accepted job 23312106 for Stage A. The generation hash is `1786b025f6e5075bd03c7680ad900f50287799548ee83f5a7ac7c73797d6b153`. The submission receipt is `build/private_candidate_endorsement_independent40_selection_20260914a/stage_a_submission.json`. Complete Stage-A outputs still require authentication before Stage B. No independent result is claimed at submission.

## Continuation: Blind review and output authentication

The independent extraction and blind AI review are underway. We will compare model predictions with separate source-based Codex judgments on the same 40 new pages. The review package contains complete source texts and coding instructions, but no prediction seeds. Its manifest hash is `3a9261150894e753993719865e18c93c8b60085bc18d233473e073768d33d0d2`. Its instruction hash is `2b8acca3bc602ff4e059548e003906db3af33f438ce1f750a241f5e5160321a5`. I authenticated all 40 prepared source files.

Two `gpt-5.6-sol` agents review parents 1–20 and 21–40 independently of model predictions. Each must inspect complete text, preserve exact name spans, and record ambiguous coding separately. Their outputs identify the actual reviewer model and cannot count as human review. No completed independent reference is claimed at dispatch.

The origin checker requires the exact revision-b input, job 23312106, all 292 output rows, and all 40 parents. It preserves technical failures. I uploaded the checker to the isolated Hive project. Authentication remains pending until the job finishes.

The token-budget checker now also supports Stage B's separate output reservation and rejects empty inputs. The earlier Stage-A receipt retains the checker hash used at its creation. A future Stage-B receipt must authenticate its own input and current checker. The combined checker and review-helper suite passed 24 tests in 3.41 seconds.

At the latest scheduler check, job 23312106 was running after 8 minutes 15 seconds. Four output files existed; the complete metadata did not yet exist. No restart, new inference submission, promotion, or human confirmation occurred. Complete origin authentication must precede Stage-B construction.

The job subsequently completed with exit zero after 8 minutes 37 seconds. Origin authentication confirmed all 292 outputs with zero technical failures. The output inventory hash is `0c46b2033834a04423e2314671e176b15cc97aa2171027c5f8e15f5d2a850bbc`. I copied all five output files, metadata, and origin receipt into separate local paths.

Local Stage-B construction reauthenticated that exact origin inventory. It produced 639 batches, 2,512 targets, and 11 input files for all 40 pages. The manifest hash is `4f6113518037916c014c51a26ff7927b151915e2d0eaa4a5af5b493e13e61148`. The handoff preserves one unclear line and 94 unowned continuations. These counts are unresolved extraction conditions, not confirmed missing endorsers. The context radius remains zero because the prior context experiment was not promoted. Stage-B inference remains unsubmitted until its exact token-budget check passes.

## Continuation: Independent Stage-B submission

Stage B passed the exact cached-tokenizer check on all 639 prompts. The largest prompt contains 8,274 tokens. Its minimum margin is 8,110 tokens after the 16,384-token output reservation. The required margin is 256. The receipt is `build/private_candidate_endorsement_independent40_selection_20260914a/stage_b_token_budget.json`. Its checker hash matches the current local script.

I reauthenticated the complete local Stage-B input and checked the live Hive queue, GPU inventory, drains, and reservations. No reservation exists. The scheduler's test-only check accepted one Blackwell GPU, two CPUs, 96 GB, and 45 minutes on high. Test-only job 23312492 did not run inference. The two unrelated account jobs remain untouched.

Slurm accepted actual job 23312502. A direct scheduler check confirmed it running at 13 seconds. The remote wrapper and runner hashes match the inspected local files. The frozen generation hash is `b59c797550559e8907230d9c0e44a349bb778080661542b355570da8909fb950`. The submission receipt is `build/private_candidate_endorsement_independent40_selection_20260914a/stage_b_submission.json`. This bounded run uses the same free Hive model and prompts, with context radius zero.

No complete Stage-B output or independent agreement result is claimed at submission. Next, authenticate all output files at origin and locally. Freeze the completed blind AI review before comparing it with predictions. Any seeded audit must remain a separately identified review pass. Human-review flags remain false.

## Continuation: Completed blind reference

The two blind agents inspected all 40 complete source texts and produced explicit AI-only judgments. I authenticated the prepared source inventory and independently validated every review file. The frozen reference contains 2,372 accepted identity occurrences and 209 uncertain occurrences, with 47 source-spanned uncertainty notes. Seven pages contain neither named endorsers nor uncertainty. A further page has an unattributed statement and no named identity; it is not counted as an AI zero.

The frozen reference is `build/private_candidate_endorsement_independent40_codex_review_20260914a/blind_reviewed_reference.json`. Its hash is `8d6f5dbad5fbd2dc392bc1ad45f0048bad5fac4d9552d5f677e08d293cebcb79`. It binds all 40 review-file hashes, both source authority hashes, exact spans, and actual `gpt-5.6-sol` reviewer identities. Neither agent inspected model seeds or the other review pass. The reference remains provisional AI evidence, not human truth.

The helper now rejects child human-confirmation flags and missing reviewer-model provenance. Its freeze command requires exactly 40 regular review files and the assigned reviewer identities. The helper suite passed 17 tests in 3.36 seconds. The separate Stage-B origin checker passed seven tests. It authenticates both stages, explicitly propagates sample round four, checks all 639 responses and job 23312502, and preserves technical failures. Compilation and whitespace checks passed.

A separate strong Codex agent now audits complete texts and blind judgments for parents 7, 12, 15, 20, 31, 36, and 40. The audit is read-only and blind to predictions. Any supported correction must preserve the original frozen reference and record separate source evidence. At the latest scheduler check, Stage B remained running after 3 minutes 4 seconds and was warming model kernels. No independent prediction comparison exists yet.

## Continuation: Source audit and comparison preparation

The separate source audit found one accepted organization attribution that needs reconsideration. Parent 31 identifies Advanced Tactical Armament Concepts, LLC in Lee Ploszaj's CEO heading. The endorsement statement explicitly acts on behalf of HPR Ammunition. Separate endorsement by Advanced Tactical is not established. I inspected source characters 7,380–8,110 and confirmed the accepted name coordinates `[7550,7590]`. The proposed correction changes only that organization's status to uncertain. The frozen blind reference remains unchanged for the initial comparison.

The audit also identified calibration questions. Parent 15 retains 143 date-uncertain identities despite explicit endorsement direction. Its old publication date, later office titles, and footer leave dates unresolved. Three Mike Huckabee occurrences on parent 31 may receive support from a section heading, but that proposal is less certain. No date exclusion or automatic status change was applied. Parents 7, 12, 20, 36, and 40 had no concrete error in the bounded audit.

The audit is `build/private_candidate_endorsement_independent40_codex_review_20260914a/blind_semantic_audit.json`. I authenticated its reference and source-manifest hashes and checked the supported correction's name and evidence spans. It distinguishes primary source checks from an unverified lower-confidence proposal. All human and promotion flags remain false.

A strong Codex agent now implements the independent comparison adapter and regression tests. It must consume the actual standard converter task, not require an invented task schema. The converter's task has no top-level sample-round field. The pinned Stage-A source manifest establishes round four. The comparison must verify all 40 source identities and common metadata, retain unresolved blocks, and separate accepted, uncertain, and unmatched occurrences. The original diagnostic's pure multiset helpers remain reusable; its old source pins do not control this independent sample.

At the latest scheduler check, job 23312502 remained running after 5 minutes 3 seconds. Its first output file existed. The complete output check remains pending. The integrated fourteen-file test suite is running; no passing total is claimed before it finishes. After complete origin authentication, build a uniquely identified unconfirmed model-review task, then compare it against the frozen blind reference.

The integrated fourteen-file suite subsequently passed all 181 tests in 44.98 seconds. It covers the source adapters, runner, converters, original diagnostic, evaluator, review app, independent source selection, token checker, both origin checks, and blind-review helper. It does not yet cover the new independent comparison adapter, which remains under implementation. Whitespace checks passed.

## Continuation: Verified independent comparison adapter

The comparison adapter is implemented. It compares the new model's identity occurrences with the frozen blind AI judgments on the same 40 complete pages. Exact coordinate matches and normalized-name occurrence multisets remain separate. Accepted references receive matching priority over uncertain references. Each prediction contributes once; duplicate predicted coordinates are rejected. Unresolved extraction blocks and all AI uncertainty remain in the private disagreement file. These rates describe AI agreement, not human accuracy.

The script is `scripts/diagnose_candidate_endorsement_independent40.py`. It requires an explicit converted-task hash and refuses existing output files. It authenticates the frozen reference, coding instructions, complete source inventory, all 40 explicit review-file hashes, and the separately pinned round-four Stage-A source manifest. It matches sources by document ID and checks the standard task's six metadata fields. No invented top-level sample-round field is required.

I authenticated the actual frozen reference through the new adapter. I also constructed a standard converter task in memory from actual authenticated source parents and synthetic empty seeds. The adapter accepted its actual task shape. This check wrote no synthetic files and does not establish a completed model result.

The agent's completed adapter contains 37 tests, including that constructor integration. I independently ran the adapter and supporting diagnostic/review tests with `.venv-release/bin/python`. All 74 tests passed in 4.23 seconds. The earlier 26-test run preceded the constructor and additional authority tests. Whitespace checks passed.

At that scheduler check, job 23312502 remained running after 8 minutes 35 seconds. Five output files existed. The complete origin check still required all 11 files and run metadata. No resubmission or independent agreement estimate had occurred.

## Continuation: Complete independent run and AI comparison

The independent extraction is complete, with retained technical failures. Job 23312502 finished after 13 minutes 46 seconds with deliberate quality-check exit code 2. All 639 responses and 11 output chunks exist; 37 responses failed stage-output validation, all with normal stop finish reasons rather than token-length truncation. Origin authentication and local reauthentication both passed. The Stage-B output inventory hash is `98f49a1ad5042645674b4fb3d58619e697d8b426c66e614f5ed3298edb54f84d`. The receipt is `build/private_candidate_endorsement_independent40_selection_20260914a/stage_b_origin_attestation.json`. No resubmission occurred.

The standard converter preserves valid siblings from failed batches. It produced 2,489 unconfirmed identity occurrences and 11 unresolved target blocks across all 40 pages and 174,852 source characters. The 37 failed batches retained 137 accepted target slots, including 51 optional-field-repaired slots; the latter are a subset, not additional records. The task is `build/private_candidate_endorsement_independent40_model_review_20260914a/task.json`, hash `483f88f4bbb557c9252990cff9192afd2e5cabe6aa34b8d97358c6d6dfada1e5`. Its authentication and conversion report retain both-stage pins and all failure evidence. All human confirmations and consequential-use authorizations remain false.

We compare this model's identity occurrences with the frozen blind AI review on the same independent 40 pages. Agreement is high but incomplete. Exact coordinates match 2,250 of 2,372 AI-accepted occurrences (94.86%); 122 accepted reference occurrences lack a match and 65 predictions lack any reference match. Normalized names match 2,260 accepted occurrences (95.28%), with 112 accepted occurrences unmatched and 55 predictions without a reference match. Both comparisons match 174 uncertain occurrences and leave 35 uncertain occurrences unmatched. The normalized prediction partition closes: 2,260 + 174 + 55 = 2,489.

The results are `build/private_candidate_endorsement_independent40_evaluation_20260914a/diagnostic.json` and `private_disagreements.json`, generated by the authenticated independent diagnostic. These rates measure AI agreement, not human accuracy. Unmatched predictions are not established false positives. Different pages prevent a paired improvement claim relative to the old sample. Context radius remains zero and the context change remains unpromoted.

Two fresh strong Codex agents inspected all complete sources, 2,489 seeds, and all 11 failed blocks without seeing blind labels, semantic-audit proposals, or diagnostic outputs. The first half produced 1,099 accepted rows, 40 uncertain rows, and 20 uncertainty blocks. The second half produced 1,381 accepted rows, 27 uncertain rows, and 14 uncertainty blocks. Each agent reports helper validation of all 20 owned files. Primary authentication and cross-pass comparison remain the next step; the seed-aware pass is also AI-only evidence.

The independent model-review app served successfully at localhost port 8782 in an isolated test browser. It starts with 0/40 pages confirmed, blocks incomplete export, and reports no console errors. Physical mouse selection correctly identified Democratic Congressional Campaign Committee at source offsets [177,220]. I inspected the screenshot `output/playwright/endorsement-independent40-clean.png`; source and coding panels remain readable. This checks the new artifact's startup and selection, not completed human coding. No synthetic confirmation was saved. Final AI-reviewed package assembly remains pending cross-pass adjudication.

## Continuation: Primary validation and source disagreements

I authenticated the complete source inventory and validated all 40 seed-aware review files. Each file has the assigned reviewer identity and the actual model `gpt-5.6-sol`. Counts close at 2,480 accepted rows, 67 uncertain rows, and 34 uncertainty blocks. The independent diagnostic and source-review helper tests passed all 54 tests in 4.67 seconds.

The disagreements include genuine calibration differences. I inspected parent 15's displayed 2015 publication date, explicit campaign endorsement text, and 2022 footer. The blind pass marks all 143 identities uncertain. The seed-aware pass accepts them but retains a separate date uncertainty block. Neither pass establishes the election date of each endorsement. Preserve that uncertainty without a new date exclusion or an automatic acceptance rule.

I inspected parent 31's organization statement again. The seed-aware pass independently marks Advanced Tactical Armament Concepts uncertain. It accepts HPR Ammunition, which the statement explicitly represents. This agrees with the separate source-audit correction proposal. Mike Huckabee's heading and broad party-support quotation remain a calibration question; no automatic acceptance change is justified.

The initial model disagreement file identifies 122 exact-coordinate missing AI-accepted occurrences. Twenty overlap unresolved extraction blocks. These overlap counts are not a complete causal trace. Forty-one of the 112 normalized missing accepted occurrences occur on parent 12. I inspected its flight-attendant alias pair, CRES spokesperson quotation, and four historical individual names. Adjacent aliases and personal versus organizational attribution require separate review. Unmatched counts alone do not establish confirmed extraction errors.

Two bounded agents now implement the authenticated cross-pass comparison and the final app adapter. They own separate scripts and tests. The app must preserve original unresolved extraction blocks and AI uncertainty, with every human confirmation false. The primary agent will approve a separately pinned final AI reference after source adjudication. The initial blind reference and independent model evaluation remain unchanged.

The cross-pass comparison is complete. Its script reconstructs and authenticates both complete inventories before comparison. The frozen seed reference hash is `54f3fb130bc1a61e5ec7e23d23303fbe13b0a25bafa0f85a9764b3c630cfb2fe`. Outputs are in `build/private_candidate_endorsement_independent40_codex_comparison_20260914a`. The primary agent inspected the script and independently reran its tests with the review-helper tests. All 31 tests passed in 11.00 seconds.

The passes share 2,528 exact identity occurrences. The blind pass alone contains 53; the seed-aware pass alone contains 19. They differ on 158 shared acceptance judgments, including 143 date-uncertain entries on parent 15. They also differ on 17 parent uncertainty records and one parent disposition. The private evidence retains 21 pages with disagreements or uncertainty. These differences need source adjudication; they do not establish which pass is correct.

A strong Codex agent now adjudicates complete sources for the 14 pages with identity or status differences. It must write separate source-reviewed outputs and source-spanned decisions. It must retain unresolved dates and attribution as uncertain. The primary agent will preserve agreed rows on other pages and retain both passes' uncertainty records. No final reference or final human app is approved yet.

## Continuation: Verified independent app adapter

The independent app adapter passes its current tests. I read its complete implementation and inspected the tests' artifact and export checks. It authenticates the source task, source inventory, instructions, explicit AI reference hash, and all 40 review-file contents. It requires a separate task identity and reference approval. It preserves all 11 original failed blocks, adds source uncertainty blocks, and leaves every parent, child, and block unconfirmed.

The script is `scripts/build_candidate_endorsement_independent40_codex_review_app.py`. Its current 18 tests passed independently in 10.68 seconds. Temporary tests use the unchanged blind reference and explicit test-only approval. They verify embedded task bytes, authentication hashes, incomplete-export rejection, and no overwrite. They do not establish a final package or completed human review.

The integrated 17-file suite passed all 250 tests in 61.89 seconds. It covers the original pipeline corrections, context option, independent selection, both input and origin adapters, diagnostics, source reviews, cross-pass comparison, and the independent app adapter. Whitespace checks passed. The first suite command used an incorrect test filename and ran no tests; I corrected it before the successful run.

Source adjudication remains active for the 14 membership/status-difference pages. A separate agent now implements the final-reference assembler. It must authenticate both original references and require every assigned adjudication. It will preserve agreed rows on other pages and merge retained uncertainty. An issue can leave the pending list only when a source-spanned decision explicitly resolves it. The primary agent will inspect and approve the assembled reference before the final app build.

## Continuation: Completion audit before final assembly

The existing artifacts support the completed modules. The final independent reference and app remain unproven. I inspected current files rather than treating prior status entries as completion evidence. I also read the handcode and Playwright instructions for the final app checks. The required Node launcher exists.

| Requirement | Current evidence | Verdict |
| --- | --- | --- |
| Preserve original authority | Current task hash `c92cb24a…` and original AI-reference hash `56abd8a8…` match recorded pins. | Verified. |
| Diagnose original omissions | Current diagnostic partitions all 1,877 predictions. The source trace partitions 65 exact missing accepted occurrences into five recorded causes. | Verified AI diagnostic, not human accuracy. |
| Correct partial-batch evaluation | The current replay reports exact agreement with the original task and records independently valid sibling recovery. Integrated tests cover this behavior. | Verified bookkeeping correction. |
| Evaluate bounded context change | The paired report retains both authenticated inventories, 40 pages, and unresolved blocks. Normalized accepted matches rise by 11; unmatched predictions rise by 10. | Mixed result; no promotion. |
| Audit saved source dates | Current source audit contains 40 pages, 70 segments, and 300 matched archive records. It records 11 post-election-only and 10 straddling pages. | Verified metadata audit; no date exclusion. |
| Prepare independent sample | Current selection contains 40 pages, sample round four, seed 20263827, and exclusion hashes. The pool records 142 approved additional exclusions from 1,764 pages. | Verified bounded independent sample. |
| Evaluate independent outputs | The current diagnostic retains all 40 pages, 2,489 predictions, and 11 unresolved blocks. It reports 95.28% normalized agreement with AI-accepted occurrences. | Verified independent AI agreement. |
| Preserve initial review evidence | Current blind and seed reference hashes match `8d6f5dba…` and `54f3fb13…`. | Verified. |
| Deliver final independent human package | The adapter passes tests, but actual adjudicated reference assembly and browser inspection remain pending. | Incomplete; goal remains active. |

The audit does not authorize full-corpus work, publication, human-truth claims, or a definition change. The remaining work is final source adjudication, authenticated assembly, final app checks, and a completed artifact audit.

The bounded source adjudication is now complete. I authenticated the source inventory and independently validated all 14 new review files. Counts close at 827 accepted rows, 195 uncertain rows, and 49 retained uncertainty blocks. The decision receipt binds both original review hashes and each new review hash. I independently checked those bindings and all 40 decision evidence segments against exact source coordinates.

I inspected the recorded decisions. They preserve disjoint name occurrences, distinguish titles from names, and retain shared-local multiplicity uncertainty. Parent 12 retains valid repeated occurrences but marks ambiguous public support and spokespeople uncertain. Parent 15 retains all 143 date-uncertain rows. Parent 31 retains ambiguous candidate choice, Huckabee attribution, and Lesa's personal-versus-organizational support. Advanced Tactical remains uncertain; HPR has explicit organization support. No new date exclusion or coding definition was introduced.

The assembler exists but final assembly remains pending. Primary inspection found missing receipt bindings in its first implementation. I requested checks for top-level reference pins, parent identities, adjudicated file hashes, row counts, and exact decision evidence. The agent must add these checks and tamper tests before actual assembly. No initial reference or final app was overwritten.

## Completed workflow and final audit

The final independent AI reference and human-review app are complete. The strengthened assembler authenticates top-level reference pins, parent identities, source coverage, each adjudication hash, row counts, and exact decision evidence. Eleven additional tamper cases cover these checks. The current assembler and app tests passed all 40 cases independently. A read-only check authenticated the actual 14 adjudications before assembly.

The assembled reference contains 2,361 accepted rows, 221 uncertain rows, 57 grouped source uncertainty blocks, and seven AI-zero pages. The reference is `build/private_candidate_endorsement_independent40_codex_review_20260914a/final_ai_reference/ai_reviewed_reference.json`. Its hash is `2ff01fdd64310a46e9fbb7b27c9bfa3b847e8cbcf1e857daf82e775cc45b2473`. Its authentication file records both initial references, all adjudication hashes, and the decision receipt. The primary approval record limits use to the private human-review package. It claims neither human truth nor a manual primary review of every source.

The final app is `build/private_candidate_endorsement_independent40_human_review_20260914a/index.html`. The task hash is `e0320a3551455d0fb4c267fac671d92ef235792f2944509acfcd0417406aa0be`. The HTML hash is `0e8916910ab52d1374abd82170fd29f47c43e70652bf5e67dc75234ad9e29234`. All source bytes and 174,852 source characters remain unchanged. Every parent, child, and source block remains unconfirmed. The 68 pending blocks contain all 11 model failures and 57 source uncertainties.

The app serves at `http://127.0.0.1:8783/`. A live listener check and HTTP 200 response verified the server. Physical mouse selection reports the correct name and offsets [177,220]. Keyboard checks verify parent confirmation, splitting, child confirmation, and explicit source-block review. Autosave retains a synthetic split after reload. The app blocks incomplete export. The browser reports zero errors and warnings. I inspected both selection and clean screenshots under `output/playwright/`. I cleared all synthetic state in the isolated session and verified 0/40 confirmed, 0/2,582 children confirmed, and 0/68 blocks reviewed.

The final integrated 18-file suite passed all 272 tests in 66.47 seconds. Python compilation and whitespace checks passed. Earlier 29-test and 250-test results preceded the strengthened assembler; they do not replace this current verification. The original task, original AI reference, initial blind reference, and seed-aware reference retain their recorded hashes.

The completion audit now proves every approved module. The original comparison and source trace explain provisional disagreements. The evaluator reproduces all 1,877 original predictions and preserves 39 independently valid siblings. The bounded context run is authenticated and evaluated but remains unpromoted. The date audit and independent exclusion census preserve their approved scope. The independent model outputs, blind review, seed-aware review, adjudication, and final package have explicit provenance and current checks. The final package is a real generated artifact, not a temporary test output.

The completed result is an AI-reviewed development evaluation and a pending human-review task. The 95.28% independent rate describes agreement with the initial blind AI-accepted reference, not human accuracy. No full-corpus work, paid API calls, production integration, release, or new coding definitions occurred. Human coding is the next separate research step. No required work remains within this goal's approved local scope.
