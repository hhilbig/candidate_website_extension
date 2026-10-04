# Plan: Improve the endorsement pipeline

Date: 2026-09-14.
Status: Completed approved local workflow. Human validation and full-corpus work remain separate decisions.

## Context

We compare the existing round-four extraction on 40 complete source pages with the separate Codex AI review. The comparison identifies omissions, unsupported records, and name-boundary errors. It does not establish human accuracy.

The source task is `build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json`. The provisional reference is `build/private_candidate_endorsement_codex_ai_review_20260914a/ai_reviewed_results_v2.json`. Both files remain unchanged.

## Recommended approach

Build a local diagnostic with exact coordinate comparisons and normalized-name comparisons. Separate AI-accepted, AI-uncertain, and unmatched predictions. Preserve the source evidence for each disagreement. Trace omissions to unresolved extraction blocks or source classifications where evidence permits.

Inspect the current code for general implementation bugs. Apply only fixes that preserve the coding definitions and source sample. Add regression tests. Prepare a review app from the current AI reference for later human use. Inspect saved metadata for source dates and missing context.

An independent sample must exclude prior development pages and related source components. Prepare it only when the current inventory and approved exclusion rules support that choice. Do not open an existing sealed reserve or call a model merely to complete a task list.

## Critical files

- `scripts/review_candidate_endorsement_codex.py`: review validation and provenance.
- `scripts/evaluate_candidate_endorsement_round4_regression.py`: existing comparison functions.
- `src/candidate_endorsement_round4.py`: extraction and name alignment.
- `scripts/build_candidate_endorsement_round4_human_review_app.py`: existing app and export validation.
- `quality_reports/session_logs/`: durable results and decisions.

## Verification

Run the relevant tests, Python compilation, and artifact checks. Verify all 40 source identities, record partitions, and hash bindings. Inspect the generated app and validate its export separately from human completion.

## Non-goals

No human labels, paid API calls, purchased credits, full-corpus submissions, production integration, or publication. Pause before a material definition or sample change. A new bounded Hive rerun requires a justified correction and resource checks.

## Progress on 2026-09-14

The diagnostic and source trace are complete. The evaluator now preserves independently valid records from partially failed batches. This correction retains 39 records and reproduces all 1,877 authoritative seed occurrences. It changes evaluation bookkeeping, not the extracted sample or AI labels.

Read-only source context is implemented behind an opt-in radius. Radius zero preserves the existing contracts. Radius ten adds adjacent lines and classified support headings without expanding name ownership. All 498 batches passed the exact tokenizer check. Bounded Hive job 23300739 completed, and its comparison shows mixed results below.

The separate human-review app contains all 1,960 AI seeds and 88 pending blocks. Its startup, source selection, autosave, keyboard controls, split, block review, and export/import mechanics passed isolated browser checks. No synthetic browser confirmations were saved as human results.

Source metadata now covers 300 archived URLs. Eleven pages have only post-election captures and ten straddle election day. Capture dates do not establish endorsement dates. No page was excluded. The independent pool census retains 1,622 pages after preserving existing exclusions and additionally excluding the current candidate cycles and linked source components. No new sample was drawn.

The relevant suite passed 107 tests. Python compilation and whitespace checks passed. The session log records exact artifacts and remaining work.

The continuation added an authenticated comparison for the context run. An old-run replay reproduced all baseline counts, including 17 unresolved blocks. The final integrated suite passed 150 tests. Complete context outputs passed authentication and evaluation.

## Remaining decisions and work

Job 23300739 completed all 498 responses. It returned exit code 2 because 23 responses failed technical checks. Both stages passed inventory authentication. The comparison preserved all 40 source pages and all 17 unresolved blocks.

Context improved agreement slightly but increased unmatched predictions. Normalized accepted matches rose from 1,031 to 1,042; unmatched predictions rose from 80 to 90. Exact accepted matches rose from 1,028 to 1,031; unmatched predictions rose from 83 to 105. This evidence does not justify promotion. The paired result is `build/private_candidate_endorsement_codex_context_evaluation_20260914a/paired_comparison.json`.

The user approved continuation with the proposed independent 40-page sample, including 32 legacy-positive pages and eight legacy-zero pages. A separate Codex agent prepares the sample with the prior constraints, a fresh seed, and preserved exclusions. No new reserve or quota change is approved.

The independent assignment is now frozen. It includes 40 pages in 38 source components, with 22 Democratic and 18 Republican pages. Seed 20263827 preserves the previous seed formula. All prior constraints pass with zero optimization gap and no excluded-source overlap. The corrected Stage-A inputs pass both the runner and review source loader. All 292 prompts pass the exact tokenizer check.

Bounded Stage-A job 23312106 completed all 292 responses with zero technical failures. Origin and local authentication agree on inventory hash `0c46b2033834a04423e2314671e176b15cc97aa2171027c5f8e15f5d2a850bbc`. The revision-b input remains authoritative. Stage B contains 639 batches and 2,512 targets for all 40 pages. All prompts passed the exact token check.

Bounded Stage-B job 23312502 completed all 639 responses. It returned quality-check exit code 2 because 37 responses failed technical validation. Both stages passed origin and local authentication. The converter retained 2,489 unconfirmed identity occurrences and 11 unresolved blocks on all 40 pages. Context radius remains zero.

The frozen blind AI reference contains 2,372 accepted occurrences, 209 uncertain occurrences, and 47 uncertainty notes. Its hash is `8d6f5dbad5fbd2dc392bc1ad45f0048bad5fac4d9552d5f677e08d293cebcb79`. The independent comparison matches 2,260 accepted occurrences after name normalization (95.28%), with 112 accepted reference occurrences unmatched and 55 predictions without a reference match. These are AI-agreement counts, not human accuracy or established false-positive counts. The different independent sample does not establish improvement over the old sample.

Both separate seed-aware strong Codex agents completed full-source reviews without inspecting blind labels or diagnostic results. Together they produced 2,480 accepted rows, 67 uncertain rows, and 34 source uncertainty blocks. Both passes passed authentication and comparison. Source adjudication and final package construction then completed. The initial reference and initial comparison remain unchanged. The basic independent model-review app passed HTTP startup, mouse selection, incomplete-export blocking, and zero-console-error checks in an isolated browser. These checks did not create human confirmations.

Missing headings, shared surnames, organization compounds, and archive timing remain definition questions before any related rule change. Human review remains necessary for human-validated accuracy. Full-corpus work, production integration, and release remain unapproved.

The independent app adapter passes 18 focused tests. Source adjudication covers 14 pages with identity or status differences. The final-reference assembler passes 22 focused tests. The final integrated suite passes 272 tests. Primary reference approval, final app construction, browser checks, and the completion audit are complete. Temporary test outputs remain separate from the final package.

## Completed deliverables

The final source adjudication and assembly are complete. The reference contains 2,361 AI-accepted occurrences, 221 uncertain occurrences, and 57 source uncertainty blocks. Its hash is `2ff01fdd64310a46e9fbb7b27c9bfa3b847e8cbcf1e857daf82e775cc45b2473`. The separate approval record limits use to the private human-review package. Initial blind labels and initial evaluation remain unchanged.

The final app is `build/private_candidate_endorsement_independent40_human_review_20260914a/index.html`. Its task contains all 40 pages, 2,582 unconfirmed rows, and 68 pending blocks, including all 11 original model failures. Task and HTML hashes pass authentication. The app serves at `http://127.0.0.1:8783/`; the observed HTTP response is 200.

The final integrated suite passes 272 tests. Python compilation and whitespace checks pass. The primary agent inspected rendered screenshots and verified mouse selection, keyboard decisions, row splits, child confirmation, block review, autosave, and incomplete-export rejection. The isolated browser returned to 0/40 confirmed after synthetic checks. No human results were created.

The completion audit verifies the original diagnostic, sibling recovery, bounded context evaluation, date audit, independent selection and evaluation, AI review provenance, and final human package. Context remains unpromoted because its paired result is mixed. The completed scope requires no further local work. Obtain human coding before human-validated accuracy claims. Request separate approval before full-corpus work, new costs, definition changes, or release.
