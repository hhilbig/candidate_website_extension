# Hive candidate-page extraction

**Date:** 2026-08-22  
**Status:** full production extraction and technical validation passed; human
coding package ready; substantive validation pending

## Decision

Do not add the extracted variables to the Dataverse release yet. The revised
Hive pilot passes its technical gates, but a human-coded validation sample is
still required before the fields can be described as research-ready. The
current local release remains the authoritative release candidate.

## Question and source data

This extension asks whether archived candidate pages can support CLARA-style,
evidence-linked variables about policy positions, biographies, and
endorsements. It compares a model's structured records with the visible text of
the archived page from which each record was extracted. It does not use outside
information.

The authoritative input is
`build/release_v2_1_candidate/raw_corpus.parquet`, SHA-256
`c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3`.
After deduplicating identical candidate, route, and text combinations, the
production universe contains 23,871 documents: 14,326 issue pages, 6,962
biography pages, and 2,583 endorsement pages.

## Extracted records

The proposed long-format records are:

- issues: nearest heading, faithful policy claim, and verbatim evidence;
- biography: field, value, and verbatim evidence, where field is occupation,
  education, prior elected office, prior public service, military service,
  local roots, family, or other;
- endorsements: endorser name, organization, endorser type, and verbatim
  evidence.

Every evidence field must be one exact contiguous span of the supplied text.
An exact text match is a technical provenance check, not proof that the model's
interpretation is substantively correct.

## Pilot design

The pilot contains 360 documents, 120 per route. Selection is deterministic
with seed 20260822 and balances election year and party within route. The six
input chunks and their hashes are recorded in
`build/hive_candidate_extraction/pilot_export/manifest.json`.

Inference ran on UC Davis Hive under the free `publicgrp` account with
`Qwen/Qwen3.6-27B-FP8`, exact model revision
`e89b16ebf1988b3d6befa7de50abc2d76f26eb09`, and frozen runtime-lock SHA-256
`4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3`.
Temperature was zero and model thinking was disabled. No commercial API
produced any value.

## What the burn-in runs showed

The first run, Hive job 21096248, completed in 10 minutes 3 seconds but failed
the technical gate. It parsed 293 of 360 documents (18.61 percent parse error)
and exactly supported 93.34 percent of extracted evidence fields. The failures
were diagnostic: the 1,800-token output ceiling cut off long JSON arrays, and
the model sometimes joined separate source passages with ellipses.

The implementation was changed in two ways. The prompt now requires a short,
exact, contiguous source span and forbids stitched evidence. The model context
and output ceilings increased to 32,768 and 8,192 tokens. The same documents
were then rerun from scratch rather than selectively repairing failed outputs.

The revised run, Hive job 21096543, completed in 18 minutes 38 seconds and
passed all predeclared technical gates:

| Check | Result |
|---|---:|
| Documents accounted for | 360 / 360 |
| Parsed documents | 353 / 360 |
| Parse-error rate | 1.94% |
| Schema errors among parsed documents | 0 |
| Exactly supported evidence records | 5,872 / 6,097 |
| Exact evidence-support rate | 96.31% |

By route, exact evidence support is 97.77 percent for biography records, 98.54
percent for issue records, and 95.07 percent for endorsement records. The seven
remaining parse failures are one very long issue page and six endorsement
pages with unusually long lists. They remain explicit failures; no record was
silently dropped or repaired.

The next three runs tested overlapping page segmentation. A parent page is the
deduplicated archived page. An inference window is one exact character span of
that page, with stable parent ID, window ID, character offsets, and source-text
hash. Adjacent windows overlap so a fact near a boundary remains observable.
The importer retains each window's output and failure status, then groups exact
duplicate records at the parent level without discarding their original output
variants.

| Hive job | Window rule | Windows | Parse errors | Exact evidence support |
|---|---|---:|---:|---:|
| 21118702 | 12,000 characters; 1,200 overlap | 389 | 5 (1.29%) | 96.62% |
| 21119743 | 6,000 characters; 600 overlap | 501 | 2 (0.40%) | 96.85% |
| 21121071 | 6,000/600 generally; endorsements 3,000/300 | 629 | 0 | 98.51% |

The two failures under the 6,000-character rule were dense endorsement lists.
The route-specific rule removed both while leaving the already successful issue
and biography windows unchanged. This final pilot passed with 629/629 parseable
windows and no schema errors. It is the production specification.

## Production design

The final deterministic export contains 35,213 inference windows for all 23,871
parent pages: 20,673 issue windows, 8,444 biography windows, and 6,096
endorsement windows. The export has 587 atomic 60-window chunks. All remote
chunks match the local manifest's individual SHA-256 hashes.

Production uses a 30-shard Slurm array with at most four Blackwell GPUs active
at once. Each shard owns a disjoint modulo partition of the sorted chunks,
checks exact input/output IDs before every restart, writes chunks through an
atomic rename, and records separate run metadata. The importer requires exact
coverage of every input window, all 30 shard metadata records, the pinned model
and runtime identity, and the technical quality gates.

The importer writes both provenance and analysis views. Segment-level JSONL
retains every model response and failure. Parent-level JSONL retains every
overlap variant. Three flat Parquet tables expose issue statements, biography
facts, and endorsements with stable record IDs, candidate and page provenance,
parent parsing status, representative verbatim evidence, overlap-variant counts,
and the pinned model and runtime identity.

## Production results

The production array completed all 587 chunks and all 30 shards. It returned
35,213 unique window IDs for 35,213 inputs. The initial pass contained 15
truncated JSON responses: eight issue windows and seven endorsement windows.
All repairs ran on Hive with the same pinned Qwen model family, model revision,
runtime lock, temperature, and source corpus. Raising the output ceiling fixed
the seven endorsement lists; these rows carry
`repair_method = higher_output_ceiling`. The eight issue failures, marked
`repair_method = recursive_subsegment`, were localized to an
archived passage with unmatched quotation marks and one looping response. They
were repaired with progressively smaller overlapping windows. For the final
seven quote-bearing microsegments, quotation-mark characters were replaced by
spaces in the retry prompt only. The importer restored each model evidence span
to its exact bounding substring in the untouched archived text and rejected
any span that could not be re-anchored exactly.

The completed import passes every predeclared technical gate:

| Check | Result |
|---|---:|
| Inference windows accounted for | 35,213 / 35,213 |
| Parsed windows | 35,213 / 35,213 |
| Schema-valid windows | 35,213 / 35,213 |
| Truncated inputs | 0 |
| Exactly supported evidence items | 440,403 / 449,108 |
| Exact evidence-support rate | 98.06% |

The flat analysis views contain 211,267 issue records, 88,602 biography
records, and 138,640 endorsement records. Their 438,509 record IDs are unique.
No record is missing its candidate ID, parent-page ID, or evidence field. The
views retain 8,463 records whose selected representative evidence did not pass
the exact-match check, mark them explicitly, and do not treat them as validated
records.

## Why release remains gated

A balanced spot check found that exact evidence can still accompany an
incorrect interpretation. For example, the phrase "Brian Schatz for Senate"
was treated as evidence of prior elected office even though the phrase alone
does not establish that biographical fact. Some endorsement records likewise
use a name on an endorsement-route page as evidence without a sentence that
itself states the endorsement. The endorsement route is also only just above
the 95 percent technical evidence threshold.

Before release, human-code a stratified validation sample
for each released field. The review must separately score whether the record is
present in the page, whether the normalized value is faithful, whether the
record type is correct, and whether the evidence alone supports the record.
The compact coding package contains five records per observed route/subtype
(75 record judgments) and five complete parent pages per
route (15 recall judgments). Record coding separately evaluates presence,
normalized-value fidelity, record type, and whether the evidence alone supports
the record. Page coding asks whether the model omitted any requested record.
The package includes CSV, JSON, and a formatted Excel workbook with constrained
yes/no fields. Its scorer fails closed on incomplete coding. The source coding
files and hash manifest are under
`build/hive_candidate_extraction/human_validation_full/`. The workbook is
`outputs/01a02986-09e9-7f63-a60e-0baa2b677a2f/candidate_extraction_validation.xlsx`.

On 2026-08-24, the `family` biography field was judged technically extractable
but substantively irrelevant to the intended public dataset. It remains in the
internal extraction artifacts for provenance but must be excluded from released
tables and release-validation metrics. The binding scope decision is recorded in
`config/candidate_extraction_release_scope.json`.

The same review excluded the residual `other` biography field. Its contents are
heterogeneous, often substantively irrelevant, and can duplicate other extraction
families. The review retained `local_roots` and `occupation` subject to temporal
interpretation rules: historical place connections cannot imply current residence,
and former occupations must remain explicitly historical.

The review also found that `prior_elected_office` mixes current and former
officeholding. Any public table must rename it `elected_office_experience` and
include `office_timing`, coded `current`, `former`, or `unclear`.

Likewise, `prior_public_service` must be released as
`public_service_experience` with `service_timing` coded `current`, `former`, or
`unclear`.

For endorsements, the current `organization` value is withheld because it
conflates organization, affiliation, title, and jurisdiction. The current
extraction can release endorser name and type; a future pass must represent those
four concepts separately. Bare list entries must retain page-level endorsement
context or be marked context-dependent rather than described as self-supporting
evidence.

## Validation result

The audit is complete but is not a fully independent human validation. Hanno
directly coded or approved 50 of the 75 record-level judgments. At his request,
Codex completed the remaining 25 record judgments and all 15 page-recall checks.
The completed artifacts label this provenance explicitly.

All 75 sampled records were present in the supplied page text except one inferred
church-membership record. Normalized values were faithful for 65 of 75 records,
and record types were correct for 65 of 75. Only 38 of 75 selected evidence
excerpts supported the complete record without surrounding context. Thirty-three
records (44 percent) passed all four dimensions. Excluding the ten records in the
out-of-scope `family` and `other` biography fields does not solve the central
problem: 25 of 65 release-scope records (38.5 percent) pass all four dimensions.
The main reason is that bare names and titles on endorsement pages do not state
endorsement without the page heading or list context.

Eleven of 15 sampled pages contained every requested record. The four pages with
omissions were William Cassidy's biography and the issue pages for Shelley
Berkley, Blake Moore, and Teresa Leger. The completed audit therefore does not
support releasing the current derived tables as-is. A release candidate must
apply the field exclusions and renames above, redesign endorsement evidence to
retain page-level context, and repair or regenerate the failed normalized values,
types, and missed records before a new independent validation pass.

Completed coding tables and scores are under
`build/hive_candidate_extraction/human_validation_completed/`. The completed
workbook is
`outputs/01a02986-09e9-7f63-a60e-0baa2b677a2f/candidate_extraction_validation_completed.xlsx`.

## Reproducible artifacts

The exporter, Hive runner, importer, schemas, and tests are in
`scripts/export_candidate_extraction.py`,
`scripts/hive/extract_candidate_pages_vllm.py`,
`scripts/import_candidate_extraction.py`,
`scripts/build_candidate_extraction_validation.py`,
`scripts/score_candidate_extraction_validation.py`,
`scripts/merge_candidate_extraction_validation.py`,
`scripts/build_candidate_extraction_validation_workbook.mjs`,
`src/candidate_extraction.py`, and
`tests/test_candidate_extraction.py`. Pilot inputs and both local imports are
under `build/hive_candidate_extraction/`; build artifacts are not release files.

## Glossary

**Route:** Page category used to choose an extraction schema.  
**Technical gate:** A deterministic check of coverage, JSON/schema validity,
and verbatim evidence matching.  
**Substantive validation:** Human assessment that the extracted interpretation
is correct, not merely well formed and traceable to source text.
