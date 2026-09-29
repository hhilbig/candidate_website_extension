# Candidate extraction extension: README and codebook

This document describes the separate candidate-extraction extension to the
Candidate Campaign Websites data. It answers one question: what does each
extension file measure, and how can a user join and audit the extracted issue,
biography, and endorsement records? It does not describe or revise the
corrected core Dataverse release.

## Current state

The corrected core data were published separately as Dataverse version 3.0.
This extension is released with the core files as version 4.0 and does not
change any core file. It uses extraction schema version 2.2, built from bundle
`bundle_20260924_v22`.

The extension was validated by a model coder, not by humans. The section
"Validation and release rule" below gives the rule, the results, and the
columns that were withheld because the check did not support them.

A release-ready package may still contain route-level `withheld_validation`
rows. In this release that status marks 23 candidate-year routes where no
extracted item aligned exactly to the page text, so no record could be
released. Every private model item and overlap conflict is accounted for.

Schema version 2.0 remains a compatibility format for prior audit artifacts.
Schema version 2.2 shares every generation input with 2.1 and adds one
typographic fallback to the aligner.

## Files, row units, and joins

The extension has three routes: `issues`, `biography`, and `endorsements`.
Each route starts from pages with the corresponding `page_type` in the
corrected core `raw_corpus.parquet`.

| File | One row represents | Unique key | Required row count |
|---|---|---|---:|
| `candidate_extraction_status.csv` | One candidate-year and route, including unobserved routes | (`candidate_cycle_id`, `route`) | 45,123 |
| `candidate_extraction_documents.parquet` | One candidate-route text document after identical raw texts are deduplicated across captures | `parent_document_id` | 23,871 |
| `candidate_extraction_segments.parquet` | One inference window within a parent document | `segment_id` | 35,213 |
| `candidate_issue_statements.parquet` | One released issue statement after overlap resolution | `record_id` | Set by the frozen v2.1 bundle |
| `candidate_biography_facts.parquet` | One released biography fact after overlap resolution | `record_id` | Set by the frozen v2.1 bundle |
| `candidate_endorsements.parquet` | One released endorsement after overlap resolution | `record_id` | Set by the frozen v2.1 bundle |

The document inventory is fixed at 14,326 issue documents, 6,962 biography
documents, and 2,583 endorsement documents. The segment inventory is fixed at
20,673 issue windows, 8,444 biography windows, and 6,096 endorsement windows.

Use the following joins:

1. Join `candidate_extraction_status.csv` to the corrected core
   `candidate_year_universe.csv` on `candidate_cycle_id`. The status table has
   three rows per candidate-year, one per route.
2. Join status rows to documents on (`candidate_cycle_id`, `route`). One
   candidate-route can have zero, one, or several deduplicated documents.
3. Join segments to documents on `parent_document_id`. This is a many-to-one
   join.
4. Join a long record to its parent document on `parent_document_id` and to
   its record-carrying segment on `segment_id`. For issue and biography rows,
   this is the canonical overlap-resolution segment. For endorsements, it is
   the exact `record_evidence_carrier_segment_id` selected by the parent line
   ledger.
5. For issue and biography rows, `source_segment_ids` lists every overlapping
   segment that supplied an equivalent variant. For endorsements, it is the
   sorted unique union of the line-inventory owner and record-evidence carrier
   segments. Explode the list before joining it to
   `candidate_extraction_segments.parquet`. Do not treat list position as
   meaningful.

`candidate_cycle_id` is the candidate-year key. It is not a stable person
identifier across election cycles. `candidate_icpsr` carries the core
ICPSR-compatible display label, normally `Last, First`. It is descriptive
metadata, not the extension join key.

## Source observability and extraction status

The status table separates whether source text was observed from what happened
during extraction. A zero is therefore distinguishable from missing source
material.

`source_observability` has three values:

| Value | Meaning |
|---|---|
| `not_observed` | The corrected raw corpus has no row for this candidate-route. |
| `observed_empty` | Raw rows exist, but their source text is blank and no inference document can be formed. |
| `observed_extractable` | At least one nonblank source document is available for the route. |

`extraction_status` has five values:

| Value | Meaning |
|---|---|
| `not_extracted_no_source` | No extractable document exists. `n_documents`, `n_segments`, and `n_records` are zero. |
| `observed_positive` | The route passed the technical and human gates and at least one exact aligned record was released. Other model items or overlap-conflict groups may be fully accounted and withheld privately. |
| `observed_zero` | Every observed segment passed the technical and human gates, and the route produced no record. This is a measured zero. |
| `failed` | At least one segment has missing output, truncation, parse failure, invalid schema, an output array at its route cap, or a repeated model item that triggers the pre-alignment guard. No failed route can be called an observed zero. |
| `withheld_validation` | Either the release gate is pending, or that gate passed but this candidate-route has no public record and at least one fully accounted private model item or overlap-conflict group was withheld. The first case is nonpublishable; the second is honest route-level missingness and does not by itself block a release-ready package. |
| `excluded_not_campaign_site` | Every document for this candidate-route was excluded because its host is an official government site or the archived site is not the candidate's. The route has no released records. |

Any truncated input, parse or schema failure, pre-alignment guard failure, or
missing output makes the candidate-route `failed`. A missing, ambiguous, or semantically unsupported
model item is retained privately with a stable ID and reason and cannot become
a public record. A semantic disagreement between overlapping variants is also
retained privately, but a fully accounted conflict is withholding rather than
a technical failure. If another exact record is released, the route is
`observed_positive`; if every candidate record is withheld, the route is
`withheld_validation`. A conflict-only route is never `observed_zero`.

## Source URLs, text transformation, and hashes

`wayback_urls` contains the archived `web.archive.org` capture URLs retained
in the corrected raw corpus. `original_urls` contains the embedded HTTP or
HTTPS target decoded from each Wayback URL. An original URL is historical
provenance; it does not imply that the URL is live now. Use a Wayback URL to
retrieve the archived source. Both columns are sorted unique lists. They are
not positional pairs. Decode a particular Wayback URL when a one-to-one link
is needed.

Identical raw text can occur at several capture timestamps or URLs. Those rows
collapse to one parent document. `n_raw_occurrences` reports how many raw rows
were collapsed. `first_capture_timestamp` and `last_capture_timestamp` are the
earliest and latest retained Wayback timestamps for those occurrences. They
are digit strings in Wayback timestamp format, not ISO dates.

The extraction source is not identical to the raw source in every row. Before
segmentation and inference, the pipeline replaces every non-overlapping
literal three-code-point sequence `#+#` with one U+000A line feed (`\n`) using
Python `str.replace`. It performs no other whitespace, punctuation, case, or
Unicode normalization. The public provenance values are:

| Field | Meaning |
|---|---|
| `source_text_transform_version` | `candidate_extraction_source_text_v1` |
| `source_text_transform_rule` | The exact `#+#` to line-feed rule above |
| `source_text_transform_changed` | Whether the transformed parent text differs from the raw text |
| `raw_text_sha256` | SHA-256 of the untransformed `text_snap_content` string encoded as UTF-8 |
| `extraction_source_text_sha256` | SHA-256 of the full transformed parent text encoded as UTF-8 |
| `segment_text_sha256` | SHA-256 of the transformed text in one inference window encoded as UTF-8 |
| `source_corpus_sha256` | SHA-256 of the exact corrected `raw_corpus.parquet` file bytes |

The extension does not duplicate raw parent text or segment text. Users can
use the source URLs and hashes to audit records against the corrected core
corpus.

## Inference display and raw JSON canonicalization

The inference prompt uses a reversible display of the transformed source
text. The display doubles literal backslashes and escapes U+0000 through
U+0009 and U+000B through U+001F. It renders a source double quote as the
visible six-character spelling `\u0022`. It preserves line feeds so that page
layout remains visible, and it also preserves DEL and C1 controls. Decoding
the complete display restores the source exactly. The display changes neither
the source text nor the hashes and offsets described below.

The pipeline retains the model's exact raw output privately. Before any
canonicalization or parsing, a lexical validator checks the fixed separator
form produced by the pinned xgrammar runtime when arbitrary whitespace is
disabled. Every colon and comma outside a quoted JSON string must be followed
by exactly one U+0020 space. A space anywhere else outside a string fails, as
does any structural tab, line feed, or carriage return. For a normally stopped
generation, a violation receives the private technical status
`raw_structural_whitespace_error`; it cannot enter alignment or a public table.
A non-stop generation keeps `output_truncated` or `generation_incomplete` as
its primary status because truncation itself may cut off a required separator.
The private aggregate still audits and counts raw-whitespace violations in
non-stop output. This check ignores characters inside quoted strings and uses
the same quote and backslash state as the canonicalizer.

After that check, one narrow canonicalizer escapes literal C0 controls only
when they occur inside JSON string content. It does not repair invalid escapes,
duplicate keys, unmatched structure, non-finite constants, or controls in
invalid positions. The canonicalizer still reports JSON whitespace outside a
string separately, but the fixed-separator gate now rejects such output before
strict parsing. The importer recomputes the raw and canonical UTF-8 hashes,
character and byte counts, replacement counts, and counts by code point. The
canonicalized output string is not stored.

For issue and biography rows, the public segment table reports the display and
canonicalizer identifiers and specification hashes, plus whether
canonicalization was applied and the number of replaced characters. The
endorsement ledger can use several Stage-A1, Stage-A2, and Stage-B generations
for one source segment, so no singular per-segment output count or
canonicalization result represents it. Those endorsement fields are null. The
table does not expose raw output, canonical output, either output hash,
display-text hashes, or per-code-point diagnostics. Exact alignment always
uses the unchanged transformed source and the strictly parsed object. The
fixed-separator specification and aggregate result remain in the private
bundle and the nonpublic validation report. The release packager and
human-coding scorer require that aggregate gate to pass.

Every private Hive output also records `user_message_sha256`, the SHA-256 of
the exact UTF-8 user message supplied to the model. The importer reconstructs
that message from the frozen input row with the authoritative message builder
and rejects a missing or different hash. The per-window message hash and the
private display-adjustment reports are not public columns.

## Pre-alignment model-item guards

Two deterministic guards run after strict JSON parsing and before exact-span
alignment. The first applies route-specific array caps. The issue and biography
guided schemas set `maxItems` to 128 and 96, respectively. Their equality cases
fail because a guided decoder that stops at `maxItems` may have suppressed
additional items. The endorsement guided schema deliberately omits `maxItems`:
the suffix-n preflight showed a dense bare-name and carried-context continuation
recall regression when that decoder boundary was present. The independent
post-generation guard remains fail-closed. It rejects any endorsement output
with 256 or more items before alignment. A count above any route cap also
indicates tampering or a decoder bypass. Every cap failure rejects the entire
window.

The second guard canonicalizes each model item as compact, key-sorted Unicode
JSON and counts exact matches across the complete route array. It fails the
entire window when any exact item occurs at least three times, including when
the copies are not consecutive. Two exact copies remain allowed. If both
guards fail, `array_item_cap_reached` takes precedence over
`model_item_repetition_error`.

A guard-failure row is valid JSON, so the private bundle retains its parsed
`model_extraction`, model-extraction hash, both guard reports, and the combined
failure decision. It has no aligned extraction, cannot supply a record, and
is a technical failure. It cannot contribute to `observed_zero`. The release
packager, release validator, handcoding builder, and handcoding scorer all
require zero guard-failure rows.

Guard IDs, specification hashes, route caps, row reports, and aggregate counts
remain private. No guard column is added to the six public files. The issue
and biography extraction-contract and generation-configuration hashes
transitively bind those guard specifications. Public endorsement rows instead
bind the frozen endorsement line-ledger contract described below.

## Character offsets and exact alignment

All offsets are zero-based, half-open Python Unicode code-point indices. For a
span with start `s` and end `e`, the selected text is `text[s:e]`. The unit is
a Python Unicode code point, not a UTF-8 byte, UTF-16 code unit, or displayed
grapheme. Offsets refer to the transformed extraction source, not the original
`text_snap_content` containing `#+#`.

`segment_start_char` and `segment_end_char` locate a segment within its full
parent document. Most long-record fields without `parent` in their names are
local to the canonical segment. The corresponding `*_parent_start_char` and
`*_parent_end_char` fields locate the same exact string in the transformed
parent document. Parent offsets equal the segment start plus the segment-local
offset. The endorsement context exceptions are defined in the next section:
parent-document context has only parent offsets, and route-URL context has no
text offsets.

The v2.1 model returns copied strings but no offsets. Deterministic code finds
all overlapping, byte-for-byte and Unicode-identical Python substring matches
within the supplied segment. It assigns offsets only when the copied string
occurs exactly once. It never normalizes text or silently selects the first
match.

Every released issue-evidence, biography-evidence, and endorsement-record
anchor has `*_exact_match_count = 1`. The ordinary alignment method is
`unique_exact_unicode_substring_v1`. When the model copies the visible
reversible display spelling of a quote or control character, the importer may
use `unique_exact_inference_display_substring_v1`. That method is accepted only
when one display-boundary match maps back to one exact source span and the
re-escaped source span reproduces the model string. Endorsement context uses
the additional scope-specific methods below.

An issue statement may have no unique source heading. In that case
`source_heading`, its four local and parent offset fields, and only those
fields are null; `source_heading_exact_match_count` is 0 and
`source_heading_alignment_method` is `not_applicable`.

The private frozen bundle preserves the original model extraction,
`model_extraction_sha256`, each withheld model item, and the complete
`exact_alignment` accounting. These values are not copied into the public
files. `source_model_item_id` is the deterministic link from a released record
to its private model item. Nonblank model items that fail exact-span or
semantic support remain private. They make a route `withheld_validation` when
no other record is released; they do not turn a positive route into a failure.
The private accounting also retains every display-anchor adjustment and every
endorsement-context extension projection, including the raw model excerpt,
match counts, recovered source span, final projected span, disposition, and
stable report ID. The importer and packager recompute those reports from the
frozen model object and source and require typed exact equality. No raw model
excerpt or adjustment report is released.

## Endorsement context provenance

An endorsement record always has exact `record_evidence`. The separate context
provenance explains why that record is evidence of support rather than a bare
name. `endorsement_context_evidence_source_scope` has three values:

| Scope | Public evidence and offsets | Interpretation |
|---|---|---|
| `segment` | Context text is non-null and exact in the canonical segment. Local and parent offsets are non-null. | The segment itself contains direct support text or the controlled structural endorsement heading. |
| `parent_document` | Context text is non-null and exact elsewhere in the same transformed parent document. Local offsets are null; parent offsets and the record-to-context distance are non-null. | A controlled structural endorsement heading is carried from the parent page into a segment that does not contain it. |
| `page_or_section_route_context` | Context text and all local and parent context offsets are null. The exact qualifying Wayback URL, decoded original URL, and decoded original path are non-null. | No controlled literal support heading exists. The frozen URL classifier identifies the archived page route as an endorsement page. |

`endorsement_context_evidence` may be null only for
`page_or_section_route_context`. That URL fallback has
`endorsement_context_evidence_exact_match_count` equal to 0 and
`endorsement_context_evidence_alignment_method` equal to
`page_or_section_route_context`. It is provenance for human review, not
self-supporting text. The human release gate still requires 100 percent
sampled combined record-plus-context support.

For exact segment context, the alignment method is either
`unique_exact_unicode_substring_v1` for direct support copied from the segment
or `provided_exact_segment_span` for a controlled structural heading. A model
context that extends that heading through additional exact display text is
projected back to the supplied heading and receives
`provided_exact_segment_span_after_context_extension_projection_v1`. For
carried parent context, it is `provided_exact_parent_span`. The segment table
records the context ID, source scope, derivation method, parent occurrence,
scope bounds, source-segment link when one exists, distance rule, URL
classifier provenance, and structural-heading specification. It does not copy
the context text. The long endorsement row links to that attestation through
`endorsement_context_id` and carries only the evidence and fields needed to
interpret that record.

Endorsement windows also carry a deterministic complete-line envelope. The
three flags state whether the segment starts mid-line, ends mid-line, and
contains at least one complete line. When a complete line exists, six offsets
locate the envelope start and end in segment-local source, parent-source, and
inference-display coordinates. When no complete line exists, all six offsets
are null and every model item in that window is withheld. Record evidence that
intersects a clipped leading or trailing partial line is withheld with the
private reason `clipped_segment_boundary`. The public segment table exposes
the flags and bounds, but no segment text.

The two public scope columns answer different questions.
`endorsement_context_source_scope` in the segment table describes the
deterministic context attestation supplied with that inference window.
`endorsement_context_evidence_source_scope` in the long table describes the
evidence used for that released record. They agree for
`page_or_section_context` records. An `explicit_text` record instead has a
long-table scope of `segment` because its direct support text is exact within
the record's own segment, even when the separate segment attestation uses a
parent-document or route-URL scope.

## Three-pass endorsement line ledger and Stage B

**Selected production architecture.** Hanno selected the round-four
implementation for production on September 15, 2026. Round four uses two model
stages and no adjudication stage. The
[production freeze record](candidate_extraction_production_freeze.md) states its
frozen contract, its failure accounting, and its coverage gate. The rest of this
section describes the three-pass line ledger, which is now historical evidence.
Its artifacts and hashes stay valid and still replay.

The one-stage suffix-p output remains complete historical and private route
provenance, but it does not supply public endorsement records. Issues and
biography continue to use their one-stage outputs. Endorsements use a
parent-document line ledger so every eligible source line is decided once
before records are extracted.

Stage A1 reconstructs the transformed parent page, creates an exact inventory
of source lines, and classifies each real target line. Stage A2 sends the 206
deterministically selected exception lines through a second model pass. Its
schema for each batch is derived from the exact exception reason, allowed
anchors, and eligible terminal roles for each slot. The manifest, batch,
output, generation configuration, and run metadata bind that exact schema
profile and the frozen construction rule.

Stage A3 is a bounded human dependency needed to finish the line decisions.
It contains exactly the 14 residual lines derived from the authenticated
suffix-f Stage-A2 evaluation. The importer authenticates the reviewed task and
HTML bytes, reconstructs the residual selection, validates one typed decision
per row, replays the human merge, and requires all unchanged Stage-A2 content
gates to pass. Until the results and imported Stage-A3 artifact exist and pass
that replay, Stage B is forbidden.

Stage B receives only the record anchors in the final three-pass line ledger,
together with their exact record blocks, and extracts zero or more
endorsements for each anchor. Stage A1 uses target-count variants for 1 through
16 real slots. Stage A2 uses batch-conditioned schemas. Stage B uses
target-count variants for 1 through 8 real slots. The importer rebuilds every
input batch from the frozen source, replays each model validator and the human
dependency, recomputes Stage-B alignment, and requires typed exact agreement
with every private batch, output, decision, block, and alignment report.

Only Stage-B `aligned_records` can enter `candidate_endorsements.parquet`.
Stage decisions, target blocks, raw and parsed model outputs, alignment reports,
and withheld records remain in the private bundle. The release validator also
rejects any public raw, model, decision, block, guard, or private-report field.

Every public endorsement carries these 14 lineage fields:

```text
line_inventory_id, source_line_id, stage_a_batch_id, stage_a_slot,
stage_b_batch_id, stage_b_slot, model_record_index,
carrier_alignment_source_model_item_id, inventory_owner_segment_id,
record_evidence_carrier_segment_id, target_block_alignment_method,
target_block_exact_match_count, record_evidence_parent_start_char,
record_evidence_parent_end_char
```

The line-inventory owner records which source segment first owned the source
line. The record-evidence carrier records the segment that contains the exact
public evidence span. These can differ, and the public record is assigned to
the carrier, never silently to the owner. `target_block_alignment_method` is
`unique_exact_target_block_substring_v1` for a unique source-text match or
`unique_exact_target_block_inference_display_substring_v1` for a unique
reversible-display match. In either case,
`target_block_exact_match_count = 1`, and the evidence parent offsets must
reproduce the exact span in the transformed parent page.

One endorsement document or source segment can contribute to several Stage-A1,
Stage-A2, and Stage-B batches. A singular prompt, JSON schema, generation configuration,
user-message template, output-token count, or one-stage alignment identity
would therefore be false provenance. The public document and segment columns
remain present for a stable cross-route schema, but those fields are null on
endorsement rows. `extraction_contract_sha256` instead equals the frozen
endorsement line-ledger contract hash. The shared model, revision, runtime,
source transformation, context provenance, URL provenance, and text-free
complete-line boundaries remain populated. Issue and biography rows retain
their exact one-stage prompt, schema, generation, user-template, token, and
alignment values.

## Segment overlap and record resolution

Long pages are split into overlapping windows. For issue and biography routes,
equivalent records can therefore appear in more than one segment. The
one-stage resolver groups those variants by parent location and a conservative
semantic identity. It reports `variant_count`, all `source_segment_ids`, a
canonical `segment_id`, `overlap_resolution`, and
`overlap_resolution_method`.

Three resolution values are release-eligible:

| `overlap_resolution` | Meaning | Selection method |
|---|---|---|
| `single_variant` | One segment supplied the record. | `only_variant` |
| `equivalent_overlap` | Several segments supplied identical aligned items. | `stable_segment_key_among_exact_equivalents` |
| `equivalent_semantics_distinct_exact_evidence` | Semantic fields agree, but overlapping exact evidence boundaries differ across windows. | Issue records prefer a valid exact source heading, then the shortest exact evidence, then the stable segment key; other routes use the last two steps |

If variants have different non-offset semantic payloads, the group receives
the private status `conflict_withheld` with method
`withheld_distinct_semantic_payloads`. No record from that group is public. The
private bundle retains every variant and a stable conflict-group ID, so the
conflict remains fully accounted and does not fail the technical gate. The
pipeline never resolves a conflict by taking the first variant.

For issue statements, exactly equal `claim` and `statement_kind` values define
the substantive identity within an overlap group. Variants with those same
values may resolve even when their overlapping exact evidence boundaries
differ. `source_heading` is provenance, so the resolver first prefers a
validated exact heading over a null heading, then the shortest exact evidence,
then the stable segment key. A claim or statement-kind disagreement remains a
private conflict.

Endorsements do not use the superseded one-stage overlap variants. Each
released endorsement is one Stage-B aligned record with
`overlap_resolution = line_ledger_single_record`,
`overlap_resolution_method = two_stage_parent_line_ledger_v1`, and
`variant_count = 1`. Its `segment_id` is the record-evidence carrier. Its
`source_segment_ids` contains the carrier and, when different, the inventory
owner. Stage-B target accounting prevents one source-line anchor from being
silently omitted or selected twice.

The public status, document, and segment tables report conflict counts. A
segment count is the number of distinct conflict groups touching that segment;
a document count is the number within that parent; a status-row count sums the
document counts for the candidate-route. A route with both released records
and private conflicts is `observed_positive`. A route whose only candidate
records are conflicts is `withheld_validation`, never `observed_zero`.

## Public schemas

Column order is part of the release contract. In Parquet, URL and segment-ID
collections are `list<string>`; offsets and counts are `int64`; flags are
Boolean; all remaining fields are UTF-8 strings. Parquet fields are physically
nullable so that optional route fields can be represented, but the validator
enforces the narrower logical null rules stated below. Empty strings are not a
substitute for null.

### `candidate_extraction_status.csv`

```text
candidate_cycle_id, route, source_observability, extraction_status,
n_raw_pages, n_documents, n_segments, n_records,
n_withheld_overlap_conflict_groups
```

`n_raw_pages` counts corrected raw-corpus rows before blank-text filtering.
`n_documents` counts distinct nonblank raw texts after candidate-route
deduplication. `n_segments` counts inference windows. `n_records` counts
released long records. `n_withheld_overlap_conflict_groups` counts fully
accounted private conflict groups for the candidate-route. All five counts are
nonnegative integers and are never null.

### `candidate_extraction_documents.parquet`

```text
parent_document_id, candidate_cycle_id, candidate_icpsr, state, office, year,
party, route, raw_text_sha256, extraction_source_text_sha256,
extraction_source_n_chars, n_raw_occurrences, first_capture_timestamp,
last_capture_timestamp, wayback_urls, original_urls, segment_count,
processed_segment_count, record_count, withheld_overlap_conflict_group_count,
processing_status, schema_version, model_id, model_revision,
runtime_lock_sha256, prompt_sha256, json_schema_sha256,
extraction_contract_sha256, generation_config_sha256, source_corpus_sha256,
alignment_id, alignment_spec_sha256, semantic_validation_spec_sha256,
endorsement_context_provenance_spec_sha256, model_item_count,
user_message_template_id, user_message_template_sha256, aligned_item_count,
withheld_item_count, endorsement_context_provenance_version,
endorsement_context_url_classifier_id,
endorsement_context_url_classifier_spec_sha256,
endorsement_context_structural_heading_spec_sha256,
source_text_transform_version, source_text_transform_rule,
offset_coordinate_system, source_text_transform_changed
```

`parent_document_id` is a deterministic SHA-256 identifier built from the
candidate-cycle, route, and raw-text hash. `extraction_source_n_chars` is the
number of Python Unicode code points after the source transform.
`segment_count`, `processed_segment_count`, and `record_count` summarize the
linked public rows. For issues and biography, `model_item_count`,
`aligned_item_count`, and `withheld_item_count` summarize all one-stage model
items for the parent. For endorsements, they summarize the Stage-B aligned and
withheld record accounting. Their accounting must be exact.
`withheld_overlap_conflict_group_count` counts distinct fully accounted private
conflict groups in the parent. The four endorsement-context identity fields
are populated for endorsement documents and null for the other routes; the
shared context-provenance specification hash is populated for every route.

Issue and biography documents carry their exact one-stage `prompt_sha256`,
`json_schema_sha256`, `generation_config_sha256`, alignment fields, and
user-message-template fields. Those singular fields are null for endorsement
documents because one parent can use several Stage-A1, Stage-A2, and Stage-B
batches.
Endorsement `extraction_contract_sha256` instead carries the frozen line-ledger
contract hash. The shared model, revision, runtime, and context provenance stay
populated.

Document `processing_status` is `processed`, `failed`, or
`withheld_validation`. A release-ready package can contain `processed`
documents and fully accounted `withheld_validation` documents. A `failed`
document blocks release.

### `candidate_extraction_segments.parquet`

```text
segment_id, parent_document_id, candidate_cycle_id, route, segment_index,
segment_count, segment_start_char, segment_end_char, segment_text_sha256,
processing_status, schema_valid, input_truncated, finish_reason, stop_reason,
output_tokens, max_output_tokens, extracted_record_count, schema_version,
model_id, model_revision, runtime_lock_sha256, prompt_sha256,
json_schema_sha256, extraction_contract_sha256, generation_config_sha256,
source_text_transform_version, inference_display_escape_id,
inference_display_escape_spec_sha256,
json_string_control_canonicalizer_id,
json_string_control_canonicalizer_spec_sha256,
json_control_canonicalization_applied,
json_control_canonicalization_count, offset_coordinate_system, alignment_id,
alignment_spec_sha256, semantic_validation_spec_sha256,
endorsement_context_provenance_spec_sha256, exact_alignment_status,
user_message_template_id, user_message_template_sha256, model_item_count,
aligned_item_count, withheld_item_count,
withheld_overlap_conflict_group_count,
endorsement_context_provenance_version, endorsement_context_id,
segment_starts_mid_line, segment_has_complete_line,
segment_first_complete_line_start_char,
segment_first_complete_line_parent_start_char,
segment_first_complete_line_inference_display_start_char,
segment_ends_mid_line, segment_last_complete_line_end_char,
segment_last_complete_line_parent_end_char,
segment_last_complete_line_inference_display_end_char,
endorsement_context_source_scope, endorsement_context_derivation_method,
endorsement_context_parent_start_char, endorsement_context_parent_end_char,
endorsement_context_parent_exact_match_count,
endorsement_context_parent_occurrence_ordinal,
endorsement_context_line_index_zero_based,
endorsement_context_scope_parent_start_char,
endorsement_context_scope_parent_end_char,
endorsement_context_source_segment_id,
endorsement_context_local_start_char, endorsement_context_local_end_char,
endorsement_context_record_to_context_distance_rule,
endorsement_context_qualifying_wayback_url,
endorsement_context_qualifying_original_url,
endorsement_context_qualifying_original_path,
endorsement_context_source_urls_sha256,
endorsement_context_url_classifier_id,
endorsement_context_url_classifier_spec_sha256,
endorsement_context_structural_heading_spec_sha256,
endorsement_context_no_literal_anchor_reason
```

`segment_index` is zero-based within a parent. Consecutive segments can
overlap. The ordered segment bounds must reconstruct the full transformed
parent source. `schema_valid` reports whether the aligned extraction satisfies
the route schema. `input_truncated` must be false. For issue and biography
segments, a successful deterministic run has `finish_reason = stop`, positive
`output_tokens`, and `output_tokens <= max_output_tokens`; `stop_reason` is
null. Endorsement `output_tokens` and `max_output_tokens` are null because a
source segment can contribute to several ledger batches.

`inference_display_escape_id` and its specification hash identify the
reversible source display used in the prompt.
`json_string_control_canonicalizer_id` and its specification hash identify the
narrow pre-parse transformation. For issue and biography segments,
`json_control_canonicalization_applied` is true exactly when
`json_control_canonicalization_count` is positive. Both fields are null for
endorsement segments because no single ledger generation represents the source
segment.

`exact_alignment_status` describes the deterministic accounting of the
model's items. Its possible private or sampling values are `complete`,
`partial_withheld`, `all_withheld`, and `invalid_schema`. A release segment
can be `complete` or `partial_withheld`; only its exact aligned items can enter
a long table. An `all_withheld` segment remains `withheld_validation`, and an
`invalid_schema` segment fails. Every status requires
`model_item_count = aligned_item_count + withheld_item_count`. On endorsement
rows, these counts describe ledger records assigned to the evidence carrier,
not the superseded one-stage endorsement items.

`withheld_overlap_conflict_group_count` counts distinct private conflict groups
touching the segment. The endorsement-context fields are populated only for
endorsement segments and are null for issue and biography segments. For a
complete-line envelope, local offsets index the undisclosed transformed
segment, parent offsets equal the segment start plus the local offsets, and
display offsets index the reversible text shown to the model. All six are null
when `segment_has_complete_line` is false. For a
textual scope they locate the selected structural context in the transformed
parent, record its exact occurrence and governed parent range, and identify a
source segment when one contains that context. For a route-URL scope the text
location fields are null, `endorsement_context_parent_exact_match_count` is
zero, `endorsement_context_no_literal_anchor_reason` explains the fallback,
and the qualifying URLs, decoded path, classifier identity, and source-URL-list
hash are non-null.

The source manifest records the inference-display identity and a complete-line
census inside `endorsement_context_provenance`. The frozen full export contains
6,096 endorsement segments: 6,055 have a complete line and 41 do not. The first
source/display boundary differs in 169 segments and the last differs in 988.
Direct exports must reproduce their census from their own rows. Derived
preflight and stress subsets inherit the full-export census, so the importer
authenticates the full source-manifest hash and separately recomputes the
selected-row census rather than comparing the 44-row subset to 6,096. The
private bundle retains both the upstream full-manifest hash and the recomputed
selected census as separate fields. The suffix-p importer also requires the
authoritative corpus hash and either the exact frozen full-export manifest bytes
or an exact reference to that full-export hash. The public release packager and
human-validation sampler accept only a bundle imported directly from the frozen
full export; derived preflight and stress bundles remain diagnostic artifacts.

Segment `processing_status` is `processed`, `failed`, or
`withheld_validation`. A release-ready package can contain `processed`
segments and fully accounted `withheld_validation` segments. A `failed`
segment blocks release.

#### What counts as evidence for a record

A record's evidence span may be a fragment of a coordinated list, such as
"and the U.S. State Department" or "strengthening Medicare benefits". The
extraction splits coordinated lists on purpose, so one item of a list becomes
one record, and the span covers only that item.

**A fragment is adequate evidence when the sentence containing it attributes it
to the candidate.** It is not adequate when the governing text attributes it to
another party, and such records were judged wrong in the accuracy check. Hanno
set this rule on September 20, 2026.

The stricter alternative, where a span must stand alone without its sentence,
was considered and rejected. Roughly one record in five marks a fragment, almost
all in the issues route, so the choice moves measured accuracy by about ten
points. Any accuracy figure for this dataset must state which rule it used.

#### Windows withheld at the output cap

One class of window carries no extraction and is released as withheld rather
than dropped: `failed_output_cap_unrepaired`. Greedy decoding repeated one item
until it reached the 24,576-token output limit, so the model's answer was cut
off and cannot be parsed.

Decoding is deterministic, so rerunning the same window returns the same bytes.
The seed is the only field a repair may change, because every other field
defines the measurement instrument and must stay identical across the corpus.
Two repair attempts ran, each changing only the seed:

| Attempt | Seed | Windows attempted | Recovered |
|---|---|---|---|
| 1, the main run | 20260824 | 35,213 | 35,177 parsed, 36 at the cap |
| 2 | 20260917 | 36 | 12 |
| 3 | 20260918 | 24 | 9 |

**15 windows remain at the cap. Ten of them lose their content.** Each records
the attempts made against it, their seeds, and the hash of each attempt's
output, so the outcome is an examined result rather than a silent gap.

The other five are endorsement windows, and four of them still carry records.
Under the round-four architecture an endorsement row's records come from the
parent line ledger rather than from that one-stage window, so a window which hit
the cap does not stop the two-stage pipeline from reading the page. Those rows
carry `failed_output_cap_unrepaired` as their superseded one-stage status and
the round-four outcome as their current status.

So the released corpus loses content for ten windows of 35,213, which is 0.028
percent, and `failed_output_cap_unrepaired` appears as a processing status on
exactly those ten rows.

A withheld window contributes no records to the three long tables. Do not read
it as a page with no endorsements, no issues, or no biography; read it as a page
the extraction could not process. Exclude these windows from any denominator
that is meant to count observed content.

### Fields shared by all three long tables

```text
record_id, parent_document_id, segment_id, source_segment_ids,
candidate_cycle_id, candidate_record_key, variant_count, overlap_resolution,
overlap_resolution_method, source_model_item_id
```

**Repeated records.** A row is one record on one parent document, and each
archived version of a page is its own parent document when its text differs at
all. A page that changed only a date or a banner therefore yields the same
records again. About half of all rows repeat a record already present for the
same candidate-year: 52 percent of issue rows, 49 percent of biography rows,
and 60 percent of endorsement rows. Candidates with many archived versions,
mostly in years before 2018, carry the most repeats.

`candidate_record_key` identifies a record within a candidate-year across page
versions. It is a SHA-256 hash of `candidate_cycle_id`, the route, and the
record's key text after case folding and whitespace collapse: `evidence` for
issues, `field` and `value` for biography, and `endorser_name` for
endorsements. Keep one row per `candidate_record_key` before counting
statements, facts, or endorsers per candidate. Counting raw rows measures how
often the campaign's pages were archived, not what the campaign said.

All shared identifiers are required. `record_id` is unique within and across
the three long tables. `segment_id` identifies the canonical segment;
`source_segment_ids` is a nonempty sorted unique list containing that segment.
`variant_count` equals the number of contributing variants, not necessarily
the number of distinct source segments.

### `candidate_issue_statements.parquet`

The exact column order is the shared fields above followed by:

```text
source_heading, source_heading_start_char, source_heading_end_char,
source_heading_parent_start_char, source_heading_parent_end_char, claim,
statement_kind, evidence, evidence_start_char, evidence_end_char,
evidence_parent_start_char, evidence_parent_end_char,
source_heading_exact_match_count, source_heading_alignment_method,
evidence_exact_match_count, evidence_alignment_method
```

`claim` is a concise faithful statement derived from the exact `evidence`.
`source_heading` preserves the nearest exact heading when one is available.
`statement_kind` is one of `support`, `oppose`, `proposal`, `past_action`,
`current_action`, or `unclear`, but the last two values are withheld:
`statement_kind` is null on those rows. The extension does not add a normalized topic,
issue taxonomy, or ideology field.

Not every issue row states a position. The model-coded check on September 28,
2026 found no position in 20 of 60 sampled records. Weighted by each kind's
share of the table, about one row in five states no position. The three
patterns are press-release link titles or bare issue names, text about another
person such as an attack on an opponent, and quoted third-party endorsements.
`claim` and `evidence` matched the page in all 60 records, so the text is
faithful even when it states no position. `statement_kind` was correct in 53 of
60, and all seven errors were `current_action` rows that hold a future
promise. `unclear` rows stated no position in 9 of 9. Both kinds are therefore
withheld, and those rows keep their claim and evidence with a null kind.

### `candidate_biography_facts.parquet`

The exact column order is the shared fields above followed by:

```text
field, value, relationship, evidence, evidence_start_char,
evidence_end_char, evidence_parent_start_char, evidence_parent_end_char,
evidence_exact_match_count, evidence_alignment_method
```

`field` is one of `occupation`, `education`, `elected_office_experience`,
`public_service_experience`, `military_service`, or `local_roots`. The field
list has no residual category, so a fact that fits none of the six goes to the
nearest one. In practice that is usually `public_service_experience`, which
also holds facts such as party activism. The model-coded check found `field`
wrong on 3 of 10 sampled `public_service_experience` records and on 1 of 50
records in the other five fields.

The extraction also codes a `timing` value (`current`, `former`, or
`unclear`). **This field is not released.** The model-coded check on September
28, 2026 found it wrong on 8 of 60 sampled records, in both directions. The
model keys on past-tense narration, not on the state at the capture date.

`relationship` is required only for `local_roots` and must itself be a
verbatim substring of `evidence`. It records the page's stated relationship to
a place, such as growing up or previously working there. It is not a claim
about where the candidate lives now. Timing describes what the archived page
states at the capture date, not present-day truth.

### `candidate_endorsements.parquet`

The exact column order is the shared fields above followed by:

```text
endorser_name, entity_type, record_evidence, record_evidence_start_char,
record_evidence_end_char, record_evidence_parent_start_char,
record_evidence_parent_end_char, endorsement_context_evidence,
endorsement_context_evidence_start_char,
endorsement_context_evidence_end_char,
endorsement_context_evidence_parent_start_char,
endorsement_context_evidence_parent_end_char, support_mode,
endorsement_context_id, endorsement_context_evidence_source_scope,
endorsement_context_record_distance_chars,
endorsement_context_qualifying_wayback_url,
endorsement_context_qualifying_original_url,
endorsement_context_qualifying_original_path,
record_evidence_exact_match_count, record_evidence_alignment_method,
endorsement_context_evidence_exact_match_count,
endorsement_context_evidence_alignment_method, line_inventory_id,
source_line_id, stage_a_batch_id, stage_a_slot, stage_b_batch_id,
stage_b_slot, model_record_index, carrier_alignment_source_model_item_id,
inventory_owner_segment_id, record_evidence_carrier_segment_id,
target_block_alignment_method, target_block_exact_match_count
```

`title`, `affiliation` and `jurisdiction` are extracted but **not released**.
`record_evidence` holds the whole source block, not a name span. A block whose
boundary reaches into the neighbouring entry puts that neighbour's line into
the evidence, and the three fields can then describe another person. The
model-coded check on September 28, 2026 found them wrong on 11 of 60 sampled
records. A user who needs a stated title can read it from `record_evidence`.
`endorser_name` is not affected and was correct in 58 of 60 sampled records.

`entity_type` is `individual`, `organization`, or `unclear`. The model-coded
check did not score it separately. A name-pattern scan found it consistent
with the endorser name in nearly all rows, but it has no measured accuracy.

#### Why there is no endorser category

The extraction assigns each endorser a category: `private_individual`,
`government_official`, `party_official`, `labor_union`, `media_outlet`,
`other_organization`, `other_individual`, or `unclear`. **This field is not
released.**

An accuracy check on September 20, 2026 found the category wrong on seven of 33
sampled endorsement records, and the error is systematic rather than random. A
sitting official is recorded as `other_individual` when the office appears in a
section heading, such as "State Senators", rather than on the record line
itself. The same records carry `government_official` correctly when the line
names the office. Anyone filtering on `government_official` would therefore
undercount officials, and would not see that they had.

`entity_type` and the evidence text are released, so a user who needs a
category can build one from published fields and state their own rule. That is more honest than shipping a field whose error
depends on how a campaign laid out its endorsement page.

`record_evidence` identifies the endorser and any attributes the page states.
Not every endorsement row is an endorsement. The model-coded check found 9 of
60 sampled records without support for an endorsement, and the model also
captures ratings and awards, such as an interest-group grade. `endorsement_context_evidence` establishes that
the record is an endorsement rather than a bare name.

`support_mode` is `explicit_text` when `record_evidence` contains a direct
support verb. It is `page_or_section_context` when a bare name or title relies
on the segment, parent-document, or route-URL context defined above. Context
text is required and exact for the first two scopes; it is null only for the
classifier-qualified route-URL fallback. The legacy conflated `organization`
field is never public.

The line-ledger fields bind each record to the exact final source-line decision
after Stage A3, the Stage-B extraction slot, target block, inventory owner, and
evidence carrier. The two parent evidence offsets earlier in the schema
complete the 14 required lineage values. Every value is non-null and typed
exactly; Boolean values cannot substitute for integer slots, indices, counts,
or offsets.

For a segment or parent-document context, the extraction instruction makes the
exact supplied support context govern every complete person or entity entry in
the page text. This rule expressly covers continuation segments that begin in
the middle of a list and do not repeat the heading. The model must extract each
complete supported name or name-and-title line, but skip a clipped or
incomplete first line, the candidate's own name, contact-form labels, and page
interface text.

## Frozen model and provenance contract

The extension uses one pinned open-weight model on UC Davis Hive. No released
value depends on a paid model API. The following one-stage v2.1 identity
governs issue and biography records and the historical private endorsement
run:

| Component | Frozen value |
|---|---|
| `schema_version` | `2.1` |
| `model_id` | `Qwen/Qwen3.6-27B-FP8` |
| `model_revision` | `e89b16ebf1988b3d6befa7de50abc2d76f26eb09` |
| `runtime_lock_sha256` | `4f46101fa493e38b312af034b8d740c612bd0001bab4cb0f624f4a9e1c49f5b3` |
| `extraction_contract_sha256` | `82530ab0f4126e1640dcffbe48a66bffdfab1538fc40240b5f3e55ab0eed5574` |
| `alignment_id` | `unique_exact_unicode_substring_v1` |
| `alignment_spec_sha256` | `c55aed337762f712c7a4edcf9dd109a94c881b936955cfa5e4d6ba8c114114e5` |
| `semantic_validation_spec_sha256` | `0ef0dc40eda7cd1ebfb83f4042360c432b8876315a8966756b5e79ccc795c9a5` |
| `endorsement_context_provenance_spec_sha256` | `b7689a181c3b9d323ee7dff10978b53d9f51e5e68d7ea709948a6529f483edb5` |
| `endorsement_context_provenance_version` | `endorsement_context_provenance_v2` |
| `endorsement_context_url_classifier_id` | `src.extract_text.classify_page_type_v1` |
| `endorsement_context_url_classifier_spec_sha256` | `0d7bf3f04e6c4ed56ccb6d8f2e7b3f8ef8de7adff075539125cf8bda9e349159` |
| `endorsement_context_structural_heading_spec_sha256` | `739d04edb1d06c40321d14f28495b34f2aa8b9f18484f41073b1265392249bd4` |
| `inference_display_escape_id` | `minimal_non_lf_c0_backslash_u0022_v2` |
| `inference_display_escape_spec_sha256` | `92566223fd29db40a5b6031833a99992b58c4939d206ba8517d2309f9fb1724b` |
| `json_string_control_canonicalizer_id` | `raw_json_string_c0_escape_v1` |
| `json_string_control_canonicalizer_spec_sha256` | `64844ef4d08777e33c01795cb86a937200b8f5d5770a6ff0724b86f50c8ec5a9` |
| `raw_structural_whitespace_validator_id` | `xgrammar_fixed_separator_json_whitespace_v1` |
| `raw_structural_whitespace_validator_spec_sha256` | `4a03ebdf798ceab3f841b7359d59ad17f34033c2f6e677edd74551935ffd8774` |
| `array_item_cap_spec_id` | `route_array_item_cap_fail_closed_v1` |
| `array_item_cap_spec_sha256` | `d5c09cd2a12821248ad579aac6980866632a6fd96df17efb598c65080d5ad150` |
| Route array caps | `issues = 128`, `biography = 96`, `endorsements = 256` |
| `model_item_repetition_spec_id` | `exact_canonical_model_item_multiplicity_v1` |
| `model_item_repetition_spec_sha256` | `c3942d9e703af8a83136c4c86eb20f4ed01ddb9627c30a457dac6b2fdcf10d3b` |
| Exact-item repetition failure threshold | `3` |
| `user_message_template_id` | `candidate_id_context_boundary_and_quote_safe_display_user_message_v6` |
| `user_message_template_sha256` | `ace05e7a19cc203b824506d41b5ebf0f5a3aacc39dd871cbbd11e9d97c8ee7d7` |
| `generation_config_version` | `candidate_extraction_generation_v2` |
| `generation_config_sha256` | `edfe53dd421acb30f1da4bc17404be4b5415a3d6060b77650c448d76edafc13f` |
| `structured_decoding_backend` | `xgrammar` |
| `structured_decoding_disable_any_whitespace` | `true` |
| `seed` | `20260824` |
| `max_model_len` | `32768` |
| `max_output_tokens` | `24576` |
| `source_corpus_sha256` | `c53d36b24b293ac5d47706aa67731f1c107d6a679cb948c9ad71a70b5c8ce7c3` |

The route-specific hashes are:

| Route | `prompt_sha256` | `json_schema_sha256` |
|---|---|---|
| `issues` | `e7ad149cbbdfa5657b32235c3421adc4f5ca89fa564f7075e6a26e26f00fcb19` | `c5b9922e9447cc5c573bd3eeefa432710a0657e37c484c46e30dc0e83a41c719` |
| `biography` | `d32cf8c87062d607cc1ee09e9485b90f2cc1a581559944583dc4839efa177960` | `ee197f260e3e2242d65ac05644d6cba72c2c2e3114fe127c579be6070ab3ffd7` |
| `endorsements` | `6a369a82639adf1ab6c068863b1a07f2ab5856eb48ffd0d639240c46be837f27` | `267901032a0bebbca582446d0df81ea508493bd970c51c061a4cd31ebf6ec3c6` |

Public endorsements use this additional frozen contract. The contract payload
includes the Stage-A3 authentication, so the value below advanced from
`d8072b6269438b1742c47f1a25ff271edab32add331a18db9898d439f9b25bb0` to its
current value when the human adjudication completed. The Stage-A2 artifacts
were built before that adjudication and still embed the earlier value. Both
values are correct for their own moment. Do not change either one.

| Component | Frozen value |
|---|---|
| Historical Stage-A1 contract | `ac8d6ed7f06e58a7961e08f8d50f7a86907f1bada12f1c70f5e9963011a335c2` |
| `line_ledger_contract_sha256` | `37dd3a17f5b70e377df4e2b9f1c8213d7909b5838cf99b6c28676e7ac7c8c4fc` |
| `line_inventory_spec_sha256` | `b1d79e313b42a1d1e1826f6c7687dc5b513ade605f4033d1b18e8b85dbf34aa0` |
| `line_batch_spec_sha256` | `33248f961b0d4929430cbc53d6eb36503885e61c225d787b56d9f645419f1d76` |
| `prompt_token_count_spec_sha256` | `587c65388f06ac5ca2e61bb81c053912d2204b6fb80e8b20a0051b4e14f3f755` |
| Stage-A1 prompt | `a5a592019dbc1aee631f11c6bbdd7b61f2d59fc173050fddc0d8b1cb1f7b1c4a` |
| Stage-A1 schema catalog | `bbb78a98b19a0119c6f20e32b95d6585dee2678bbbb2479bfd2c8b69a8b31d8f` |
| Stage-A1 schema variants | `line_decision_targets_01_v1` through `line_decision_targets_16_v1` |
| Stage-A2 prompt | `f2cf469cae24b7e3c48f1def3b176ee647cd412895886f14716eb6530eaeaf0e` |
| Stage-A2 schema catalog | `2fc6d94f0fd657004ed0bf733136d80d0bb991b17e80c0ee79d0ad2e33cfb927` |
| Stage-A2 schema rule | `aece88a3989c2140aea04b0dad7652e26e860cba1ac1ed9b066fea8382c8bb9e` |
| Stage-A2 adjudication specification | `ea397860de9e5f543aabdad071269c780dba9a49fe03441d4f7c9b61168e2c5a` |
| Stage-A2 frozen input manifest | `caa26dbd4b978ba71859c58f78a8b35c689daa2c873cd44e9fd9bed63b3f6191` |
| Stage-A2 generation configuration | `446aa533312fffacea3ebaaa4642e0c353dc1cccb8b49fe6ee23e7216d101ba6` |
| Stage-A3 specification | `642498154a4c7346736b6e025c17893842b23ae7c22222e83631f6c10fe0a2b6` |
| Stage-A3 reviewed task | `e834f23d55804bc138d6dac1441dfbd96ab0449ddd34e1eeb0edf2585d96bfe5` |
| Stage-A3 reviewed HTML | `ba9649cb0538ba6422fa6469f5455b38b0567ed988f47e5e060826a8659b73f3` |
| Stage-B prompt | `ddc90ae105620211907edc553848c717e807bbd74aa0a6a762129eb3c904c95f` |
| Stage-B schema catalog | `913011667e4625f87f8bceb0f618e0c6068fe8bdd4cbca7867e1edb88b7304ad` |
| Stage-B schema variants | `record_extraction_targets_01_v1` through `record_extraction_targets_08_v1` |
| Stage-B upstream-provenance specification | `3624c77d94915deff44693521628e3e8cee881cae4d82a7f57d4fe9066f97089` |
| Line-ledger seed | `20260825` |
| Stage-A1 maximum output tokens | `2048` |
| Stage-A2 maximum output tokens | `2048` |
| Stage-B maximum output tokens | `16384` |

Public endorsements in the production release use the round-four contract
instead. The freeze record states the decision and its consequences.

| Round-four component | Frozen value |
|---|---|
| `round4_production_contract_sha256` | `f8c755ce5097345cd5e99755c62f470ac6fdfdfadc116d4b8ecc41da7797522b` |
| Production binding | `0ff9120fd4930c113d5752d973a68f54fc699ce4360920b749dbb9e2c5b48c72` |
| Development binding, unchanged | `a5c482b99244d3541e45c72621f358f60562c857e57220e28195cb480e4a8f40` |
| Stage-A prompt | `35a8a6cc907568fa6e4d7e19ec0e92cd2e926f1cec7e7fd99a027e1637549546` |
| Stage-B prompt | `bb51ffe6cd0c0f5d026cfb4e2f2448e8f98d4447613a724d40a7d67a034f56a4` |
| Stage-A schema catalog | `e45b50b8b1502b01d578b333c71deabd5b549fc8678567d251332c49a5fa1ed1` |
| Stage-B schema catalog | `feebaf613ac4ea9425aa73ba93bec225e40d867b1ebe235566170bf6f9213d7a` |
| Record projection specification | `7418bfc2c88ca0351a33b3d5312a9290bfdf5e842530b3d127c3977e1a739f76` |
| Failure accounting specification | `a331a7721f2a347a2271a7f6a7c3f0bac03481df2f2c9db0782b54c58c4422e3` |
| Coverage specification | `a873e6800c569ca7bd176d1510d6afccf2930334bae6bf3e80e0336ae1997b78` |
| Round-four seed | `20260907` |
| Stage-A slots and maximum output tokens | `16` and `2048` |
| Stage-B slots and maximum output tokens | `4` and `16384` |

The six public files keep their schema. Round four fills the same endorsement
columns and the same 14 lineage fields as the line ledger.

Every batch, output, restart check, metadata file, and manifest binds the schema
catalog, exact variant ID and hash, and exact number of real target slots.
Stage A2 additionally binds its batch-conditioned schema rule. A padding
decision for a real target or a real decision in a padding slot fails coverage.
Earlier failed preflights remain immutable. The reviewed suffix-f Stage-A2
output is valid model evidence, but its content gate remains `NO_GO` until the
14 Stage-A3 rows are human-coded and the unchanged gates pass. No Stage-B or
release bundle may be built before that result.

The v2.1 guided schemas express biography and endorsement conditional rules as
disjoint `oneOf` branches and contain no regular-expression patterns. Exact-copy
anchors omit `minLength`: issue and biography `evidence`, endorsement
`record_evidence`, and explicit-text `endorsement_context_evidence`. This avoids
constraining the first decoded anchor character while leaving the six-file
public column schemas and semantic inclusion rules unchanged. The ordinary
Python model-item validator separately applies the full Unicode `str.strip()`
nonblank check before any item can enter the aligned extraction. Every non-null
copied anchor must then either have a unique exact match or be bound to a
deterministically provided exact context span under the scope rules above. An
empty or whitespace-only anchor makes the entire inference window
`failed_schema`, even when another item in that window is valid. Such a window
cannot contribute a resolved record, an `observed_positive` or `observed_zero`
status, a handcoding task, or a release. Generative free-text fields retain
`minLength = 1`.

The earlier suffix-n contract is a frozen NO-GO audit, not release input. The
preceding suffix-o contract is also a frozen NO-GO audit. It
passed all deterministic checks on 44 Hive windows but failed the predeclared
endorsement recall, clipped-boundary, context-copy, quote-copy, and resolved-
record content gates. The suffix-p contract recorded above adds exact prompt
boundary provenance, display-to-source alignment with auditable private
reports, and exact per-window user-message hashes. It must pass a fresh
all-route preflight before any larger rerun.

`generation_config_sha256` hashes the canonical complete generation
configuration. That configuration binds the schema, model, revision, runtime,
route prompts and schemas, extraction, alignment, display, canonicalization,
endorsement-context, array-cap, and exact-item-repetition specifications, user
message template, seed, context and output token limits, zero temperature,
disabled thinking, disabled compact repair, and the complete
structured-decoding policy. The engine-level xgrammar
policy disables arbitrary whitespace between JSON tokens. In the pinned
xgrammar runtime, the resulting grammar still emits one fixed U+0020 after
each colon and comma. The separate raw-output validator requires exactly that
form. It does not alter text inside JSON strings or the source text used for
exact evidence alignment.
Every private output row and run-metadata object must independently attest
`structured_decoding_backend = xgrammar` and
`structured_decoding_disable_any_whitespace = true`; a missing or weaker value
fails the technical gate. They also attest the guard IDs, hashes, and caps.
Issue and biography document and segment rows expose the frozen one-stage
generation hash, not raw model generations or the private engine and guard
attestation fields. Endorsement rows expose the line-ledger contract hash and
null the singular one-stage prompt, schema, generation, user-template, token,
and alignment fields. Documents and segments repeat only provenance that can
be represented truthfully at their row unit.

The private bundle keeps two separate count ledgers. Each run-metadata file
reports only inference newly performed in that invocation; a restart can
therefore report zero or a subset of its selected shard. A zero-work metadata
file must set `recovery_attestation_without_inference = true` and every
inference counter, count mapping, and maximum to zero. Conversely, a metadata
file without that recovery attestation must report at least one processed
window. Its selected-window count still covers the complete assigned shard.
A separate
`all_output_aggregate` is recomputed from every frozen output row and reconciles
parse, schema, exact-support, token, alignment, withholding, and JSON-control
counts for the full corpus. It separately counts strict-JSON outputs, all model
items parsed before guards, each guard-failure status, repeated exact-item
groups, the maximum exact multiplicity, and the maximum item count by route.
The release gate requires selected-shard counts to match source and output
coverage exactly. Every attempt-local counter, count-mapping component, and
maximum must be no larger than the independently recomputed all-output ledger.
The gate never treats `*_this_run` counts as full-corpus totals.
Across run-metadata files, additive counters are summed, while
`output_tokens_max_this_run`, `max_exact_model_item_multiplicity_this_run`, and
each route in `route_model_item_count_max_this_run` use the maximum shard value
rather than the sum of shard maxima.

The private `private_output_provenance_summary` independently recomputes the
fixed-separator result from every raw output. It records the validator ID and
specification hash, numbers of checked, valid, and invalid outputs, colon and
comma counts, required separator-space counts, and violations by type. The
bundle, release packager, and handcoding gate require zero invalid outputs.
These fields are not copied into the six public extension files.

## Privacy and interpretation rules

The biography extension excludes `family` and `other`. The approved exclusion
removes 25,549 of 88,602 legacy biography records (28.8 percent). Those legacy
categories remain internal and cannot be promoted into a public derived table.
The extension also excludes a standalone current-residence field, street
addresses, and post-office-box addresses. An address-like local-roots value or
evidence span fails semantic validation and is withheld.

Local-roots, occupation, and office or service facts describe the archived
page at its capture date. They must not be interpreted as the candidate's
current residence, current job, or current office. Short exact evidence spans
and source URLs remain public for auditability, subject to the exclusions
above.

Issue records preserve the source's own claims and headings. They do not infer
ideology. Endorsement records release the endorser's name and entity type with
the evidence text. They do not expose the legacy generic `organization` field.

## Validation and release rule

The approved plan required independent human validation of every route. That
validation was not carried out. On September 28, 2026 the release rule was
changed: the release uses a model-coded check, and it publishes only the
columns that the check supports. The check is an estimate from one model coder
per sample. It is not human validation, and users should treat its figures as
approximate.

**Design.** Seven batches drawn with seed 20260922 from the frozen bundle: 60
precision records per route, stratified by statement kind or field or
endorser category, 20 complete-page recall pages per route, and the 11 known
omissions. A separate Claude Opus agent coded each batch against the coding
rules written into the task. Sampled parents exclude all tuning parents.

**Gate.** Each route passes when it has at least 60 precision records and 20
recall pages, record-level recall is at least 0.90, and all 11 known omissions
are confirmed. The per-column figures below are published and do not gate the
release; the column decisions were made from them.

| Route | Record recall | Checked dimension | Correct, of 60 |
|---|---:|---|---:|
| Issues | 0.91 | text states a position | 40 |
| Issues | | `claim` faithful to the evidence | 60 |
| Issues | | `evidence` supports the statement | 60 |
| Issues | | `source_heading` correct | 59 |
| Issues | | `statement_kind` correct | 53 |
| Biography | 0.94 | fact present | 60 |
| Biography | | `value` faithful | 59 |
| Biography | | `field` correct | 56 |
| Biography | | `relationship` correct | 60 |
| Biography | | evidence supports the fact | 55 |
| Endorsements | 0.98 | endorser present | 60 |
| Endorsements | | `endorser_name` correct | 58 |
| Endorsements | | record evidence identifies the endorser | 47 |
| Endorsements | | record and context support an endorsement | 51 |

The known-omission check confirmed 11 of 11. Precision samples are stratified,
so a per-route figure is not a population rate. Weighted by each statement
kind's share of the table, about 80 percent of issue rows state a position.

**Withheld columns.** Biography `timing` (wrong on 8 of 60), endorsement
`title`, `affiliation` and `jurisdiction` (wrong on 11 of 60), and the
endorser category (withheld since September 20, 2026). Their definitions stay
in this codebook so that users know what was extracted and why it is absent.

**Excluded pages.** Official government hosts (`.gov`) are not campaign
websites and are excluded: 631 parent documents. Three candidate-years are
excluded because the archived site is not the candidate's: `H0AL02160-2020`
(`anthonybrown.com`, a different candidate in Maryland, 36 parent documents),
`S2GA00167-2022` (`gagop.org`, the Georgia Republican Party, 3), and
`H8IN07291-2018` (`borderlessu.com`, a web studio's placeholder page, 1). The
status table keeps these candidate-year routes. Their counts are recomputed
from the released tables, and a route whose documents were all excluded has
`extraction_status = excluded_not_campaign_site`.

**Development provenance.** The endorsement route was developed with a
private 40-parent recall pilot, frozen in
`quality_reports/candidate_endorsement_40_parent_recall_pilot_selection_spec_2026-08-26.json`,
and a 14-row Stage-A3 adjudication. Neither is a validation sample. The
repaired eight-parent prerequisite is authenticated under residual-repair
contract `37dd3a17f5b70e377df4e2b9f1c8213d7909b5838cf99b6c28676e7ac7c8c4fc`.
The line-ledger prompt and schema catalog bindings include
`276ca9a244a582ec74022b99e18ad1fcf163535fead962bf4f4eba36d37447ca`
and `825e67b8e39acbddc6ea571dde19639a24ba096047f8751a4beb70b8dcc76023`.

The frozen report is
`quality_reports/candidate_extraction_model_coded_validation_2026-09-27.json`
in the project repository. The release summary carries the same per-column
figures in `released_column_accuracy`.

## Glossary

**Anchor:** A copied source string that must be aligned exactly, such as issue
evidence, a source heading, or endorsement context.

**Canonical segment:** For issue and biography records, the one segment
selected deterministically to carry public local offsets when several
overlapping segments contain an equivalent record. For endorsements, the
record-evidence carrier is the public segment.

**Context attestation:** The deterministic segment, parent-heading, or
route-URL provenance supplied with an endorsement inference window. A released
record links to it through `endorsement_context_id`.

**Inference window:** A contiguous, possibly overlapping segment of one
transformed parent document supplied to the model.

**Line ledger:** The endorsement process that assigns roles to every eligible
parent-page source line in two model passes plus one bounded human dependency
pass, then extracts records only from the final record anchors.

**Line-inventory owner:** The source segment that deterministically owns a
parent-page line in the inventory. It can differ from the segment carrying the
released record evidence.

**Parent document:** One distinct nonblank raw text for one candidate-route,
after identical texts across captures are deduplicated.

**Overlap conflict:** A group of overlapping variants whose substantive fields
disagree. Every variant remains private and accounted, and the group produces
no public long record.

**Route:** One extraction task: issues, biography, or endorsements.

**Record-evidence carrier:** The source segment containing the exact public
endorsement evidence span. Public endorsement records join to this segment.

**Route-URL context:** A fallback endorsement attestation used only when no
controlled literal support heading exists and the frozen URL classifier labels
the archived page route as endorsements.

**Withheld item:** A model item retained in the private audit bundle but
excluded from public long tables because it failed schema, exact-alignment,
semantic, or overlap checks.
