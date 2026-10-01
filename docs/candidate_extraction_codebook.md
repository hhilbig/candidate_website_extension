# Extracted variables: codebook

This document describes six files, added in version 4.0, that hold issue statements, biography facts, and endorsements extracted from the archived campaign websites in `raw_corpus.parquet`. The core files are unchanged from version 3.0 and are described in `codebook.md`.

## Overview

We extracted three kinds of records from every page that the core corpus classifies as an issues, biography, or endorsements page (`page_type`). An open-weight language model (Qwen3.6-27B, FP8) read each page in overlapping windows and returned structured records. Every released record carries a quote from the page, and we release a record only if that quote appears in the page text exactly once. Records whose quote could not be located are withheld, not corrected.

Most variables reproduce what the page says: a paraphrased claim and the sentence it rests on, a biographical fact in short form, or the name of an endorser. Three variables are codes assigned by the model: the kind of issue statement, the biography field, and whether an endorser is a person or an organization. We do not assign topics, ideology, or endorser categories.

| File | One row is | Rows |
|---|---|---:|
| `candidate_extraction_status.csv` | A candidate-year and route (issues, biography, endorsements) | 45,123 |
| `candidate_extraction_documents.parquet` | A distinct page text for one candidate-year and route | 23,177 |
| `candidate_extraction_segments.parquet` | One model input window within a page | 34,322 |
| `candidate_issue_statements.parquet` | An issue statement on one page | 287,255 |
| `candidate_biography_facts.parquet` | A biography fact on one page | 104,124 |
| `candidate_endorsements.parquet` | An endorser named on one page | 153,108 |

`candidate_extraction_manifest.json` lists each file's rows, columns, bytes, and SHA-256 hash.

## Repeated records

Each distinct version of a page is its own document, and a page that changed only a date or a banner produces the same records again. About half of all rows repeat a record already present for the same candidate-year: 52 percent of issue rows, 50 percent of biography rows, and 60 percent of endorsement rows. Candidates whose sites were archived often carry the most repeats.

`candidate_record_key` identifies a record within a candidate-year across page versions. It hashes `candidate_cycle_id`, the route, and the record's text after case folding and whitespace collapse: `evidence` for issues, `field` and `value` for biography, and `endorser_name` for endorsements. Keep one row per key before counting anything per candidate. After this step the files hold 137,114 issue statements for 2,304 candidate-years, 52,365 biography facts for 2,454 candidate-years, and 60,899 endorsers for 902 candidate-years. Paraphrases of one fact ("Born in Joplin" and "Born in Joplin, Missouri") remain separate records.

## Joins

`candidate_cycle_id` links every file to `candidate_year_universe.csv`. The status table has exactly three rows per universe candidate-year. Documents join to status on (`candidate_cycle_id`, `route`), segments and records join to documents on `parent_document_id`, and a record joins to the window that carries it on `segment_id`. `source_segment_ids` lists every overlapping window that returned the same record. `record_id` is unique across the three record tables.

The documents table codes party as `democrat` and `republican`, as `raw_corpus.parquet` does; the universe, crosswalk, and provenance files use `D` and `R`. Offset columns can be null, so read them as nullable integers (`Int64` in pandas).

## Status and coverage

The status table separates a candidate who said nothing from a page we never saw.

| `extraction_status` | Meaning | Rows |
|---|---|---:|
| `observed_positive` | At least one record was released | 5,660 |
| `observed_zero` | Pages were read and no record was found | 372 |
| `not_extracted_no_source` | The corpus has no page of this type for the candidate-year | 39,033 |
| `withheld_validation` | Pages were read, but no record could be matched exactly to the page text | 23 |
| `excluded_not_campaign_site` | All pages were excluded (see below) | 35 |

`not_extracted_no_source` is not a zero. Most candidate-years have no page of a given type, because the site had none, the Wayback Machine did not archive it, or the crawler did not reach it. Among the 7,348 captured candidate-years, the share with at least one record is as follows.

| Office and years | Captured | Issues | Biography | Endorsements |
|---|---:|---:|---:|---:|
| House 2018–2024 | 5,787 | 0.32 | 0.35 | 0.13 |
| Senate 2002–2010 | 403 | 0.14 | 0.14 | 0.04 |
| Senate 2012–2016 | 449 | 0.31 | 0.30 | 0.06 |
| Senate 2018–2024 | 709 | 0.36 | 0.37 | 0.11 |

Coverage is similar by party for issues and biography. Endorsement records are more common for Democrats (15 percent) than Republicans (10 percent). Senate 2002 and 2004 are nearly empty.

## Excluded pages

We excluded pages that are not the candidate's campaign site. Official government pages (`.gov` hosts) account for 654 documents. Three candidate-years were archived from another site: `H0AL02160-2020` points to `anthonybrown.com`, a different candidate's site in Maryland; `S2GA00167-2022` points to the Georgia Republican Party; and `H8IN07291-2018` points to a web studio's placeholder page. These three account for 40 documents. The status table keeps their rows; `n_documents`, `n_segments`, and `n_records` count only released documents, and `n_raw_pages` still counts all pages in the core corpus.

## Variables

### Record tables: shared columns

| Column | Meaning |
|---|---|
| `record_id` | Unique record identifier |
| `parent_document_id` | The page the record comes from |
| `segment_id` | The window that carries the record's offsets |
| `source_segment_ids` | All overlapping windows that returned an equivalent record |
| `candidate_cycle_id` | Candidate-year key |
| `candidate_record_key` | Same record within a candidate-year across page versions (see above) |
| `variant_count`, `overlap_resolution`, `overlap_resolution_method` | How overlapping windows that returned the same record were merged |
| `source_model_item_id` | Identifier of the model output item |

### Offsets

Every quoted field (`evidence`, `source_heading`, `record_evidence`, `endorsement_context_evidence`) comes with four offsets. `*_start_char` and `*_end_char` index the window; `*_parent_start_char` and `*_parent_end_char` index the whole page. Offsets are zero-based, half-open Python code-point indices into the page text after one change: every `#+#` separator in `text_snap_content` is replaced by a line feed. To recover a page's text, match `raw_text_sha256` in the documents table to the SHA-256 hash of the UTF-8 encoded `text_snap_content` in `raw_corpus.parquet`. Offsets are null when the quoted field is null. Endorsement context found elsewhere on the page, outside the model's window, has page offsets but no window offsets. A record's own quote (`evidence`, `record_evidence`) occurs exactly once in the page. A context quote, such as a repeated "Endorsements" heading, can occur more than once, and `*_exact_match_count` gives the number of occurrences. `*_alignment_method` records how a quote was found. About 9 percent of issue quotes were located only after curly and straight quotation marks were treated as equal; the released quote is still the page's own text.

### `candidate_issue_statements.parquet`

| Column | Meaning |
|---|---|
| `claim` | A short paraphrase of the position, written by the model from `evidence` |
| `evidence` | The quoted page text the claim rests on |
| `source_heading` | The nearest page heading above the statement, if one exists (null for 37 percent of rows) |
| `statement_kind` | `support`, `oppose`, `proposal`, or `past_action`; null where the model returned `current_action` or `unclear` |

We withheld two statement kinds. The model gave `current_action` mostly to future promises, and `unclear` rows stated no position. Those rows keep their claim and evidence.

### `candidate_biography_facts.parquet`

| Column | Meaning |
|---|---|
| `field` | `occupation`, `education`, `elected_office_experience`, `public_service_experience`, `military_service`, or `local_roots` |
| `value` | The fact in short form |
| `relationship` | For `local_roots` only: the stated tie to the place, such as "grew up in" (a quote from `evidence`) |
| `evidence` | The quoted page text |

A fact describes the page at its capture date, not the candidate's current status.

### `candidate_endorsements.parquet`

| Column | Meaning |
|---|---|
| `endorser_name` | Name of the endorser as printed |
| `entity_type` | `individual` or `organization` |
| `record_evidence` | The page text that names the endorser |
| `endorsement_context_evidence` | Page text that marks the list as endorsements, such as a heading; null when only the page address does |
| `support_mode` | `explicit_text` if the record itself says "endorses" or similar; `page_or_section_context` if the page or section establishes it |
| `endorsement_context_evidence_source_scope` | Where the context was found: the window, the page, or the page address |
| `endorsement_context_qualifying_*_url`, `..._path` | The page address that qualifies a list, when the context comes from the address |
| Remaining columns | Line and batch identifiers that tie each record to the model pass that produced it |

### Documents, segments, and status

The documents table gives each page's candidate, party, state, office, year, route, first and last capture timestamps, character count, and record counts. The same text often appears in several captures, so `wayback_urls` and `original_urls` are lists, and `n_raw_occurrences` counts the corpus rows that carry this text. The segments table gives each window's position in the page, processing status, and output counts. Both tables also record the model, prompt, schema, and alignment specification behind every row as identifiers and SHA-256 hashes; these are constant within this release. The status table gives, for each candidate-year and route, whether pages were observed (`source_observability`), the `extraction_status` above, and counts of pages, documents, windows, records, and withheld overlap conflicts.

## Validation

We did not validate the records by hand. Instead, a separate language model (Claude Opus) coded a stratified sample against written coding rules: 60 records per route, 20 complete pages per route to measure recall, and 11 known facts the extraction had missed in development. Each route had to capture at least 90 percent of the records on the sampled pages, and the extraction had to recover all 11 known facts. All three routes met both conditions.

| Route | Record recall | Check | Correct, of 60 |
|---|---:|---|---:|
| Issues | 0.91 | Text states a position | 40 |
| | | Claim faithful to evidence | 60 |
| | | Heading correct | 59 |
| | | Statement kind correct | 53 |
| Biography | 0.94 | Value faithful | 59 |
| | | Field correct | 56 |
| | | Evidence supports the fact | 55 |
| Endorsements | 0.98 | Endorser name correct | 58 |
| | | Evidence identifies the endorser | 47 |
| | | Record is an endorsement | 51 |

The sample is stratified by statement kind, field, and endorser category, so these are not population rates. Weighted by each kind's share, about 80 percent of issue rows state a position. All seven statement-kind errors were `current_action`, which we withheld. The same check led us to withhold biography timing (wrong in 8 of 60) and endorser titles, affiliations, and jurisdictions (wrong in 11 of 60). An earlier check led us to withhold endorser categories. `entity_type` was not scored.

## Known limitations

Before using the records, users should know where the extraction is weakest. We list the problems we found in our own review, with their size.

Issue statements include text that states no position. The most common forms are press-release titles, bare topic labels ("Healthcare"), and passages about an opponent. About 3 percent of distinct statements have three words or fewer; some are real positions ("I am pro-life") and many are not. Counts of statements per candidate also differ by party: Democratic candidate-years average 74 distinct statements and Republican candidate-years 45. Comparisons of counts across candidates should account for this.

Biography facts sometimes describe a family member. We found about 30 military-service facts that belong to a parent or spouse and about 50 occupation values that are only family roles ("mother"). About a fifth of distinct `elected_office_experience` values are committee or caucus posts, not offices. `public_service_experience` collects facts that fit no other field, including legislative accomplishments. A missing field is not evidence that the candidate lacks it.

Endorsement lists vary in kind. Some sites list hundreds of private supporters, a few list visitor comments, and pages laid out as tables can yield broken names ("Tawil Melanie"). In the validation sample, 9 of 60 rows were not supported as endorsements, and the model also captured interest-group ratings and awards. Counts of endorsers therefore measure a site's list format as much as elite support.

About 800 rows contain encoding errors carried over from the archived pages, such as "JuliÃ¡n". They are concentrated in about 40 candidate-years, and one 2010 Senate site accounts for 561 of them. We did not repair them, because each quote must match the page text exactly.

The source pages are the Wayback Machine captures in the core corpus. Pages that load their text with JavaScript were often archived without it, and those candidates often have no records.
