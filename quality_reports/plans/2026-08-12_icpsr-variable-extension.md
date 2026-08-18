# Plan — Reproduce ICPSR 226001 coded variables, then extend them to our years

**Date:** 2026-08-12
**Author:** Claude session
**Status:** approved

## Context

We hold a candidate-website corpus (House 2018–2024, Senate 2002–2024; 10,601
candidate-years, 1,175,582 page rows) built to ICPSR 226001's `websites_clean`
schema, with a 100% FEC-id and 81% DIME crosswalk. Pons confirmed (2026-07-05)
that he has collected only 2018 House and recommends releasing on Dataverse, so
there is no duplication risk and the venue is settled.

The release ambition is a dataset that **merges directly into ICPSR 226001 and
carries the same coded variables, coded the same way**, plus extras. Today we
ship text only. The repo's codebook states that ICPSR's snapshot-to-candidate
aggregation rule is unknown, which is what has blocked extending their variables.

Scoping investigation on 2026-08-12 changed that picture:

1. **The aggregation rule is solved.** Drop empty pages (`n_char > 0`), take the
   mean over pages within a snapshot `date`, then the mean over snapshot dates.
   This reproduces `candidates_complexity.csv` **exactly (<1e-6) for 99.8–99.9%
   of all 13,020 rows** on all four tested variables. Competing rules scored far
   worse (row-level mean 33%, page-sum-by-date 35%, mean/mean without the empty
   filter 67%).
2. **`topic_words.csv` is a fitted, year-agnostic model** — 7,669 words × 31
   topics, each word's loadings summing to 1 (a p(topic|word) distribution). It
   applies to any year, including ours. `candidates_topics.csv` rows sum to
   exactly 1.0 and are unique on `candidate_year_stage`.
3. **`word_scores.csv` (the ideology scaling) is year-keyed and stops at 2018**,
   refit per cycle. It cannot extend to 2020–2024. Per decision below, omitted.
4. **Join key works perfectly**: 13,020 of 13,020 complexity rows match
   `websites_clean` on `candidate|state|district|year|stage`.

Intended outcome: `topics` and `complexity` variable families, validated against
ICPSR's published values, computed for all 16 of our office-years, so the
released dataset stacks onto theirs with genuinely comparable coded columns.

## Recommended approach

Validate each family against ICPSR's own published output *before* computing it
for our corpus. Every stage has a pass/fail threshold; a family that fails
validation is dropped from v1 rather than shipped with a caveat.

### Stage 0 — Lock the aggregation rule (mostly done)

Formalize the verified rule as a reusable function and characterise the ~0.1%
residual (expect ties/rounding or candidate-years whose pages are all empty).
Confirm it holds within each `year × stage × data_source` cell, not just overall,
so we know it is not an artifact of pooling.

**Pass:** ≥99% exact on all four variables, with no year or stage below 95%.

### Stage 1 — Reproduce `candidates_topics.csv` (the real open question)

Hypothesis: per candidate-year-stage document, count words restricted to the
7,669-word vocabulary, then `topic_j = Σ_w freq(w) · p(topic_j | w)`, normalised
to sum to 1. `tfidf_weight` ranges 0–7668 over 7,669 rows, so it is a rank or
document-frequency index, not a multiplier — determine which by testing both
weighted and unweighted variants.

Fit the document at the level the aggregation rule implies (Stage 0), and test
the variants: raw counts vs. tf-idf-weighted counts, lowercasing, and whether
the document is the concatenation of all snapshots or the per-snapshot mean of
topic vectors.

**Pass:** mean absolute error < 0.01 per topic cell and Spearman > 0.95 on the
dominant topic, against the 13,020 published rows.

### Stage 2 — Reproduce the per-row complexity inputs from text

Distinct from Stage 0, which reproduced the *aggregation* given ICPSR's per-row
values. This stage asks whether the per-row values are derivable from stored
text at all. Already measured:

| Variable | Derivable from `text_snap_content`? |
|---|---|
| `n_char` | **Yes** — exactly `len(text)`, 100% |
| `n_words` | No — whitespace tokens match only 19.9%; tokenizer must be identified |
| `n_tags`, `n_clean_tags` | **No** — HTML-structure counts fixed at scrape time; `#+#` segments match only 13.5%, and the cleaned text has the separators stripped |
| `entropy`, `TTR`, `MATTR`, `subordinates` | Untested — all text-derived, so expected recoverable |

Identify the `n_words` tokenizer (try `nltk.word_tokenize`, regex `\w+`, and
`str.split` on punctuation-stripped text) and the definitions of `entropy`,
`TTR`, `MATTR`, `subordinates`. Ship only the variables that validate.

`n_tags`/`n_clean_tags` are almost certainly unrecoverable for our corpus (our
`n_clean_tags` is a legacy zero column). They are HTML-structure noise, not
substantive measures; document the gap rather than fabricate them.

**Pass:** per-variable. Each validated variable is shipped; each failure is
documented as a known gap in the codebook.

### Stage 3 — Apply the validated pipeline to our corpus

Run the same code over `raw_corpus_icpsr.parquet` (1,175,582 rows, all 16
office-years), producing one row per candidate-year at ICPSR's grain with the
validated topic and complexity columns. Compute from the **raw corpus under
ICPSR's aggregation**, never from `panel_candidate_year.csv`, whose longest-text
collapse would create a construction break at the 2016/2018 boundary.

Run locally: the corpus is ~1.5 GB and the droplet has only ~1.7 GB RAM. Pull it
down and process year-by-year with a `ParquetWriter`, matching the existing
streaming approach in `scripts/harmonize_deliverable.py`.

Sanity checks against ICPSR's distributions: topic rows must sum to 1.0; topic
means for our House 2018–2024 should sit in a plausible range relative to their
House 2002–2016 rather than jumping discontinuously.

### Stage 4 — Report and codebook

Write `quality_reports/icpsr_variable_extension_<date>.md`: the aggregation rule
with its match rate, per-family validation results, the variables shipped, the
variables dropped and why. Update `docs/deliverable_codebook.md`, replacing the
"aggregation rule is UNKNOWN" caveat with the verified rule.

## Critical files

- `scripts/icpsr_replicate_coding.py` — **new.** The whole pipeline: aggregation
  function, topic projection, complexity measures, validation harness. One
  script, `--validate` (against ICPSR) and `--apply` (to our corpus) modes.
- `docs/deliverable_codebook.md` — replace the unknown-aggregation caveat;
  document new columns and the `n_tags`/`n_clean_tags` gap.
- `KNOWLEDGE_BASE.md` §6 — record the Pons outcome (Dataverse, no duplication)
  and the resolved aggregation rule.
- `quality_reports/icpsr_variable_extension_<date>.md` — **new.** Findings.
- Read-only inputs in the local ICPSR 226001 package directory:
  `websites_clean.parquet`, `candidates_topics.csv`, `candidates_complexity.csv`,
  `topic_words.csv`, `_scraper.py`. **Never write to this directory.**

## Reused existing functions

- `scripts/harmonize_deliverable.py` — the streaming `ParquetWriter` pattern and
  the `ICPSR_COLS`/`INT_COLS` schema contract; reuse both rather than re-deriving.
- `src/extract_text.py` — `_tag_visible()` and `extract_visible_text()` already
  mirror `_scraper.py`'s `tag_visible`/`get_page_content`, including the `#+#`
  separator, which is why our text is comparable to theirs.
- `scripts/build_panel.py` — candidate-year keying and the text-quality flag.
- `scripts/coverage_audit.py` — tarball streaming, if raw text is needed beyond
  what the corpus parquet holds.

## Verification

1. **Stage 0:** ≥99% exact match on `n_char`, `n_words`, `n_tags`,
   `n_clean_tags` across all 13,020 rows; no `year × stage` cell below 95%.
2. **Stage 1:** topic MAE < 0.01 per cell over 13,020 rows; dominant-topic
   Spearman > 0.95; all reproduced rows sum to 1.0 ± 1e-9.
3. **Stage 2:** report exact-match rate per variable; ship only those that pass.
4. **Stage 3:** output has one row per captured candidate-year (expect 10,601),
   unique on `(candidate, state, office, year)`, zero topic rows failing the
   sum-to-1 check, and no year with >20% missing on a shipped variable. Flag any
   drop above that threshold before proceeding.
5. **Spot-check:** hand-verify three candidate-years end to end, at least one
   Senate and one from a thin-text candidate, confirming the numbers trace back
   to specific snapshots.

## Non-goals

- **The ideology score** (`score`, `score_all`, `nuance_score`, `mnir`) — omitted
  from v1 by decision. Word weights are refit per year and stop at 2018, and
  refitting needs primary-stage text we never collected.
- **`candidates_similarity.csv`** — needs both opponents captured; at our capture
  rate the usable sample is too thin for v1.
- **Rights posture and the Dataverse deposit** — separate task, still open.
- **The 7 unpushed commits** — separate, not blocking.
- Re-scraping, primary-stage collection, and the CJK post-process.

## Note

Project convention is that approved plans live in `quality_reports/plans/`
(template `templates/plan.md`). Copy this there on approval.
