# Extending ICPSR 226001's coded variables to our corpus

**Date:** 2026-08-12
**Plan:** `quality_reports/plans/2026-08-12_icpsr-variable-extension.md` (approved)
**Code:** `scripts/icpsr_replicate_coding.py`
**Output:** `data/deliverable/panel_icpsr_compat.csv` (10,601 candidate-years)

## Why this was run

We want to release our corpus (House 2018–2024, Senate 2002–2024) as a dataset
that stacks onto ICPSR 226001 and carries the same coded variables, computed the
same way, so a user can pool their years and ours without a construction break.
ICPSR ships coded variables but not text; we have text but no coded variables.
Closing that gap requires reproducing their coding exactly enough to trust it.

The blocker on record was that their snapshot-to-candidate aggregation rule was
undocumented, so we did not know how to turn per-page measurements into the
candidate-level numbers they publish. This report resolves that, then reports
which variable families survive validation and which do not.

## What was compared

Throughout, "reproduce" means: take ICPSR's **own** text
(`websites_clean.parquet`, 1,026,318 pages), recompute a variable with a
candidate rule, and compare against ICPSR's **own** published value for the same
candidate-year (`candidates_complexity.csv` / `candidates_topics.csv`, 13,020
rows each). Only after a rule reproduces their numbers do we apply it to ours.
Agreement is reported as the share of the 13,020 rows matching to within 1e-6.

**Grain.** ICPSR's candidate unit is
`candidate x state x district x year x stage x data_source`. The `data_source`
component matters and is easy to miss: the same candidate-year-stage can appear
twice, once as `primary` and once as `general_wayback`, and in the raw files the
two are distinguished **only by the casing of the candidate name**
(`ADAMS, SANDY` vs `Adams, Sandy`). Keying without `data_source` collapses those
pairs and silently corrupts 18 rows.

## Result 1: the aggregation rule is solved exactly

The rule is:

1. drop pages with `n_char == 0`
2. take the **mean over pages** within a snapshot `date`
3. take the **mean over snapshot dates**

It is an unweighted two-level mean, not a sum, and the empty-page filter is
required. This reproduces ICPSR's published values **exactly for 13,020 of
13,020 rows (100.00%) on all four of `n_char`, `n_words`, `n_tags`,
`n_clean_tags`**, in every year and every stage, with zero residual.

Competing rules, for contrast: a flat mean over page rows reproduces 33%,
summing pages within a date then averaging 35%, and the correct two-level mean
*without* the empty-page filter 67%. The filter and the two-level structure are
both load-bearing.

**Implication.** Any variable we compute per page can now be aggregated to
ICPSR's candidate grain with confidence. This unblocks everything below and
should replace the "aggregation rule is UNKNOWN" caveat in the codebook.

## Result 2: two variables reproduce exactly, two approximately, four not at all

Separately from aggregation, we asked whether each variable can be **derived
from stored text at all**. Some cannot, because they were computed from HTML at
scrape time and the archived HTML is gone.

| Variable | Status | Evidence |
|---|---|---|
| `n_char` | **exact** | `len(text)`, 100.00% over 60,000 pages |
| `n_words` | **exact** | `len(re.findall(r'\w+', text.lower()))`, 100.00% |
| `TTR` | **approximate** | corr 0.9990, median ratio 1.0015 |
| `MATTR` | **approximate** | window 200; corr 0.9982, median ratio 1.0000 |
| `n_tags`, `n_clean_tags` | **not derivable** | HTML-structure counts fixed at scrape time |
| `entropy` | **not reproducible** | four definitions tested, best \|corr\| 0.32 |
| `subordinates` | **not reproducible** | needs their dependency-parsing pipeline |

Terms: **TTR** (type-token ratio) is distinct words divided by total words.
**MATTR** (moving-average TTR) is the same ratio averaged over a sliding window
of fixed length, which removes TTR's mechanical dependence on document length.

On the two approximations: the *same* tokenizer that reproduces `n_words`
exactly reproduces TTR only near-exactly (12.8% bit-exact). Since a count and a
type-set built from one token list cannot disagree, their TTR must be computed
at a different stage of their pipeline, on differently preprocessed text. Seven
tokenizer variants were tried (case-sensitive, alphabetic-only, minimum length
two, whitespace); none closed the gap, and `lower \w+` remained best. The
measures are substantively the same (correlation 0.999, median ratio 1.00), so
they are shipped under an explicit `_approx` suffix rather than dropped or
passed off as exact.

The MATTR window was found by searching 10, 20, 25, 50, 100, 200, 500 against
their published values; agreement peaks sharply at **200** (corr 0.9982, ratio
1.0000) and degrades either side (window 50: corr 0.858; window 500: corr 0.966).

`entropy` was the one genuine surprise. Word-frequency Shannon entropy
correlates **negatively** with their `entropy` (-0.32), and normalising by
`log(n_tokens)` or `log(n_types)` only reaches +0.31. Given the file also
carries `subordinates`, their `entropy` is most likely a syntactic measure from
a parsing toolkit rather than a lexical one. It is omitted rather than
approximated, because an approximation with correlation 0.3 would be a different
variable wearing the same name.

## Result 3: the topic family is not reproducible, and this is a hard negative

`candidates_topics.csv` gives each candidate-year a distribution over 31
Manifesto-Project-style categories. `topic_words.csv` looked like the fitted
model: 7,669 stemmed words x 31 topics, each word's row summing to 1, i.e.
p(topic | word), and not keyed by year, so applicable to our years.

It is not sufficient. Reconstructions tested, all against their 13,020 published
rows, threshold MAE < 0.01:

| Reconstruction | MAE | top-1 accuracy |
|---|---|---|
| linear projection of word counts | 0.0290 | 19.5% |
| tf-idf weighted projection | 0.0290 | 34.0% |
| hard argmax assignment | 0.0340 | 11.5% |
| EM / mixture inference | 0.0372 | 20.0% |
| + learned per-topic scaling | 0.0272 | 38.3% |
| + full 31x31 learned map | 0.0272 | 38.3% |
| **predicting the training mean** | **0.0302** | **29.3%** |

The last row is the point. The best calibrated reconstruction (0.0272) barely
improves on predicting the same average distribution for every candidate
(0.0302). Our features carry almost no candidate-specific information about
their topic values, so this is not a tuning problem.

Two checks confirm the failure is real rather than an artifact:

1. **The join is correct.** Every reconstruction beats its own
   randomly-permuted baseline (e.g. 0.0290 vs 0.0324), so we are matching the
   right candidates; there is genuine but weak signal.
2. **The needed inputs are absent.** `tfidf_weight` turned out to be a
   permutation of 0..7668, i.e. the alphabetical column index, not a weight, so
   the idf vector is not in the package. The document-level inference step of
   the fitted model is not shipped. And the three directories that would have
   held the intermediates, `websites_topics/`, `topics_complexity/`, and
   `embeddings/`, are **empty in the download**.

**Implication.** Topics are dropped from v1. Extending them would require the
full ICPSR package (DOI 10.3886/E232001V1) or the fitted model object.

## What was produced

`data/deliverable/panel_icpsr_compat.csv`, one row per captured candidate-year.

- 10,601 rows, **0 duplicates** on `(candidate_icpsr, state, office, year)`,
  matching the corpus candidate count exactly
- built from all 1,175,582 corpus page rows
- missingness: 0 on `icpsr_n_char`/`icpsr_n_words`, 1 row on the TTR/MATTR pair;
  worst office-year missing share 0.06%, far under the 20% flag threshold
- columns: identifiers and keys carried through unchanged, plus
  `icpsr_n_char`, `icpsr_n_words`, `icpsr_ttr_approx`, `icpsr_mattr_approx`,
  `icpsr_n_valid_snap`, `icpsr_n_valid_pages`

**Nothing existing was modified.** This is a separate join-able table; our own
`n_char`, `text_quality`, panel columns and corpus are untouched. Where our
construction differs from theirs (our candidate-year panel collapses to the
longest-text snapshot; ICPSR averages over snapshots) both now exist side by
side and the user picks.

One implementation detail worth stating: our corpus stores `date` as a Wayback
`YYYYMMDDHHMMSS` timestamp while ICPSR's snapshot grain is the day, so ours is
truncated to `YYYYMMDD` before aggregating. Without this, two captures on the
same day would count as two snapshots for us and one for them.

### Spot-checks

Three candidate-years were recomputed from the raw parquet independently and
compared to the shipped panel. All four variables matched to 1e-9 in every case:
a large Senate case (McCain 2008, 12,466 pages across 262 snapshot-days), a
degenerate thin case (Connor SC 2016, a single 6-character page), and an
ordinary House case (Evans NC 2022, 2 pages).

## Caveats for downstream use

1. **Use MATTR, not TTR, for cross-year comparison.** At the 2016/2018 boundary
   between their data and ours, median `n_char` rises 51% (1,624 to 2,452) and
   median TTR falls 6.9%. TTR falls mechanically as documents lengthen, so most
   of that movement is length, not vocabulary. MATTR, which fixes the window,
   moves only 2.5% (0.684 to 0.667). Some of the length increase is a real
   trend (ICPSR's own median rises steadily from 875 in 2002 to 1,624 in 2016);
   we have not separated the remainder from a possible scraper difference.
2. **MATTR degenerates on short documents.** 20.8% of candidate-years have fewer
   than 200 words, i.e. fewer than one window, so their MATTR falls back to
   plain TTR. 224 rows (2.1%) have TTR = MATTR = 1.0 because the document is a
   handful of words. Filter on the existing `text_quality` flag or on
   `icpsr_n_words` before using either measure.
3. **Snapshot density is uneven across years** (median 4 snapshot-days, max 297),
   the known scraper-version artifact. The aggregation rule averages over
   snapshots, so a candidate with 297 snapshot-days and one with 1 are weighted
   equally at the candidate level, but the former's mean is far better estimated.
4. **Senate has no ICPSR counterpart**, so those 2,926 rows are standalone; the
   compatibility columns make them comparable in construction, not in coverage.

## Glossary

- **stage** — ICPSR's election round; 1 is the primary, 2 the general. Our
  corpus is general-election only, so all our rows are stage 2.
- **data_source** — whether the page came from ICPSR's dedicated primary scrape
  or their general Wayback scrape. Part of their key; ours is all
  `general_wayback`.
- **snapshot** — one archived capture of a candidate site, comprising the
  homepage and any subpages reached from it (the "pages" of this document).
