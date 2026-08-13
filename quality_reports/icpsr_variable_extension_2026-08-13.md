# Extending ICPSR 226001's coded variables to our corpus

**Date:** 2026-08-13 (supersedes the 2026-08-12 report)
**Plan:** `quality_reports/plans/2026-08-12_icpsr-variable-extension.md`
**Code:** `scripts/icpsr_replicate_coding.py`
**Output:** `data/deliverable/panel_icpsr_compat.csv`

## Why this was run, and what changed mid-way

We want to release our corpus (House 2018–2024, Senate 2002–2024) as a dataset
that stacks onto ICPSR 226001 and carries the same coded variables, computed the
same way. ICPSR ships coded variables but not text; we have text but no coded
variables.

The 2026-08-12 report concluded that the topic family, `entropy`, and
`subordinates` were **not reproducible** from the public deposit. That
conclusion was correct about the deposit and wrong about the world. Vincent Pons
replied on 2026-08-13 pointing to `7_topics.py` in the full replication package
(openICPSR project 226001, `US/US Code/`). With their code in hand, all three
turned out to be reproducible, and the reason my reconstruction had failed
became obvious.

This report supersedes the earlier one. The aggregation-rule finding stands
unchanged and is now confirmed against their source.

## What was compared

Every validation number below comes from the same loop: take **ICPSR's own
text** (`websites_clean.parquet`, 1,026,318 pages, House 2002–2018), run our
reimplementation over it, and compare against **their own published values**
(13,020 candidate-year rows). Our scrape plays no part. That isolates the coding
step: a mismatch cannot be blamed on a different scrape.

Our corpus enters only at the `--apply` stage, after the coding is validated.

## Result 1: the aggregation rule, confirmed

Derived from their data alone on 2026-08-12, then confirmed line-for-line
against `4_complexity.R`:

```r
group_by(candidate, state, district, year, data_source, stage, date, party) %>%
  summarise_all(mean) %>% select(-date) %>%
group_by(candidate, state, district, year, data_source, stage, party) %>%
  summarise_all(mean)
```

Drop empty pages, mean over pages within a snapshot day, mean over snapshot
days. Reproduces **13,020 of 13,020 rows exactly (100.00%)** on `n_char`,
`n_words`, `n_tags`, `n_clean_tags`, every year and stage, zero residual.

`data_source` is part of the key. The same candidate-year-stage appears as both
`primary` and `general_wayback`, distinguished **only by the casing of the
candidate name** (`ADAMS, SANDY` vs `Adams, Sandy`). Keying without it silently
corrupts 18 rows.

## Result 2: what reproduces, and how well

| Variable | Agreement | Shipped as |
|---|---|---|
| `n_char` | exact, 100.00% | `icpsr_n_char` |
| `n_words` | exact, 100.00% | `icpsr_n_words` |
| `TTR` | corr 0.9997, ratio 1.0000 | `icpsr_ttr_approx` |
| `MATTR` | corr 0.9995, ratio 0.9999 | `icpsr_mattr_approx` |
| `entropy` | corr 0.9978, ratio 0.9997 | `icpsr_entropy_approx` |
| topics (31) | MAE 0.0054 vs 0.0296 baseline | `icpsr_topic_*` |

Three of these were only solvable with their code:

**`n_words`** is `str_count(t, '\w+')`, which I had already found empirically.
But **TTR and MATTR use a different tokenizer** in the same script: strip `'s`,
remove punctuation, symbols, numbers and URLs, lowercase, then keep only
`^[a-zA-Z]+$`. Adopting it moved TTR from corr 0.9990 / ratio 1.0015 to corr
0.9997 / ratio 1.0000, eliminating the bias. The MATTR window is **200**,
confirming the value I had found by search, with a documented fallback to TTR
for documents under one window.

**`entropy` is not a document statistic at all.** It is the term-count-weighted
mean of *per-word* Google Books entropy, read from `ngrams_en_2008.csv`:

```r
entropy <- (snap_dfm_nona %*% weights_nona$entropy) / rowSums(snap_dfm_nona)
```

That is why every document-level Shannon entropy I tried correlated
*negatively* with theirs. Given the right input file it reproduces at 0.9978.

**Topics are a supervised classifier, not a topic model.** `7_topics.py` trains
`TfidfVectorizer` + `SVC(kernel='rbf', C=20, probability=True,
random_state=2163)` on Manifesto Project quasi-sentences and calls
`predict_proba`. `topic_words.csv` is that classifier applied to each single
vocabulary word, and `tfidf_weight` is the vocabulary index. No linear
projection of per-word probabilities could ever reproduce an RBF-kernel SVM,
which is why the earlier attempt failed at MAE 0.0272 against a 0.0302 baseline.

Identifying the training corpus took a version sweep, since the Manifesto data
is stripped from the deposit for licensing reasons. Their vocabulary has 7,668
terms; corpus version **2021-1** yields 7,704 with 98.8% overlap, while 2020-2
yields 7,085 (90.7%) and 2019-2 only 6,036 (77.0%). Rebuilding their documents
under their own rule — `groupby(candidate, year, state, district, stage)` joined
by spaces — produces exactly **13,020 documents, matching their file count**.

Note the topic document rule is *different* from the complexity rule: plain text
concatenation, and `data_source` is not in the key.

## Result 3: what is still not shipped

- **`n_tags`, `n_clean_tags`** — HTML-structure counts fixed at scrape time. The
  archived HTML is gone and the cleaned text has `#+#` stripped. Not recoverable
  in principle.
- **`entropy_missing`** — reproduces at corr 0.952, below the 0.99 bar applied
  to everything else. It is the share of tokens absent from the Google Books
  dictionary, so it is the measure most sensitive to tokenizer edge cases, and
  it is a diagnostic rather than a substantive variable.
- **`subordinates`** — the definition is now known (count of POS tag `IN`
  divided by `n_words`), but ICPSR used openNLP's Maxent tagger. nltk's averaged
  perceptron tagger gives corr 0.9752 on a 60-candidate sample. That is a
  related measure computed with a different model, not their variable.

Each is a judgement about labelling, not capability. Any of them can be shipped
on request, clearly marked.

## Caveats for downstream use

1. **Use MATTR, not TTR, across years.** At the 2016/2018 boundary median
   document length rises 51%; TTR falls mechanically with length, MATTR does not.
2. **MATTR degenerates on short documents.** 20.8% of candidate-years have fewer
   than 200 words, one MATTR window, so their MATTR falls back to plain TTR.
3. **Not bit-exact is not the same as wrong.** TTR, MATTR and entropy have
   ratios of 1.000 and correlations above 0.997. The residual is quanteda's ICU
   word-boundary tokenizer, which I chose not to reimplement.
4. **Topic values are close but not identical**, because the Manifesto corpus
   version differs by 1.2% of vocabulary and the SVM's Platt scaling is fitted
   by internal cross-validation. Top-1 topic agreement is 88%.

## Open: is our corpus comparable to theirs?

Identical coding on a systematically different corpus still produces a break at
the boundary, and a reader would misread that break as a change in candidate
behaviour. Three differences are already known:

- median `n_char` jumps 51% at 2016/2018, larger than their own 2002–2016 trend
- our snapshot density is uneven (median 4 snapshot-days per candidate, max 297)
- they carry third parties (836 libertarian, 317 green, 251 independent); we are
  pure R/D

This is not yet tested. The sharpest available test is the **House 2018
overlap**: 76 candidates appear in both corpora, so running our pipeline on our
2018 text and comparing to their 2018 published values holds candidates and year
fixed and varies only the scrape. With n=76 it can detect a large systematic
shift, not a subtle one. Tracked as a follow-up.

## Reproducing this

```
python scripts/icpsr_replicate_coding.py --validate          # against ICPSR
python scripts/icpsr_replicate_coding.py --fetch-manifesto   # needs API key
python scripts/icpsr_replicate_coding.py --apply --topics    # our corpus
```

**The Manifesto training corpus is redistribution-restricted** and is therefore
not committed, exactly as ICPSR excluded it from their own deposit. Fetch it
with your own Manifesto Project API key, stored at `~/.manifesto_api_key`. The
Google Books ngram file and the sub-topic mapping come from openICPSR project
226001 and live under `data/external/` (gitignored).

## Glossary

- **quasi-sentence** — the Manifesto Project's unit of coding: a sentence or
  clause assigned one CMP category. The classifier's training rows.
- **CMP code** — Manifesto Project category code, e.g. `503`. `sub_topics_mapping.csv`
  collapses 81 of them into the 31 consolidated topics.
- **stage** — ICPSR's election round; 1 primary, 2 general. Our corpus is
  general-election only.
- **snapshot** — one archived capture of a candidate site, comprising the
  homepage and the subpages reached from it.
