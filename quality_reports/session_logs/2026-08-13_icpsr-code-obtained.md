# Session log — 2026-08-13 (ICPSR replication code obtained)

**Session type:** implementation (reversal of a prior negative result)
**Report:** `quality_reports/icpsr_variable_extension_2026-08-13.md`

## What prompted it

Yesterday's report concluded that topics, `entropy` and `subordinates` were not
reproducible. I mailed Pons asking for whatever produced `candidates_topics.csv`.
He replied in three hours: the code is in the full replication package,
`7_topics.py`, openICPSR project 226001 under `US/US Code/`.

We had only the `226001-V1` **data** folder, never the code folder.

## What I did

- Downloaded `US-Code` (11 scripts, 34 KB zipped) plus two inputs:
  `ngrams_en_2008.csv` (8.9 MB) and `sub_topics_mapping.csv`. Hanno logged in;
  I did not touch credentials and stopped at the terms-of-use page each time.
- Rewrote `scripts/icpsr_replicate_coding.py` against their actual source.
- Regenerated `panel_icpsr_compat.csv`: 10,601 rows, 48 columns, 0 duplicates,
  0 topic rows failing the sum-to-1 check.

## What their code revealed

- **The aggregation rule I reverse-engineered on 08-12 is exactly right**, now
  confirmed line-for-line against `4_complexity.R`, including `data_source` in
  the key.
- **`n_words` and TTR/MATTR use different tokenizers.** Adopting theirs moved
  TTR from corr 0.9990 / ratio 1.0015 to corr 0.9997 / ratio 1.0000. MATTR
  window 200 confirmed, along with the fallback to TTR below one window.
- **`entropy` is not a document statistic.** It is the term-weighted mean of
  per-word Google Books entropy from `ngrams_en_2008.csv`. That is why every
  document-level Shannon entropy I tried correlated *negatively* with theirs.
- **Their "topic model" is a supervised SVM**, `TfidfVectorizer` + `SVC(rbf,
  C=20, probability=True, seed 2163)` trained on Manifesto quasi-sentences.
  `topic_words.csv` is that classifier applied to each single vocabulary word.
  No linear projection could reproduce an RBF kernel, so my earlier attempt was
  structurally doomed rather than badly tuned.
- **`subordinates`** is POS tag `IN` divided by `n_words`, via openNLP's Maxent
  tagger.

## Method note worth keeping

The Manifesto training corpus is stripped from the deposit for licensing. I
identified the version by **matching vocabulary size against their published
`topic_words.csv`**: theirs 7,668 terms, version 2021-1 gives 7,704 at 98.8%
overlap, 2020-2 gives 7,085, 2019-2 gives 6,036. Rebuilding their documents then
produced exactly 13,020, matching their file count.

## Decisions

- Shipped: `n_char`, `n_words` (exact), `ttr`/`mattr`/`entropy` (`_approx`,
  corr >= 0.997), 31 `icpsr_topic_*` columns (MAE 0.0054 vs 0.0296 baseline).
- Not shipped: `n_tags`/`n_clean_tags` (impossible), `entropy_missing` (corr
  0.952), `subordinates` (corr 0.975 with a different tagger). All are labelling
  judgements and can be produced on request.
- The 08-12 report is kept and marked superseded, since it records what the
  public deposit alone supports.
- The Manifesto corpus is **not committed** — redistribution-restricted, exactly
  as ICPSR excluded it. Fetch with `--fetch-manifesto` and a personal API key.

## Open threads

- **Corpus comparability is untested and is the next task.** Identical coding on
  a systematically different corpus still breaks the series. Sharpest test is the
  House 2018 overlap, 76 candidates in both corpora, holding candidates and year
  fixed. n=76 detects a large shift, not a subtle one.
- Rights posture for the Dataverse deposit. Note their README section 3.1
  asserts the right to publish the scraped website text, which is a closer
  precedent than I credited earlier.
- Unchanged: mac2 git hygiene, `classify_pages_llm` run, OpenFEC-vs-Wikidata
  hit-rate, CJK post-process.
