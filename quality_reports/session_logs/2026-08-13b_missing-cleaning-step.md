# Session log — 2026-08-13 (part 2: the missing cleaning step)

**Session type:** defect discovery and repair
**Reports:** `quality_reports/corpus_comparability_2026-08-13.md` (revised),
`quality_reports/icpsr_variable_extension_2026-08-13.md` (Result 2b added)

## What happened

Earlier today I reproduced ICPSR's coded variables and shipped them. Later the
same day I found that every one of them was computed on the wrong input.

`websites_clean` — the file both `4_complexity.R` and `7_topics.py` read — is the
**output** of a boilerplate filter in `2_website_aggregation.R`. Before anything
is counted they strip URLs, keep only letters and punctuation (dropping all
digits), split on `#+#` into tags, and keep only tags with **≥10 words containing
sentence punctuation and a capital letter**. `n_tags` and `n_clean_tags` are the
counts either side of that filter.

Our text never had it applied. `n_char` came out ~1.5× too high and TTR, MATTR,
entropy and the topics all inherited the error.

## Why the validation passed anyway

The validation ran our coding against their published values **using their
text**, which was already cleaned. A test built that way cannot detect a missing
preprocessing step: the step had already been applied to the input. The test was
sound; the inference I drew from it — that the coding was therefore correct for
*our* text — was too broad.

**Generalisable lesson:** validating a pipeline stage against someone's
intermediate output only certifies that stage. It says nothing about the stages
upstream of their intermediate. Check what produced the file you are validating
against, not just the file.

## The path that found it

Worth recording because the wrong hypothesis was productive.

1. Observed a 51% length jump at the 2016/2018 boundary; attributed it to crawl
   depth, since we take far more pages per snapshot-day.
2. Wrote the comparability report around that.
3. Built a homepage-only variant to test it, since ICPSR's 2002–2012 crawl is
   effectively homepage-only.
4. **The test falsified the hypothesis**: holding depth fixed *widened* the gap
   from 1.51 to 1.73. Our homepages are themselves 1.7× longer than theirs.
5. That forced the question "what else could differ", which pointed at their
   text-preparation script, which we already had.

The depth work was not wasted — it produced the experiment that ruled depth out.

## Validation without their raw text

We hold no copy of their pre-cleaning text (searched Dropbox, the recession
repo, Downloads; the recession project has only derived `.rds` files). The
recipe is fully explicit, so the implementation was validated on five
independent signals rather than byte-for-byte:

| Check | ICPSR | Ours |
|---|---|---|
| `n_clean_tags / n_tags` median | 0.147 | 0.147 |
| median `n_tags` | 44.0 | 46.0 |
| cleaned text containing a digit | 0.000% | 0.000% |
| containing a colon | 0.000% | 0.000% |
| containing a double space | 0.000% | 0.000% |

The last three are structural predictions from reading their regex, checked on
20,000 of their pages.

## Result

| | Before | After |
|---|---|---|
| boundary step (their 2016 → our 2018) | +51% | **+0.9%** |
| overlap `n_char` ratio | 1.65 | **1.003** |
| overlap `n_char` per-candidate corr | 0.60 | **0.856** |
| overlap `n_words` corr | 0.65 | 0.879 |
| overlap `MATTR` corr | −0.09 | 0.217 |

Median `n_clean_tags` is 7.0 on both sides of the boundary.

**Depth turned out not to matter.** We still capture 8.75× their pages on the
overlapping candidates, and the length ratio is 1.00. Deeper crawling adds
boilerplate; their filter removes exactly that.

**MATTR remains weak per candidate** (0.217). Cleaning did not rescue it, so its
near-zero correlation is inherent rather than a boilerplate artifact: it depends
on which specific pages each scrape caught. Safe for distributions, not for
comparing one candidate across the two corpora.

## Decisions

- `n_tags` and `n_clean_tags` are now shipped. I had called them unrecoverable
  because the `#+#` separators were gone — they are gone from *their* cleaned
  file, and our corpus still has them. I generalised without checking.
- 610 candidate-years (5.8%) have no page surviving the filter — parked domains
  and navigation-only captures, concentrated in early Senate years (13.3% in
  2014). Their coded columns are left **missing rather than zero**: we have a
  capture but no substantive prose.
- The homepage-only variant is kept. It no longer serves its original purpose,
  but it is the depth-fixed comparison for anyone who wants one, and its coverage
  is 91.4% after cleaning.

## Open threads

- Our House 2018 sits slightly low against the trend. Attributed to parked
  domains and runaway crawls, consistent with its January capture spike, but not
  directly tested.
- Rights posture for the Dataverse deposit — still the release blocker.
- Unchanged: mac2 git hygiene, `classify_pages_llm`, OpenFEC-vs-Wikidata
  hit-rate, CJK post-process.
