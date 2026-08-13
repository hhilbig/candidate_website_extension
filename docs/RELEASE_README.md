# U.S. Congressional Candidate Websites, 2002–2024

Archived campaign-website text for U.S. House and Senate general-election
candidates, collected from the Internet Archive's Wayback Machine.

This dataset **extends** Di Tella, Kotti, Le Pennec and Pons (2025), *Keep Your
Enemies Closer* (openICPSR 226001), which covers U.S. House 2002–2018. It adds:

- **House 2018–2024**
- **Senate 2002–2024**

Senate has no counterpart in the original data, so those years are new rather
than an extension.

## Files

| File | Grain | Rows | Size |
|---|---|---|---|
| `raw_corpus.parquet` | snapshot × page | 1,175,582 | 1.4 GB |
| `panel_candidate_year.csv` | candidate × year, with text | 10,601 | 405 MB |
| `panel_icpsr_compat.csv` | candidate × year, coded variables | 10,601 | 16 MB |
| `candidate_crosswalk.csv` | candidate × year, identifiers | 10,601 | 2.4 MB |
| `release_roster.csv` | candidate × year, **all attempted** | 18,250 | 2 MB |

Start with `release_roster.csv` if you care about selection, and
`panel_icpsr_compat.csv` if you want ready-made variables.

## Coverage, and why the roster matters

We captured **10,601 candidate-years, 58% of the 18,250 we attempted** and 60%
of those for which we found a usable campaign URL. Coverage is not random: it
depends on whether a campaign had a website and whether the Wayback Machine
archived it.

`release_roster.csv` lists every candidate we attempted, with `has_url` (a usable
campaign URL was found) and `captured` (at least one page of text was scraped).
**Use it as the denominator.** Without it the corpus looks like a census and is
not one.

| Office | Years | Attempted | Captured | % of those with a URL |
|---|---|---|---|---|
| House | 2018–2024 | 13,175 | 7,675 | 53–65% |
| Senate | 2002–2024 | 5,075 | 2,925 | 44–72% |

An audit found no systematic partisan capture bias (mean Democrat–Republican
difference 1.3 percentage points).

## The coded variables

`panel_icpsr_compat.csv` reproduces the variables from ICPSR 226001 so our years
stack onto theirs. Each was validated by recomputing **their** published values
from **their** text, then applying the identical code to ours.

| Column | Agreement with their published values |
|---|---|
| `icpsr_n_char`, `icpsr_n_words` | exact, 100% |
| `icpsr_ttr_approx` | corr 0.9997, ratio 1.0000 |
| `icpsr_mattr_approx` | corr 0.9995, ratio 0.9999 |
| `icpsr_entropy_approx` | corr 0.9978 |
| `icpsr_n_tags`, `icpsr_n_clean_tags` | median 46 vs 44; 7.0 vs 7.0 at the boundary |
| `icpsr_topic_*` (31) | MAE 0.0054 vs a 0.0296 predict-the-mean baseline |

`_approx` is literal: those are not bit-exact, because the original uses
quanteda's ICU word-boundary tokeniser, which we did not reimplement.

The topic columns come from ICPSR's own classifier — a support-vector machine
trained on Manifesto Project quasi-sentences, not a topic model. They sum to 1
and give issue **salience**, not position.

Columns ending `_home` repeat the same measures using homepage pages only. Use
them when comparing against ICPSR's 2002–2012 years, where their crawl was
effectively homepage-only.

### Read this before pooling across 2016/2017

Text is cleaned exactly as the original does before anything is counted: URLs
stripped, digits dropped, the page split into visual components, and only
components with at least 10 words containing sentence punctuation and a capital
letter retained. That removes navigation, menus and buttons. **Skipping this
step inflates character counts by roughly 1.5× and makes values incomparable.**

After it, the series joins cleanly: their 2016 median `n_char` is 1,624 and our
2018 is 1,638, a 0.9% step.

Two limits from a direct test on 41 candidates present in both collections:

- `n_char` and `n_words` are comparable in level **and** per candidate
  (correlation ≈ 0.87).
- **`TTR` and `MATTR` are not comparable per candidate.** MATTR correlates only
  0.22 across the two collections for the same candidate-year. Use them for
  distributions, not to compare an individual candidate between datasets.

We capture about 8.75× as many pages per snapshot as the original. That does not
propagate into the coded variables, because the extra pages are mostly
boilerplate and the cleaning removes them.

## Known limitations

1. **Selection.** 42% of attempted candidate-years have no text. Use the roster.
2. **Missing rather than zero.** 610 candidate-years (5.8%) have no page
   surviving the cleaning filter — parked domains and navigation-only captures.
   Their coded columns are empty, not 0.
3. **Thin text.** About 10% of captured candidate-years have very little text,
   mostly JavaScript-rendered sites the Wayback Machine archived as empty
   shells. Flagged by `text_quality` in `panel_candidate_year.csv`.
4. **Uneven snapshot density.** Some office-years keep every Wayback snapshot;
   later ones were deduplicated to roughly one per three months. Normalise
   before comparing snapshot counts across years. `icpsr_n_valid_snap` gives the
   count used.
5. **Runaway crawls.** 0.78% of snapshot-days hold 24.5% of all pages. Exclude
   them with `icpsr_runaway_flag`, or set your own threshold with
   `icpsr_max_pages_1day`. The two-level averaging already absorbs most of this.
6. **General election only.** No primary-stage websites, so this does not
   support designs that need primary-election positions.
7. **Party scope.** Democratic and Republican candidates only. The original
   includes third parties.
8. **One spurious record.** `Exon, J James` (NE Senate 2002) appears in the
   corpus but not the roster; he left the Senate in 1997 and was not a 2002
   candidate. Drop it.

## Provenance

Every page row carries `snap_url`, the exact Wayback URL the text came from, so
any observation can be traced to its archived source and re-fetched.

Identifiers: `cand_id` (FEC) is populated for 100% of captured candidate-years;
`bonica_rid` (DIME) for 81%, the gap being 2024, which the DIME release we used
does not cover.

## Terms of use

This dataset is released under a Creative Commons Attribution 4.0 International
(CC BY 4.0) licence, which applies to the compilation, the derived variables,
and the documentation. This matches the licence of openICPSR 226001, from which
this dataset is extended.

The website text is an extract of publicly archived pages captured by the
Internet Archive's Wayback Machine. Each row records `snap_url`, the exact
archived URL from which its text was taken, so every observation is traceable to
its public source and can be re-fetched. The material is redistributed for
non-commercial academic research.

Copyright in the underlying campaign material remains with its authors. If you
are a rights-holder and wish material removed, contact <CONTACT ADDRESS> and we
will act on substantiated requests.

## Citation

If you use this dataset, please cite it and the original:

> Di Tella, R., Kotti, R., Le Pennec, C., and Pons, V. (2025). *Keep Your
> Enemies Closer: Strategic Platform Adjustments during U.S. and French
> Elections.* openICPSR 226001.

## Reproduction

Code: <repository URL>. The pipeline rebuilds every derived file from the raw
corpus. Two inputs are **not** redistributed here because their licences forbid
it, matching the original deposit: the Manifesto Project training corpus (free
registration at manifesto-project.wzb.eu) and the Google Books n-gram
frequencies used for `entropy` (openICPSR 226001).
