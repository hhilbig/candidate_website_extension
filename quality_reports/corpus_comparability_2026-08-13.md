# Is our corpus comparable to ICPSR 226001's?

**Date:** 2026-08-13
**Code:** scratch analysis over `data/deliverable/raw_corpus_icpsr.parquet` and
ICPSR's `websites_clean.parquet`
**Companion:** `quality_reports/icpsr_variable_extension_2026-08-13.md`

## Why this was run

We can now compute ICPSR's coded variables on our text and reproduce their
published values almost exactly. That settles the *coding*. It does not settle
whether the numbers are comparable, because identical code applied to a
systematically different corpus still produces a break at the 2016/2018
boundary — and a reader would misread that break as a change in how candidates
campaign.

This report asks a narrower question than "are the corpora the same": **which
scrape-design differences move the coded variables, and by how much.**

## What was compared

Two designs, because each is confounded in a different way.

1. **The House 2018 overlap.** 41 candidates appear in both corpora (matched on
   last name, first name, state, district). Holding the candidate and the year
   fixed means any difference is the scrape. This is the clean design, but it is
   small and it lands on their weakest year.
2. **Corpus-level descriptives by year**, for both collections: pages per
   snapshot-day (crawl depth), snapshot-days per candidate, and the month of
   capture. Confounded by era, but it shows whether the overlap year is
   representative.

Both sides are measured with the same code (`page_measures` and
`icpsr_aggregate` from `scripts/icpsr_replicate_coding.py`), so no difference
below comes from the coding.

## Result 1: their 2018 is anomalous, so do not benchmark on it alone

| Their year | pages/snapshot-day | snapshot-days per candidate | Modal month | Oct–Nov share |
|---|---|---|---|---|
| 2002 | 1.0 | 46 | Oct | 53% |
| 2004 | 1.0 | 11 | Oct | 45% |
| 2006 | 1.0 | 12 | Oct | 48% |
| 2008 | 1.0 | 11 | Oct | 55% |
| 2010 | 1.0 | 9 | Oct | 78% |
| 2012 | 1.0 | 3 | Oct | 44% |
| 2014 | 3.0 | 34 | Oct | 48% |
| 2016 | 3.0 | 13 | Oct | 58% |
| **2018** | **2.0** | **3** | **Jun** | **0%** |

Every normal year of theirs is October-modal with roughly half of captures in
October–November. Their 2018 has **no** October–November captures at all and
peaks in June, on top of being their thinnest year (76 scored candidates against
our 1,764).

This matters procedurally. A first pass comparing our 2018 to their 2018 showed
almost disjoint capture windows and looked like a severe finding. Against their
2002–2016 practice the timing difference is moderate. **The overlap comparison
below therefore gives an upper bound on the difference, not a typical value.**

## Result 2: their own series has an internal break at 2014

Their crawl depth goes from 1.0 pages per snapshot-day in 2002–2012 to 3.0 in
2014–2016. In the earlier years they were capturing essentially the homepage
alone; from 2014 they followed subpages.

So the 2016/2018 boundary is not the only construction break a pooled series
carries. There is one inside ICPSR's own data, and it sits in the middle of
their published range.

## Result 3: our crawl is genuinely deeper, and that is the main real difference

Ours runs 4 to 10 pages per snapshot-day depending on office-year, against their
1.0 early and 3.0 late. This difference holds against their good years, not just
against 2018, and it is the mechanism most likely to drive the 51% jump in
median document length at the boundary. More subpages means more text per
snapshot.

## Result 4: our capture timing is internally inconsistent

| Our office-year | January | Oct–Nov | pages/snapshot-day |
|---|---|---|---|
| house 2018 | **38.9%** | 29.4% | 4.0 |
| house 2020 | 4.1% | 55.2% | 6.0 |
| house 2022 | 3.5% | 8.2% | 4.0 |
| house 2024 | 0.5% | 6.3% | 4.0 |
| senate 2002–2024 | 1.6–14.5% | 11.1–46.8% | 2.0–10.0 |

The October–November share swings from 6.3% to 55.2% across our own years. The
low-concentration years (house 2022 and 2024 at 8.2% and 6.3%) are the ones
built with the 3-month deduplication and 200-snapshot cap; the raw-CDX years
keep Wayback's natural density, which peaks near the election. This is the same
scraper-version artifact already documented for snapshot density, showing up in
the time dimension.

Practical consequence: our late-campaign coverage is not constant across our own
years, so *our* series has internal breaks too.

## Result 5: the January 2018 spike is a crawl artifact, not a timing bias

House 2018 shows 38.9% of captures in January, far outside every other
office-year. It is not a broad early-cycle bias. On 2018-01-13 alone, **32
candidates produced 26,226 pages, about 820 pages each**. A small number of
sites yielded enormous page counts on single days.

That led to a general check of page concentration across the whole corpus:

| Pages per candidate snapshot-day | |
|---|---|
| median | 5 |
| 90th percentile | 25 |
| 99th percentile | 93 |
| 99.9th percentile | 279 |
| maximum | 20,946 |

**0.78% of snapshot-days (729 of 93,509) hold 24.5% of all pages**, and the top
1% of candidates hold 40.3%. Pages on those runaway days are not filler: their
mean `n_char` is 7,291 against 4,167 elsewhere.

The two-level aggregation limits the damage — a snapshot-day contributes one
value to the candidate mean regardless of how many pages it holds — so a
20,946-page day does not swamp the candidate-level number. But for those
candidates the day's mean is taken over a very large and probably repetitive
page set.

## Result 6: the overlap comparison, 41 candidates

| Measure | ICPSR | Ours | Ratio | Per-candidate corr |
|---|---|---|---|---|
| pages per snapshot | 2.0 | 5.2 | 2.61 | 0.76 |
| snapshot-days | 3.0 | 15.0 | 5.00 | 0.19 |
| `n_char` | 1,542 | 2,538 | 1.65 | 0.60 |
| `n_words` | 253 | 389 | 1.54 | 0.65 |
| `TTR` | 0.652 | 0.600 | 0.92 | 0.25 |
| `MATTR` | 0.683 | 0.668 | **0.98** | **−0.09** |

The MATTR row carries the most important message, and it cuts against the advice
in the previous report. Its **medians agree to within 2%**, which is why MATTR is
the right choice for comparing distributions across the boundary. But its
**per-candidate correlation is essentially zero**. For the same candidate in the
same year, the two corpora give unrelated MATTR values.

The two facts are consistent: MATTR is length-robust by construction, so the
aggregate is stable, but at the individual level it is dominated by which
specific pages each scraper happened to capture.

## What this means for the release

1. **Do not pool raw `n_char` or `n_words` across the 2016/2018 boundary**
   without adjustment. The 1.5–1.65 ratio is largely crawl depth, not candidate
   behaviour.
2. **Use MATTR for distributional comparisons across years, never for
   candidate-level comparison between the two corpora.** The previous report
   recommended MATTR without this second half; that recommendation was
   incomplete.
3. **Document that both corpora have internal breaks**: theirs at 2014 (crawl
   depth 1.0 → 3.0), ours across scraper versions (capture timing and snapshot
   density).
4. **Consider shipping a page-count flag** so users can exclude runaway
   snapshot-days. 0.78% of snapshot-days holding 24.5% of pages is a defensible
   robustness filter.

## Limits of this analysis

- **n = 41.** Detects large systematic shifts; a subtle one would not survive.
- The overlap lands on **their worst year**, so the ratios above overstate what
  you would see against 2014–2016.
- Name matching is on last name, first name, state and district. 41 of their 76
  candidates matched; the remaining 35 are unmatched names rather than known
  non-overlaps, so the overlap set is not a random sample of their 2018.
- Nothing here tests topic values across corpora. That would need their 2018
  topic values, which exist for only 76 candidates.

## Glossary

- **snapshot-day** — one calendar day on which a candidate's site was captured.
  ICPSR's aggregation averages within the day, then across days.
- **crawl depth** — pages per snapshot-day, i.e. how many subpages the scraper
  followed from the homepage.
- **raw-CDX year** — an office-year scraped before the 3-month deduplication and
  200-snapshot cap were added, so it keeps every snapshot the Wayback CDX API
  returned.
