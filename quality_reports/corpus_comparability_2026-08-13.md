# Is our corpus comparable to ICPSR 226001's?

> **REVISED 2026-08-13 (same day). The original diagnosis was wrong.**
>
> This report attributed the 51% document-length gap at the 2016/2018 boundary
> to crawl depth. It is not depth. ICPSR's `websites_clean` — the input to both
> `4_complexity.R` and `7_topics.py` — is the output of a **boilerplate filter**
> in `2_website_aggregation.R` that we had never applied. Their text keeps only
> `#+#` segments with at least 10 words containing sentence punctuation and a
> capital letter, and drops all digits. Ours kept navigation, menus and buttons.
>
> Applying their filter closes the gap: their 2016 median `n_char` is 1,624 and
> our 2018 is 1,638, a **0.9% step**. On the 41 overlapping candidates the ratio
> goes from 1.65 to **1.003** and the per-candidate correlation from 0.60 to
> **0.856**. We still capture 8.75× their pages, so depth is real but does not
> propagate: deeper crawling adds boilerplate, and their filter removes it. The
> depth analysis below is retained as description. See "Correction" at the end.

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

## Correction: the length gap is a missing cleaning step, not crawl depth

Added after the analysis above, and it supersedes its central claim.

**What the depth hypothesis predicted, and what happened.** If deeper crawling
drove the length gap, holding depth fixed should shrink it. We built a
homepage-only variant to test exactly that, since ICPSR's 2002–2012 crawl is
effectively homepage-only. Holding depth fixed made the gap **wider**:

| | Their 2016 | Our 2018 | Ratio |
|---|---|---|---|
| Full crawl | 1,624 | 2,452 | 1.51 |
| Homepage only | 1,237 | 2,145 | **1.73** |

Our homepages are themselves 1.7× longer than theirs, so depth was never the
mechanism. That result falsified the hypothesis this report was built on.

**The actual cause.** `2_website_aggregation.R` runs nine steps before anything
is counted:

1. strip URLs
2. normalise `&amp;` and curly apostrophes
3. `str_extract_all("([a-zA-Z.?!\'-,;]|[#\+#])+")` — **keep only letters and
   punctuation; all digits are dropped**
4. split on `#+#` into "tags" (visual components)
5. `n_tags` = number of components
6. **keep only components with ≥10 words that contain `[?!.]` and `[A-Z]`**
7. `n_clean_tags` = number surviving
8. rejoin survivors with spaces, `str_squish`
9. *then* compute `n_char` and `n_words`

Step 6 removes navigation, menus and button labels. Our text never had it
applied, so our counts included that material and theirs did not.

**Why the earlier validation did not catch it.** We validated our coding against
their published values using *their* text — which was already cleaned. That test
cannot detect a missing preprocessing step, because the step had already been
applied to the input. The test was sound; the inference drawn from it was too
broad.

**Evidence that our implementation of their filter is right.** We hold no copy of
their pre-cleaning text, so this is validated indirectly, on five independent
signals:

| Check | ICPSR | Ours |
|---|---|---|
| `n_clean_tags / n_tags`, median | 0.147 | 0.147 |
| median `n_tags` | 44.0 | 46.0 |
| cleaned text containing a digit | 0.000% | 0.000% |
| containing a colon | 0.000% | 0.000% |
| containing a double space | 0.000% | 0.000% |

The digit, colon and double-space rates are structural predictions from reading
their regex, checked on 20,000 of their pages. All hold.

**Effect on the boundary.** Median `n_char` per candidate-year, House
(final figures from the full rebuild):

| | 2014 | 2016 (theirs) | 2018 | 2020 | 2022 | 2024 |
|---|---|---|---|---|---|---|
| uncleaned (as first shipped) | 1,540 | 1,624 | 2,452 | 2,555 | 2,491 | 2,782 |
| **cleaned** | 1,540 | 1,624 | **1,638** | 1,847 | 1,815 | 2,054 |

Their 2016 to our 2018 is a **0.9% step**, against 51% before. Median
`n_clean_tags` matches exactly at 7.0 on both sides of the boundary, and the
upward trend continues sensibly through 2024.

**Effect on the 41 overlapping candidates**, the design that holds candidate and
year fixed:

| Measure | Before cleaning | After cleaning |
|---|---|---|
| `n_char` ratio | 1.65 | **1.003** |
| `n_char` per-candidate corr | 0.60 | **0.856** |
| `n_words` ratio | 1.54 | 1.056 |
| `n_words` per-candidate corr | 0.65 | **0.879** |
| `TTR` ratio / corr | 0.92 / 0.25 | 0.984 / 0.390 |
| `MATTR` ratio / corr | 0.98 / −0.09 | 1.000 / **0.217** |

`n_char` and `n_words` are now comparable both in level and per candidate.
**MATTR is not.** Cleaning lifted its correlation from −0.09 to 0.217, still
weak, so the near-zero figure was not a boilerplate artifact: MATTR is
inherently unstable at the candidate level between two different scrapes,
because it depends on which specific pages each one caught. TTR behaves the
same way at 0.39.

**Why depth stopped mattering.** We still capture 8.75× their pages on the
overlapping candidates, and the length ratio is nonetheless 1.00. Deeper
crawling adds mostly boilerplate, and their filter removes exactly that. This is
the resolution of the whole question: crawl depth is a real difference between
the collections that does not propagate into the coded variables once the
cleaning is applied.

**A new signal the cleaning exposes.** 610 candidate-years (5.8%) have *no* page
surviving the filter — parked domains and navigation-only captures. They are
concentrated in early Senate years (13.3% in 2014, 12.5% in 2012). Their coded
columns are missing rather than zero, which is the honest encoding: we have a
capture but no substantive prose.

**What this changes in the conclusions above.** Recommendation 1 ("do not pool
raw `n_char`/`n_words` across the boundary") stands, but the reason is the
cleaning step, not crawl depth, and it is now fixed rather than a caveat. The
descriptive depth facts — their 2014 break, our swing across scraper versions,
their anomalous 2018 — remain accurate as descriptions. `n_tags` and
`n_clean_tags`, previously called unrecoverable, fall straight out of steps 5
and 7, because our text retains the `#+#` separators theirs had already lost.

## Glossary

- **snapshot-day** — one calendar day on which a candidate's site was captured.
  ICPSR's aggregation averages within the day, then across days.
- **crawl depth** — pages per snapshot-day, i.e. how many subpages the scraper
  followed from the homepage.
- **raw-CDX year** — an office-year scraped before the 3-month deduplication and
  200-snapshot cap were added, so it keeps every snapshot the Wayback CDX API
  returned.
