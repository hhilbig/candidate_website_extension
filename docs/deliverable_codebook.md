# Deliverable codebook: ICPSR/DIME-compatible candidate-website data

**Built:** 2026-07-01 · **Updated:** 2026-08-14 · **Scope:** House 2018–2024 +
Senate 2002–2024 (extends ICPSR 226001, Di Tella/Kotti/Le Pennec/Pons).
**Ideology scoring is deferred.** No ideology or populism score columns are
produced. Topics **are** reproduced, using the original authors' classifier
obtained from their replication package; the earlier note that they were
unreproducible referred to the data-only download and no longer applies.

## Products (droplet `data/deliverable/`)

| File | Grain | Rows | Committed? |
|---|---|---|---|
| `raw_corpus_icpsr.parquet` | snapshot × page (ICPSR `websites_clean` schema) | 1,101,303 | no (1.5 GB; regenerable) |
| `panel_candidate_year_icpsr.csv` | candidate × year (pre-score, **with text**) | 9,944 | no (424 MB) |
| `panel_candidate_year_icpsr_meta.csv` | same, **without `text`** | 9,944 | yes (3 MB) |
| `candidate_crosswalk.csv` | candidate × year — the id/convention bridge | 9,944 | yes (2.5 MB) |
| `panel_icpsr_compat.csv` | candidate × year — ICPSR-compatible coded columns | 9,944 | yes (1.6 MB, copy at `quality_reports/coverage_audit/csv/`) |

All are regenerable in ~8 min via the pipeline in "Regeneration" below.

## ICPSR-compatible coded columns (`panel_icpsr_compat.csv`)

Added 2026-08-12 by `scripts/icpsr_replicate_coding.py`. This is a **separate,
join-able table**: it adds nothing to and removes nothing from the existing
products. Join on `(candidate_icpsr, state, office, year)`, unique, 0 duplicates.

Every column was validated by reproducing ICPSR's *own published values* from
ICPSR's *own text* before being applied to ours. Full evidence:
`quality_reports/icpsr_variable_extension_2026-08-12.md`.

- `icpsr_n_char` — mean characters. **Exact** reproduction (100.00%).
- `icpsr_n_words` — mean words, `re.findall(r'\w+', text.lower())`. **Exact**
  (100.00%).
- `icpsr_ttr_approx` — type-token ratio. corr 0.9997, median ratio 1.0000.
- `icpsr_mattr_approx` — moving-average TTR, **window 200**, falling back to TTR
  for documents under one window. corr 0.9995, median ratio 0.9999.
  TTR/MATTR use a different tokenizer from `n_words` (strip `'s`, drop
  punctuation/symbols/numbers/URLs, lowercase, keep `^[a-zA-Z]+$`), which is why
  they are near-exact rather than exact: quanteda tokenises on ICU word
  boundaries.
- `icpsr_entropy_approx` — **not** a document statistic. It is the
  term-count-weighted mean of *per-word* Google Books entropy, from
  `ngrams_en_2008.csv`. corr 0.9978, median ratio 0.9997.
- `icpsr_topic_*` — 31 columns, one per consolidated Manifesto category, summing
  to 1. Produced by ICPSR's own classifier: `TfidfVectorizer` + `SVC(rbf, C=20,
  probability=True, random_state=2163)` trained on Manifesto Project
  quasi-sentences (corpus version 2021-1). MAE 0.0054 against their published
  values, versus 0.0296 for a predict-the-mean baseline; top-1 agreement 88%.
  **Note the topic document rule differs from the complexity rule**: all page
  text for the candidate-year is concatenated with spaces, and `data_source` is
  not part of the key.

  **These measure issue attention, not position, and not within-party
  ideology.** Pooled, they track DIME CF-scores in the expected directions
  (Welfare State r = −0.27, Equality −0.25, Constitutionalism +0.17). Within
  party the relationship nearly vanishes: mean |r| ≈ 0.03, and among Republicans
  the sign agrees with the pooled direction 52% of the time, i.e. chance.
  The same holds in ICPSR's own data. Their published topics correlate with
  their own text-derived ideology score at mean |r| = 0.033 within Democrats and
  0.049 within Republicans, so this is a limit of the instrument, not of this
  extension. Ours in fact reach a higher pooled correlation against an
  *external* measure (0.27) than theirs do against their own internal one (0.16).
- `icpsr_n_valid_snap`, `icpsr_n_valid_pages` — snapshot-days and pages entering
  the aggregate.
- `icpsr_max_pages_1day` — largest page count on any single snapshot-day for this
  candidate-year. Median 5, p90 29, p99 142, maximum 20,946.
- `icpsr_share_pages_runaway` — share of this candidate-year's pages that came
  from snapshot-days with more than 100 pages.
- `icpsr_runaway_flag` — `icpsr_max_pages_1day > 100`. True for 171
  candidate-years (1.72%), concentrated in the raw-CDX years (senate 2002 6.5%,
  senate 2010 5.8%, senate 2006 5.3%).

  A small number of candidate-days produced runaway crawls: 0.78% of
  snapshot-days hold 24.5% of all pages. The two-level mean already absorbs most
  of this, since a day contributes one value however many pages it holds:
  dropping flagged rows moves median `icpsr_n_char` only from 2,558 to 2,530.
  Treat the flag as a robustness filter, not a required correction. The raw
  maximum is exposed so you can set your own threshold rather than inheriting
  the 100-page one.

**Prefer `icpsr_mattr_approx` over `icpsr_ttr_approx` for cross-year
*distributions*, but not for candidate-level comparison between the two
corpora.** TTR falls mechanically as documents lengthen; median document length
rises 51% at the 2016/2018 boundary, moving TTR 6.9% but MATTR only 2.5%. On the
41 candidates present in *both* corpora, however, MATTR medians agree to 2%
while the **per-candidate correlation is about zero**. For one candidate in one
year the two collections give unrelated values, because MATTR is dominated by
which specific pages each scraper captured. See
`quality_reports/corpus_comparability_2026-08-13.md`.

**Do not pool raw `icpsr_n_char` / `icpsr_n_words` across the 2016/2018
boundary** without adjustment. Ours run 1.5–1.65× theirs on the same candidates,
driven mainly by crawl depth (we take 4–10 pages per snapshot-day, they take
1.0 in 2002–2012 and 3.0 in 2014–2016), not by candidate behaviour. Note their
own series also breaks internally at 2014 when their crawl depth changed.

**Filter short documents first.** 20.8% of candidate-years have <200 words, i.e.
below one MATTR window, so their MATTR degrades to plain TTR; 224 rows (2.1%)
have TTR = MATTR = 1.0. Use the existing `text_quality` flag or `icpsr_n_words`.

### The aggregation rule (resolved 2026-08-12)

ICPSR's snapshot → candidate rule, previously recorded here as unknown, is:

1. drop pages with `n_char == 0`
2. mean over pages within a snapshot `date`
3. mean over snapshot dates

Verified **exact on 13,020 of 13,020 rows (100.00%)** for `n_char`, `n_words`,
`n_tags`, `n_clean_tags`, in every year and stage. Their candidate grain includes
`data_source`: the same candidate-year-stage can appear as both `primary` and
`general_wayback`, distinguished only by name casing (`ADAMS, SANDY` vs
`Adams, Sandy`). Our `date` is a `YYYYMMDDHHMMSS` timestamp and is truncated to
the day before aggregating, to match their day-level snapshot grain.

### Text cleaning (applied before every measure)

ICPSR's `websites_clean` is the **output** of a boilerplate filter in
`2_website_aggregation.R`, and both `4_complexity.R` and `7_topics.py` read that
cleaned file. Every column above is therefore computed on cleaned text:

1. strip URLs; normalise `&amp;` and curly apostrophes
2. keep only letters and punctuation, so **all digits are dropped**
3. split on `#+#` into "tags" (visual components) → `icpsr_n_tags`
4. keep only tags with **≥10 words containing `[?!.]` and `[A-Z]`** →
   `icpsr_n_clean_tags`
5. rejoin the survivors, then count

Step 4 removes navigation, menus and button labels. Skipping it inflates
`n_char` by roughly 1.5× and makes the values incomparable to theirs.

Validated indirectly (we have no copy of their pre-cleaning text): the surviving
ratio `n_clean_tags/n_tags` matches at the median (0.147 both), median `n_tags`
46 vs their 44, and their cleaned text contains no digits, colons or double
spaces across 20,000 sampled pages.

### Not shipped

- **`entropy_missing`** — corr 0.952, below the 0.99 bar used for every other
  column. It is the share of tokens absent from the Google Books dictionary, so
  it is the most tokenizer-sensitive measure, and it is a diagnostic.
- **`subordinates`** — the definition is known (POS tag `IN` divided by
  `n_words`) but ICPSR used openNLP's Maxent tagger; nltk's perceptron tagger
  gives corr 0.9752. That is a related measure from a different model, not
  theirs.

All three can be produced on request, clearly labelled.

### Inputs and licensing

`--topics` needs the Manifesto Project training corpus, which is
**redistribution-restricted** and therefore not committed. ICPSR excluded it
from their own deposit for the same reason. Fetch it with
`--fetch-manifesto` and your own API key at `~/.manifesto_api_key`.
`ngrams_en_2008.csv` and `sub_topics_mapping.csv` come from openICPSR project
226001 and live under `data/external/` (gitignored).

## How to merge

- **To DIME:** join `cand_id` → DIME `Cand.ID` (exact 1:1), or use the carried
  **`bonica_rid`**. 100% of rows have `cand_id`; 81% have `bonica_rid` (the gap
  is 2024 plus a few 2022, because local DIME ends at 2022. Use a newer release for
  those).
- **To the ICPSR website panel (House):** **vertical stack** — our House
  2018–2024 rows append below ICPSR's House ≤2016. No year overlap (we replace
  ICPSR's sparse 2018: their 76 scored D/R vs our 1,667). Shared keys:
  `candidate_icpsr` / `name_key` / `candidate_year_stage` / `state` /
  `district_id` / `year` / `stage` / `party_icpsr` / `data_source`.
- **To county/district panels (House):** via `district_id` × `year`, exactly as
  `great-recession-proj/R/build/main/clean_data_49b_website_district_merge.R`.
- **Senate is standalone** — same conventions, links to DIME via `cand_id`
  (`seat == federal:senate`), but has **no `district_id`** (state-based); it has
  no county/district framework in the parent project.

## Column dictionary

### `candidate_crosswalk.csv` / `panel_candidate_year_icpsr_meta.csv`
Identity & keys:
- `candidate` — our original cleaned name (provenance only).
- `candidate_icpsr` — **"Last, First Middle Suffix"** (ICPSR `candidates_scores`
  convention), built from the FEC raw name.
- `name_key` — **lowercase snake `lastname_firstname`** (ICPSR
  `websites_clean` internal key).
- `candidate_year_stage` — `candidate_icpsr + "_" + year + "_2"` (ICPSR key).
- `cand_id` — FEC candidate id (e.g. `H8TX22106`). **100% populated.**
- `bonica_rid` — DIME recipient id (e.g. `cand140755`). 81% populated.
- `ICPSR`, `NID` — legislator / OpenSecrets ids from DIME (when matched).
- `fec_raw_name` — FEC "LAST, FIRST" source string (crosswalk only).
Geography / office:
- `state` — 2-letter. `district` / `district_num` — raw / numeric district.
- `district_id` — House **`ST##`** (at-large = `01`); **Senate = NA**.
- `office` — house/senate. `seat` — DIME `federal:house|senate` (when matched).
- `year` — election year. `stage` — 2 (D/R general only).
- `party` — original R/D. `party_icpsr` — **democrat/republican**.
- `cand_ici` — FEC incumbency (I/C/O). `data_source` — `general_wayback`.
DIME covariates (when matched): `cfscore` (static CF-score),
`cfscore_dyn` (year-specific), `dwnom1`/`dwnom2` (DW-NOMINATE), `dime_district`,
`dime_ico`.
Text quality (panel_meta only): `text_quality` (usable/thin/empty),
`n_char`, `n_words`, `n_snapshots_available`, `n_pages`, `sel_date`,
`page_types`, `urlkey`.

### `panel_candidate_year_icpsr.csv`
All of the above **plus `text`**, the concatenated longest-text snapshot
(pages joined by `#+#`, homepage first). One text blob per candidate-year.

### `raw_corpus_icpsr.parquet` (ICPSR `websites_clean` schema + our ids)
`candidate_icpsr, name_key, cand_id, bonica_rid, candidate_year_stage, state,
district, district_id, office, year, party (democrat/republican), stage,
data_source, date (Wayback YYYYMMDDHHMMSS), urlkey, snap_url, page_type,
n_char, n_words, n_tags, n_clean_tags (legacy=0), n_snap (distinct snapshots
for the candidate-year), text_snap_content`. One row per scraped page across
all retained snapshots: the un-collapsed corpus for re-scoring.

## Decisions & caveats

- **Ideology scoring deferred.** No `score`/`nuance_score`/`mnir` columns. To
  add: obtain the full ICPSR package (DOI 10.3886/E232001V1, `US/US Code/`
  `3_ideology_scaling.R`). Note their `word_scores.csv` is refit **per year and
  stops at 2018**, so it cannot be applied to 2020–2024 without a construction
  break, and refitting needs primary-stage text we never collected.
- **The snapshot→candidate aggregation rule is now KNOWN** (resolved 2026-08-12,
  verified exact on all 13,020 ICPSR rows; see the section above). Score the
  `raw_corpus_icpsr.parquet` under that rule, not our longest-text panel, to
  avoid a construction break at the 2016→2018 boundary.
- **DIME ≤2022.** 2024 (and a few 2022) candidates lack `bonica_rid`/cfscore
  (2024 match ≈57%). Refresh with a newer DIME release when scoring.
- **2018 = ours** (ICPSR's 2018 is too sparse). **Senate standalone** (no
  district framework). **At-large** House districts → `district_id` `ST01`; one
  House candidate has NA `district_id` (unparseable FEC district).
- **`*_preelec`** (9 columns): every coded measure recomputed on pages archived
  on or before election day, defined as the Tuesday after the first Monday in
  November. 22.6% of pages post-date the vote, rising to 42.8% for House 2020,
  and a lapsed or hijacked domain is no longer campaign material. Present for
  92.1% of candidate-years; missing where a candidate has no pre-election
  snapshot (242 cases beyond the 546 with no text at all). It correlates 0.985
  with the all-year value and moves the yearly median by under 2% except Senate
  2006 (-8.3%) and Senate 2018 (-4.2%), so it matters for individual candidates
  more than for aggregates. Use it when the question is about the campaign
  rather than the calendar year.
- **`icpsr_first_snap_day`**, **`icpsr_last_snap_day`**,
  **`icpsr_snap_span_days`**: when in the cycle the candidate was observed, as
  YYYYMMDD integers and the span in days. The median span is 248 days, so a
  candidate-year usually pools captures across most of the year rather than
  representing one moment. Per-page timestamps are in the page-level corpus.

- **`on_ballot`** / **`general_votes`** (in `release_roster.csv` and
  `panel_icpsr_compat.csv`): did this candidate appear on the general election
  ballot, and with how many votes. Matched to the MIT Election Data and Science
  Lab returns (House doi:10.7910/DVN/IG0UN2, Senate doi:10.7910/DVN/PEJ5QU) on
  state, district and year, then on surname with a compatible first name. The
  returns write "DEBBIE WASSERMAN SCHULTZ" where the roster writes "Wasserman
  Schultz, Debbie", so the last one, two and three name tokens are each tried as
  the surname; without that, every multi-word surname was missed (91 candidates,
  including Ocasio-Cortez and McMorris Rodgers). Scoring every unmatched row
  against the names in its own race leaves 13 close pairs the rule still misses,
  mostly married and maiden name variants, so sensitivity is about 99%.
  Only 3,037 of 9,944 captured candidate-years are on the ballot; the rest filed
  with the FEC and lost or left a primary. Capture rates differ sharply between
  the two groups (84% against 52%), so restrict on this column before treating
  the corpus as a sample of general election candidates. `general_votes` is
  missing where `on_ballot` is false. The panel's match uses clean "Last, First"
  names and is the more reliable of the two; the roster's uses raw FEC display
  names, which are noisier.

- **`text_quality`** (usable 85.1% / thin 9.4% / empty 5.5%): thin/empty are
  JS-rendered or placeholder/parked sites whose text Wayback did not archive,
  plausibly missing-not-at-random; filter or robustness-test, don't impute.
- **No column codebook exists for ICPSR's own score columns** (`score` vs
  `score_all`, `nuance_score`, `mnir`), which the download does not document.

## Regeneration

On the droplet (repo root, venv):
1. `python scripts/recover_fec_ids.py` → `data/deliverable/fec_id_crosswalk.csv`
2. locally: `Rscript scripts/export_dime_subset.R /tmp/dime_cand_subset.csv`;
   `scp` to droplet `data/dime_cache/`
3. `pip install pyarrow` (once); `python scripts/harmonize_deliverable.py`
