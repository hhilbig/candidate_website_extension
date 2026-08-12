# Deliverable codebook — ICPSR/DIME-compatible candidate-website data

**Built:** 2026-07-01 · **Updated:** 2026-08-12 (ICPSR-compatible coded columns)
· **Scope:** House 2018–2024 + Senate 2002–2024 (extends ICPSR 226001, Di
Tella/Kotti/Le Pennec/Pons). **Ideology scoring is deferred** — no
ideology/populism score columns are produced. **Topics are not reproducible**
from the shipped ICPSR artifacts (see below).

## Products (droplet `data/deliverable/`)

| File | Grain | Rows | Committed? |
|---|---|---|---|
| `raw_corpus_icpsr.parquet` | snapshot × page (ICPSR `websites_clean` schema) | 1,175,582 | no (1.5 GB; regenerable) |
| `panel_candidate_year_icpsr.csv` | candidate × year (pre-score, **with text**) | 10,601 | no (424 MB) |
| `panel_candidate_year_icpsr_meta.csv` | same, **without `text`** | 10,601 | yes (3 MB) |
| `candidate_crosswalk.csv` | candidate × year — the id/convention bridge | 10,601 | yes (2.5 MB) |
| `panel_icpsr_compat.csv` | candidate × year — ICPSR-compatible coded columns | 10,601 | yes (1.6 MB, copy at `quality_reports/coverage_audit/csv/`) |

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
- `icpsr_ttr_approx` — type-token ratio. **Approximate**: corr 0.9990, median
  ratio 1.0015 vs their `TTR`. Not bit-exact; their TTR comes from a different
  preprocessing stage than their `n_words`.
- `icpsr_mattr_approx` — moving-average TTR, **window 200** (fitted against
  their values). corr 0.9982, median ratio 1.0000 vs their `MATTR`.
- `icpsr_n_valid_snap`, `icpsr_n_valid_pages` — snapshot-days and pages entering
  the aggregate.

**Prefer `icpsr_mattr_approx` over `icpsr_ttr_approx` for cross-year work.** TTR
falls mechanically as documents lengthen; median document length rises 51% at the
2016/2018 boundary between their data and ours, moving TTR 6.9% but MATTR only
2.5%.

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

### Not reproducible

- **Topics** (31 Manifesto-style proportions). `topic_words.csv` alone is
  insufficient: `tfidf_weight` is an alphabetical column index rather than a
  weight, the idf vector and the model's document-inference step are absent, and
  `websites_topics/`, `topics_complexity/`, `embeddings/` are empty in the
  download. Best reconstruction scored MAE 0.0272 against a 0.0302
  predict-the-mean baseline. Would need the full package (DOI 10.3886/E232001V1).
- **`entropy`, `subordinates`** — not recoverable from text; their `entropy`
  correlates *negatively* (-0.32) with word-frequency entropy, so it is likely
  syntactic. Omitted rather than approximated.
- **`n_tags`, `n_clean_tags`** — HTML-structure counts fixed at scrape time; the
  cleaned text has the `#+#` separators stripped and the archived HTML is gone.

## How to merge

- **To DIME:** join `cand_id` → DIME `Cand.ID` (exact 1:1), or use the carried
  **`bonica_rid`**. 100% of rows have `cand_id`; 81% have `bonica_rid` (the gap
  is 2024 + a few 2022 — local DIME ends at 2022; use a newer DIME release for
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
All of the above **plus `text`** — the concatenated longest-text snapshot
(pages joined by `#+#`, homepage first). One text blob per candidate-year.

### `raw_corpus_icpsr.parquet` (ICPSR `websites_clean` schema + our ids)
`candidate_icpsr, name_key, cand_id, bonica_rid, candidate_year_stage, state,
district, district_id, office, year, party (democrat/republican), stage,
data_source, date (Wayback YYYYMMDDHHMMSS), urlkey, snap_url, page_type,
n_char, n_words, n_tags, n_clean_tags (legacy=0), n_snap (distinct snapshots
for the candidate-year), text_snap_content`. One row per scraped page across
all retained snapshots — the un-collapsed corpus for re-scoring.

## Decisions & caveats

- **Ideology scoring deferred.** No `score`/`nuance_score`/`mnir` columns. To
  add: obtain the full ICPSR package (DOI 10.3886/E232001V1, `US/US Code/`
  `3_ideology_scaling.R`). Note their `word_scores.csv` is refit **per year and
  stops at 2018**, so it cannot be applied to 2020–2024 without a construction
  break, and refitting needs primary-stage text we never collected.
- **The snapshot→candidate aggregation rule is now KNOWN** (resolved 2026-08-12,
  verified exact on all 13,020 ICPSR rows — see the section above). Score the
  `raw_corpus_icpsr.parquet` under that rule, not our longest-text panel, to
  avoid a construction break at the 2016→2018 boundary.
- **DIME ≤2022.** 2024 (and a few 2022) candidates lack `bonica_rid`/cfscore
  (2024 match ≈57%). Refresh with a newer DIME release when scoring.
- **2018 = ours** (ICPSR's 2018 is too sparse). **Senate standalone** (no
  district framework). **At-large** House districts → `district_id` `ST01`; one
  House candidate has NA `district_id` (unparseable FEC district).
- **`text_quality`** (usable 85.1% / thin 9.4% / empty 5.5%): thin/empty are
  JS-rendered or placeholder/parked sites whose text Wayback did not archive —
  plausibly missing-not-at-random; filter or robustness-test, don't impute.
- **No column codebook exists for ICPSR's own score columns** (`score` vs
  `score_all`, `nuance_score`, `mnir`) — undocumented in the download.

## Regeneration

On the droplet (repo root, venv):
1. `python scripts/recover_fec_ids.py` → `data/deliverable/fec_id_crosswalk.csv`
2. locally: `Rscript scripts/export_dime_subset.R /tmp/dime_cand_subset.csv`;
   `scp` to droplet `data/dime_cache/`
3. `pip install pyarrow` (once); `python scripts/harmonize_deliverable.py`
