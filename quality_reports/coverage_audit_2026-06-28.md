# Coverage & Quality Audit — all collected data

**Date:** 2026-06-28 · **Author:** automated audit (`scripts/coverage_audit.py`)
· **Corpus:** House 2018–2024 + Senate 2002–2024 (16 office-years) ·
**Scope note:** House 2002–2016 is excluded (already in ICPSR 226001).

Figures: `quality_reports/coverage_audit/figures/`. Tidy data:
`quality_reports/coverage_audit/csv/`.

## 0. Method

One row per scraped page across 16 `.tar.gz` on the droplet, streamed without
extraction. Definitions reuse the pipeline's own code: a **valid URL** is one
`_clean_campaign_url` accepts; a candidate is **captured** if it has ≥1 snapshot
file; **low-text** is `n_char < 50`; **CJK spam** is `is_cjk_spam` (>30% CJK
chars). CJK uses a 1-in-10 file sample by default (full pass where flagged).

## 1. Executive summary

**Verdict: the corpus is complete in coverage and clean in quality. The one
material issue is cross-year *consistency*, not correctness: snapshot density
varies ~10× across years because of scraper-version differences, so per-candidate
snapshot/text volume is not comparable across years without a normalization
step.**

- **Scale:** 10,601 captured candidates, 141,000 snapshots, **1,175,582 page
  rows**.
- **Coverage:** capture rate is **45–72% of valid-URL candidates** every year
  (the documented norm), reproducing the KB §4.3/§4.5 numbers exactly. ~97% of
  rostered candidates have a valid URL in every year.
- **Quality:** **0 scrape errors** and **0 empty-text rows** everywhere; <1%
  low-text rows; CJK spam ≤0.3%; median ~2,600 chars/page.
- **Integrity:** schema uniform (17 cols, all 16 years); **0 snapshot dates
  outside the election-year window** anywhere; negligible duplicate rows.
- **Selection:** no systematic partisan capture bias (mean D–R gap +1.3 pts,
  sign flips by year); a few small-population states have zero capture.
- **Key issue:** House 2018/2020 and Senate 2002–2016 predate the 3-month
  dedup + 200-snapshot cap, so they hold raw CDX snapshots (up to ~9,500 for one
  candidate); House 2022/2024 + Senate 2018 are normalized (~4); Senate
  2020/22/24 are internally mixed. See §3.

### Master table (per office-year)

| Office | Year | Roster | Valid-URL % | Captured | % of valid-URL | Page rows | Snaps/cand (mean) | n_char med | Errors |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| House | 2018 | 3038 | 96.5 | 1765 | 60.2 | 384,103 | 24.7 | 3342 | 0 |
| House | 2020 | 3155 | 96.8 | 1880 | 61.5 | 258,074 | 22.7 | 2645 | 0 |
| House | 2022 | 3569 | 97.1 | 2253 | 65.0 | 46,158 | 3.2 | 2371 | 0 |
| House | 2024 | 3463 | 97.0 | 1777 | 52.9 | 39,152 | 3.3 | 2748 | 0 |
| Senate | 2002 | 72 | 95.8 | 31 | 44.9 | 7,881 | 35.6 | 1441 | 0 |
| Senate | 2004 | 130 | 96.9 | 81 | 64.3 | 22,578 | 24.1 | 2911 | 0 |
| Senate | 2006 | 215 | 98.1 | 132 | 62.6 | 56,237 | 35.6 | 2803 | 0 |
| Senate | 2008 | 284 | 97.5 | 200 | 72.2 | 60,764 | 21.8 | 3342 | 0 |
| Senate | 2010 | 409 | 97.6 | 223 | 55.9 | 42,797 | 12.7 | 3013 | 0 |
| Senate | 2012 | 430 | 96.3 | 257 | 62.1 | 36,302 | 14.8 | 3006 | 0 |
| Senate | 2014 | 493 | 96.6 | 323 | 67.9 | 88,894 | 24.7 | 2493 | 0 |
| Senate | 2016 | 511 | 96.7 | 297 | 60.1 | 45,326 | 13.9 | 2578 | 0 |
| Senate | 2018 | 561 | 96.4 | 265 | 49.0 | 5,156 | 3.5 | 2466 | 0 |
| Senate | 2020 | 572 | 97.6 | 321 | 57.5 | 24,560 | 9.8 | 2654 | 0 |
| Senate | 2022 | 693 | 97.4 | 435 | 64.4 | 37,530 | 10.7 | 2515 | 0 |
| Senate | 2024 | 707 | 97.3 | 361 | 52.5 | 20,070 | 6.2 | 2617 | 0 |

## 2. Coverage  (`fig_coverage.pdf`)

Capture of valid-URL candidates is 45–72% across the corpus, with no year
collapsing — even the weakest (Senate 2002 at 45%, n=31) reflects small-roster
noise, not pipeline failure. The recent cycles dip (House 2024 53%, Senate 2024
53%, Senate 2018 49%) consistent with **Wayback archival recency**: newer
candidate sites are less-archived in their election-year window. A re-pass
6–12 months out would backfill some.

The coverage-gap decomposition (roster → no-valid-URL / valid-but-no-snapshot /
captured) shows the un-captured remainder is dominated by **valid URL but no
Wayback snapshot** (archival gap), not URL-resolution failure: ~97% of
candidates have a valid URL every year, so the ~3% no-URL slice is small and
stable. The missing third is mostly challengers/minor candidates whose sites
Wayback never archived.

## 3. Snapshot-density consistency — the key finding  (`fig_snapshots.pdf`)

Mean snapshots per candidate ranges from **3.2 (House 2022) to 35.6 (Senate
2002)**, and the single-candidate max ranges from **8 to 9,552**. This is a
scraper-version artifact, in three regimes:

- **Normalized** (3-month dedup ⇒ ~4/candidate, 200 cap): **House 2022, House
  2024, Senate 2018** — mean 3.2–3.5, max ≤13.
- **Un-normalized** (raw CDX, every archived snapshot kept): **House 2018/2020,
  Senate 2002–2016** — mean 12–36, max 154–**9,552** (House 2018 "Eddie Mr Maga
  Hamilton" 9,299; House 2020 "Arturo Pacheo Reyes" 9,552). These predate the
  dedup/cap logic.
- **Mixed**: **Senate 2020/2022/2024** — the candidates captured in the original
  March run retain raw snapshots while the recovery-scraped candidates are
  deduped (mean 6–11, a few candidates >200). Senate 2016 is similar (resumed
  onto an old partial).

Pages *per snapshot* is uniform (~5–9) across all years, so the difference is
purely in snapshot count, i.e. temporal deduplication — not page depth.

**This matters for downstream causal work:** "number of snapshots" and "total
text" per candidate are not comparable across years. **Recommendation:** before
analysis, normalize to one snapshot per candidate per time bucket (e.g. nearest
to a fixed pre-election date, or one per quarter), using
`audit_candidate_level.csv`. This is a downstream choice, not a re-scrape; the
raw extra snapshots in the older years are usable, just denser.

## 4. Data quality  (`fig_quality.pdf`)

- **Text richness:** median 1,441–3,342 chars/page; the n_char distributions are
  unimodal and similar across years (the corpus is real site text, not nav
  stubs).
- **Low-text / empty:** `n_char==0` rows are **0.0% everywhere**; `<50`-char rows
  are 0.04–0.93%. Negligible.
- **CJK spam:** ≤0.0% for 15 years; Senate 2014 is 0.29% on a full pass (the
  1-in-10 sample read 0.599%). The deferred CJK post-process remains unwarranted.
- **Scrape errors:** `scrape_error == 0` in every progress file; all progress
  files present.
- **Page types** classify sensibly (homepage/issues/news/biography/action lead;
  `other` is the plurality, as expected for heterogeneous campaign sites).

## 5. Integrity

- **Schema:** all 16 years carry the identical 17-column schema in order
  (`schema_ok` true; spot-confirmed across old vs new tarballs).
- **Date-window validity:** **0 snapshot dates fall outside [YYYY-01-01,
  YYYY-12-31]** in any year — the CDX from/to window held; date_min/date_max sit
  inside the election year everywhere.
- **Duplicate page rows** (same candidate+date+snap_url): tiny — House 2018 (21),
  2020 (135 = 0.05%), 2022 (10), 2024 (42), Senate 2006 (11); 0 elsewhere.
  Concentrated in the un-normalized years; drops out under the §3 normalization.
- **Value validity:** `state`/`party` values valid; `data_source` uniform. One
  captured file in Senate 2002 does not match a roster row (name-encoding) —
  flagged, not dropped.

## 6. Temporal coverage  (`fig_temporal_selection.pdf`, p.1)

Snapshots fall within the election year in every case. Normalized years show the
quarterly dedup clustering; un-normalized years spread more continuously across
months. No year is empty in any quarter for the captured set.

## 7. Selection checks  (`fig_temporal_selection.pdf`, p.2)

- **Party:** no systematic partisan capture bias — mean D–R gap +1.3 pts, sign
  flips year to year (range −7.8 to +10.7). Recent Senate cycles (2020/22/24)
  tilt D by 5–11 pts; worth noting for those years but not a corpus-wide bias.
- **Geography:** a handful of small-population states have zero capture in a
  given year (Guam in House 2018/2020 with 3–4 rostered; AK/AL/AR/IN/ME/MS/VA in
  one Senate year each, each ≤4 rostered). These are small-n, not systematic
  geographic gaps.

## 8. Anomalies (`audit_anomalies.csv`, 27 flags)

All benign and explained above: duplicate-row counts (§5), extreme-snapshot
candidates (the §3 un-normalized incumbents/celebrities — McCain 447, Kerry 265,
Graham 226, etc.), zero-capture small states (§7), and one roster-unmatched
captured file (§5). No empty/corrupt/unreadable files; no schema breaks; no
out-of-window dates.

## 9. Limitations & downstream guidance

1. **Normalize snapshot density before cross-year analysis** (§3) — the single
   must-do. Pick one snapshot per candidate per time bucket.
2. **Wayback recency** depresses 2024 (and Senate 2018) capture; a re-pass in
   6–12 months could recover more.
3. **No URL-source split:** the roster keeps only the final `website_url`, not
   whether OpenFEC or Wikidata produced it, so the waterfall hit-rate cannot be
   recovered from the roster alone (open KB §6 item).
4. **JS-rendered sites:** un-rendered captures show as low-text, not errors; the
   <1% low-text share bounds this.
5. CJK post-process stays deferred (≤0.3%).

## Appendix

- Reused: `_clean_campaign_url` (`src/scrape_wayback.py`), `is_cjk_spam`
  (`src/extract_text.py`).
- Run: `python scripts/coverage_audit.py --out-dir /tmp/coverage_audit --cjk
  sample` on the droplet (2m33s); `Rscript scripts/coverage_audit_figures.R`
  locally.
- Outputs: `audit_per_year.csv`, `audit_by_party.csv`, `audit_by_state.csv`,
  `audit_pagetype.csv`, `audit_snap_month.csv`, `audit_nchar_sample.csv`,
  `audit_anomalies.csv` (tracked); `audit_candidate_level.csv` (droplet artifact).
