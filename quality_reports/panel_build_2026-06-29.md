# Candidate-year panel build

**Date:** 2026-06-29 · **Script:** `scripts/build_panel.py` ·
**Input:** the 16 snapshot tarballs · **Output:** `data/panel/` (droplet)

## What this resolves

The coverage audit (`coverage_audit_2026-06-28.md`) found snapshot density is
inconsistent across years (mean 3.2–35.6 snapshots/candidate, max up to 9,552),
a scraper-version artifact. Per `KNOWLEDGE_BASE.md` §2/§3.1 the analysis unit is
**candidate × year** and downstream "picks the longest-text snapshot per
candidate." This panel does exactly that, collapsing every captured
candidate-year to one representative observation and normalizing away the
density inconsistency.

## Method

For each captured candidate-year CSV: group rows by `date` (= one Wayback
snapshot), pick the snapshot whose pages sum to the most `n_char` (tie-break:
latest `date`), concatenate that snapshot's pages with the `#+#` separator
(homepage first, then by descending length), emit one panel row. Read-only over
the raw tarballs; the raw data is untouched.

## Result

- **10,601 candidate-years** — reproduces the audit's captured total exactly,
  and every per-(office,year) count matches `audit_per_year.csv` `captured_n`.
- Text: median **13,286** chars, mean 38,516, p10 806, p90 75,252; **0 empty
  rows**.
- Snapshots collapsed: median 4, mean 13.3, max 9,552; 1,261 candidate-years
  had a single snapshot; 172 collapsed from ≥100.
- Party mix R 5,516 / D 5,085 (pure R/D — confirms the stage=2 general-election
  coverage; no `other`).
- **Panel key = (candidate, state, office, year)**, verified unique. The 6
  same-name (candidate, office, year) collisions are distinct people in
  different states (e.g. Roque "Rocky" De La Fuente RI & CA, 2020).

## Outputs

| File | Contents | Disposition |
|---|---|---|
| `data/panel/panel_candidate_year.csv` (422 MB) | one row/candidate-year, **with** concatenated `text` | droplet artifact (see below) |
| `data/panel/panel_candidate_year_meta.csv` (1.3 MB) | same rows **without** `text` | committed at `quality_reports/coverage_audit/csv/` |

Schema: `candidate, state, district, office, year, party, stage, sel_date,
n_snapshots_available, n_pages, page_types, urlkey, [text,] n_char, n_words`.

**On the full panel's storage:** it is **deterministically regenerable** in
~3 min from the committed `scripts/build_panel.py` + the tarballs (which are on
the droplet and backed up to mac2), so it is NOT separately backed up —
regenerating is cheaper and safer than syncing 422 MB over the slow uplink.
Pull it from the droplet or rebuild locally when needed.

## Caveats (for downstream)

- **Deduped years select from a pre-thinned set.** House 2022/24 + Senate 2018
  (and partly Senate 2016/2020/22/24) had the 3-month dedup applied during
  scraping, so "longest-text" is chosen from the latest-per-quarter snapshots,
  not all Wayback captures. The latest late-campaign capture is usually also the
  most complete, so the bias is minor; not re-scraped.
- **Thin sites exist:** p10 text is ~800 chars; a few candidate-years (e.g.
  placeholder/parked pages captured thousands of times) collapse to a short
  page. `n_char` / `n_snapshots_available` let downstream filter these.
- **Panel = captured candidate-years only.** Left-join to
  `data/rosters/roster_{office}_{year}.csv` for the full candidate set
  (un-captured → NA text).

## Verification done

- Row count 10,601; per-year counts == audit `captured_n`; 0 empty text.
- By construction the selected snapshot's total `n_char` ≥ any single page.
- Spot-checked old-dense (House 2018 Hamilton 9,299→1; Senate 2002 Graham
  226→Nov-6 capture) and new (House 2024, Senate 2018) candidate-years.
