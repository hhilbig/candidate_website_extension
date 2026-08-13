# Knowledge base — Candidate Website Extension

Substantive reference for the project. Update this when design decisions,
data structures, or literature anchors change. Keep `CLAUDE.md` short by
pushing detail here.

---

## 1. Research question

This is a **data collection** project rather than an analysis project. It
extends the candidate-website corpus from Di Tella, Kotti, Le Pennec, and
Pons (2025, ICPSR 226001), which covers U.S. House campaigns 2002-2016, to
two new domains: U.S. House 2018-2024 and U.S. Senate 2002-2024. The output
is a panel of archived candidate-website text that downstream projects can
score (e.g., for populism, issue salience, valence) and merge with election
returns.

Primary deliverable: per-candidate snapshot CSVs at
`data/snapshots/{office}/{year}/`, schema documented in `SPEC.md` §5.

---

## 2. Identification strategy

Not applicable — no econometric estimation lives in this repo. Downstream
analysis happens in the parent research project, which consumes the
snapshot CSVs.

Methodological choices that matter for downstream identification:

- **Coverage cutoffs.** We collect Democratic and Republican general-election
  candidates only (`stage=2`). Primary candidates require flipping
  `stage=1` in the roster.
- **Snapshot window.** CDX queries are constrained to the election year
  itself (Jan 1 – Dec 31), `matchType=exact` on the base URL. Subpages
  arrive via in-page link following, one level deep.
- **Snapshot retention.** No temporal dedup, no per-candidate snapshot cap
  (matches the original ICPSR scraper). Downstream code is expected to
  pick e.g. the longest-text snapshot per candidate.

---

## 3. Data

### 3.1 Panel structure

- **Source rosters:** `data/rosters/roster_{office}_{year}.csv`
  built by `src/build_candidate_roster.py`. Columns: candidate, state,
  district, office, year, party, website_url.
- **Output snapshots:** `data/snapshots/{office}/{year}/{candidate}.csv`,
  one row per page per snapshot. Schema in SPEC.md §5.
- **Unit of observation:** candidate × election year × snapshot × page.
  Downstream typically collapses to candidate × year.

### 3.2 Coverage of key covariates

| Variable | Coverage | Source | Notes |
|---|---|---|---|
| candidate name, state, party | full | FEC bulk `cn{YY}.zip` | Names parsed from `LASTNAME, FIRSTNAME` |
| district | House only | FEC | Senate rows have no district |
| website_url | partial | OpenFEC → Wikidata waterfall | ... unverified hit-rate, document after a roster build |
| snapshot text | partial | Wayback CDX | Coverage drops for pre-2010 (Flash) and post-2018 (JS-rendered) sites |

### 3.3 Treatment variables

- N/A — no treatment defined in this repo.

### 3.4 Data-quality caveats

- JS-rendered sites (React/Next.js, increasingly common 2018+) often
  archive as blank pages. No Playwright fallback yet.
- Pre-2010 Flash sites archive blank; filtered downstream by text length.
- CJK spam captures slip in occasionally; filter added in commit `e5d1341`.
- Wayback `Connection refused` bursts trigger an auto-pause (6h) after 5
  consecutive failures; logic in commits `72e6f67`, `b44ce5e`, `4e35ce0`,
  `1a589d2`.

---

## 4. Current state

Status as of 2026-06-15 (verified live against droplet + mac2). Verify
against `logs/scrape_queue.log` on the droplet before relying on these.

### 4.1 Code components

| Component | Status |
|---|---|
| `src/build_candidate_roster.py` (FEC + OpenFEC + Wikidata waterfall) | Built |
| `src/scrape_wayback.py` (CDX + snapshot fetch + auto-pause + URL cleaning + snapshot caps) | Built, in production; droplet upgraded to canonical `main` 2026-06-15 |
| `src/extract_text.py` (HTML → visible text) | Built |
| `src/classify_pages_llm.py` (LLM page-type tagger) | Built — usage unverified |
| `src/probe_german_feasibility.py` (sister-repo probe) | Built |

The URL-cleaning + snapshot-cap logic (commit `59e752c`, `_clean_campaign_url`
/ `_dedup_snapshots` / `_sample_snapshots_stratified`, config keys
`max_snapshots_per_candidate: 200` + `snapshot_dedup_bucket_months: 3`) was
authored on mac2 and is now on `main` and on the droplet. All 6 tests in
`tests/test_new_fixes.py` pass on the droplet.

### 4.2 House scrape — current (2026-06-15)

The Apr-30 House-first pivot stalled silently mid-2020 on the droplet
(~May 19) and was never restarted; meanwhile House 2020 was finished
separately on mac2 (May 24). Reconciled 2026-06-15.

**House 2018–2024 is COMPLETE** (recovered, compressed, audited 2026-06-26).
All four years are `.tar.gz` on the droplet under `data/snapshots/house/`.

| Cycle | Captured / roster | Tarball | Off-box copy |
|---|---|---|---|
| House 2018 | — | 599M | mac2 `~/backups/.../house/2018.tar.gz` (sha256-verified) |
| House 2020 | 1880 files | 206M | mac2 (backup added 2026-06-27) |
| House 2022 | 2253 / 3569 (63.1%) | 43M | — |
| House 2024 | 1777 / 3463 (51.3%) | 49M | — |

A 2026-06-26 recovery sweep recovered 292 snapshots from 132 CDX-failed
candidates (see §4.5). House 2024's lower capture is Wayback archival
recency, not a defect. Off-box backups: only 2018 + 2020 are on mac2 so far.

### 4.3 Senate — COMPLETE (2002–2024, finished 2026-06-28)

All 12 even years are `.tar.gz` on the droplet under `data/snapshots/senate/`
(also backed up to mac2 `~/backups/.../senate/`). Final per-year captures
(captured / roster; "captured" = candidates with ≥1 snapshot):

| Year | Roster | Captured | % valid-URL | How finished |
|---|---|---|---|---|
| 2002 | 72 | 31 | — | pre-existing (complete) |
| 2004 | 130 | 81 | — | merged April dir (+2) |
| 2006 | 215 | 132 | — | merged April dir (+1) |
| 2008 | 284 | 200 | — | merged April dir (+5) |
| 2010 | 409 | 223 | — | pre-existing (complete) |
| 2012 | 430 | 257 | — | pre-existing (complete) |
| 2014 | 493 | 323 | — | merged April dir (+33) |
| 2016 | 511 | 297 | 60.1% | resume scrape (was 73) |
| 2018 | 561 | 265 | 49.0% | fresh scrape (was 0) |
| 2020 | 572 | 321 | 57.5% | full re-run (was 64) |
| 2022 | 693 | 435 | 64.4% | full re-run (was 132) |
| 2024 | 707 | 361 | 52.5% | full re-run (was 37) |

Quality (2016–2024, `scripts/quality_check.py`): **0 scrape errors** all
years; capture 49–64% of valid-URL candidates (Senate norm, matches House);
median ~2,500 chars/page; <1% low-text rows. The 2020/22/24 re-runs each
re-tarred to a verified superset of the old near-empty tarball (0 old
candidates lost). **House 2002–2016 stays excluded (ICPSR).**

**The full ICPSR extension data collection is now COMPLETE:**
House 2018–2024 + Senate 2002–2024.

- [LEARN:scraping] The Senate 2020/22/24 March runs predate
  `_clean_campaign_url`, so the failed-URL log lines are RAW values
  (`https://none`, facebook URLs, trailing `;`/`,`, missing-colon
  `https//...`). `scripts/build_recovery_roster.py` (log-grep + clean-match)
  therefore CANNOT cleanly match them (it correctly fails loud on ~14-20
  unmatched). Use a **full-roster re-run** (`--office senate --year YYYY`
  after extracting the tarball) instead: the current scraper cleans the junk
  URLs, recovers the legit trailing-punctuation ones, and the 6h-pause
  absorbs refusals. The mini-roster builder is the right tool only when the
  original run already used the URL-cleaning scraper (e.g. House 2022/2024).
- [LEARN:data] CJK spam in the old Senate tarballs is empirically
  negligible (2008: 0.0%, 2014: 0.4%, 2022/2024: 0.0%) — the CJK
  post-process is a deferred low-priority hygiene pass, not a blocker.
- **Do not delete either copy** (tarball or April dir) for 2004/06/08/14
  until the merge is verified (Mar-12 rule).

### 4.4 Pace observations

- House 2020 (mac2): ~3155 cands over ~4 days at ~90-260 s/it; tail dominated
  by big-CDX outliers.
- [LEARN:scraping] Long-serving incumbents / presidential aspirants spike to
  multi-hour single-candidate scrapes (424+ CDX records). Account for ~5-10
  such outliers per cycle; tqdm ETAs after one outlier are useless.
- Expect House 2022 + 2024 to take roughly 1-2 weeks combined, outlier-dependent.

### 4.5 Data quality (House 2022 + 2024, FINAL post-recovery 2026-06-26)

Verdict: **usable, good quality, both years.** Final metrics after the
recovery sweep (`quality_check.py`):

| | 2022 | 2024 |
|---|---|---|
| roster | 3569 | 3463 |
| valid URL (cleanable) | 3467 (97.1%) | 3358 (97.0%) |
| captured (≥1 snap) | 2253 (**63.1%**) | 1777 (**51.3%**) |
| capture / valid-URL | 65.0% | 52.9% |
| page rows | 46,158 | 39,152 |
| snaps/cand (median, mean) | 4, 3.20 | 4, 3.32 |
| pages/snap (median, mean) | 4, 6.42 | 4, 6.64 |
| n_char (median) | 2371 | 2748 |
| <50-char rows | 0.3% | 0.4% |
| scrape errors | 0 | 0 |

- **Temporal spread / page depth:** snapshots/candidate cluster at ~4; each
  snapshot is ~6 page rows (homepage + depth-1 subpages) — real site crawls,
  not single landing pages. The 200-snapshot cap never binds (max 10).
- **Text richness high, corruption nil** in both years (0 `scrape_error`).
- **2024 capture (51%) runs ~12 pts below 2022/2020 (63%) — this is Wayback
  archival recency, NOT a defect.** Roster URL quality is identical (~97%
  valid both years); among valid-URL candidates the gap persists (52.9% vs
  65.0%) and the "no snapshots found" bucket is larger for 2024 (1183 vs
  972). Web.archive.org has simply crawled/indexed less of the recent 2024
  cycle. A re-pass 6–12 mo out could recover more as Wayback backfills.
- **Output schema** (per-candidate CSV `data/snapshots/house/<year>/<name>
  (<state>).csv`): candidate,state,district,office,year,party,stage,date,
  urlkey,snap_url,page_type,data_source,n_tags,n_clean_tags,text_snap_content,
  n_char,n_words. One row per page.

**Recovery sweep (DONE 2026-06-26):** the residual CDX-failed candidates
(48 in 2022, 84 in 2024 — Wayback "connection refused" after 3 retries) were
recovered via targeted mini-rosters built from `CDX query failed after 3
attempts for <url>` log lines, matched to roster rows via `_clean_campaign_url`,
re-run with a fresh progress file (`run_recovery_sweep.sh`). Recovered **292
snapshots** (2022: 105 from 50 cands; 2024: 187 from 86). Confirmed transient:
the first retried candidate returned 7 records immediately. NOT a full-roster
resume (which re-queries all ~3.5k and re-incurs refusals).

---

## 5. Literature anchors (in `literature/`)

- No `literature/` directory yet. Add PDFs as they become relevant.
- Primary upstream paper: Di Tella, Kotti, Le Pennec, Pons (2025), "The
  Economics of Populism," ICPSR 226001-V1.

---

## 6. Open questions / TODO

- [x] ~~Monitor House 2022 → 2024 scrape; compress each year on completion.~~
  DONE. Queue finished 2026-06-20 (2022 06-18, 2024 06-20). Both compressed
  2026-06-26 to `data/snapshots/house/{2022,2024}.tar.gz` (43 M / 49 M),
  file-counts verified (2253 / 1777) before dir deletion.
- [x] ~~Recovery sweep after the queue finishes.~~ DONE 2026-06-26 — 292
  snapshots recovered (see §4.5). **House 2018–2024 collection now COMPLETE.**
- [x] ~~Off-machine backup of House 2018 + 2020.~~ DONE — `2018.tar.gz`
  (2026-06-26) + `2020.tar.gz` (2026-06-27) streamed to mac2
  `~/backups/candidate_website_extension/house/`, both sha256-verified.
- [x] ~~Senate completion.~~ DONE 2026-06-28 — merged April dirs
  (2004/06/08/14), resumed 2016, fresh-scraped 2018, full-re-ran
  2020/22/24. All 12 years tarballed + backed up to mac2 (§4.3). The
  CJK post-process is DEFERRED (empirically <0.5% of rows, see §4.3).
  **Full ICPSR extension data collection COMPLETE (House 2018–24 +
  Senate 2002–24).**
- [ ] mac2 git hygiene: mac2's `main` is on rewritten pre-scrub history
  and never re-fetched; code is safe (identical to `main`), but `git fetch
  && git reset --hard origin/main` would clean it up (untracked run scripts
  are preserved). Now due (scrape finished).
- [ ] Document the OpenFEC vs Wikidata hit-rate for each cycle.
- [x] ~~Decide whether to add a Playwright fallback for JS-rendered sites.~~
  DECIDED 2026-06-30: **not worth it.** Probed the thin candidate-years (~10%
  of the panel, <800 chars): the archived HTML holds almost no text
  (`Loading…`/Wix/Squarespace/React shells) — content is client-rendered and
  Wayback did not archive the SPA backend calls, so it is not recoverable even
  by rendering; the rest are genuinely empty (parked/coming-soon/404). Flagged
  instead via the panel's `text_quality` column (usable/thin/empty); see
  `quality_reports/panel_build_2026-06-29.md`.
- [ ] `classify_pages_llm.py` has never been run (no `data/page_type_llm.csv`
  on the droplet). Decide whether to run it on the completed House+Senate
  years and document its model (gpt-5-nano) + cost profile.
- [ ] (Deferred, low priority) CJK post-process over the pre-2026-03-11
  tarballs — <0.5% of rows; `is_cjk_spam` already filters new scrapes.
- [x] ~~Ask Pons whether he already collected this, and where to release.~~
  ANSWERED 2026-07-05. He has **only 2018 House**, nothing else, and had
  planned it for a new project but not started — so no duplication and no
  scoop risk. Venue recommendation: **Dataverse**. He did **not** answer the
  copyright question for the scraped text, so the rights posture is still ours
  to decide (see below).
- [x] ~~Resolve ICPSR's snapshot→candidate aggregation rule.~~ SOLVED
  2026-08-12: drop empty pages → mean over pages within a snapshot day → mean
  over snapshot days. Verified **exact on 13,020/13,020** ICPSR rows. Their
  grain includes `data_source` (same candidate-year-stage can appear twice,
  distinguished only by name casing). See
  `quality_reports/icpsr_variable_extension_2026-08-12.md`.
- [x] ~~Extend ICPSR's coded variables to our years.~~ DONE 2026-08-13.
  Pons pointed us to `7_topics.py` in the **full** replication package
  (openICPSR 226001, `US/US Code/`), which overturned the 08-12 verdict that
  topics/`entropy`/`subordinates` were unreproducible. `panel_icpsr_compat.csv`
  (10,601 rows, 48 cols) now ships: `icpsr_n_char`/`icpsr_n_words` **exact**;
  `icpsr_ttr_approx`/`icpsr_mattr_approx` corr 0.9997/0.9995 ratio 1.0000;
  `icpsr_entropy_approx` corr 0.9978; **31 `icpsr_topic_*` columns** at MAE
  0.0054 vs a 0.0296 predict-the-mean baseline. Key insights: their `entropy` is
  a per-word **Google Books** lookup, not a document statistic; their "topic
  model" is a **supervised RBF-kernel SVM** trained on Manifesto Project
  quasi-sentences (corpus version 2021-1, identified by vocabulary match), so no
  linear projection of `topic_words.csv` could reproduce it. Report:
  `quality_reports/icpsr_variable_extension_2026-08-13.md`.
  Not shipped: `n_tags`/`n_clean_tags` (HTML gone), `entropy_missing` (corr
  0.952), `subordinates` (corr 0.975 — openNLP Maxent tagger vs nltk).
- [x] ~~Check whether our corpus differs systematically from ICPSR's.~~ DONE
  2026-08-13 — `quality_reports/corpus_comparability_2026-08-13.md`. Yes, it
  does. **Crawl depth is the main real difference**: ours 4–10 pages per
  snapshot-day vs their 1.0 (2002–2012) and 3.0 (2014–2016), which drives the
  1.5–1.65× length ratio on the 41 overlapping House 2018 candidates. **Their
  own series breaks internally at 2014** (crawl depth 1.0→3.0), and **ours
  breaks across scraper versions** (Oct–Nov capture share swings 6.3%–55.2%;
  deduped years are flatter). **Their 2018 is anomalous** (0% Oct–Nov vs their
  usual ~50%), so the overlap ratios are an upper bound. **MATTR medians agree
  to 2% but its per-candidate correlation across corpora is ~0** — safe for
  distributions, unsafe per candidate. Page concentration: 0.78% of
  snapshot-days hold 24.5% of all pages (max 20,946 pages in one day).
- [ ] Consider shipping a page-count flag so users can exclude runaway
  snapshot-days (see the comparability report).
- [ ] **Rights posture for the Dataverse deposit** — the open blocker on
  release. Pons did not address copyright for the scraped website text.
  Recommendation on record: mirror ICPSR 226001's rights statement and keep
  `snap_url` provenance on every row (noting they distribute derived scores,
  not text, so it is an imperfect precedent).
