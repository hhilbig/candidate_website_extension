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

### 4.3 Senate — IN PROGRESS (audited live 2026-06-27)

Full per-year state and the completion plan are in the approved plan
`quality_reports/plans/` + this session's log. Reconciled inventory:

| Year | Roster | Captured | Other copy | Action |
|---|---|---|---|---|
| 2002 | 72 | 31 (tar) | — | complete — leave |
| 2004 | 130 | 79 (tar) | +2-file Apr dir | merge dir→tar |
| 2006 | 215 | 131 (tar) | +1-file Apr dir | merge |
| 2008 | 284 | 195 (tar) | +5-file Apr dir | merge |
| 2010 | 409 | 223 (tar) | — | complete — leave |
| 2012 | 430 | 257 (tar) | — | complete — leave |
| 2014 | 493 | 290 (tar) | +33-file Apr dir | merge |
| 2016 | 511 | 73 (dir) | progress exists | resume scrape |
| 2018 | 561 | 0 | roster exists | fresh scrape |
| 2020 | 572 | 64 (tar) | — | **full re-run** |
| 2022 | 693 | 132 (tar) | — | **full re-run** |
| 2024 | 707 | 37 (tar) | — | **full re-run** |

**Correction (overturns the prior "2020/22/24 complete, just need CJK"
note):** the logs show those three years ran their full rosters in
mid-March but 488/491/654 candidates hit Wayback "CDX connection refused"
(old scraper, no auto-pause), so only 64/132/37 were captured. They are
near-empty and need re-scraping, NOT a CJK pass.

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
- [x] ~~Off-machine backup of 2018.~~ DONE 2026-06-26 — `2018.tar.gz`
  (599 MB) streamed to mac2 `~/backups/candidate_website_extension/house/`,
  sha256-verified. NOTE: **2020.tar.gz is now droplet-only** (the May mac2
  copy was cleaned up) — back it up to mac2 too (cheap, same path).
- [ ] Senate disentangling (deferred): per-year decide tarball vs
  uncompressed-dir provenance for 2004/2006/2008/2014; resume Senate 2016
  (~83/511); clean-start Senate 2018 (missing). Apply CJK spam filter as a
  post-process to old Senate March tarballs (2002–2014).
- [ ] mac2 git hygiene: mac2's `main` is on rewritten pre-scrub history
  and never re-fetched; code is safe (identical to `main`), but `git fetch
  && git reset --hard origin/main` would clean it up (untracked run scripts
  are preserved). Do after the scrape.
- [ ] Document the OpenFEC vs Wikidata hit-rate for each cycle.
- [ ] Decide whether to add a Playwright fallback for JS-rendered post-2018
  candidate sites.
- [ ] Confirm `classify_pages_llm.py` is being run on completed years and
  document its model + cost profile.
