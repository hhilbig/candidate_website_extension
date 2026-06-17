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

Active queue script: **`run_house_2022_2024.sh`** on droplet (untracked).
Scrapes only the two remaining cycles (2018/2020 already complete). Launched
2026-06-15 20:35 CEST. Watcher: **`watch_house_queue.sh`** (droplet) Slacks
on silent death (process gone with no "queue complete" line) or >1h stall.

| Cycle | Status | Location |
|---|---|---|
| House 2018 | ✅ Complete (1765 files) | droplet `data/snapshots/house/2018.tar.gz` (572M) |
| House 2020 | ✅ Complete (1880 files, 2002/3155 w/ snapshots) | droplet `2020.tar.gz` (197M) + uncompressed on mac2 |
| House 2022 | ⏳ In progress (3569 cands) | droplet, scraping now with fixed scraper |
| House 2024 | ⏸ Queued (3463 cands) | droplet, runs after 2022 |

House 2020 was consolidated mac2 → droplet (tar-pipe; verified 1880 files /
1,288,894,636 bytes identical). The droplet's old inferior 2020 partial
(old scraper, captured email-URL junk) was deleted after verification.

### 4.3 Senate — deferred, NOT touched 2026-06-15

Senate is out of scope for the House push. Droplet still holds March
tarballs (2002/2010/2012/2020/2022/2024) plus Apr-rescrape pairs for
2004/2006/2008/2014 where uncompressed dir vs `.tar.gz` provenance is
ambiguous (tarballs look like complete March copies; some dirs are tiny
Apr fragments). **Do not delete either copy without per-year disentangling.**
Senate 2016 = partial rescrape (~83/511); Senate 2018 = still missing.

### 4.4 Pace observations

- House 2020 (mac2): ~3155 cands over ~4 days at ~90-260 s/it; tail dominated
  by big-CDX outliers.
- [LEARN:scraping] Long-serving incumbents / presidential aspirants spike to
  multi-hour single-candidate scrapes (424+ CDX records). Account for ~5-10
  such outliers per cycle; tqdm ETAs after one outlier are useless.
- Expect House 2022 + 2024 to take roughly 1-2 weeks combined, outlier-dependent.

### 4.5 Data quality (House 2022, observed mid-run 2026-06-17 at 2057/3569)

Verdict: **usable, good quality.** Metrics from the in-progress 2022 log +
output:

- **Capture rate ≈ 63%** of candidates yield ≥1 snapshot (1285 captured of
  ~2057 processed). Matches House 2020 (2002/3155 = 63%) almost exactly. The
  ~29% "no snapshots found" + ~3% invalid-URL are normal (challengers / minor
  candidates with no archived site, or bad roster URLs).
- **Temporal spread:** snapshots/candidate cluster at 3–4 across the election
  year (the 3-month dedup buckets working as designed). Cap of 200 never binds
  (max observed 8).
- **Page depth:** 4187 selected snapshots → ~22,600 page rows (~5.4 pages per
  snapshot = homepage + depth-1 subpages). So each captured candidate is a
  real site crawl, not a single landing page.
- **Text richness:** median 1888 chars/page, mean 3658, max 79K. Only ~1% of
  rows <50 chars (nav-only/junk). High signal-to-noise.
- **Corruption negligible:** 4 snapshot-level errors (`scrape_error=1`) of
  4183 completed.
- **Output schema** (per-candidate CSV `data/snapshots/house/<year>/<name>
  (<state>).csv`): candidate,state,district,office,year,party,stage,date,
  urlkey,snap_url,page_type,data_source,n_tags,n_clean_tags,text_snap_content,
  n_char,n_words. One row per page.

**Residual loss (recoverable): ~30 candidates (~1.5%) per cycle** whose CDX
query failed after 3 retries due to Wayback "connection refused" (Wayback is
much flakier now — 2430 refusals in a 37h run at threads=1, vs ~0 in March;
the Retry(3)+6h-auto-pause logic absorbs nearly all). These candidates are
left **unmarked** in the progress file (verified), so a resume re-attempts
them. Recover via a targeted mini-roster of the failed URLs (grep
`CDX query failed after 3 attempts` from the year log) — NOT a full-roster
resume (which re-queries all 3569 and re-incurs refusals). See §6 TODO.

---

## 5. Literature anchors (in `literature/`)

- No `literature/` directory yet. Add PDFs as they become relevant.
- Primary upstream paper: Di Tella, Kotti, Le Pennec, Pons (2025), "The
  Economics of Populism," ICPSR 226001-V1.

---

## 6. Open questions / TODO

- [ ] Monitor House 2022 → 2024 scrape (droplet `run_house_2022_2024.sh`,
  watcher `watch_house_queue.sh`). Compress each year on completion
  (verify tarball file-count before deleting the dir; never compress a
  year currently scraping — Mar-12 race rule).
- [ ] **Recovery sweep after the queue finishes** (see §4.5): for each
  cycle, grep `CDX query failed after 3 attempts` from `logs/house_<year>.log`,
  build a mini-roster of just those candidate URLs, and re-run the scraper on
  it (ProgressTracker skips done snapshots). Recovers the ~1.5% of candidates
  lost to transient Wayback connection-refused. Do this BEFORE compressing
  each year.
- [ ] Off-machine backup of completed House data: 2018 currently exists
  ONLY on the droplet (single copy). 2020 is on droplet + mac2. Consider
  pushing tarballs off-box.
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
