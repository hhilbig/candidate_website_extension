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

Status as of 2026-04-25 22:40 UTC. Verify against `logs/scrape_queue.log`
on the droplet before relying on these.

### 4.1 Code components

| Component | Status |
|---|---|
| `src/build_candidate_roster.py` (FEC + OpenFEC + Wikidata waterfall) | Built |
| `src/scrape_wayback.py` (CDX + snapshot fetch + auto-pause) | Built, in production |
| `src/extract_text.py` (HTML → visible text) | Built |
| `src/classify_pages_llm.py` (LLM page-type tagger) | Built — usage unverified |
| `src/probe_german_feasibility.py` (sister-repo probe) | Built (commit 170eabf) |

### 4.2 Scrape queue (Apr 23 restart)

Queue script: `run_scrape_queue.sh` on droplet (untracked, lives only on
droplet). Original order was Senate 2004-2024 → House 2018-2024 → re-scrape
Senate 2016, 2018.

**2026-04-26 decision: cut the queue short after Senate 2018.** The only
output-changing commit between the March scrapes and now is the CJK spam
filter (`e5d1341`, Mar 11). All April commits are reliability-only
(auto-pause). Re-scraping Senate 2020/2022/2024 (already CJK-filtered in
March) is wasted compute. The duplicate Senate 2016/2018 re-scrape block at
the end of the script is also redundant once those years complete in the
main loop. House 2018-2024 still needs to be collected separately — it has
never been scraped.

Watcher: `/root/queue_killer.sh` (PID 1775926, launched 2026-04-26 20:10
UTC) polls `logs/scrape_queue.log` every 60s and kills queue PID 1751785
when `DONE  senate 2018` appears with a date ≥ 2026-04-26. Logs to
`/root/queue_killer.log`.

| Year | Status | Notes |
|---|---|---|
| Senate 2002 | Done (Mar 12 tar) | Pre-restart, not requeued |
| Senate 2004 | Done in restart (Apr 25) | New tar pending |
| Senate 2006 | Done in restart (Apr 25) | New tar pending |
| Senate 2008 | Done in restart (Apr 25) | New tar pending |
| Senate 2010 | Done (Mar 12 tar) | Pre-restart, not requeued |
| Senate 2012 | Done (Mar 12 tar) | Pre-restart, not requeued |
| Senate 2014 | Done in restart (Apr 26 08:32 UTC) | New tar pending |
| Senate 2016 | In progress (Apr 26) | Stuck on Marco Rubio (424 CDX records, ~6h) |
| Senate 2018 | Queued — will run after 2016 | Then watcher kills the queue |
| Senate 2020 | Done (Mar 24 tar) — **NOT re-scraping** | Already CJK-filtered in March; no re-scrape value |
| Senate 2022 | Done (Mar 24 tar) — **NOT re-scraping** | Already CJK-filtered in March; no re-scrape value |
| Senate 2024 | Done (Mar 24 tar) — **NOT re-scraping** | Already CJK-filtered in March; no re-scrape value |
| House 2018 | NOT YET COLLECTED | 4.8M progress file from earlier abandoned run; needs separate queue run |
| House 2020 | NOT YET COLLECTED | ~3000+ candidates expected |
| House 2022 | NOT YET COLLECTED | ~3000+ candidates expected |
| House 2024 | NOT YET COLLECTED | ~3000+ candidates expected |

### 4.3 Pace observations

Apr 23 restart pace is uneven:

- Senate 2004: 130 cands → ~2 days (likely many auto-pause cycles)
- Senate 2006: 215 cands → ~1.5 hours
- Senate 2008: 284 cands → ~2 hours

ETA for the full queue is hard to estimate — house years (3000+ each)
could take a long time. The auto-pause feature (6h sleep after 5
consecutive Wayback `Connection refused` errors) is what dominates
the worst-case runtime.

---

## 5. Literature anchors (in `literature/`)

- No `literature/` directory yet. Add PDFs as they become relevant.
- Primary upstream paper: Di Tella, Kotti, Le Pennec, Pons (2025), "The
  Economics of Populism," ICPSR 226001-V1.

---

## 6. Open questions / TODO

- [ ] **Set up a separate House 2018-2024 scrape run** after the current
  queue is killed post-Senate-2018. Need a new queue script with just
  `for year in 2018 2020 2022 2024; do run_year house $year; done`.
  House 2018 has a 4.8M progress file from earlier — will resume from
  partial state.
- [ ] Compress the new Senate 2004/2006/2008/2014 snapshot dirs once the
  queue is dead (avoid the Mar-12 race-condition failure mode — never
  compress a year that is currently being scraped).
- [ ] Apply CJK spam filter as a post-process to existing Senate
  2002/2004/2006/2008/2010/2012/2014 tarballs — same effect as a
  re-scrape, in seconds rather than days.
- [ ] Document the OpenFEC vs Wikidata hit-rate for each cycle once a
  roster build completes.
- [ ] Decide whether to add a Playwright fallback for JS-rendered post-2018
  candidate sites.
- [ ] Confirm `classify_pages_llm.py` is being run on completed years and
  document its model + cost profile.
