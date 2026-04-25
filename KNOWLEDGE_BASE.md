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

Status as of 2026-04-25 — verify against `logs/scrape_queue.log` on the
droplet before relying on these.

| Component | Status |
|---|---|
| `src/build_candidate_roster.py` (FEC + OpenFEC + Wikidata waterfall) | Built |
| `src/scrape_wayback.py` (CDX + snapshot fetch + auto-pause) | Built, in production |
| `src/extract_text.py` (HTML → visible text) | Built |
| `src/classify_pages_llm.py` (LLM page-type tagger) | Built — usage unverified |
| `src/probe_german_feasibility.py` (sister-repo probe) | Built (commit 170eabf) |
| Senate 2002-2014 snapshots | Tarballs on droplet from March; 2004/2006/2008/2014 currently being re-scraped (Apr 23 restart) |
| Senate 2016-2024 snapshots | Done in March; present as tar.gz on droplet |
| House 2018-2024 snapshots | Not in current Apr 23 restart queue. House 2018 progress file exists from earlier run. |
| Re-scrape of Senate 2016 / 2018 | Pending — not in current queue |

---

## 5. Literature anchors (in `literature/`)

- No `literature/` directory yet. Add PDFs as they become relevant.
- Primary upstream paper: Di Tella, Kotti, Le Pennec, Pons (2025), "The
  Economics of Populism," ICPSR 226001-V1.

---

## 6. Open questions / TODO

- [ ] Verify why the 2026-04-23 droplet queue restart skipped Senate
  2010/2012 and dropped House 2018-2024 entirely.
- [ ] Document the OpenFEC vs Wikidata hit-rate for each cycle once a
  roster build completes.
- [ ] Decide whether to add a Playwright fallback for JS-rendered post-2018
  candidate sites.
- [ ] Re-scrape Senate 2016 and 2018 (flagged in MEMORY notes from March).
- [ ] Confirm `classify_pages_llm.py` is being run on completed years and
  document its model + cost profile.
