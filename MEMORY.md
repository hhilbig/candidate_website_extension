# MEMORY — [LEARN] log

Append `[LEARN:category] YYYY-MM-DD -- note` entries when corrected. Keep
terse — this file is loaded into context every session.

## Tagging convention

- `[LEARN:claude] YYYY-MM-DD -- title` — Claude Code session
- `[LEARN:codex]  YYYY-MM-DD -- title` — Codex session (if used)
- `[LEARN:both]   YYYY-MM-DD -- title` — applies to both
- `[HANDOFF]      YYYY-MM-DD -- title` — ephemeral; clear after use

Untagged `[LEARN]` entries are legacy.

---

## Environment

## R / analysis

## Data

## Pipelines

- `[LEARN:scraping] 2026-04-26` — Outlier candidates with huge archived
  subpage trees (long-serving incumbents like David Vitter, presidential
  aspirants like Marco Rubio with 400+ CDX records) can take 6+ hours
  each on a single candidate. Don't try to skip them — they're legitimate
  data. Account for ~5-10 such outliers per senate cycle when estimating
  ETAs; tqdm extrapolations after one outlier are useless.
- `[LEARN:scraping] 2026-04-26` — Before launching a re-scrape, check
  the actual diff between the code that produced the existing data and
  the current code (`git log src/scrape_wayback.py src/extract_text.py
  src/utils.py`). Only output-changing commits (e.g., new content
  filters, parser changes) warrant a re-scrape. Reliability-only
  commits (auto-pause, retry tweaks) don't change output and re-scrape
  is pure waste.
- `[LEARN:bash-ops] 2026-04-26` — Modifying a running bash script in
  place is unreliable: bash may continue reading the OLD inode via its
  open file descriptor. To stop a long-running queue at a specific
  point, write an external watcher script that polls the log and kills
  the queue PID when a target line appears. The 30-second `sleep`
  between iterations gives a clean kill window.
- `[LEARN:scraping] 2026-06-15` — The pre-`59e752c` scraper choked on
  roster "URLs" that are actually email addresses or free text
  (`sitkasilk@aol.com`, `...@gmail.com`, `n/a`): CDX resolves them to
  generic domains and returns the 10000-record limit, producing huge
  irrelevant captures and stalls. Fix = `_clean_campaign_url` +
  snapshot dedup/caps (commit `59e752c`). Always run this version for
  new scrapes; the droplet was upgraded to it 2026-06-15.
- `[LEARN:bash-ops] 2026-06-15` — A queue script with top-level `set -e`
  dies *before* its FAIL/Slack branch when the scraper exits nonzero, so
  failures go silent. Wrap the scraper call in `set +e` / `set -e`. The
  Apr-30 House queue stalled silently mid-2020 (~May 19) for this class
  of reason and nobody noticed for ~6 weeks. Pair the queue with a
  death/stall watcher (`watch_house_queue.sh`) that Slacks when the
  process vanishes with no "queue complete" line.
- `[LEARN:infra] 2026-06-15` — macOS ships rsync 2.6.9 (no `--info=*`).
  For mac↔droplet snapshot transfers use a tar-pipe
  (`ssh A 'cd r && tar czf - path' | ssh B 'cd r && tar xzf -'`) or
  rsync with `--stats`. Droplet has no Tailscale, so mac2→droplet must
  route through the local Mac. Verify a transfer by comparing file count
  AND total bytes on both ends (du differs by FS block rounding; byte
  sum does not).
- `[LEARN:scraping] 2026-06-17` — Wayback "Connection refused" is far worse
  now than the March threads=1≈0 note: a 37h House-2022 run logged ~2430
  refusals. Retry(3) + 6h-auto-pause (fires after 5 consecutive refused
  candidates) absorbs nearly all; net loss ≈30 candidates/cycle (~1.5%) whose
  CDX query failed after 3 attempts. CRITICAL: a failed CDX raises
  `WaybackConnectionRefused`, which `run_scrape` catches and skips WITHOUT
  calling `mark_done`, so the candidate stays unmarked → a resume re-attempts
  it. Recover via a targeted mini-roster of the `CDX query failed after 3
  attempts` URLs (grep the year log), NOT a full-roster resume (re-queries all
  ~3569 and re-incurs refusals). Per-snapshot download errors instead get
  marked `scrape_error=1` (only 4 in 2022) and are NOT auto-retried.
- `[LEARN:data] 2026-06-17` — Healthy House capture profile (use as a sanity
  baseline): ~63% of candidates yield ≥1 snapshot (rest have no archived
  site); 3–4 snapshots/candidate across the election year (3-month dedup
  buckets); each snapshot → ~5 page rows (homepage + depth-1 subpages);
  median ~1900 chars/page; <1% of rows <50 chars. If capture rate drops well
  below ~60% or pages/snapshot collapses to ~1, suspect a scraper/URL
  regression, not just "no data".
