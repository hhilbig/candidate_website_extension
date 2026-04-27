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
