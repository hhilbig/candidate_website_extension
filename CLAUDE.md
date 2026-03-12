# Candidate Website Extension

## Project Overview
Extends ICPSR 226001 (Di Tella, Kotti, Le Pennec, and Pons 2025) which covers House 2002-2016. This project collects candidate website snapshots from the Wayback Machine for:
- **House 2018-2024**
- **Senate 2002-2024**

Do NOT re-collect House 2002-2016 — already in ICPSR dataset. Governor races are out of scope.

## Architecture
- **URL discovery:** OpenFEC API → Wikidata SPARQL waterfall
- **Snapshot scraping:** `src/scrape_wayback.py` queries Wayback Machine CDX API, downloads snapshots, extracts text
- **Text extraction:** `src/extract_text.py`
- **Progress tracking:** `ProgressTracker` in `data/progress/` — scraper resumes from where it left off

## Droplet (DigitalOcean)
- SSH: `ssh root@REDACTED-IP`
- Queue runner: `run_scrape_queue.sh` (processes years sequentially, `--threads 1`)
- Also has `run_rescrape_2016_2018.sh` (standalone script, but re-scrape is also appended to main queue)
- Logs: `logs/scrape_queue.log` (queue), `logs/{office}_{year}.log` (per-year)
- Completed years compressed as `.tar.gz` in `data/snapshots/senate/`

## Important Rules
- **threads=1 only** for Wayback scraping — higher thread counts cause "Connection refused" errors and data loss
- **Never compress a year currently being scraped** — wait until the year is fully done
- **Never run overlapping tar/compress commands** on the same files — race conditions destroyed Senate 2016+2018 data on Mar 12
- **Compress completed years** to save disk space (droplet has 24GB total)
