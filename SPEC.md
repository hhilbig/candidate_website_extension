# Specification: Candidate Website Extension

## 1. Goal

Extend the Di Tella, Kotti, Le Pennec, and Pons (2025) U.S. House candidate website corpus (ICPSR 226001-V1, covering 2002-2016) in two directions:

1. **Forward in time**: House candidate websites for 2018, 2020, 2022, 2024.
2. **Across offices**: Senate candidate websites for 2002-2024.

This repo handles **scraping only**. Downstream analysis (LLM scoring, merging with election data) stays in the parent research project.

## 2. Data Sources

### Wayback Machine CDX API

Primary source for archived web content.

- **Endpoint**: `https://web.archive.org/cdx/search/cdx`
- **Query strategy**: For each candidate URL, query all snapshots within the election-year window (January 1 through December 31).
- **Deduplication**: Python-side monthly dedup — one snapshot per (normalized URL, month), keeping the latest timestamp. No server-side `collapse` parameter, which was dropping subpages.

### Candidate Rosters

| Source | Offices | What it provides |
|--------|---------|-----------------|
| FEC bulk candidate files (`cn.txt`) | House, Senate | Candidate name, state, district, party, committee ID |
| OpenFEC API | House, Senate | Campaign website URL (from committee filings) |
| Wikidata SPARQL | House, Senate | Official website URL (from structured data) |

FEC filings do not directly include candidate website URLs in the bulk `cn.txt` file. Website URLs are resolved via a waterfall: OpenFEC API first, then Wikidata SPARQL for remaining candidates.

## 3. Scope

| Office     | Years         | Est. candidates/cycle | Total est. |
|------------|---------------|-----------------------|------------|
| House      | 2018-2024     | ~800-900 (general, D+R) | ~3,500   |
| Senate     | 2002-2024     | ~60-70 (general, D+R)   | ~700     |

House 2002-2016 is already covered by ICPSR 226001 and is **not** re-collected.

Scope is limited to Democratic and Republican general-election candidates. Primary candidates can be added by setting `stage=1` in the roster.

## 4. Scraping Approach

Adapted from the Di Tella et al. `_scraper.py` with the following improvements:

### CDX Query
- Query CDX API with `matchType=prefix` to capture subdomains and path variations.
- Filter to `statuscode:200` and `mimetype:text/html`.
- Deduplicate Python-side: one snapshot per (normalized URL, month), keeping latest timestamp per group.

### Page Fetching
- Strip Wayback Machine toolbar HTML using known markers (`<!-- END WAYBACK TOOLBAR INSERT -->`, `<!-- FILE ARCHIVED ON`).
- Parse cleaned HTML with BeautifulSoup + lxml.
- Handle frames/iframes recursively (important for early-2000s sites).
- Follow internal links (subpages) within the same domain, up to configurable depth and count limits.

### Rate Limiting
- Configurable minimum delay between requests (default 100ms).
- Exponential backoff on 429 "Too Many Requests" responses (up to 6 minutes).
- Session reset on persistent connection errors.

### Resumability
- CSV-based checkpoint files track which snapshot URLs have been processed.
- On restart, already-processed URLs are skipped.

### Parallelism
- `ThreadPoolExecutor` with configurable thread count (default 8).
- All threads share a single thread-safe rate limiter. Each thread maintains its own HTTP session.

### Known Limitations
- **JavaScript-rendered sites**: Post-2018 candidates increasingly use React/Next.js. Wayback Machine captures may be incomplete for these. A future extension could add a Playwright/Selenium fallback.
- **Social media replacing websites**: Some 2022-2024 candidates lack standalone websites.
- **Flash content**: Pre-2010 sites using Flash are captured as blank pages; filtered out by text length.

## 5. Output Schema

One CSV per candidate per year, stored in `data/snapshots/{office}/{year}/`.

| Column | Description |
|--------|-------------|
| `candidate` | Candidate full name |
| `state` | Two-letter state abbreviation |
| `district` | Congressional district (House only) |
| `office` | `house` or `senate` |
| `year` | Election year |
| `party` | `D` or `R` |
| `stage` | `1` = primary, `2` = general |
| `date` | Wayback snapshot timestamp (YYYYMMDDHHMMSS) |
| `urlkey` | Original website URL |
| `snap_url` | Full Wayback URL of this page |
| `data_source` | `wayback_cdx` |
| `n_tags` | Reserved (HTML tag count, for compatibility) |
| `n_clean_tags` | Reserved |
| `text_snap_content` | Extracted visible text (chunks joined by `#+#`) |
| `n_char` | Character count of extracted text |
| `n_words` | Word count of extracted text |

Multiple snapshots per candidate are retained. Downstream deduplication selects the longest-text snapshot per candidate.

## 6. Candidate Roster Pipeline

### House and Senate (FEC + OpenFEC + Wikidata)

1. Download FEC bulk candidate file (`cn{YY}.zip`) for the cycle.
2. Filter to target office, D+R parties.
3. Parse candidate names from FEC format (`LASTNAME, FIRSTNAME`).
4. Query OpenFEC API for campaign website URLs (using candidate committee IDs).
5. For candidates still missing URLs, query Wikidata SPARQL for official website URLs.
6. Output merged roster with all available URLs.

### Output

`data/rosters/roster_{office}_{year}.csv` with columns:
```
candidate, state, district, office, year, party, website_url
```

## 7. Configuration

All parameters are in `config/config.yaml`:

- **scope**: Which offices and years to process.
- **wayback**: Rate limits, timeouts, retry counts, user agent.
- **scraping**: Thread count, subpage depth/limits, text separator, excluded domains.
- **url_sources**: OpenFEC API key env var, Wikidata settings.
- **roster**: FEC bulk download URL template.
- **output**: Directory paths for rosters, snapshots, and progress files.

## 8. CLI Interface

### Build a roster
```bash
python -m src.build_candidate_roster --office house --year 2022
python -m src.build_candidate_roster --office senate --years 2002-2024
```

### Scrape websites
```bash
python -m src.scrape_wayback --office house --year 2022
python -m src.scrape_wayback --roster data/rosters/roster_senate_2020.csv --threads 4
```

## 9. Differences from Original Scraper

| Feature | Original (`_scraper.py`) | This project |
|---------|-------------------------|-------------|
| Offices | House only | House, Senate |
| Configuration | Hardcoded paths and params | YAML config file |
| Logging | `print()` statements | Python `logging` module |
| CLI | Thread count via `sys.argv` | Full `argparse` CLI |
| Rate limiting | Fixed `time.sleep(70/15)` | Configurable with exponential backoff |
| Checkpointing | Single progress CSV | Per-roster progress files |
| Parallelism | `threading.Thread` manual split | `ThreadPoolExecutor` |
| Roster building | External (pre-built CSV) | Integrated FEC + OpenFEC + Wikidata pipeline |
| URL discovery | Manual/external | OpenFEC API + Wikidata SPARQL waterfall |
| Output structure | Flat directory | `{office}/{year}/` hierarchy |

## 10. Citation

If using this data, cite the original dataset:

> Di Tella, Rafael, Laura Kotti, Caroline Le Pennec, and Vincent Pons. 2025. "Replication Data for: The Economics of Populism." ICPSR 226001-V1. https://doi.org/10.3886/E226001V1
