# Specification: Candidate Website Extension

This file documents the collection pipeline and its intermediate CSV output.
The current release population, products, and field definitions are documented
in [the release README](docs/RELEASE_README.md) and
[codebook](docs/deliverable_codebook.md).

## 1. Goal

Extend the Di Tella, Kotti, Le Pennec, and Pons (2025) U.S. House candidate website corpus (ICPSR 226001-V1, covering 2002-2016) in two directions:

1. **Forward in time**: House candidate websites for 2018, 2020, 2022, 2024.
2. **Across offices**: Senate candidate websites for 2002-2024.

The repository also contains the release-building, validation, and
ICPSR-compatibility code.

## 2. Data Sources

### Wayback Machine CDX API

Primary source for archived web content.

- **Endpoint**: `https://web.archive.org/cdx/search/cdx`
- **Query strategy**: For each candidate URL, query with `matchType=exact` for all snapshots within the election-year window (January 1 through December 31). Exact matching returns only captures of the base URL itself; subpages are discovered via link following during scraping, matching the original ICPSR methodology.
- **Deduplication**: CDX records are grouped into three-month buckets before
  retrieval, and at most 200 snapshots are selected per candidate. Content
  deduplication within each snapshot removes duplicate page text.

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
| House      | 2018-2024     | Democratic and Republican FEC candidates | 4 cycles |
| Senate     | 2002-2024     | Democratic and Republican FEC candidates | 12 cycles |

House 2002-2016 is already covered by ICPSR 226001 and is **not** re-collected.

The release population consists of Democratic and Republican candidates whose
FEC-recorded election year equals the target year, plus reviewed
general-election ballot exceptions. General-election status is an attribute,
not a population filter.

## 4. Scraping Approach

Adapted from the Di Tella et al. `_scraper.py` with the following improvements:

### CDX Query
- Query CDX API with `matchType=exact` on the base URL only, matching the original ICPSR scraper methodology. Subpages are discovered via link following in `scrape_snapshot()`, not via CDX. Using `prefix` would return every archived subpage URL (potentially 10,000+ records per candidate), while `exact` returns only homepage snapshots (typically tens to low hundreds).
- Filter to `statuscode:200` and `mimetype:text/html`.
- Group snapshots into three-month buckets and select no more than 200 per
  candidate.

### Page Fetching
- Strip Wayback Machine toolbar HTML using known markers (`<!-- END WAYBACK TOOLBAR INSERT -->`, `<!-- FILE ARCHIVED ON`).
- Parse cleaned HTML with BeautifulSoup + lxml.
- Handle frames/iframes recursively (important for early-2000s sites).
- Follow all internal links (subpages) within the same domain, one level deep. No subpage cap, matching the original scraper.
- Content dedup within each snapshot: duplicate page texts are removed (matches original `.drop_duplicates(subset='snap_content')`).

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
- **JavaScript-rendered sites**: Wayback Machine captures may contain empty
  shells when archived JavaScript cannot be reconstructed.
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
| `page_type` | Page category: `homepage`, `issues`, `biography`, `news`, `endorsements`, `constituent_services`, `action`, `other` |
| `data_source` | `wayback_cdx` |
| `n_tags` | Reserved (HTML tag count, for compatibility) |
| `n_clean_tags` | Reserved |
| `text_snap_content` | Extracted visible text (chunks joined by `#+#`) |
| `n_char` | Character count of extracted text |
| `n_words` | Word count of extracted text |

Multiple snapshots per candidate are retained in the collection output. The
release panel selects the snapshot date with the largest total amount of
extracted text.

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
- **scraping**: Thread count, subpage depth, text separator, excluded domains.
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

If using this data, cite this release and the original dataset:

> Hilbig, Hanno. 2026. “U.S. Congressional Candidate Websites, 2002–2024.”
> Version 1.0.0. Harvard Dataverse.

> Di Tella, Rafael, Randy Kotti, Caroline Le Pennec, and Vincent Pons. 2025.
> “Keep Your Enemies Closer: Strategic Platform Adjustments during U.S. and
> French Elections.” openICPSR 226001. <https://doi.org/10.3886/E226001V1>
