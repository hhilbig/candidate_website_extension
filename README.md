# Candidate Website Extension

Extends the Di Tella, Kotti, Le Pennec, and Pons (2025) U.S. House candidate website corpus (2002-2016) forward to 2018-2024 and across offices to Senate races. Scrapes archived candidate websites from the Wayback Machine and extracts visible text for downstream analysis. Uses an OpenFEC + Wikidata waterfall to discover candidate website URLs.

## Scope

This project collects data that is **not** already in the ICPSR 226001 dataset:

| Office | Years | Source | Status |
|--------|-------|--------|--------|
| House 2002-2016 | ICPSR 226001 (Di Tella et al.) | Already collected | DO NOT re-collect |
| House 2018-2024 | This project (FEC + OpenFEC + Wikidata + Wayback) | To collect |
| Senate 2002-2024 | This project (FEC + OpenFEC + Wikidata + Wayback) | To collect |

## Installation

```bash
git clone https://github.com/hhilbig/candidate_website_extension.git
cd candidate_website_extension
pip install -r requirements.txt
```

## Quick Start

### 1. Build a candidate roster

```bash
# House candidates for 2022
python -m src.build_candidate_roster --office house --year 2022

# Senate candidates for all available years
python -m src.build_candidate_roster --office senate --years 2002-2024
```

### 2. Scrape websites from Wayback Machine

```bash
# Scrape using a roster file
python -m src.scrape_wayback --office house --year 2022

# Or specify a roster directly
python -m src.scrape_wayback --roster data/rosters/roster_senate_2020.csv

# Control parallelism and logging
python -m src.scrape_wayback --office house --year 2022 --threads 4 --log-level DEBUG
```

## Configuration

Edit `config/config.yaml` to adjust:

- **scope**: Which offices and years to process
- **wayback**: Rate limits, timeouts, retry behavior
- **scraping**: Thread count, subpage crawl depth, excluded domains
- **url_sources**: OpenFEC API key, Wikidata settings
- **output**: Directory paths for all outputs

## Output Format

Scraped data lands in `data/snapshots/{office}/{year}/` as CSV files (one per candidate). Each row is a single page from a single Wayback Machine snapshot:

| Column | Description |
|--------|-------------|
| `candidate` | Candidate name |
| `state` | State abbreviation |
| `district` | District number (House only) |
| `office` | `house` or `senate` |
| `year` | Election year |
| `party` | `D` or `R` |
| `date` | Snapshot timestamp |
| `text_snap_content` | Extracted visible text |
| `n_char` / `n_words` | Text length metrics |

See [SPEC.md](SPEC.md) for the full schema and detailed specification.

## Project Structure

```
candidate_website_extension/
├── README.md                          # This file
├── SPEC.md                            # Detailed specification
├── requirements.txt                   # Python dependencies
├── config/
│   └── config.yaml                    # Configuration
├── src/
│   ├── build_candidate_roster.py      # FEC roster builder + URL waterfall
│   ├── scrape_wayback.py              # Core Wayback Machine scraper
│   ├── extract_text.py                # HTML → text extraction
│   ├── name_utils.py                  # Candidate name normalization
│   ├── utils.py                       # Rate limiting, checkpointing, logging
│   └── url_sources/                   # URL discovery modules
│       ├── openfec.py                 # OpenFEC API
│       └── wikidata.py                # Wikidata SPARQL
└── data/                              # Output directory (.gitignored)
    ├── rosters/                        # Candidate roster CSVs
    ├── snapshots/{office}/{year}/      # Scraped website content
    └── progress/                       # Checkpoint files for resumability
```

## Citation

This project extends:

> Di Tella, Rafael, Laura Kotti, Caroline Le Pennec, and Vincent Pons. 2025. "Replication Data for: The Economics of Populism." ICPSR 226001-V1. https://doi.org/10.3886/E226001V1
