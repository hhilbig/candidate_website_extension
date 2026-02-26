# Candidate Website Extension

Extends the Di Tella, Galiani, and Torrens (2025) U.S. House candidate website corpus (2002-2016) forward to 2018-2024 and across offices to Senate and gubernatorial races. Scrapes archived candidate websites from the Wayback Machine and extracts visible text for downstream analysis. Uses [ScrapeGraphAI](https://github.com/ScrapeGraphAI/Scrapegraph-ai) for intelligent roster building from Ballotpedia.

## Installation

```bash
git clone https://github.com/hhilbig/candidate_website_extension.git
cd candidate_website_extension
pip install -r requirements.txt
playwright install  # Required for ScrapeGraphAI browser automation
```

## Quick Start

### 1. Build a candidate roster

```bash
# House candidates for 2022 (FEC + Ballotpedia)
python -m src.build_candidate_roster --office house --year 2022

# Senate candidates for all available years
python -m src.build_candidate_roster --office senate --years 2002-2024

# Governor candidates (Ballotpedia via ScrapeGraphAI)
python -m src.build_candidate_roster --office governor --year 2022
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
- **scrapegraph**: LLM model for Ballotpedia extraction (requires API key for the chosen LLM provider)
- **output**: Directory paths for all outputs

## Output Format

Scraped data lands in `data/snapshots/{office}/{year}/` as CSV files (one per candidate). Each row is a single page from a single Wayback Machine snapshot:

| Column | Description |
|--------|-------------|
| `candidate` | Candidate name |
| `state` | State abbreviation |
| `district` | District number (House only) |
| `office` | `house`, `senate`, or `governor` |
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
│   ├── scrape_wayback.py              # Core Wayback Machine scraper
│   ├── build_candidate_roster.py      # FEC/Ballotpedia roster builder
│   ├── extract_text.py                # HTML → text extraction
│   └── utils.py                       # Rate limiting, checkpointing, logging
└── data/                              # Output directory (.gitignored)
    ├── rosters/                        # Candidate roster CSVs
    ├── snapshots/{office}/{year}/      # Scraped website content
    └── progress/                       # Checkpoint files for resumability
```

## Citation

This project extends:

> Di Tella, Rafael, Sebastian Galiani, and Gustavo Torrens. 2025. "Replication Data for: The Economics of Populism." ICPSR 226001-V1. https://doi.org/10.3886/E226001V1
