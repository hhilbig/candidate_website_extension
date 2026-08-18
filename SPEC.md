# Collection specification

This file documents the scraping pipeline and its intermediate CSV files. The
[release README](docs/RELEASE_README.md) defines the released population and
products; the [codebook](docs/deliverable_codebook.md) defines their fields.

## Coverage and sources

The project adds House candidates from 2018 through 2024 and Senate candidates
from 2002 through 2024 to the House corpus in ICPSR 226001. It does not
recollect the House years from 2002 through 2016.

| Source | Use |
|---|---|
| FEC candidate master files | Candidate name, office, state, district, party, and committee ID |
| OpenFEC | Campaign website URL |
| Wikidata | Fallback website URL |
| Wayback Machine CDX API | Archived homepage captures |

Website discovery uses OpenFEC first and Wikidata for remaining candidates.
The release later restricts the roster to Democratic and Republican candidates
whose FEC-recorded election year equals the target year, plus reviewed ballot
exceptions.

## Collection rules

For each candidate URL, the scraper:

1. Queries the Wayback CDX API for successful HTML homepage captures between
   January 1 and December 31 of the election year.
2. Groups captures into three-month buckets and selects at most 200 snapshots.
3. Removes the Wayback toolbar, parses the archived HTML, handles frames, and
   follows same-domain homepage links one level deep.
4. Removes duplicate page text within a snapshot.
5. Saves progress after each processed snapshot so interrupted runs can resume.

The query uses `matchType=exact`; subpages come from link following rather than
the CDX index. Requests share a configurable rate limiter and use exponential
backoff after HTTP 429 responses. Parallel workers maintain separate HTTP
sessions.

## Intermediate output

The scraper writes one CSV per candidate-year under
`data/snapshots/{office}/{year}/`. Each row is one archived page from one
snapshot and contains:

- candidate and contest fields: `candidate`, `state`, `district`, `office`,
  `year`, `party`, and `stage`;
- archive fields: `date`, `urlkey`, `snap_url`, `page_type`, and `data_source`;
- extracted content: `text_snap_content`, `n_char`, and `n_words`;
- compatibility fields: `n_tags` and `n_clean_tags`.

The release retains page-level rows in `raw_corpus.parquet`. Its candidate-year
panel selects the snapshot date with the most total extracted text.

## Configuration and commands

`config/config.yaml` controls office-years, timeouts, retries, thread count,
crawl depth, excluded domains, URL sources, and output paths.

Build rosters:

```bash
python -m src.build_candidate_roster --office house --year 2022
python -m src.build_candidate_roster --office senate --years 2002-2024
```

Retrieve archived sites:

```bash
python -m src.scrape_wayback --office house --year 2022
python -m src.scrape_wayback --roster data/rosters/roster_senate_2020.csv --threads 4
```

## Limitations

- Archived JavaScript applications may contain little or no recoverable text.
- Some recent candidates used social-media pages rather than standalone sites.
- Flash-based pages may appear blank.
- Linked pages can resolve to captures near, rather than exactly at, the
  requested homepage timestamp.
