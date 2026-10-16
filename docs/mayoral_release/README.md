# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

**Draft for review, October 16, 2026. Not yet published.** The citation is **TO DECIDE** until the Dataverse DOI exists.

This dataset holds the archived campaign websites of the winner and the runner-up of mayoral elections in the 100 largest US cities, from January 2010 to September 14, 2026. All pages come from the Internet Archive's Wayback Machine. Each page keeps its archived address and archive date, so every record can be traced to its source.

The dataset accompanies our archived campaign websites of US House and Senate candidates (Harvard Dataverse, doi:10.7910/DVN/BZ2JRS). Text is extracted with the same code, so page text from the two datasets is comparable.

## Files

| File | Rows | One row is |
|---|---:|---|
| `mayoral_cities.csv` | 100 | A city |
| `mayoral_city_years.csv` | 1,700 | A city and a calendar year, with the result of the election search |
| `mayoral_elections.csv` | 464 | A recorded mayoral election |
| `mayoral_finalists.csv` | 882 | A winner or runner-up of a recorded election, with the status of their website text |
| `mayoral_page_corpus.parquet` | 38,333 | An archived page of a finalist's campaign website |
| `mayoral_finalist_panel.csv` | 704 | A finalist with usable text, with the text of all usable pages joined |

`CODEBOOK.md` defines every column and the keys that link the files. `manifest.json` gives each file's row and column counts, size and SHA-256 checksum.

In Python:

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
population = finalists[finalists["eligible"] == "True"]           # the 860 finalists
panel = pd.read_csv("mayoral_finalist_panel.csv")
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]                            # the pages in the panel
before = usable[usable["phase"] == "before"]                       # pages archived before election day
```

In R:

```r
library(readr)
library(arrow)

finalists <- read_csv("mayoral_finalists.csv", col_types = cols(.default = "c"))
panel <- read_csv("mayoral_finalist_panel.csv")
pages <- read_parquet("mayoral_page_corpus.parquet")
usable <- pages[pages$release_usable, ]
```

## Population

The cities are the 100 largest incorporated places by 2020 Census population. For each city and year from 2010 to 2026, we searched official sources for mayoral elections (`mayoral_city_years.csv`). An election is in the population if it decided who became mayor and had at least two candidates; in a runoff system this is the runoff. 430 of the 464 recorded elections qualify. Their winners and runners-up, taken from official results, are the 860 finalists (`eligible` = `True`).

## Collection

We combined an older roster of candidate URLs with web searches by AI agents (Claude models with web access) and scripted searches of the Wayback Machine and Common Crawl. `url_lead_basis` records how each site was found. A site counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election, and a second agent checked every site found by search. For each site we downloaded every HTML page the Wayback Machine holds from 18 months before to 120 days after the election, taking the copy nearest the election. Text is extracted with the House and Senate code.

## Usable pages

A page is usable (`release_usable`) if it was archived within 365 days of the election, is not a duplicate, is not excluded by a page rule (empty, parked, spam, error and listing pages; see `exclusion_reason`), and its site passed the site check. For the site check, an open-weight language model (Qwen3.6-27B) read each site, and an agent reviewed every site the model doubted; 14 of 722 sites were excluded. 21,040 of the 38,333 pages are usable.

## Coverage

704 of the 860 finalists (82 percent) have usable text, and 302 of the 430 elections (70 percent) have text for both finalists. `text_status` gives the reason when text is missing:

| Status | Finalists | Meaning |
|---|---:|---|
| `text` | 704 | Usable pages exist |
| `no_url_lead` | 75 | No search found a campaign website |
| `site_not_archived` | 37 | A website is known, but the archive holds no copy within a year of the election |
| `no_usable_pages` | 15 | Pages were downloaded, but none is usable |
| `excluded_site_check` | 13 | The site check excluded every downloaded site |
| `check_incomplete` | 11 | The last search could not finish because the search engine or the archive stopped responding |
| `site_rejected` | 5 | The search rejected the only known site and found no other |

Coverage is higher for winners than for runners-up (88 versus 76 percent), in larger cities (92 percent in the 25 largest, 75 percent in those ranked 76–100), and in later years (76 percent in 2010–2013, 87 percent in 2022–2026).

## Limitations

1. The Wayback Machine did not archive every site or every page, and sites built with tools such as Wix or Squarespace often keep little text in the archive.
2. Menus and footers repeat across pages. The panel's `text_clean` keeps only sentence-like segments.
3. Pages after the election can contain victory messages; `phase` restricts pages to before the election.
4. AI agents and a language model found and checked the websites. Nobody validated these checks by hand.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0), the same terms as the House and Senate dataset. Copyright in the campaign material stays with its authors. Each page row records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

**TO DECIDE** after the Dataverse DOI is assigned.
