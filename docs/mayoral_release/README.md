# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

**Draft for review, October 17, 2026 (version 5). Not yet published.** The citation is **TO DECIDE** until the Dataverse DOI exists.

This dataset holds the archived campaign websites of the winner and the runner-up of mayoral elections in the 100 largest US cities, from January 2010 to September 14, 2026. All pages come from the Internet Archive's Wayback Machine. Each page keeps its archived address and archive date, so every record can be traced to its source.

The dataset accompanies our archived campaign websites of US House and Senate candidates (Harvard Dataverse, doi:10.7910/DVN/BZ2JRS). Page text is extracted with the same code in both datasets. The House/Senate panel keeps one snapshot per candidate (the one with the most text), while this panel joins all usable pages. For a like-for-like comparison, build documents from the page corpus.

## Files

| File | Rows | One row is |
|---|---:|---|
| `mayoral_cities.csv` | 100 | A city, with its 2020 population and counties |
| `mayoral_city_years.csv` | 1,700 | A city and a calendar year, with the result of the election search |
| `mayoral_elections.csv` | 464 | A recorded mayoral election |
| `mayoral_finalists.csv` | 882 | A winner or runner-up of a recorded election, with the status of their website text |
| `mayoral_page_corpus.parquet` | 44,215 | An archived page of a finalist's campaign website |
| `mayoral_ledb_crosswalk.csv` | 882 | A finalist, with the candidate and contest IDs of the same person in ALGED |
| `mayoral_finalist_panel.csv` | 710 | A finalist with usable text, with the text of all usable pages joined, and separately the text from before the election |

`CODEBOOK.md` defines every column and the keys that link the files. `manifest.json` gives each file's row and column counts, size and SHA-256 checksum.

In Python:

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
population = finalists[finalists["eligible"] == "True"]           # the 860 finalists
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]                            # the pages in the panel
before = usable[usable["phase"] == "before"]                       # pages archived before election day
print(panel["text_before"].iloc[0][:500])                           # pre-election text of one finalist
```

In R:

```r
library(readr)
library(arrow)

finalists <- read_csv("mayoral_finalists.csv", col_types = cols(.default = "c"))
panel <- read_csv("mayoral_finalist_panel.csv", col_types = cols(city_id = "c"))
pages <- read_parquet("mayoral_page_corpus.parquet")
usable <- pages[pages$release_usable, ]
```

## Population

The cities are the 100 largest incorporated places by 2020 Census population. For each city and year from 2010 to 2026, we searched official sources for mayoral elections (`mayoral_city_years.csv`). An election is in the population if it decided who became mayor and had at least two candidates; in a runoff system this is the runoff. 430 of the 464 recorded elections qualify. Their winners and runners-up, taken from official results, are the 860 finalists (`eligible` = `True`).

## Collection

We combined an older roster of candidate URLs with web searches by AI agents (Claude models with web access) and scripted searches of the Wayback Machine and Common Crawl. `url_lead_basis` records how each site was found. A site counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election, and a second agent checked every site found by search. For each site we downloaded every HTML page the Wayback Machine holds, archived between 567 days before and 148 days after the election. For each page address we took the copy nearest the election and, where that copy is from election day or later, also the latest copy from the year before the election. Text is extracted with the House and Senate code.

## Usable pages

A page is usable (`release_usable`) if it was archived within 365 days of the election, is not excluded by a page rule (empty, parked, spam, error, listing and non-HTML pages; see `exclusion_reason`), is not a duplicate, and its site passed the site check. Pages archived earlier or later stay in the corpus but are not usable. A page check then reads each remaining page: the language model labels whether the page belongs to this mayoral campaign (`page_check`), and pages only about another race of the candidate, another office or no campaign are excluded (`not_this_campaign`). Pages about an earlier round of the same race, such as a primary, count as this campaign. Claude Opus checked a random sample of the model's labels and decided by hand the pages that the model dated to the election year or the year before. Of two copies of the same page, the kept copy is a usable one from before the election where one exists. For the site check, an open-weight language model (Qwen3.6-27B) read each site that had pages in the window, and an agent reviewed every site the model doubted. A second model (Claude Opus) then audited a random sample of 50 sites and re-reviewed every site the agent had decided, settling disagreements in the archive; it confirmed all 36 sampled sites that the language model had passed alone and changed 6 of the agent's 82 decisions. 10 sites are excluded. 20,076 of the 44,215 pages are usable, from 717 of the 743 sites with pages in the window, and 16,778 of the usable pages (84 percent) were archived before election day.

## Coverage

710 of the 860 finalists (83 percent) have usable text, and 306 of the 430 elections (71 percent) have text for both finalists. `text_status` gives the reason when text is missing:

| Status | Finalists | Meaning |
|---|---:|---|
| `text` | 710 | Usable pages exist |
| `no_url_lead` | 75 | No search found a campaign website |
| `site_not_archived` | 32 | A website is known, but the archive holds no copy within a year of the election |
| `no_usable_pages` | 16 | Pages were downloaded, but none is usable |
| `excluded_site_check` | 11 | The site check excluded every downloaded site |
| `check_incomplete` | 11 | The last search could not finish because the search engine or the archive stopped responding |
| `site_rejected` | 5 | The search rejected the only known site and found no other |

Coverage is higher for winners than for runners-up (88 versus 77 percent), in larger cities (92 percent in the 25 largest, 74 percent in those ranked 76–100), and in later years (77 percent in 2010–2013, 87 percent in 2022–2026).

In some elections the runner-up received very few votes, for example under 1 percent; `top_two_vote_share` identifies these contests.

## Link to ALGED

`mayoral_ledb_crosswalk.csv` links each finalist to the American Local Government Elections Database (ALGED; de Benedictis-Kessner, Lee, Velez and Warshaw, OSF project mv5e6, CC BY 4.0), which covers mayoral elections to 2021 and adds party, incumbency and campaign-finance variables. The file holds only ALGED's IDs; join ALGED's `ledb_candidatelevel.csv` on `ledb_candid`. A match requires the same city, year and person by name; `ledb_match` gives the basis or the reason for no match.

## Limitations

1. The Wayback Machine did not archive every site or every page, and sites built with tools such as Wix or Squarespace often keep little text in the archive.
2. Menus and footers repeat across pages, and text volume varies widely between finalists. The panel's `text_unique` keeps each segment once (62 percent of the characters of `text`), and `text_clean` keeps only sentence-like segments. Some sites repeat pages in a second language (for example `/en/` and `/es/`).
3. `phase` comes from the archive date. A page archived after the election can contain victory messages, and a page archived before it can be older than the campaign. The panel's `text_before` holds only pages archived before election day.
4. AI agents and language models found and checked the websites, and a second model audited the checks. Nobody validated them by hand. The page check removes most pages from other races on a reused site, but a model label can be wrong in either direction.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0), the same terms as the House and Senate dataset. Copyright in the campaign material stays with its authors. Each page row records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

**TO DECIDE** after the Dataverse DOI is assigned.
