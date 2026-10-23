# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

This dataset holds the archived campaign websites of the winner and the runner-up of mayoral elections in the 100 largest US cities, from January 2010 to September 14, 2026. In the 13 of these cities that hold partisan mayoral primaries, it also holds the websites of the top two candidates of the decisive primary. All pages come from the Internet Archive's Wayback Machine. Each page keeps its archived address and archive date, so every record can be traced to its source.

## Files

| File | Rows | One row is |
|---|---:|---|
| `mayoral_cities.csv` | 100 | A city, with its 2020 population and counties |
| `mayoral_city_years.csv` | 1,700 | A city and a calendar year, with the result of the election search |
| `mayoral_elections.csv` | 464 | A recorded mayoral election |
| `mayoral_finalists.csv` | 882 | A winner or runner-up of a recorded election, with the status of their website text |
| `mayoral_page_corpus.parquet` | 46,466 | An archived page of a finalist's or primary candidate's campaign website |
| `mayoral_ledb_crosswalk.csv` | 882 | A finalist, with the candidate and contest IDs of the same person in ALGED |
| `mayoral_finalist_panel.csv` | 710 | A finalist with usable text, with the text of all usable pages joined (each text once), and separately the text from before the election |
| `mayoral_primary_elections.csv` | 56 | A decisive partisan primary |
| `mayoral_primary_candidates.csv` | 105 | A first- or second-placed candidate of a decisive primary, with the status of their website text |
| `mayoral_primary_panel.csv` | 63 | A primary candidate with usable text from the year before the primary |

`CODEBOOK.md` defines every column and the keys that link the files. `manifest.json` gives each file's row and column counts, size and SHA-256 checksum.

In Python:

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
population = finalists[finalists["eligible"] == "True"]
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]
```

In R:

```r
library(readr)
library(arrow)

panel <- read_csv("mayoral_finalist_panel.csv", col_types = cols(city_id = "c"))
usable <- subset(read_parquet("mayoral_page_corpus.parquet"), release_usable)
```

## Population

The cities are the 100 largest incorporated places by 2020 Census population. For each city and year from 2010 to 2026, we searched official sources for mayoral elections (`mayoral_city_years.csv`). An election is in the population if it decided who became mayor and had at least two candidates; in a runoff system this is the runoff. 430 of the 464 recorded elections qualify. Their winners and runners-up, taken from official results, are the 860 finalists (`eligible` = `True`).

Primaries. In cities with partisan mayoral primaries, the race is often decided in the primary of the dominant party. For these cities the dataset adds the decisive primary: the primary of the party whose nominee won the general election. 13 cities qualify (Baltimore, Buffalo, Charlotte, Fort Wayne, Indianapolis, Louisville, New York, Philadelphia, Pittsburgh, St. Louis, Tucson, Washington and Winston-Salem). They held 56 decisive primaries from 2010 to 2026, 55 Democratic and 1 Republican, including the Washington primary of June 16, 2026, whose general election is after the end of the data. 7 primaries had one candidate (`unopposed` = `True`). One primary is listed although it was not decisive: in Indianapolis 2011 the Republican nominee won the general election after an unopposed primary, so the contested Democratic primary of that year is also included. The first- and second-placed candidates, from official results, are the 105 primary candidates. 59 of them are also finalists of the following general election (`finalist_candidate_election_id`); for the other 46 we searched for a website in the same way as for the finalists. Candidates who placed third or lower are not in the data.

## Campaign periods

Many races have two rounds: a primary or first round, then the general election or a runoff. For each of the 430 elections, `mayoral_elections.csv` gives whether the race had two rounds (227) or one (203) (`rounds`) and the date of the first round (`first_round_date`, with its source in `first_round_source`). Each page carries `campaign_period`: `first_round` for a capture on or before the first round, `final_round` for one after it, and `single_round` for a race with one round. Of the usable finalist pages, 3,733 are from the first round, 11,057 from the final round and 6,117 from single-round races. `campaign_period` refers only to the first round; combine it with `phase` to keep pages from before election day.

## Collection

We combined an older roster of candidate URLs with web searches by AI agents (Claude models with web access) and scripted searches of the Wayback Machine and Common Crawl. `url_lead_basis` records how each site was found. A site counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election, and a second agent checked every site found by search. For each site we downloaded every HTML page the Wayback Machine holds from the period around the election. For each page address we took the copy nearest the election and, where that copy is from election day or later, also the latest copy from the year before the election. The sites of the 46 searched primary candidates were downloaded in the same way, with the primary as the reference date.

## Usable pages

A page is usable (`release_usable`) if it was archived within 365 days of the election, is not excluded by a page rule (`exclusion_reason`), is not a duplicate (`duplicate_of`), and its site passed the site check. Pages archived earlier or later stay in the corpus but are not usable. The site check confirmed that each website belongs to this candidate's campaign for this election: an open-weight language model (Qwen3.6-27B) read every site, an AI agent reviewed every site the model doubted, and a second model (Claude Opus) audited a sample of the decisions. 10 finalist sites and 1 primary-candidate site are excluded. The page check then labeled each remaining page (`page_check`). Pages only about another race of the candidate, another office or no campaign are excluded (`not_this_campaign`); pages about an earlier round of the same race count as this campaign. Of the 44,215 finalist pages, 20,907 are usable, from 717 of the 743 sites with pages in the window, and 16,949 of them (81 percent) were archived before election day.

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

## Primary coverage

`mayoral_primary_panel.csv` holds the text of the year before the primary: pages that pass the page rules and the site check, archived in the 365 days up to and including the primary date, one copy per text and address (the copy nearest the primary). For a primary candidate who is also a finalist, the text comes from the finalist's websites (`text_source` = `finalist_pages`); for the others, from the website found for the primary (`own_search`). For a person in both panels, the primary text is mostly a subset of the finalist `text` (the primary-period part of the same pages), so stacking the two panels counts that text twice; use `campaign_period` in the corpus to split a finalist's pages by round. 63 of the 105 primary candidates have text: 40 of the 59 who are also finalists and 23 of the 46 others. In 17 of the 49 contested primaries both candidates have text. `text_status` in `mayoral_primary_candidates.csv` gives the reason when text is missing: no usable pages in the primary year (26), no website found (8), no copy in the archive (5), an incomplete search (2) and a site excluded by the site check (1).

## Link to ALGED

`mayoral_ledb_crosswalk.csv` links each finalist to the American Local Government Elections Database (ALGED), which adds party, incumbency and campaign-finance variables for mayoral elections up to 2021. `CODEBOOK.md` gives the source and the join.

## Limitations

1. The Wayback Machine did not archive every site or every page, and sites built with tools such as Wix or Squarespace often keep little text in the archive.
2. Text volume varies widely between finalists. Menus and footers repeat across pages (`text_unique` counts each segment once), and some sites repeat pages in a second language (for example `/en/` and `/es/`).
3. `phase` comes from the archive date. A page archived after the election can contain victory messages, and a page archived before it can be older than the campaign.
4. First-round text is thinner than final-round text, because the download took the copy of each address nearest the general election. Some finalists in two-round races, for example both Chicago finalists in 2019, have no usable first-round page.
5. AI agents and language models found and checked the websites, and a second model audited the checks. Nobody validated them by hand, and a model label can be wrong in either direction.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material stays with its authors. Each page row records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
