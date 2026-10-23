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

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
population = finalists[finalists["eligible"] == "True"]
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]
```

## Population

The cities are the 100 largest incorporated places by 2020 Census population. For each city and year from 2010 to 2026, we searched official sources for mayoral elections (`mayoral_city_years.csv`). An election is in the population if it decided who became mayor and had at least two candidates; in a runoff system this is the runoff. 430 of the 464 recorded elections qualify. Their winners and runners-up, taken from official results, are the 860 finalists (`eligible` = `True`).

Primaries. In cities with partisan mayoral primaries, the race is often decided in the primary of the dominant party. For these 13 cities the dataset adds the decisive primary: the primary of the party whose nominee won the general election. There are 56 such primaries (7 with one candidate) and 105 first- and second-placed candidates, from official results; 59 of them are also finalists. In Indianapolis 2011 the Republican primary was unopposed, so the contested Democratic primary of that year is also included. Candidates who placed third or lower are not in the data.

## Campaign periods

Many races have two rounds: a primary or first round, then the general election or a runoff. `mayoral_elections.csv` gives for each of the 430 elections whether the race had two rounds (227) or one (203) and the date of the first round. Each page carries `campaign_period`: `first_round` for a capture on or before the first round, `final_round` for one after it, and `single_round` for a race with one round. `campaign_period` refers only to the first round; combine it with `phase` to keep pages from before election day.

## Collection and checks

We combined an older roster of candidate URLs with web searches by AI agents (Claude models with web access) and scripted searches of the Wayback Machine and Common Crawl. `url_lead_basis` records how each site was found. A site counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election, and a second agent checked every site found by search. For each site we downloaded every HTML page the Wayback Machine holds from the period around the election. For each page address we took the copy nearest the election and, where that copy is from election day or later, also the latest copy from the year before the election. The websites of the 46 primary candidates who are not finalists were found and downloaded in the same way, with the primary as the reference date.

A page is usable (`release_usable`) if it was archived within 365 days of the election, is not excluded by a page rule (`exclusion_reason`), is not a duplicate (`duplicate_of`), and its site passed the site check. Pages archived earlier or later stay in the corpus but are not usable. The site check confirmed that each website belongs to this candidate's campaign for this election: an open-weight language model (Qwen3.6-27B) read every site, an AI agent reviewed every site the model doubted, and a second model (Claude Opus) audited a sample of the decisions. 10 finalist sites and 1 primary-candidate site are excluded. The page check then labeled each remaining page (`page_check`). Pages only about another race of the candidate, another office or no campaign are excluded (`not_this_campaign`); pages about an earlier round of the same race count as this campaign. Of the 44,215 finalist pages, 20,907 are usable, and 81 percent of these were archived before election day.

## Coverage

710 of the 860 finalists (83 percent) have usable text, and 306 of the 430 elections (71 percent) have text for both finalists. `text_status` gives the reason when text is missing. Coverage is higher for winners than for runners-up (88 versus 77 percent), in larger cities (92 percent in the 25 largest, 74 percent in those ranked 76–100), and in later years (77 percent in 2010–2013, 87 percent in 2022–2026).

63 of the 105 primary candidates have text from the year before the primary, and in 17 of the 49 contested primaries both candidates have text. For a primary candidate who is also a finalist, the primary text comes from the finalist's websites, so stacking the two panels counts that text twice; use `campaign_period` in the corpus to split a finalist's pages by round.

## Link to ALGED

`mayoral_ledb_crosswalk.csv` links each finalist to the American Local Government Elections Database (ALGED), which adds party, incumbency and campaign-finance variables for mayoral elections up to 2021. `CODEBOOK.md` gives the source and the join.

## Limitations

1. The Wayback Machine did not archive every site or every page, and sites built with tools such as Wix or Squarespace often keep little text in the archive.
2. Website text is uneven. Text volume varies widely between finalists, menus and footers repeat across pages (`text_unique` counts each segment once), and some sites repeat pages in a second language. `phase` comes from the archive date, so a page archived after the election can contain victory messages, and a page archived before it can be older than the campaign.
3. First-round text is thinner than final-round text, because the download took the copy of each address nearest the general election. Some finalists in two-round races, for example both Chicago finalists in 2019, have no usable first-round page.
4. AI agents and language models found and checked the websites, and a second model audited the checks. Nobody validated them by hand, and a model label can be wrong in either direction.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material stays with its authors. Each page row records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
