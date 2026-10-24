# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

This dataset contains the archived campaign websites of mayoral candidates in the 100 largest US cities between January 2010 and September 2026. For each election, I include the winner and the runner-up. In cities with partisan mayoral primaries – where the race is often decided in the primary – the data also include the top two candidates of that primary. All pages were downloaded from the Internet Archive's Wayback Machine, and for each page I keep the archived address as well as the capture date.

## Files

| File | Rows | One row is |
|---|---:|---|
| `mayoral_cities.csv` | 100 | A city |
| `mayoral_city_years.csv` | 1,700 | A city and a calendar year |
| `mayoral_elections.csv` | 464 | A recorded mayoral election |
| `mayoral_finalists.csv` | 882 | A winner or runner-up of a recorded election |
| `mayoral_page_corpus.parquet` | 46,466 | An archived campaign web page |
| `mayoral_finalist_panel.csv` | 710 | A finalist with usable text, with that text joined |
| `mayoral_primary_elections.csv` | 56 | A decisive partisan primary |
| `mayoral_primary_candidates.csv` | 105 | A top-two candidate of a decisive primary |
| `mayoral_primary_panel.csv` | 63 | A primary candidate with usable text, with that text joined |
| `mayoral_ledb_crosswalk.csv` | 882 | A finalist, with the IDs, estimated party and incumbency of the same person in ALGED |

Column definitions and the keys that link the files are in `CODEBOOK.md`; checksums for each file are in `manifest.json`.

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]
```

## Population

The population consists of all elections that decided who became mayor (i.e. the runoff in cities that hold one) and in which at least two candidates ran. Winners and runners-up are taken from official results (`eligible` = `True`). Because the race in cities with partisan primaries is often effectively settled in the primary of the dominant party, I also include, for these cities, the primary of the party that went on to win the general election, along with its top two candidates.

## Text

For each campaign website, I downloaded all pages the Wayback Machine holds for the period around the election. Pages are counted as usable (`release_usable`) when they were archived within a year of the election, contain campaign content, are not duplicates of other pages, and are part of the candidate's campaign in this election rather than, e.g., an earlier campaign of the same candidate. I checked the last two conditions with language models for every website and page, and a second model then audited a sample of those decisions. The two panel files contain the joined usable text of each candidate. `phase` records whether a page was archived before or after election day, and `campaign_period` records, for races with two rounds, whether it was archived before or after the first round.

## Coverage

Of the 860 finalists in the 430 elections, 710 (83 percent) have usable text – 88 percent of winners and 77 percent of runners-up – and for 71 percent of elections I have text for both finalists. Coverage is somewhat higher in larger cities. Where text is missing, the reason is given in `text_status`.

| Year | Elections | Finalists with text | Winners (%) | Runners-up (%) | Both finalists (%) |
|---|---:|---:|---:|---:|---:|
| 2010 | 16 | 23 of 32 | 56 | 88 | 56 |
| 2011 | 32 | 50 of 64 | 84 | 72 | 69 |
| 2012 | 23 | 30 of 46 | 70 | 61 | 52 |
| 2013 | 32 | 55 of 64 | 91 | 81 | 75 |
| 2014 | 21 | 33 of 42 | 90 | 67 | 57 |
| 2015 | 30 | 50 of 60 | 87 | 80 | 73 |
| 2016 | 21 | 37 of 42 | 95 | 81 | 81 |
| 2017 | 33 | 57 of 66 | 97 | 76 | 76 |
| 2018 | 22 | 28 of 44 | 77 | 50 | 45 |
| 2019 | 31 | 52 of 62 | 90 | 77 | 68 |
| 2020 | 28 | 47 of 56 | 89 | 79 | 71 |
| 2021 | 26 | 48 of 52 | 88 | 96 | 88 |
| 2022 | 25 | 38 of 50 | 88 | 64 | 56 |
| 2023 | 27 | 46 of 54 | 89 | 81 | 74 |
| 2024 | 32 | 58 of 64 | 97 | 84 | 84 |
| 2025 | 24 | 45 of 48 | 96 | 92 | 92 |
| 2026 | 7 | 13 of 14 | 100 | 86 | 86 |
| All | 430 | 710 of 860 | 88 | 77 | 71 |

## Linking to other data

`mayoral_ledb_crosswalk.csv` links each finalist to the American Local Government Elections Database (ALGED), which covers mayoral elections up to 2021, and carries over ALGED's estimated party and incumbency; the ALGED IDs in the file can be used to join any other ALGED variable. Because `city_id` is simply the Census place code, merging with Census data is straightforward; `CODEBOOK.md` has examples of both joins.

## Limitations

The main limitation is that the Wayback Machine did not archive every website, or every page of a website, so for some candidates little text is available – this is particularly so for the first round of two-round races, because for each page I downloaded the copy closest to the final election. The amount of text also differs a lot between candidates, and menus, footers and the like show up on many pages of the same site (`text_unique` keeps each segment once). A page archived before the election may also predate the campaign. Last, websites were identified and checked using AI models, and I did not hand-validate these decisions.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material remains with the authors. Every page comes with its archived source address (`snap_url`), and rights holders who want material removed can email me at hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
