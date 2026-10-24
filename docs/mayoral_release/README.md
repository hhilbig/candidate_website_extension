# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

This dataset contains the archived campaign websites of mayoral candidates in the 100 largest US cities between January 2010 and September 2026. For each election, I include the winner and the runner-up. In cities with partisan mayoral primaries, I also include the top two candidates of the decisive primary. All pages come from the Internet Archive's Wayback Machine, and each page keeps its archived address and date.

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

`CODEBOOK.md` defines every column and the keys that link the files. `manifest.json` gives each file's checksum.

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]
```

## Population

I define the population as the elections that decided who became mayor and had at least two candidates. In cities with a runoff, this is the runoff. I take the winner and the runner-up of each election from official results (`eligible` = `True`). In cities with partisan primaries, the race is often decided in the primary of the dominant party. For these cities, I therefore add the primary of the party that won the general election and its top two candidates.

## Text

For each campaign website, I downloaded the pages that the Wayback Machine holds from the period around the election. I consider a page usable (`release_usable`) if it was archived within a year of the election, contains campaign content, is not a duplicate, and belongs to the candidate's campaign for this election. Language models checked each website and each page against the last two conditions, and a second model audited a sample of these decisions. The two panels join the usable text of each candidate. Each page also records whether it was archived before or after election day (`phase`) and, in races with two rounds, before or after the first round (`campaign_period`).

## Coverage

The population contains 430 elections and 860 finalists. Of these finalists, 710 (83 percent) have usable text: 88 percent of winners and 77 percent of runners-up. For 71 percent of the elections, I have text for both finalists. Coverage is also higher in larger cities. When text is missing, `text_status` records the reason.

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

`mayoral_ledb_crosswalk.csv` links each finalist to the American Local Government Elections Database (ALGED), which covers mayoral elections up to 2021. The crosswalk includes ALGED's estimated party and incumbency, and its IDs allow a join to all other ALGED variables. Because `city_id` is the Census place code, the files also join directly to Census data. `CODEBOOK.md` gives examples of both joins.

## Limitations

I note three limitations. First, the Wayback Machine did not archive every website or every page, and some websites keep little text in the archive. Text from the first round of a two-round race is particularly thin, because I downloaded the copy of each page that is closest to the final election. Second, text volume varies widely between candidates, and menus and footers repeat across pages (`text_unique` keeps each text segment once). A page archived before the election can also be older than the campaign. Third, AI models found and checked the websites, and I did not validate these decisions by hand.

## Terms of use

I release the compilation and documentation under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material remains with its authors. Each page records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
