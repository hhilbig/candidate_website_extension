# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

This dataset holds the archived campaign websites of the winner and the runner-up of mayoral elections in the 100 largest US cities, from January 2010 to September 14, 2026. In cities with partisan mayoral primaries, it also holds the websites of the top two candidates of the decisive primary. All pages come from the Internet Archive's Wayback Machine, and each page keeps its archived address and date.

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
| `mayoral_ledb_crosswalk.csv` | 882 | A finalist, with the IDs of the same person in ALGED |

`CODEBOOK.md` defines every column and the keys that link the files. `manifest.json` gives each file's checksum.

```python
import pandas as pd

finalists = pd.read_csv("mayoral_finalists.csv", dtype=str)
panel = pd.read_csv("mayoral_finalist_panel.csv", dtype={"city_id": str})
pages = pd.read_parquet("mayoral_page_corpus.parquet")
usable = pages[pages["release_usable"]]
```

## Population

The cities are the 100 largest US cities by 2020 population. An election is in the population if it decided who became mayor and had at least two candidates; in a runoff system this is the runoff. Its winner and runner-up, from official results, are the finalists (`eligible` = `True`). In cities with partisan primaries, the race is often decided in the primary of the dominant party, so the dataset adds that primary and its top two candidates.

## Text

For each campaign website, we downloaded the pages that the Wayback Machine holds from the period around the election. A page is usable (`release_usable`) if it was archived within a year of the election, is a real content page, is not a duplicate, and belongs to this candidate's campaign for this election. Language models checked each website and each page for the last two conditions, and a second model audited a sample of their decisions. The panels join the usable text of each candidate.

Each page records whether it was archived before or after election day (`phase`) and, in races with two rounds, before or after the first round (`campaign_period`).

## Coverage

83 percent of the finalists have usable text, and 71 percent of the elections have text for both finalists. Coverage is higher for winners, in larger cities and in later years. `text_status` gives the reason when text is missing.

## Limitations

1. The Wayback Machine did not archive every site or every page, and some sites keep little text in the archive.
2. Text volume varies widely between candidates, and menus and footers repeat across pages (`text_unique` keeps each segment once). A page archived before the election can be older than the campaign.
3. Text from the first round of a two-round race is thinner than text from the final round.
4. AI models found and checked the websites. Nobody validated them by hand.

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material stays with its authors. Each page records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
