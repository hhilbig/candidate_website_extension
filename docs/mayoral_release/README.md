# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

This dataset contains the archived campaign websites of mayoral candidates in the 100 largest US cities between January 2010 and September 2026. For each election, we include the winner and the runner-up. In cities with partisan mayoral primaries, we also include the top two candidates of the decisive primary. All pages come from the Internet Archive's Wayback Machine, and each page keeps its archived address and date.

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

We define the population as the elections that decided who became mayor and had at least two candidates. In cities with a runoff, this is the runoff. We take the winner and the runner-up of each election from official results (`eligible` = `True`). In cities with partisan primaries, the race is often decided in the primary of the dominant party. For these cities, we therefore add the primary of the party that won the general election and its top two candidates.

## Text

For each campaign website, we downloaded the pages that the Wayback Machine holds from the period around the election. We consider a page usable (`release_usable`) if it was archived within a year of the election, contains campaign content, is not a duplicate, and belongs to the candidate's campaign for this election. Language models checked each website and each page against the last two conditions, and a second model audited a sample of these decisions. The two panels join the usable text of each candidate. Each page also records whether it was archived before or after election day (`phase`) and, in races with two rounds, before or after the first round (`campaign_period`).

## Coverage

83 percent of the finalists have usable text, and 71 percent of the elections have text for both finalists. Coverage is higher for winners, in larger cities, and in more recent elections. When text is missing, `text_status` records the reason.

## Limitations

We note three limitations. First, the Wayback Machine did not archive every website or every page, and some websites keep little text in the archive. Text from the first round of a two-round race is particularly thin, because we downloaded the copy of each page that is closest to the final election. Second, text volume varies widely between candidates, and menus and footers repeat across pages (`text_unique` keeps each text segment once). A page archived before the election can also be older than the campaign. Third, AI models found and checked the websites, and we did not validate these decisions by hand.

## Terms of use

We release the compilation and documentation under the Creative Commons Attribution 4.0 International license (CC BY 4.0). Copyright in the campaign material remains with its authors. Each page records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

Please cite the dataset as shown on its Harvard Dataverse page.
