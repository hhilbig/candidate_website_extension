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

The cities are the 100 largest incorporated places by 2020 Census population. The list is fixed, so a city that entered the top 100 after 2020 is not included.

For each city and year from 2010 to 2026, we searched official sources (city clerks, county and state election offices) for mayoral elections. Of the 1,700 city-years, 1,202 had no mayoral election, 452 had at least one, 11 had only a first round before the end of the period, and in 19 the council or another body chose the mayor. The remaining 16 could not be resolved from official sources and are listed with a note.

An election enters the population if it decided who became mayor and at least two candidates ran. In a runoff system this is the runoff. 430 of the 464 recorded elections meet the rule. The other 34 are uncontested (21), have results only from an older candidate roster that we could not check against official results (11), or could not be resolved (2). They stay in the files but outside the population.

The population is the winner and the runner-up of each of the 430 elections, taken from official results: 860 finalists, marked `eligible` = `True`.

## Collection

**Finding the website.** We started from an older roster of candidate URLs and added five searches. AI agents (Claude models with web access) searched the web, and a second, independent agent checked every site they accepted. A script tried common campaign domains (for example `lastnameformayor.com`) in the Wayback Machine and Common Crawl. A language model compared the roster URLs with their archived pages. Agents searched again for finalists whose roster URL had no archived copy near the election. A final agent search, with the same independent check, covered the 99 finalists who still had no site or whose search had not finished. A site counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election.

**Collecting pages.** For each accepted site, we listed every HTML page the Wayback Machine holds from 18 months before the election to 120 days after it. We downloaded the copy of each page nearest the election, with no cap on the number of pages. We skipped addresses that are not content pages, such as style and script files, session links and crawler loops.

**Extracting text.** The House and Senate extraction code turns each archived HTML page into text, with segments joined by `#+#`.

## Checks

| What could be wrong | Check | Result |
|---|---|---|
| A page has no campaign content (empty, parked, spam, an error page, or a list of links to other pages) | Rules on the page text and address (`exclusion_reason` in the codebook) | 7,964 pages excluded, 6,732 of them tag, category, paging and calendar listings |
| The same page appears more than once | Same text, or same address, within a finalist (`duplicate_of`) | 10,048 pages marked; kept in the corpus, left out of the panel |
| The site belongs to another person, office or election | An open-weight language model (Qwen3.6-27B) read the home page and the longest page of all 722 sites. An agent reviewed each of the 82 sites the model doubted, and only the agent's verdict can exclude a site. | 14 sites excluded: 7 with content only from another election, 3 of a different person with the same name, 3 not campaign sites, 1 for another office. One more site counts only up to election day, because others took over the domain after it. |
| The model check is unreliable | The same check on 46 pages known not to be the candidate's campaign site | 33 "no", 6 "unclear" and 7 wrong "yes", the same in all three runs. For this reason a model "no" alone never excludes a site. |

A page is usable (`release_usable`) if it was archived within 365 days of the election, no page rule excludes it, it is not a duplicate, and its site passed the site check. 21,040 of the 38,333 pages are usable.

## Coverage

704 of the 860 finalists (82 percent) have usable text. Of the 430 elections, 302 (70 percent) have text for both finalists, 377 (88 percent) for the winner, and 402 (93 percent) for at least one finalist.

Every finalist has a `text_status`:

| Status | Finalists | Meaning |
|---|---:|---|
| `text` | 704 | Usable pages exist |
| `no_url_lead` | 75 | No search found a campaign website |
| `site_not_archived` | 37 | A website is known, but the archive holds no copy within a year of the election |
| `no_usable_pages` | 15 | Pages were downloaded, but none is usable |
| `excluded_site_check` | 13 | The site check excluded every downloaded site |
| `check_incomplete` | 11 | The last search could not finish because the search engine or the archive stopped responding |
| `site_rejected` | 5 | The search rejected the only known site (another person, another office, or no campaign content near the election), and found no other |

Missing text is not random. Winners have text more often than runners-up (88 versus 76 percent), and the 25 largest cities more often than those ranked 76–100 (92 versus 75 percent). Coverage rises over time, from 76 percent for 2010–2013 to 87 percent for 2022–2026. Comparisons across candidates or cities should take this into account.

The amount of text varies. The median finalist with text has 14 usable pages and about 37,000 characters; 57 finalists have one usable page and 20 have more than 150.

## Limitations

1. The Wayback Machine did not archive every campaign site or every page of a site. A missing page does not mean the site lacked it.
2. Sites built with JavaScript tools such as Wix or Squarespace often keep little text in the archive.
3. Menus and footers repeat across pages. The panel's `text_clean` keeps only sentence-like segments, following the cleaning rule of ICPSR 226001 (codebook).
4. Pages after the election can contain victory or thank-you messages. `phase` and `days_from_election` allow restricting to pages before the election.
5. AI agents and a language model found and checked the websites, with independent agent checks. Nobody validated these checks by hand.
6. Some pages are in languages other than English (`non_latin_text`).

## Terms of use

The compilation and documentation are released under the Creative Commons Attribution 4.0 International license (CC BY 4.0), the same terms as the House and Senate dataset. Copyright in the campaign material stays with its authors. Each page row records its archived source address in `snap_url`. Rights holders may request removal by email to hhilbig@ucdavis.edu.

## Citation

**TO DECIDE** after the Dataverse DOI is assigned.

## Glossary

- **Finalist:** the winner or the runner-up of an election in the population.
- **Wayback Machine:** the Internet Archive's public archive of web pages, at web.archive.org.
- **Capture:** one archived copy of a page at one time.
- **Agent:** an AI model that searched the web and the archive and recorded its evidence.
