# Campaign websites of mayoral finalists in the 100 largest US cities, 2010–2026

**Draft for review, October 10, 2026. Not yet published.** Items marked **TO DECIDE** need a decision before publication.

This dataset contains the archived campaign websites of the two leading candidates (the winner and the runner-up) in mayoral elections in the 100 largest US cities, from January 2010 to September 14, 2026. The text comes from the Internet Archive's Wayback Machine. Each page records the archived address, the archive date and the extracted text, so every record can be traced to its source.

The dataset is a companion to our archived campaign websites of US House and Senate candidates (Harvard Dataverse, doi:10.7910/DVN/BZ2JRS). It uses the same text extraction, so page text from the two datasets can be compared.

## Files

| File | Rows | One row is |
|---|---:|---|
| `mayoral_cities.csv` | 100 | A city in the population: the 100 largest US cities by 2020 Census population |
| `mayoral_city_years.csv` | 1,700 | A city and a calendar year, 2010–2026, with the result of the election search for that year |
| `mayoral_elections.csv` | 463 | A recorded mayoral election, with its verification status |
| `mayoral_finalists.csv` | 854 | A finalist (winner or runner-up) of a recorded election, with the status of their website text |
| `mayoral_page_corpus.parquet` | 35,290 | One archived page of a finalist's campaign website, with its text and check results |
| `mayoral_finalist_panel.csv` | 650 | A finalist with usable website text, with the text of all usable pages joined |

`manifest.json` lists each data file with its row and column counts, size and SHA-256 checksum. The codebook (`CODEBOOK.md`) defines every column.

## Population

1. **Cities.** The 100 largest incorporated places by 2020 Census population (`mayoral_cities.csv`). The list is fixed; a city that grew into the top 100 after 2020 is not included.
2. **Elections.** For each city and each year from 2010 to 2026, we searched official sources (city clerks, county and state election offices) for mayoral elections. An election enters the population if it is complete, decisive and contested: it determined who became mayor, and at least two candidates ran. In a runoff system, the decisive election is the runoff. 411 elections meet this rule (`verification_status` = `verified_decisive`). The other 52 recorded elections are uncontested (19), not verified against official results (19), or unresolved (14); they are listed but their candidates are not part of the coverage counts.
3. **Finalists.** The winner and the runner-up of each of the 411 elections, from official results: 822 finalists (`eligible` = `True` in `mayoral_finalists.csv`).

Of the 1,700 city-years, 1,649 were resolved: 1,198 had no mayoral election, 437 had at least one recorded election, 11 had only an earlier round and 3 had a mayor chosen by the council. 51 city-years remain unresolved because official sources could not be found or read; they are listed with a note.

## How the websites were found and collected

1. **Finding the campaign website.** We combined an older roster of candidate URLs with four searches: a web search by agents (AI models with web access) followed by an independent agent check of every accepted site; a script that tried common campaign domain patterns (for example `lastnameformayor.com`) in the Wayback Machine and Common Crawl; a model check of older roster URLs against their archived pages; and a search for finalists whose roster URL had no archived copy near the election. A website counts only if an archived page names the candidate and shows a mayoral campaign in that city for that election.
2. **Collecting the pages.** For each accepted website we listed every HTML page the Wayback Machine holds for the site from 18 months before the election to 120 days after it, and downloaded one archived copy of each page, the copy nearest the election. There is no cap on pages per site. Links that are not content pages (style and script files, platform folders, session tokens, crawler loops, calendar views) were not downloaded.
3. **Extracting text.** Text is extracted from the archived HTML with the same code as the House and Senate dataset. Text segments are joined with `#+#`.

## Checks

| What could be wrong | Check | Result |
|---|---|---|
| A page is empty, parked, expired or spam | Rules on each page (see `exclusion_reason` in the codebook) | 1,186 pages excluded: 695 empty, 471 parked, expired, placeholder or directory listings, 21 spam |
| The same text appears on several pages of a site | Exact duplicate text within a finalist | 4,518 pages marked `duplicate_of`; kept in the corpus, left out of the panel |
| The site belongs to another person, office or election | An open-weight language model (Qwen3.6-27B) read the home page and the longest page of each of 665 sites. Every site it doubted (71) was reviewed by an agent; only the agent's verdict excludes a site. | 11 sites excluded: 3 belong to a different person with the same name, 3 are not campaign sites, 4 have content only from another election, 1 is a campaign for another office. One further site counts only up to election day, because critics took over the domain afterwards. |
| The model check is unreliable | The same check ran on 46 pages known not to be the candidate's campaign site | 33 called "no", 6 "unclear", 7 wrongly "yes", in both runs. This is why a model "no" never excludes a site by itself. |

A page is **usable** (`release_usable` in the corpus) if it was archived within 365 days before or after the election, is not excluded by a page rule, is not a duplicate, and its site passed the site check. 27,064 of the 35,290 pages are usable.

## Coverage

650 of the 822 finalists (79 percent) have usable website text. Of the 411 elections, 272 (66 percent) have text for both finalists, 348 (85 percent) for the winner, and 378 (92 percent) for at least one finalist.

Every finalist without text has a reason (`text_status`):

| Status | Finalists | Meaning |
|---|---:|---|
| `text` | 650 | Usable pages exist |
| `no_url_lead` | 100 | No campaign website was found in any search |
| `site_not_archived` | 33 | A website is known, but the archive holds no copy within a year of the election |
| `check_incomplete` | 16 | A search could not be finished because the archive did not respond; three attempts were made |
| `no_usable_pages` | 13 | Pages were downloaded, but all are empty, parked or spam |
| `excluded_site_check` | 10 | The only site belongs to another person, office or election |

**Missing text is not random.** Coverage is higher for winners (85 percent) than for runners-up (73 percent), and higher in large cities (89 percent for the 25 largest, 68 percent for cities ranked 76–100). It is similar over time: 75 percent for 2010–2013, 77 percent for 2014–2017, 80 percent for 2018–2021 and 84 percent for 2022–2026. Analyses that compare candidates or cities should take this into account.

The amount of text varies. The median finalist with text has 17 usable pages and about 43,000 characters. 45 finalists have a single usable page, and 28 have more than 150.

## Limitations

1. The Wayback Machine did not archive every campaign site or every page of a site. A missing page does not mean the site lacked it.
2. Sites built with JavaScript (for example Wix or Squarespace) often keep little text in the archive.
3. Text can include navigation menus and footers that repeat across pages. The panel provides `text_clean`, which keeps only sentence-like segments of ten or more words (the cleaning rule of ICPSR 226001).
4. Pages after the election can include victory or thank-you messages. `days_from_election` lets users restrict pages to the period before the election.
5. Website discovery used AI agents and a language model, with independent agent checks. These checks were not validated by hand.
6. Candidates' sites can be in languages other than English (flag `non_latin_text`).

## Terms of use

**TO DECIDE:** license and takedown contact. The House and Senate dataset is released under CC BY 4.0 for our compilation, with the archived source address of every page and a takedown contact. We propose the same terms here.

## Citation

**TO DECIDE** after the Dataverse DOI is assigned.

## Glossary

- **Finalist:** the winner or the runner-up of a decisive mayoral election.
- **Decisive election:** the round that determined who became mayor (the general election, or the runoff where one was held).
- **Wayback Machine:** the Internet Archive's public archive of web pages, at web.archive.org.
- **Capture:** one archived copy of a page at one time.
- **Agent:** an AI model (Claude) that searched the web and the archive and recorded its evidence; every accepted website was checked by a second, independent agent.
