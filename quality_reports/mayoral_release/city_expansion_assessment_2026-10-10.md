# Extending the mayoral website dataset to the 200 or 300 largest cities

Assessment, October 10, 2026. Read-only; no workflow, scrape, Hive job or paid API ran.

## Summary

1. Recommendation: do not extend now; run a scoped pilot of 20 cities (ranks 101–300, directly elected mayors only) after the v1 deposit is published.
2. Expected yield: about 300–400 contested elections in ranks 101–200 and 250–340 in ranks 201–300, but only about 50–65 percent and 40–55 percent of their finalists with usable text (79 percent in the current top 100).
3. Cost for ranks 101–200: about 70–105 million subagent tokens, 2–3 weeks wall clock, 3–6 hours of Hanno's time. For ranks 101–300: about 140–210 million tokens, 4–6 weeks, 6–10 hours.
4. The coverage loss comes from finding a site, not from archiving: in the top 100, about 90 percent of finalists with a URL lead have text in every rank quartile, while the share with a URL lead falls from 97 to 75 percent.
5. The largest uncertainty is how many small-city finalists ran a website at all; the pilot measures this directly, together with the cost per city.

## What is compared

The current dataset holds the winners and runners-up of decisive contested mayoral elections in the 100 largest US cities by 2020 Census population, January 2010 to September 14, 2026 (822 finalists, 650 with text). This assessment compares those cities with the cities ranked 101–300 on the same Census list. Measured rates come from the October 2026 top-100 run (`docs/status.md`, `docs/archive/status_history_2026-10-04.md`). Characteristics of the new cities come from the American Local Government Elections Database (LEDB, de Benedictis-Kessner et al. 2023, local copy `data/external/municipal/ledb_candidatelevel.csv`), which covers mayoral contests to 2021, and from the project's older URL roster.

## 1. Which cities would be added and how they differ

**City list.** I rebuilt the national ranking from the same Census TIGERweb layer that `cities.csv` uses (2020 incorporated places, plus Urban Honolulu CDP, without the separate "Louisville city" record). It reproduces the top 100; rank 100 is Richmond, VA (226,610).

| Ranks | Population range (2020) | Examples | States with most cities |
|---|---|---|---|
| 101–200 | 223,109 to 138,486 | Hialeah, San Bernardino, Tacoma, Modesto, Huntsville, Des Moines, Yonkers, Rochester NY, Fontana, Columbus GA … Denton, Warren, Pasadena CA, Waco | CA 27, TX 11, FL 8 |
| 201–300 | 137,710 to 105,643 | Cedar Rapids, Dayton, Elizabeth, Hampton, Stamford, New Haven, Sterling Heights, Coral Springs … El Cajon, Tyler, Davie, San Mateo, Brockton | CA 24, TX 13, FL 9, CO 6 |

The United States had 322 places of 100,000 or more in 2020, so rank 300 is close to the 100,000 line. Rank 300 to 330 adds cities between about 106,000 and 99,000.

**Form of government.** A web check of ICMA form-of-government shares was not possible: this session's web-search budget was used up, and Ballotpedia and ICMA pages returned no content. I used LEDB as a proxy. A city with no mayoral contest in LEDB for 2010–2021 usually has a mayor chosen by the council or a rotating mayor.

| Ranks | Cities | With LEDB mayoral contests 2010–2021 | Without |
|---|---:|---:|---:|
| 1–100 | 100 | 96 | 4 |
| 101–200 | 100 | 92 | 8 |
| 201–300 | 100 | 79 | 21 |

Without contests, ranks 101–200: Huntington Beach, Glendale CA, Santa Rosa, Corona, Roseville, Fullerton, Visalia, Macon-Bibb. Ranks 201–300: Victorville, Thousand Oaks, Concord, Clovis, Cambridge, Lowell, League City, Downey, Broken Arrow, Miami Gardens, Sugar Land, Murrieta, Ventura, Temecula, West Covina, Centennial, Sandy Springs, South Fulton, Burbank, Renton, San Mateo. Most are California and Massachusetts cities with council-selected mayors. Some (Macon-Bibb, Sugar Land, Miami Gardens, Sandy Springs, South Fulton, Centennial, Renton, League City) appear to elect their mayor directly and are LEDB gaps; this needs a source check per city. About 10 percent of ranks 101–200 and 15–20 percent of ranks 201–300 lack a directly elected mayor. Those city-years would count as `indirect_selection` and add no finalists.

Council-manager government with a directly elected but weak mayor is common at all sizes. The dataset does not code mayoral powers, so this difference is not visible in the data now.

**Election timing and electorate size (LEDB contested mayoral contests, 2010–2021).**

| Ranks | Contests | Odd-year share | November share | Median total votes | Interquartile range of votes |
|---|---:|---:|---:|---:|---|
| 1–100 | 312 | 59% | 51% | 67,400 | 37,300–110,800 |
| 101–200 | 264 | 53% | 56% | 25,100 | 16,000–37,000 |
| 201–300 | 223 | 63% | 72% | 16,600 | 11,600–24,000 |

Off-cycle elections are as common as in the top 100. Electorates are about one third (ranks 101–200) and one quarter (201–300) of the top-100 size. LEDB has no partisan-ballot field; nonpartisan ballots are the norm in all three groups as far as the current data shows.

**Websites and archiving.** Three measures point to fewer findable sites in smaller cities.

| Measure | 1–100 | 101–200 | 201–300 |
|---|---:|---:|---:|
| LEDB top two with a Ballotpedia profile, 2010–2021 | 51% | 19% | 9% |
| LEDB top two with a URL in the project's old roster, 2010–2021 | 55% | 34% | 33% |

In the current release, the decline with rank comes from URL leads, not from the archive:

| Rank quartile | Finalists | URL lead | Text | Text if URL lead |
|---|---:|---:|---:|---:|
| 1–25 | 218 | 97% | 89% | 92% |
| 26–50 | 188 | 85% | 79% | 93% |
| 51–75 | 226 | 89% | 78% | 88% |
| 76–100 | 190 | 75% | 68% | 90% |

By decile, text coverage is 89–90 percent in ranks 1–20 and 61 percent in ranks 91–100. Runners-up fall faster than winners (62 percent against 74 percent in ranks 76–100).

**Extrapolated text coverage.** A logit of text on log population (822 finalists; slope 0.94, standard error 0.18) predicts 62 percent at the median population of ranks 101–200 (about 170,000) and 54 percent at the median of ranks 201–300 (about 120,000). A straight line in rank predicts 50 and 21 percent, but rank compresses population, so the log-population fit is the better guide. The old-roster ratio (34 against 55 percent) suggests about 0.6 times the top-100 rate before new searches. I use 50–65 percent for ranks 101–200 and 40–55 percent for 201–300. These are extrapolations beyond the observed range.

## 2. Data availability for the population step

**How the top-100 step went.** 1,649 of 1,700 city-years (97 percent) were resolved. 51 remain unresolved. They concentrate in a few cities: Santa Clarita 16 (council-selected mayor without a period-specific official statement), Colorado Springs 5, Plano 4, Tulsa 3, Sacramento 3, and about 20 single years. Unresolved city-years by rank quartile: 4, 17, 7 and 23 of 425. The recorded causes:

- Official sites block automated access (HTTP 403, Cloudflare), for example Maricopa County assets, Oklahoma and Tulsa county sites, California charter sites.
- No machine-readable or archived results for early years (Memphis 2011, Atlanta 2013, Detroit 2013).
- The rule that a primary majority elects the mayor could not be confirmed from an official source (Fresno and Sacramento, later partly resolved).
- Uncontested mayoral races omitted from the ballot (Plano, Irving).

The first agent pass left 172 city-years open in 36 cities; a second agent pass cut this to 72, and a browser pass by Hanno resolved more. So about one third of cities needed a second pass, and a handful needed a person with a browser.

**Implications for ranks 101–300.**

- LEDB gives candidate names and votes for 2010–2021 in 92 and 79 of the 100 cities per tier. This helps the agents as it did for the top 100 (LEDB was the source of the old roster). 2022–2026 must be found from scratch, as before.
- Results sources are mostly county election offices (California registrars, Florida supervisors, Clarity sites) and city secretaries (Texas, which runs many city elections itself). The same blockers apply. Smaller cities more often publish results only as scanned PDFs or council minutes. I expect 4–8 percent unresolved city-years, against 3 percent now; this is a judgment, not a measurement.
- California is the largest block (51 of 200 cities). The California Elections Data Archive collects local results statewide and could serve as one source for those cities; I did not verify its current years.
- Ballotpedia covers few of these elections (19 and 9 percent profile rates above), so agents will rely on official sources and local news even more than for the top 100.

**Expected population.** LEDB lists 312 contested mayoral contests for the top 100 in 2010–2021, and the release verified 299 elections in those years (ratio about 0.96). Applying that ratio and the top-100 ratio of 2010–2026 to 2010–2021 elections (411 to 299):

| Ranks | City-years | Expected decisive contested elections | Expected finalists | Expected finalists with text |
|---|---:|---:|---:|---:|
| 101–200 | 1,700 | about 350 (300–400) | about 700 (600–800) | 350–450 |
| 201–300 | 1,700 | about 295 (250–340) | about 590 (500–680) | 235–325 |

## 3. Cost and time

**Measured top-100 rates (September 30 to October 10, 2026).**

| Stage | Subagent tokens | Workflow hours | Notes |
|---|---:|---:|---|
| Population, pilot | 1.3 M | 0.15 | 6 cities, about 210,000 tokens per city |
| Population, full run | about 25 M | about 22 (with a disk-full stop) | 82 cities, about 0.30 M per city |
| Population, second pass | 6.0 M | not recorded | 36 cities with gaps |
| Population, browser pass | none | a few | Hanno and the coordinating agent |
| URL search, stage 2 | 13.6 M | 5.1 | 473 finalists |
| Domain sweep check | 4.4 M | 1.8 | 152 finalists |
| Legacy attribution check | 4.4 M | 1.2 | 139 items |
| Repair and follow-up passes | 8.4 M plus attempt 3 | 3.7 + 8 | archive timeouts forced one agent at a time |
| Legacy-lead search | 7.9 M | 10.9 | 107 finalists |
| Other checks (sweep checks, attempt 3, release site review) | about 5–10 M, not recorded | about 5 | estimate |
| **Total** | **about 80 M** | **about 60** | plus the coordinating sessions, not counted |

Machine work: Wayback domain sweep 6,907 queries in about 8 hours; Common Crawl sweep about 14–30 hours with throttling; scrapes about 1,300–1,400 pages per hour on mac2 (6,508 pages in about 5 hours), about 30,000 pages in total; Hive site check a few GPU minutes per run plus queue time. Raw HTML is about 120–160 KB per page, about 5 GB for the top 100 on mac2. Release files: page corpus 29 MB, panel 151 MB.

The top 100 also used work done before September 30: the LEDB roster, the old URL roster and a 291,044-capture archive corpus. The roster already covers ranks 101–300 for 2010–2021 (URLs for 454 candidate-years), so that part is reusable. The archive corpus may hold captures for these cities; I did not check.

**Estimates.** Population tokens scale with cities (17 years each); website tokens scale with finalists (about 55,000 tokens per finalist in the top 100). I add 0–30 percent for poorer sources. No repair-loop development is included, because the scripts exist.

| Item | Ranks 101–200 | Ranks 101–300 |
|---|---|---|
| Population step | 35–55 M tokens, 1–2 days | 70–110 M tokens, 2–4 days |
| URL search, sweeps, checks | 30–50 M tokens, 30–45 workflow hours | 65–100 M tokens, 60–90 workflow hours |
| Domain sweeps (machine) | 8–12 hours | 15–25 hours |
| Scrape (machine) | 10–15 hours (about 12,000–18,000 pages) | 20–30 hours |
| Hive site check | under 1 GPU hour, plus queue | under 1 GPU hour, plus queue |
| Release build and documents | 4–8 agent hours | 6–10 agent hours |
| **Total subagent tokens** | **about 70–105 M** | **about 140–210 M** |
| Wall clock | 2–3 weeks | 4–6 weeks |
| Storage (raw on mac2) | 2–4 GB | 4–8 GB |
| Hanno's time | 3–6 hours | 6–10 hours |

mac2 has about 98 GB free, so storage is not a constraint. Archive rate limits are: agent workflows that query the Wayback Machine must run at low concurrency and not beside a scrape, which sets the wall clock more than token budget does.

Hanno's time covers stage approvals (six to eight decisions), browser checks of blocked official sources (the main variable part, 1–4 hours), and review of the README, codebook and deposit. About 10–15 percent of the elapsed time waits on Hanno's decisions; the rest waits on machines and the archive.

Cost per finalist with text rises: about 120,000 tokens in the top 100, and an expected 180,000–270,000 tokens in ranks 101–200 and more in 201–300, because the search cost per finalist stays similar while the yield falls.

**Largest uncertainty.** The share of small-city finalists who ran a findable, archived website decides both the yield and the repair effort; the top-100 trend gives only an extrapolation.

## 4. Value

**What a larger sample enables.**

- Variation in city size: the top 100 spans about 227,000 to 8.8 million; ranks 101–300 add 106,000–223,000, where electorates are 11,000–37,000 voters. This supports questions on how campaign content (issue breadth, national versus local themes, partisan cues) changes with electorate size.
- Government form: ranks 101–300 add more council-manager cities and weak-mayor systems, so comparisons of campaign content by mayoral power become possible, once mayoral power is coded from an external source.
- Competition and incumbency: about 650–750 more contested elections roughly double the number of elections, which helps estimates for subgroups such as open seats, runoffs or close races.
- States: more cities in Texas, Florida, Colorado, Connecticut and Michigan, so within-state comparisons get more than a handful of cities per state.

**What it does not fix.**

- Selection on having an archived site: coverage falls with size and is lower for runners-up. A larger sample adds the cities where this selection is strongest.
- No candidates beyond the top two, and no primaries: the ICPSR-style ideology scaling, which needs primary-stage text, stays out of reach.
- Before about 2014 the archive is thin for small sites; early years stay weaker.
- Small cities are not representative of all US cities; rank 300 is still above 100,000 people.

**Alternatives.**

| Option | Gain | Cost | Judgment |
|---|---|---|---|
| All of ranks 101–300 | Full population above about 105,000 | 140–210 M tokens, 4–6 weeks | Highest cost; lowest yield per token |
| All of ranks 101–200 | A clean "200 largest" frame | 70–105 M tokens, 2–3 weeks | Defensible frame; yield about 60 percent |
| Stratified sample of ranks 101–300 (for example 50 cities, sampled by rank band and state) | Size variation at a third of the cost; weights restore the population | 35–55 M tokens | Good for size-gradient questions; a "largest N" frame is simpler to explain |
| Directly elected mayors only | Removes 10–20 percent of cities that yield no finalists | small saving | Sensible filter in any option; the frame must say so |
| States with good result data (for example California and Texas) | Faster population step | lower | Confounds size with state; I would not use it as the main design |

## 5. Recommendation

**Scoped pilot, not a full extension, and only after the v1 deposit is published.**

Reasons: the v1 release still waits on license, contact and review decisions, and a parallel extension would compete for the same machines and attention. The expected yield per token is about half of the top-100 rate. The main unknown, website prevalence in small cities, can be measured cheaply.

**Pilot design.**

- Cities: 20, all with a directly elected mayor, 10 from ranks 101–200 and 10 from 201–300, sampled within rank bands and spread across at least 10 states. Include two or three California cities and two Texas cities, because these states dominate both tiers.
- Process: the existing workflows unchanged (population, search with independent check, one domain sweep, scrape, Hive site check), with at most one repair pass.
- Expected cost: about 8–12 million subagent tokens and 3–5 days wall clock; about 1 hour of Hanno's time.
- Measurements: tokens and hours per city and per finalist; share of city-years resolved; share of finalists with a URL lead and with usable text, by tier; number of cities that needed a browser check.

**Decision rule after the pilot** (planning heuristics, not fixed thresholds; Hanno should confirm them):

- Extend to all of ranks 101–200 if text coverage in the pilot's 101–200 cities is at least about 50 percent of finalists and the cost per city is within about 1.5 times the top-100 rate.
- Extend to ranks 201–300 only if those pilot cities also reach about 40 percent and a stated research question needs the 100,000–140,000 size range. Otherwise prefer a stratified sample.
- If coverage is far below these values, report the pilot as evidence on small-city campaign websites and stop.

## Glossary

- **Finalist:** the winner or the runner-up of a decisive mayoral election.
- **URL lead:** a candidate campaign website address found by any search, before the content check.
- **Usable text:** at least one archived page within 365 days of the election that passes the page rules and the site check.
- **LEDB:** American Local Government Elections Database, a candidate-level dataset of local elections up to 2021.
- **Subagent tokens:** tokens used by the Sonnet agents that the workflows launch; the coordinating session's tokens are not included.

## Sources and method notes

- City ranking: Census TIGERweb 2020 incorporated places layer (`tigerWMS_Census2020/MapServer/26`), queried October 10, 2026; scratch files only.
- Coverage by rank: `build/mayoral_release_v1_candidate/mayoral_finalists.csv` joined to `mayoral_cities.csv`.
- LEDB counts: contests with office `Mayor`, years 2010–2021, at least two candidates; top two by votes.
- Old roster: `quality_reports/mayoral_source_restart_2026-09-14/completion/v7/attribution_review_complete/candidate_urls_with_capture_reviews.csv`.
- Rates: `docs/status.md` and `docs/archive/status_history_2026-10-04.md`; token counts not recorded there are marked as estimates.
- Not verified: ICMA form-of-government shares, Ballotpedia's stated coverage scope, the California Elections Data Archive's current years, and dollar cost of tokens (depends on the plan).
