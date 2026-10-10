# Mayoral release v1: coverage review of discovery and collection

Review date: October 10, 2026. Reviewed release candidate: `build/mayoral_release_v1_candidate/` (commit `09fd8d5`). This review changed no data, code or status files.

## Summary

1. **Verdict.** Coverage can increase. Some of the gain comes from defects in the pipeline, not from limits of the archive. About 5 finalists can get text without any new search. About 10 more need one search with a working search engine.
2. **Defect 1.** Three accepted, checked campaign sites were never scraped: Sheila Jackson Lee (Houston 2023), Javiera Caballero (Durham 2021) and Jerry Weiers (Glendale 2016). The cause is the `--exclude` rule in `build_mayoral_stage3_targets.py`. One more site (Ron Nirenberg, San Antonio 2017) has 6 failed page fetches that were never retried.
3. **Defect 2.** 38 finalists in `no_url_lead` never had a working web search. Stage 2 ran out of search calls, and the repair pass did not select their cities. The group includes winners such as Levar Stoney (Richmond 2016, 2020), Helena Moreno (New Orleans 2025) and Renard Johnson (El Paso 2024).
4. **Before publication, do these steps.** (a) Scrape the 5 recoverable sites. This includes Bill Bell 2011 on billbellformayor.com. (b) Correct the status labels and the README. At least 9 of the 33 `site_not_archived` finalists and 11 of the 16 `check_incomplete` finalists have the wrong description. (c) Get Hanno's decision on one search pass for about 58 finalists who never had a working search.
5. **Population.** 6 elections (Sacramento 2012, 2016, 2020; Plano 2013, 2017, 2021) already have official vote totals. A documentation rule alone keeps them out. About 20 more large races (San Diego, Detroit 2013, Atlanta 2013, Baltimore 2011 and 2024, Tulsa) are outside the population because their results were not verified. Hanno must decide whether to reopen the population.

## What was reviewed and how

I read the status file, the discovery document, the release README and codebook, and the build scripts for targets, scrapes, corpus and release. For each of the 172 eligible finalists without text, I followed the evidence chain:

- the agents' records in every `*_url_leads.json` pass;
- the accepted leads in `linkage/`;
- the scrape targets of every batch;
- the batch inventories and fetch logs on mac2 (read-only);
- the page corpus and the site-check verdicts.

I made 29 read-only archive requests through mac2, one at a time. Each request tested one specific claim. The archive was slow: 9 requests timed out once.

**Terms.**
- **Finalist:** the winner or runner-up of a verified decisive contested election. There are 822.
- **Lead:** a URL that an agent or the build considers a possible campaign site.
- **Accepted:** an agent read an archived page in the window, and an independent checker confirmed the site.
- **`outside_window`:** the site is known, but it has no archived HTML in the 365 days before the election.
- **Target:** one finalist and one domain in a scrape batch.
- **Text window:** 365 days before or after the election. The scrape window is 18 months before to 120 days after.

## 1. The five non-text groups

### 1.1 `site_not_archived` (33): the label is wrong for 9

These 33 finalists have `url_lead = True` and no downloaded content. The README says the archive "holds no copy within a year of the election". That is true for only 24 of them.

| Subgroup | n | Finalists | Evidence | Can it be recovered? |
|---|---:|---|---|---|
| Accepted site, never scraped | 3 | Sheila Jackson Lee 2023 (sheilaforhouston.com); Javiera Caballero 2021 (javieraformayor.com); Jerry Weiers 2016 (weiersformayor.com) | Accepted in follow-up or sweep checks. Every batch targeted only an older or superseded domain. My CDX probes found in-window HTML captures for all three: Jackson Lee from 2023-04-13, Caballero from 2021-08-30, Weiers from 2016-01-11 including /about.html and /contact.html | Yes. Expect 3 of 3 |
| Fetch errors not retried | 1 | Ron Nirenberg 2017 (voteron.com) | Batch `stage3r`: 32 HTML captures and 8 queued pages. Of the 8 fetches, 6 ended `request_error`, 1 `not_available` and 1 timestamp mismatch. This is the only non-text finalist with retryable fetches | Very likely. Expect 1 |
| Archived, but not the campaign | 5 | Ruby Faye Woolridge 2019 (vote4ruby.org was her 2018 Congress site); Chad Dehne 2010 (renocitizen.com belongs to his father Sam Dehne); Mike Martinez 2014 (domain held Japanese spam); Anna Lopez Brosche 2019 (parked page); Chris Adler 2012 (Granicus city-council video pages) | Hive or agents rejected these roster URLs. The build still keeps `url_lead = True` | No new text. The label is wrong. Adler's "lead" is not a campaign site |
| Site known, no in-window HTML | 24 | For example Vi Lyles 2022 and 2023, Kelly Brough 2023, Farrah Khan 2022, Justin Outling 2022, Bob Foster 2010, Bill Harrison 2012 and 2016, Paulette Guajardo 2020, Nadine Woodward 2019, Paul Fraim 2010 | Scrape found no capture, or only empty captures. My probes: vilyles.org has no capture in the 2022 or 2023 window | Mostly no. A search for a different domain could find 1 to 3 |

Robert Weltzin 2013 (Raleigh) is in the last row. His sweep lead robertweltzin.com has a root-page capture two days after the election (20131010013853). It was never scraped. This could add 1 thin page.

### 1.2 `no_url_lead` (100): 38 never had a working web search

All 100 finalists went through both domain-pattern sweeps: 365 days before and 120 days after the election. That differs for the web search:

- **38 finalists had only the stage 2 pass.** In that pass the WebSearch budget (200 calls per workflow run) was used up, so no search ran. Their stage 2 notes say this directly, for example "WebSearch was unavailable (budget used up)".
- The repair pass of October 4 was meant to redo exactly these cases. Its input (`repair_args.json`) contains only 36 cities. It omits Richmond, El Paso, Toledo, Garland, Lubbock, Austin, Santa Ana, Scottsdale, Reno, Virginia Beach, Milwaukee and New Orleans. I could not find the selection rule. The workflow header says "112 finalists", but the omitted cities have the same note text.
- **3 more** (Burnie Mansfield 2012, John Wright 2022, Cynthia Jaquith 2017) went through repairs that still had no working search.
- **59** had a working search at least once.

The 38 include 14 winners: Levar Stoney (Richmond 2016, 2020), Helena Moreno (New Orleans 2025), Renard Johnson (El Paso 2024), Wade Kapszukiewicz (Toledo 2025), Paula Hicks-Hudson (Toledo 2015), Scott LeMay (Garland 2021, 2023), Dan Pope (Lubbock 2018), Miguel Pulido (Santa Ana 2014, 2016, 2018), Jim Lane (Scottsdale 2016) and others. Winners of large-city races in 2016–2025 almost always had a campaign site.

For comparison, the first repair pass searched 112 similar no-search entries with a working engine. It accepted 50 (45 percent) and found 9 more outside the window. Of the accepted sites in the release, about 90 percent yield usable text. The 38 are concentrated in smaller cities: Garland 7, Lubbock 6, Richmond 5, Toledo 4. Their archive coverage is thinner, so I discount the rate. **Estimate: 8 to 15 finalists with text.**

Social profiles: only 11 of the 100 have one recorded (7 Facebook). Facebook-only campaigns are not a large share of this group.

### 1.3 `check_incomplete` (16): the README overstates the attempts

The README says "three attempts were made". That is true for 4 finalists: Alan Krasnoff 2012, Lenard Myers 2020, James Knox 2013 and Allen Joines 2020. In all three passes, search tools failed. Tishaura Jones 2025 comes from the after-election sweep check, which timed out.

**11 come from the legacy-lead search of October 9, one attempt each.** None of the 11 had a working web search. The agents recorded "WebSearch refused (budget used up)". DuckDuckGo and Bing through mac2 returned bot pages. The 11 are David Michael Rice 2019, Mike Coffman 2019, Nancy Vaughan 2015, Larry Agran 2012, Ed Pope 2018, Beth Van Duyne 2014, Henk Conn 2018, Francis Suarez 2021, Mick Cornett 2014, Steve Brown 2018 and Michael Bell 2015.

My probes tested whether a simple CDX retry would help. The roster domains of Coffman (mayormikecoffman.com), Pope (voteforedpope.com), Agran (mayorlarryagran.org) and Steve Brown (stevebrown4mayor.com) have no in-window HTML capture. Brown has only a robots.txt. These finalists need a search for a different domain, not an archive retry. **Estimate: 2 to 4 with text from one working search pass.**

### 1.4 `no_usable_pages` (13): mostly sites without text in the archive

| Cause | n | Finalists |
|---|---:|---|
| JavaScript shell (old Wix): archived HTML has no body text | 5 | Greg Fischer 2018 (48 KB HTML, 7 characters of text); James Lyons 2015; Paul Soglin 2015; Kenny Smith 2017; Josh Wander 2013 ("Loading") |
| Host error page | 1 | David King 2024 ("The website is currently not available") |
| Only robots.txt, a frameset or images | 3 | Nelda Martinez 2012 (frameset to neldamartinez.nationbuilder.com; my probe found no capture of that host); Rhett Smith 2013; Sukhee Kang 2010 (image-only page) |
| Parked or expired after the election | 2 | Tommy Leeman 2024; Dave Cortese 2014 |
| Domain taken over by critics | 1 | Cyndi Lamm 2019 (pages count only until election day; the only page is from day +101) |
| Pages only before the 365-day window | 1 | Cindy Chavez 2022 (20 pages at −460 to −366 days; likely her supervisor site) |

The page rules work correctly here. The old Wix sites from 2015 point to an `_escaped_fragment_` text copy, but my probe for jameslyonsformayor.com found no such capture. Old Wix sites also kept page content in separate JSON files, which the archive sometimes holds. **Estimate: 0 to 2. Low priority.**

### 1.5 `excluded_site_check` (10): one recovery; no new search was run

The verdicts look correct. They break down as follows:
- **Different person (3):** Donald Wagner 2018 (a West Milford, NJ site); Jerramiah Healy 2013 (a DuPage County site); Bill Bell 2011 (a Birmingham, AL site).
- **Not a campaign site (3):** Jessica Qattawi 2024; Miguel Pulido 2012 (a critic's site); Michael Tubbs 2020 (an official site).
- **Content only from another election (4):** Chris Beutler 2011; Bill Bell 2013; Charles Francis 2019; Paul Fraim 2014.

Two gaps follow:
- **A reassignment.** The agent describes billbellformayor.com, which was excluded for Bill Bell 2013, as "the unchanged 2011 re-election site". Bill Bell 2011 has no text. My probe found in-window captures for the 2011 window, starting with the home page on 2011-02-07. Expect 1 finalist after a scrape and site check. I tested the same reassignment for Fraim. fraimformayor.com has no capture in the 2010 window, so Fraim 2010 cannot be recovered this way.
- **No search after exclusion.** The site check ran on October 9–10, after all searches. For these 10 finalists, the only known site was rejected. Nobody searched for another site. A search for Wagner, Healy, Tubbs, Pulido and Qattawi could find 1 to 3.

## 2. Gaps in the procedures

### 2.1 Defects (fix before publication)

1. **Target exclusion by finalist.** `build_mayoral_stage3_targets.py --exclude` drops a finalist who was a target in any earlier batch. If a later check accepts a different domain for that finalist, the new domain is never scraped. Commit `ccac2fe` corrected this by hand for Dee Margo 2020 and Jerry Weiers 2020 (batch `stage3u`). It missed Weiers 2016, Caballero 2021 and Jackson Lee 2023. The exclusion key must be finalist plus domain. I checked all finalists with text: none of them has an accepted domain that was never scraped.
2. **Fetch retry not completed for one target.** Nirenberg 2017 (see 1.1).
3. **Repair selection incomplete.** The repair pass omitted 38 no-search entries in at least 12 cities (see 1.2).
4. **Search capacity.** The 200-call WebSearch budget per workflow run was the main cause of unfinished searches. It ran out early in stage 2, in all repair passes and in the legacy search. 53 of 62 legacy-search acceptances were made without a working web search, from roster URLs, CDX guesses and the Wayback. Brave Search through mac2 worked in follow-up pass 2. A new pass must split the work into runs that stay under the budget, or use Brave as the primary engine.

### 2.2 Labels and documentation (fix before publication)

- `site_not_archived` includes 4 sites that are archived but were not collected, and 5 roster URLs that are archived but rejected. I propose a separate status, for example `site_rejected`, for archived URLs that are not the candidate's campaign site. The 4 collection defects disappear after the fix in 2.1.
- `url_lead = True` for Chris Adler 2012 refers to two Granicus city-council video pages. That is not a campaign-site lead.
- `check_incomplete`: the README sentence "three attempts were made" is true for 4 of 16. For the other 11, one attempt without a working search was made.
- Windows: the README says pages count within 365 days before or after the election. The scrape collects only to 120 days after the election. In practice the usable window is 365 days before to 120 days after. Either state this, or note that pages 121–365 days after were not collected. I do not recommend widening the scrape after the election. Those pages often belong to the next campaign, for example the later captures of billharrisonformayor.com and chrisnettlescampaign.com.

### 2.3 Rules that may exclude real sites

- **365 days before the election (acceptance and text rule).** This affected only Cindy Chavez among the non-text finalists, and her early pages are likely from another office. The rule is reasonable.
- **Page rules (empty, parked, spam).** I found no wrongly excluded campaign page among the 13 `no_usable_pages` finalists. The empty pages are truly empty in the archived HTML.
- **Frames and redirects to another host** (NationBuilder, Wix subdomains). The scraper does not follow them. This affects Nelda Martinez 2012, whose target host is not archived. The yield is too small to justify a change.
- **Domain patterns.** The sweep does not try `vote` plus first name (voteron.com), `reelect` plus last name, year suffixes, `.net` or `.us`, city abbreviations (tk4abq.com) or shared hosts (allenjoinesformayor.wordpress.com). An extended sweep needs only CDX queries on the droplet. Earlier sweeps had a low hit rate (after-election sweep: 5 accepted for 121 finalists). **Estimate: 1 to 4.**

### 2.4 Sources not tried, or tried elsewhere

| Source | Status | Judgment |
|---|---|---|
| Archived Ballotpedia pages | Agents used them case by case. For example, Nirenberg's voteron.com came from a Wayback copy of Ballotpedia. No systematic pass ran | My test on Levar Stoney (2021 copy) found only a Facebook link. A deterministic pass over about 130 finalists is cheap (no model, about 130 archive requests). **Estimate: 2 to 6 leads.** Worth combining with the search pass |
| Archive-It | Screened in August across all mayoral candidates. The channel was closed after the last batch added one URL. Archive-It public captures usually appear in the Wayback CDX | Do not repeat |
| Library of Congress web archives | Not tried | Its election collections focus on federal and state races. Expected yield near zero. Do not pursue |
| archive.today | Not tried | Sparse, no bulk interface, manual. Do not pursue |
| Campaign finance filings | Not tried. Texas and California local candidates file with the city. Those forms rarely list a website | VPAP (Virginia Public Access Project) may list websites for the 10 Virginia finalists without text. Untested. Low priority |
| Common Crawl retry (239 failed queries) | Not retried | Common Crawl gave 5 accepted sites overall. Low priority |
| Facebook-only campaigns | 7 of 100 `no_url_lead` finalists have a recorded Facebook page | Archived Facebook pages are mostly login walls, and they are not campaign websites under the release definition. Do not pursue |

### 2.5 The 32 non-eligible finalists and the 51 unresolved city-years

- **19 uncontested elections (19 finalists).** These are outside the definition, because the population requires contested elections. Keep them out.
- **13 finalists of unresolved elections:**
  - **Sacramento 2012, 2016, 2020 (6 finalists).** The county canvass gives a primary majority each time (Johnson 58.7 percent, Steinberg about 57.7 and 77.3 percent). The year stays unresolved because no source was found that states that a primary majority elects the mayor. The city charter states the rule. This is a documentation step, not new data collection.
  - **Plano 2013, 2017, 2021 (6 finalists).** The Collin County totals are in hand. The Denton County part could not be opened. Two caveats apply: the winner is not in doubt, but in 2017 and 2021 the citywide majority (and so the absence of a runoff) rests on the Collin share. Plano 2025 is uncontested.
  - **Boston 2025 (1 finalist).** The runner-up is a write-in hidden in the "all others" count. Keep it out.
- **No finalist rows exist for 26 out-of-population election records:**
  - 19 `legacy_results_unverified` records: Baltimore 2011, Detroit 2013, Newark 2010, Oklahoma City 2010, Tulsa 2016 and 2020, Memphis 2011, Corpus Christi 2011, Atlanta 2013, Virginia Beach 2018, Bakersfield 2016, Chula Vista 2010, San Diego 2012, 2013, 2014, 2016 and 2020, Colorado Springs 2011 and 2015.
  - 6 `calendar_result_missing` records, which partly duplicate the San Diego and Baltimore cases.
  - Baltimore 2024.
  - Their sources were blocked earlier (bot challenges, HTTP 403, documents that did not open). These are prominent races, and readers will notice their absence. About 21 distinct elections, or about 42 finalists, could enter. That needs result verification first, then the full discovery and scrape pipeline.
- **The 51 unresolved city-years.**
  - Santa Clarita accounts for 16; there the council selects the mayor.
  - Colorado Springs 5, Plano 4, Sacramento 3, Tulsa 3.
  - The rest are single years. Several of these overlap with the elections above. Tulsa 2024 (a November runoff) appears to be a real missing election.
  - Most of the others are probably years without an election.

## 3. Options ranked by expected gain per effort

Rates measured in this project:
- First repair pass: 112 finalists, 5.5 million tokens, 3.7 hours.
- Legacy search: 107 finalists, 7.9 million tokens, 10.9 hours, three cities at a time.
- A batch of under 10 targets with the full-site runner: under 2 hours on mac2.

| Rank | Option | Expected gain (finalists with text) | Effort | Decision needed | Before publication? |
|---:|---|---|---|---|---|
| 1 | Scrape sheilaforhouston.com, javieraformayor.com and weiersformayor.com (2016); retry the 6 Nirenberg 2017 fetches; scrape billbellformayor.com for Bell 2011; optionally robertweltzin.com. Then run the site check on the new sites and rebuild. Fix `--exclude` to key on finalist plus domain | +4 to +5 (650 → about 655) | About 3 agent hours; under 4 hours wall time; no tokens beyond a small site check | None for the 4 accepted leads (a defect fix). Bell 2011 needs the usual site check | **Yes** |
| 2 | Correct labels and README: `site_rejected` or equivalent; Adler's false lead; `check_incomplete` wording; the actual 120-day post-election window | 0 (accuracy) | About 2 agent hours | Hanno approves the new status name | **Yes** |
| 3 | Decide on Sacramento 2012, 2016, 2020 and Plano 2013, 2017, 2021 (results in hand). If added, run discovery and scrape for 12 finalists | Population +6 elections; about +9 finalists with text | About 1 day (charter citation, discovery, one batch) | **Yes: Hanno reopens the closed population step** | Worth deciding now |
| 4 | One search pass with a working engine for the 38 stage-2-only `no_url_lead` finalists, the 11 one-attempt `check_incomplete` finalists and the 9 excluded finalists without a recovery (58 in total). Add the deterministic archived-Ballotpedia lookup | +10 to +20 | About 3 to 5 million tokens; 6 to 10 hours wall time at low concurrency; then one batch | **Yes.** The repair-attempt count on the stage 2 search blocker is already 3. These 49 entries were never in a repair with a working search, but the rule in `CLAUDE.md` makes this Hanno's call | Yes, if publication can wait about 2 days |
| 5 | Verify the results of about 21 out-of-population elections (San Diego, Detroit, Atlanta, Baltimore, Tulsa and others), then discovery and scrape | Population +about 21 elections; perhaps +30 finalists with text | Several days; earlier source access failed | **Yes: reopens the population** | No. Plan as version 1.1, and name these races in the README now |
| 6 | Extended domain-pattern sweep | +1 to +4 | About 2 hours of droplet CDX, plus an agent check | No | Optional |
| 7 | Old-Wix JSON recovery; Common Crawl retry; VPAP | 0 to +3 together | Moderate | No | No |
| 8 | Archive-It again, Library of Congress, archive.today, Facebook pages, wider post-election window, uncontested elections | About 0 | — | — | No |

Steps 1 and 2 bring the release to about 655 of 822 (80 percent), and the status labels then match the evidence. With step 4, the release reaches about 665 to 670 (81 to 82 percent).

The largest uncertainty is the hit rate of step 4 in the small Texas cities (Garland, Lubbock, Laredo). There, many runners-up may never have had a campaign site.

Under `CLAUDE.md`, a deviation must be recorded in `docs/status.md` before the work. The record states the defect, the affected requirement, the smallest correction and the condition for return to the checklist. Steps 1 and 2 qualify as demonstrated defects. This review did not edit the status file.

## Evidence index

- Unscraped accepted leads:
  - Jackson Lee: `url_discovery/repair_20261004/followup/houston_url_leads.json`.
  - Caballero and Weiers 2016: `linkage/legacy_search_leads.csv` (basis `legacy_lead_sweep_wayback_capture_checked`).
  - Targets: `data/municipal_archive_acquisition/mayoral_stage3r_20261004`, `mayoral_stage3s_20261010` and `mayoral_stage3u_20261010`, each in `candidate_site_targets.csv`.
- Nirenberg 2017 fetches: mac2 `~/candidate_website_extension_stage3/data/municipal_archive_acquisition/mayoral_stage3r_20261004/runs/production/fetch_attempts.csv`, target `stage3_5d78def82ca7658696c33041`.
- Repair selection: `url_discovery/repair_20261004/repair_args.json` (36 cities).
- Stage 2 notes: `url_discovery/<city>_url_leads.json`.
- Site-check reasons: `release_site_check/agent_review_verdicts.json` and `agent_review_verdicts_2.json`.
- Empty Wix pages: raw HTML in the `runs/production` folders on mac2 for batches `stage3_20261002` and `stage3s_20261010`.
- My 29 probes:
  - Ballotpedia: 7 requests. Stoney's page was read; four others timed out; Mosby's page returned 404.
  - CDX: 22 requests. In-window HTML for sheilaforhouston.com, javieraformayor.com, weiersformayor.com (2016) and billbellformayor.com (2011).
  - No in-window HTML for vilyles.org (2022, 2023), fraimformayor.com (2010), tishaura.com (2025), mayormikecoffman.com (2019), voteforedpope.com, mayorlarryagran.org, stevebrown4mayor.com or neldamartinez.nationbuilder.com.
  - No `_escaped_fragment_` capture for jameslyonsformayor.com.
