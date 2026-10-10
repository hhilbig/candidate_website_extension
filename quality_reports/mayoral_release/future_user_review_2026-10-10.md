# Outside-user review of the mayoral release candidate (v1), 2026-10-10

## Summary

1. The release is usable for city-level and finalist-level text research, and the main documented counts match the data. Two data defects must be fixed before publication.
2. Defect 1: `days_from_election` uses the 15th of the election month, not the election date, for 12,549 pages (131 finalists). 3,349 usable pages are on the wrong side of election day.
3. Defect 2: 41 usable pages contain NUL bytes or binary data (images, compressed HTML). pandas then truncates the `text` of 6 rows in `mayoral_finalist_panel.csv` without a warning.
4. The election population has visible gaps in large cities (San Diego 2012–2020, Sacramento, Detroit 2013, Atlanta 2013). The files have no person ID, incumbency, party, vote share, or links to other datasets.
5. Usable text still includes crawler-loop and archive pages (3,308 calendar, tag, author and paging URLs; 33 bot-challenge pages). The panel joins pre- and post-election pages, and a small number of very large sites dominate it.

## How I reviewed it

I read only the files in `build/mayoral_release_v1_candidate/`. I loaded all six data files in Python, merged them on the documented keys, and recomputed the counts in the README. I built a finalist-level file with city population, a derived incumbency proxy and the winner's margin. I then ran three small analyses. I did not open any script, document or log outside the folder.

## What works

- All documented keys are unique. All merges work: every page links to a finalist, every finalist to an election, every election to a city and a city-year.
- The manifest checksums match the files.
- The README counts reproduce exactly: 411 decisive elections, 822 eligible finalists, 650 with text, 272 elections with text for both finalists, 348 for the winner, 378 for at least one. The `text_status` counts, the exclusion counts (695 empty, 471 parked, 21 spam), the median of 17 pages and about 43,000 characters, 45 single-page finalists and 28 with more than 150 pages all match.
- The coverage gradients in the README reproduce: 85% for winners against 73% for runners-up, 89% in the 25 largest cities against 68% in cities ranked 76–100.
- `release_usable` follows its documented rule in every row. `n_pages_usable` in the finalists file equals the count of usable pages in the corpus for every finalist.
- Each page carries `snap_url`, so every text can be traced to its archived source. Official result sources are given per election.

## Three small analyses

These analyses test whether the data supports typical research uses. They are not findings.

1. **Issue mentions, winners against runners-up.** I used usable pre-election pages only, with the election day recomputed from `election_date`. A binary "any mention" measure is not informative: 77–80% of finalists mention housing, public safety and schools, because long sites and navigation menus contain almost every word. Rates per 10,000 characters are more useful. The median housing rate rises from 1.3 (2010–2015) to 2.9 (2016–2020) and 4.0 (2021–2026). Winners and runners-up do not differ visibly. Users need a length-normalized measure and the boilerplate-free text.
2. **Text length by city size.** The median pre-election text is 57,000 characters in the 25 largest cities and 15,000 in cities ranked 76–100. The correlation between log population and log characters is 0.29. Coverage also falls with city size. Size therefore affects both selection into the sample and the amount of text.
3. **Before and after the election.** 40% of usable pages were captured after election day. Victory language ("mayor-elect", "transition team", "thank you for your vote") appears on 3% of pre-election pages and 8% of post-election pages. A real before/after comparison is not possible with this design. Each address has one capture, the copy nearest the election. A post-election page is therefore usually a later copy of the same pre-election content, not a new version. Runners-up have 4,924 post-election pages, which supports this reading.

A derived incumbency proxy (the finalist won the previous decisive election in the same city) marks 186 finalists, 160 of them winners. Text coverage is similar for this group (81%) and for others (79%). Close elections (margin under 10 points) have higher coverage (87%) than other elections (77%). The proxy is fragile because names are not standardized (see issue B3).

## Issues found

### A. Data defects

**A1. `days_from_election` is wrong for three batches (must fix).** In `mayoral_page_corpus.parquet`, the value for every page in batches `covered_151_20260826`, `full_discovery_740_20260826` and `openai_residual_expansion_357_20260826` is measured from the 15th of the election month. Examples: Los Angeles 2013 (election 2013-05-21; offset −6 days), Phoenix 2020 (2020-11-03; offset +12), Anchorage 2018 (2018-04-03; offset +12). This affects 12,549 pages and 131 finalists. 3,916 pages (3,349 usable) have the wrong sign. 2,985 pages captured on or after election day are labeled as pre-election. `in_text_window` changes for 49 pages, so `release_usable` and the panel may also change. If the collector chose "the copy nearest the election" from the same wrong date, the capture choice is also affected. The README tells users to use this column to drop post-election pages.

**A2. Binary data and NUL bytes in text (must fix).** 41 usable pages contain NUL bytes, and 39 usable pages from 5 finalists consist mostly of replacement or control characters. Examples: `https://daniellurie.com/` (the homepage, 6,025 characters of binary), a `.jpg` file on `daniellurie.com`, `marysheffield.com/?wordfence_lh=...`, `melvincarter.org/?et_core_page_resource=...`, `zohranfornyc.com/volunteer/events?..._rsc=...`. In `mayoral_finalist_panel.csv`, `pd.read_csv` stops at the NUL byte and silently truncates `text` for 6 finalists: Daniel Lurie (219 of 672,624 characters read), Melvin Carter, Mary Sheffield, Victoria Seaman, Deborah S. Morris and Zohran Mamdani. Python's `csv` module reads all rows correctly, so the CSV is valid but hostile to the most common reader.

**A3. Error and crawler pages count as usable (should fix).** Usable pages include 33 bot-challenge pages ("Please wait while your request is being verified..." on 10 different finalist sites; Cloudflare email-protection pages), about 180 short pages with "page not found" text, and 3,308 pages with crawler-loop addresses: `?ajaxCalendar=` (1,729), `/tag/` (658), `/author/` (240), `?page=N`, `recruiter_id`, `utm_`, `_hsenc`. The README says calendar views and session tokens were not downloaded. Text-level duplicate removal misses these because the pages differ slightly. 222 usable pages are near-duplicate addresses of another usable page (trailing slash, `www`, port `:80`), for example `acwharton.com/achievements` and `/achievements/`. One identical text appears on usable pages of 10 different finalists.

**A4. Concentration of the corpus (should document).** The ten largest finalists hold 24% of usable pages and 32% of usable characters. One Honolulu 2012 finalist (`1571550__2012-11-06__regular__915e91433b6c2b3d`) has 1,859 pages and 10.7 million characters. Eric Garcetti 2013 has 1,369 pages. Any pooled word count or topic model is dominated by these sites unless the user weights by finalist.

### B. Population and keys

**B1. Missing elections in large cities (must document; should fix if feasible).** Several well-known elections are not in the population because they are recorded as `legacy_results_unverified` or `calendar_result_missing`, or the city-year is `unresolved`. San Diego (rank 8) has only one decisive election (2024); 2012, 2014, 2016 and 2020 are missing. Baltimore 2011 and 2024, Sacramento 2012, 2016 and 2020, Detroit 2013, Atlanta 2013, Memphis 2011 and Tulsa 2016 and 2020 are missing. Plano has no decisive election. Official results for many of these are public. A referee will ask why the 8th-largest city has one election in 17 years. The README states that 52 elections are not part of the population, but it does not list the large-city gaps or say that these are not known non-elections.

**B2. City-year `status` is misleading.** `status = complete` means "at least one election recorded", not "a decisive election is in the population". 26 city-years are `complete` with no `verified_decisive` election (for example San Diego 2012, Dallas 2023, Boston 2025, Chandler 2010–2018). A user who counts `complete` gets 437, not 411.

**B3. No person identifier.** The hash at the end of `candidate_election_id` acts as a person ID only when the name string is identical. It splits about 30 people: "Ed Lee" / "Edwin M. Lee" (San Francisco 2011, 2015), "Muriel Bowser" / "Muriel E. Bowser", "Martin J Walsh" / "Marty J Walsh", "Julian Castro" / "Julián Castro", "Vi Lyles" / "Vi Alexander Lyles", "Ras Baraka" / "Ras J. Baraka". The codebook does not say that the hash is a name hash a user can reuse. Incumbency and career analyses need a stable `person_id`.

**B4. `election_id` has three formats.** The codebook says it combines `city_id`, date and type. 19 IDs have the form `legacy__666000_2012_11_san diego_CA_Mayor_mayor_1` (no leading zero, no date, spaces), and 6 have the form `calendar__0666000__2012-11-06`. The same San Diego election appears twice, once in each form.

**B5. `city_id` loses its leading zero.** The CSV stores `0644000`, but `pd.read_csv` and R's `read.csv` read it as the integer 644000. `election_id` keeps the zero. Users must pass `dtype=str`. The README should say this, or the files should be Parquet with typed columns. `city_id` and `census_geoid` are identical columns.

**B6. Santa Clarita is in the population but cannot have a direct election.** It contributes 16 of the 51 `unresolved` city-years, although `jurisdiction_note` says the council selects the mayor. The title says "100 largest US cities", but at most 99 cities elect a mayor directly. Urban Honolulu is a CDP, not an incorporated place, which conflicts with the definition "100 largest incorporated places".

**B7. 2026 is partial.** 2026 has 6 decisive elections, 9 `earlier_round_only` city-years and a September 14 cutoff. Users who pool years should know this. A flag or a separate note is needed.

### C. Columns and codes that are unclear

- `stage` mixes the round and the source quality: `general`, `runoff`, `primary_nominating`, `ranked_choice`, `ranked_choice_final`, `special`, `verified_general_election`, `unresolved`, and `single_recorded_mayoral_contest_per_city_year_stage_not_explicit`. `special` is also an `election_type`. The difference between `ranked_choice` (San Francisco 2024) and `ranked_choice_final` is not defined. 39 decisive elections have `stage = primary_nominating`; these are primaries won outright. The codebook should say so, because "runner-up of a primary" is a different kind of finalist.
- `official_votes` has no stated basis. For ranked-choice elections the values are final-round totals (the verification note says so), elsewhere they are single-round totals. There is no total-vote column, so vote shares and margins against all candidates cannot be computed. Richard H. Stopfer (Irving 2023, uncontested) has 0 votes.
- `exclusion_reason` has an undocumented combined value `empty;parked_or_placeholder`. Empty cells in Parquet are empty strings, not nulls, for `duplicate_of` and `exclusion_reason`.
- `target_id`, `batch` and `run` are internal provenance with no meaning for a user. `run = carymoon_extra` contains a candidate's name. `batch = openai_residual_expansion_357_20260826` implies that an OpenAI model took part in discovery; the README names only Claude agents and Qwen3.6-27B.
- `url_lead_basis` has ten internal codes. Most users need three facts: source of the URL, whether a person or a model checked it, and whether the site was found before or after the election.
- "The older roster" and `legacy_contest_ids` appear many times but are never described: who built it, when, and from what sources.
- `page_type` is `other` for 67% of usable pages, so it has little use for selecting issue or biography pages.
- `days_from_election` uses the UTC capture date against a local election date. This shifts some election-day captures by one day.
- The README says pages were collected from 18 months before to 120 days after the election, and that usable pages are within 365 days. The observed range is −567 to +148 days. The "365 days after" limit never binds, and the collection window is not exactly as described.
- The README says one copy of each page was downloaded, but 148 addresses appear twice for the same finalist.
- `names_candidate` is False on 894 usable pages and `other_year_only` is True on 1,771 usable pages. The codebook does not say whether users should drop them.
- `n_sites` differs from the count of usable site domains for 2 finalists. The README says 665 sites went through the site check; the corpus has 675 finalist-site pairs and 549 distinct domains.
- `jurisdiction_note` repeats "Verify election jurisdiction in official results" for 91 cities. This reads as an internal to-do.
- `city_name` uses Census names ("New York city", "Nashville-Davidson metropolitan government (balance)"). A common short name would help matching.

### D. Missing variables

For the research uses in the task, a user needs these and must now build them by hand:

- Incumbency (sitting mayor runs; open seat), and a person ID across elections.
- Party or partisan label where it exists, and a flag for nonpartisan ballots.
- Total votes cast, number of candidates, vote shares, margin, and first-round results for runoffs.
- Election timing: on-cycle with federal or state elections, or off-cycle.
- Form of government (mayor-council or council-manager) and term limits.
- City covariates: ACS demographics, income, partisanship of the city (presidential vote).
- Candidate covariates: gender, race and ethnicity, prior office.
- Links to other data: Census place FIPS as a string, state FIPS, county FIPS, and IDs for the Local Elections in America Project, the American Mayors data (de Benedictis-Kessner, Warshaw and coauthors), Ballotpedia and the House/Senate companion release.

### E. Formats and structure

- `mayoral_finalist_panel.csv` is 159 MB, mostly text; the corpus in Parquet is 30 MB. A Parquet version of the panel is smaller and keeps types.
- The corpus does not carry `city_id`, `year`, `election_date` or `official_result_role`. Every user must merge before any analysis.
- The panel joins pre- and post-election pages and orders them by site and address depth, not by time. `text_clean` drops page boundaries.
- The panel aggregates all usable pages. The README says the text is comparable with the House/Senate companion, but it does not say whether that dataset aggregates pages the same way. A user who compares the two needs the aggregation rule of both.
- The manifest has no version, date, license, column list or data types.
- There is no loading example and no example of a merge.

## Proposed improvements

| # | Proposal | Why a user needs it | Effort |
|---|---|---|---|
| 1 | Recompute `days_from_election`, `in_text_window` and `release_usable` from `election_date` for all pages; check whether the capture choice used the wrong date | Pre/post analyses and the usable set are wrong for 131 finalists | Small for the columns; medium if captures must be re-chosen |
| 2 | Remove NUL bytes; exclude pages that are mostly binary or replacement characters (new `exclusion_reason = binary`) | Prevents silent truncation and garbage tokens | Small |
| 3 | Add exclusion rules for bot-challenge pages, "page not found" pages and crawler-loop addresses (calendar, tag, author, paging, tracking parameters); normalize addresses before the duplicate check | Removes about 3,500 non-content pages; makes page counts meaningful | Small to medium |
| 4 | Add `person_id` (manual review of about 30 name variants) and `incumbent_running`, `is_incumbent`, `open_seat` | Incumbency is a core use case; the current hash splits people | Medium |
| 5 | Add `total_votes`, `n_candidates`, `vote_share`, `margin`, `vote_basis` (single round, runoff, RCV final round) per election and finalist | Competition and margin analyses | Medium |
| 6 | Add a city-level covariate file: string `place_fips`, `state_fips`, county, form of government, election timing, partisan or nonpartisan ballot, and IDs for other datasets | Linkage to standard local-politics data | Medium |
| 7 | Add a `phase` column (`pre`, `election_day`, `post`) and a pre-election panel (`text_pre`, `text_clean_pre`) | Most messaging studies want campaign-period text only | Small |
| 8 | Add a finalist-site-page Parquet file with clean text per page and page boundaries kept | Users need page-level text for topic models and section analysis | Small |
| 9 | Add finalist-level weights or a "core text" measure (for example, the homepage plus issue and biography pages, or the longest pre-election capture of each page type) | Reduces dominance of large sites; makes the unit comparable across finalists | Medium |
| 10 | Replace or explain the internal columns: drop `target_id`, `batch`, `run` from the main file or move them to a provenance file; recode `url_lead_basis` into 2–3 plain variables | Removes unexplained internal names and the candidate name in `run` | Small |
| 11 | Simplify `stage` to a controlled list and add `decisive_round_type` | Users can tell outright primaries, runoffs and RCV finals apart | Small |
| 12 | Make `city_years.status` distinguish "decisive election in population" from "election recorded but not verified"; list the large-city gaps in the README | Prevents miscounts and answers the obvious referee question | Small |
| 13 | Try to verify the missing large-city elections (San Diego, Sacramento, Baltimore, Detroit, Atlanta, Memphis, Tulsa) from certified results | Restores 15–20 prominent elections | Medium; may need new collection |
| 14 | Remove Santa Clarita from the population, or state the population as "99 cities with a directly elected mayor plus one comparison city" | The current wording conflicts with the data | Small |
| 15 | Provide all tabular files also as Parquet with typed columns; state `dtype=str` for IDs in the README | Avoids the leading-zero and NUL problems | Small |
| 16 | Add a coverage appendix: coverage by city rank, year, role, margin and incumbency, with a selection model (logit of `has_text` on these variables) and an indicator for each selection step (URL found, archived, usable) | Users need to model selection; referees will ask | Medium |
| 17 | Add a short data paper section: discovery steps with counts at each step, models used (including any OpenAI use), the validation sample, a hand-validated audit of a random sample of accepted and rejected sites | The README says the AI checks were not validated by hand; a referee will ask for a precision estimate | Medium to large |
| 18 | Add a loading example (Python and R) that reads the files, merges them and builds a pre-election finalist file | Lowers the entry cost | Small |
| 19 | Add a version number, release date, license and column schema to `manifest.json`, and a changelog | Reproducibility and citation | Small |
| 20 | Bigger change: a time-resolved page panel (several captures per address, for example monthly) | Enables real before/after and within-campaign change analyses, which the current one-capture design cannot support | Large; new collection |
| 21 | Bigger change: extend the population to all candidates above a vote threshold, or to first rounds | Primary and first-round messaging is a common research question; finalists only give a selected sample | Large |

## Ranking

**Must fix before publication**

1. A1: wrong `days_from_election` for 12,549 pages; recompute the window and the usable flag.
2. A2: NUL bytes and binary pages; the panel CSV truncates silently in pandas.
3. B1 and B2: document the missing large-city elections and fix the meaning of `status = complete`.
4. Internal and inconsistent provenance: the `openai_residual_expansion` batch name conflicts with the model description in the README; `run = carymoon_extra` names a candidate. Either document the OpenAI step or rename the field.
5. README claims that the data contradict: "calendar views were not downloaded" (1,729 usable `ajaxCalendar` pages), "one copy of each page" (148 repeats), the 18-month / 120-day window (observed −567 to +148), "incorporated places" (Urban Honolulu CDP).
6. The license and citation items marked TO DECIDE.

**Should fix**

- A3 crawler and error pages; address normalization before the duplicate check.
- B3 person ID and an incumbency variable.
- B4 one `election_id` format; B5 string IDs or Parquet; B6 Santa Clarita; B7 2026 flag.
- Total votes and vote basis; a controlled `stage` list; definitions of the older roster and `legacy_contest_ids`.
- A pre-election panel or a `phase` column; a Parquet panel.
- A coverage and selection appendix; a statement on the panel aggregation rule compared with the House/Senate release.
- A loading example and a versioned manifest.

**Could fix**

- City and candidate covariates and external IDs.
- Finalist weights or a "core text" measure.
- Better `page_type` classification.
- A hand-validated audit sample of site acceptance.
- A time-resolved page panel and an extension beyond finalists.
