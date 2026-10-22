# Codebook: campaign websites of mayoral finalists, 100 largest US cities, 2010–2026

**Draft for review, October 18, 2026 (version 6).** `README.md` describes the population, the collection, the checks and the counts.

## Keys

| File | Unique key | Links |
|---|---|---|
| `mayoral_cities.csv` | `city_id` | |
| `mayoral_city_years.csv` | `city_id`, `year` | `city_id` → cities; `election_ids` → elections |
| `mayoral_elections.csv` | `election_id` | `city_id` → cities |
| `mayoral_finalists.csv` | `candidate_election_id` | `election_id` → elections; `city_id` → cities |
| `mayoral_page_corpus.parquet` | `page_id` | `candidate_election_id` → finalists or primary candidates |
| `mayoral_finalist_panel.csv` | `candidate_election_id` | `candidate_election_id` → finalists |
| `mayoral_ledb_crosswalk.csv` | `candidate_election_id` | `candidate_election_id` → finalists; `ledb_candid`, `ledb_contest` → ALGED |
| `mayoral_primary_elections.csv` | `primary_election_id` | `city_id` → cities; `general_election_id` → elections |
| `mayoral_primary_candidates.csv` | `candidate_election_id` | `primary_election_id` → primary elections; `finalist_candidate_election_id` → finalists |
| `mayoral_primary_panel.csv` | `candidate_election_id` | `candidate_election_id` → primary candidates |

`city_id` is the 2020 Census place GEOID. `election_id` combines `city_id`, the election date and the election type. `candidate_election_id` adds a hash of the candidate's normalized name. A primary candidate has a `candidate_election_id` of its own, built in the same way from `primary_election_id`; it never equals a finalist's ID. `person_id` links the elections of one person in one city. Read ID columns as text: `city_id` has a leading zero for some states. An empty cell (an empty string in the Parquet file) means "not applicable" or "not recorded", never zero.

## `mayoral_cities.csv`

| Column | Definition |
|---|---|
| `city_id`, `census_geoid` | 2020 Census place GEOID |
| `city_name`, `state_abb` | Census place name and two-letter state code |
| `population_2020`, `population_rank_2020` | 2020 Census population and its rank among US incorporated places |
| `roster_place_fips` | Place code in the older candidate roster, where it differs from the GEOID |
| `crosswalk_status` | Match to the older roster: `exact_geoid`, `explicit_historical_alias`, or `missing_roster_city` (Santa Clarita, where the council selects the mayor) |
| `jurisdiction_note`, `jurisdiction_source_url` | Note and source for consolidated city-county governments and similar cases |
| `source_url` | Census source of the population figure |
| `county_fips` | Five-digit FIPS code of the county that holds the largest part of the city's 2020 population |
| `county_fips_all` | All counties the city lies in, largest part first, joined by `;` |
| `county_population_share` | Share of the city's 2020 population in `county_fips` (2020 Census, population of each city-county part) |

## `mayoral_city_years.csv`

| Column | Definition |
|---|---|
| `city_id`, `city_name`, `state_abb`, `year` | City and calendar year (2026 up to September 14) |
| `status` | `complete`: at least one mayoral election recorded; `no_election`: official sources show none; `earlier_round_only`: only a first round fell in the period; `indirect_selection`: the council or another body chose the mayor; `unresolved`: official sources could not settle the year |
| `audit_complete` | `True` if the status rests on official sources; `False` for unresolved years |
| `election_ids` | Recorded elections in the year (JSON list) |
| `expected_election_dates` | Election dates from the city's election calendar, where known |
| `source_url`, `audit_note` | Main official source and how the year was resolved |

## `mayoral_elections.csv`

| Column | Definition |
|---|---|
| `election_id`, `city_id`, `city_name`, `state_abb`, `year`, `election_date` | The election |
| `election_type` | `regular`, `special` or `unresolved` |
| `stage` | Round of the election: `general`; `runoff`; `primary_nominating` (a primary or first round that a candidate won outright, so it decided the office); `ranked_choice_final` or `ranked_choice` (the final round of a ranked-choice count); `special` (a special election); `verified_general_election` (a label from the older roster, results not verified); `single_recorded_mayoral_contest_per_city_year_stage_not_explicit` (the source does not name the round) |
| `verification_status` | `verified_decisive`: decided who became mayor, with at least two candidates, so in the population; `uncontested`: one candidate; `legacy_results_unverified`: results only from the older roster; `unresolved`, `unresolved_final_pair`: the result could not be verified |
| `certification_status`, `certification_source_url` | Whether the result was certified, canvassed or checked against an official source, and that source |
| `source_url`, `source_locator`, `verification_note` | Source of the result, the place in the source, and notes |
| `legacy_contest_ids` | Contest IDs in the older roster |
| `rounds` | For elections in the population: `two_rounds` (an earlier round of the same race, a primary or a first round, took place on `first_round_date`) or `single_round` (the race had one round) |
| `first_round_date` | Date of the first round of the race; equals `election_date` for `single_round` |
| `first_round_source`, `first_round_source_url` | Source of the first-round date: `primary_file` (the decisive partisan primary in `mayoral_primary_elections.csv`), `calendar_note` (the official election calendar of the city), `ballotpedia` (the archived Ballotpedia page of the election), `agent_search` (a web search by an AI agent; most sources are Wikipedia election pages), and the address of that source |

## `mayoral_finalists.csv`

One row per winner or runner-up of a recorded election. Rows with `eligible` = `True` form the population.

| Column | Definition |
|---|---|
| `candidate_election_id`, `person_id` | Finalist key; person key (same city, same last name and same first initial after mapping common nicknames, so Ed Lee and Edwin M. Lee match) |
| `election_id`, `city_id`, `city_name`, `state_abb`, `year`, `election_date`, `election_type` | The election |
| `candidate_name` | Name as in the official result |
| `official_result_role` | `winner` or `runner_up` |
| `official_votes`, `result_source_url` | Votes and the official result source |
| `top_two_vote_share` | Share of the votes of the two finalists; empty if a vote count is missing |
| `incumbent` | `True` if the finalist won the city's previous election in the population; empty for a city's first election in the data (198 eligible finalists) and for finalists outside the population |
| `eligible`, `verification_status` | Population membership; the election's `verification_status` |
| `url_lead` | `True` if a campaign website is known |
| `url_lead_basis` | Source of the website: `legacy_url_inventory` (older candidate roster, checked against archived pages); values starting with `agent_search`, `legacy_lead` or `checker_proposal` (web search by an AI agent); values starting with `domain_sweep` (common campaign domains tested in the Wayback Machine or Common Crawl). Sites from searches were confirmed by a second, independent agent |
| `campaign_urls` | Known campaign website addresses (JSON list) |
| `text_status` | Status of the website text for eligible finalists; values in `README.md` |
| `n_sites`, `n_pages_usable`, `n_char_usable` | Websites with usable pages, usable pages, and characters of the joined usable text. These are counts of usable material: 0 means no usable page, whatever the reason in `text_status`; they do not measure a candidate's campaign |

## `mayoral_page_corpus.parquet`

One row per downloaded page. Excluded and duplicate pages stay in the file with their flags.

| Column | Definition |
|---|---|
| `page_id` | Page key (one archived capture) |
| `candidate_election_id` | Finalist or primary candidate |
| `target_id` | One finalist and one website |
| `batch`, `run` | Download job that collected the page (`round_01` …; `first_150_pages`, `remaining_pages`, `supplementary_query`, `pre_election_copy`). Provenance only |
| `site_domain` | Registrable domain of the website |
| `original_url`, `snap_url` | Address of the page as archived, and the Wayback Machine address of the copy |
| `capture_timestamp` | Archive time, `YYYYMMDDhhmmss` (UTC) |
| `days_from_election`, `phase` | Days from the election to the capture (negative before it; for a primary candidate's page, from the primary), and `before`, `election_day` or `after`, both from the archive date, not the date the page was written |
| `in_text_window` | `True` if the capture is within 365 days of the election |
| `page_type` | Type from the address: `homepage`, `issues`, `biography`, `endorsements`, `news`, `action` (donate, volunteer), `constituent_services`, `other` |
| `text`, `n_char`, `n_words` | Visible text with segments joined by `#+#`, and its characters and words |
| `n_tags`, `n_clean_tags` | Number of segments, and of segments kept by the cleaning rule (see `text_clean`) |
| `text_sha256`, `raw_html_sha256` | SHA-256 of the text and of the archived HTML |
| `names_candidate`, `app_shell`, `other_year_only`, `non_latin_text`, `also_in_election` | Information flags. They do not affect `release_usable` |
| `names_candidate` | `True` if the page contains the candidate's last name |
| `app_shell` | `True` if the page has fewer than 40 words and asks for JavaScript or shows only a loading message |
| `other_year_only` | `True` if the page names years, but not the election year or the year before |
| `non_latin_text` | `True` if the page is mostly in a non-Latin script |
| `duplicate_of` | `page_id` of the copy kept instead of this page: a page of the same finalist with the same text, or the same address after removing the scheme, "www.", a trailing slash and the query. The kept copy always passes the page rules inside the 365-day window: the copy from before the election where one exists, otherwise the copy nearest the election. Copies on different sides of the first round (`campaign_period` `first_round` versus `final_round`) are never duplicates of each other. A kept copy can still be unusable if its site failed the site check. Copies at the same address can differ in text (for example image-viewer addresses that differ only in the query); only the kept copy's text is usable |
| `also_in_election` | For a usable page, the other `candidate_election_id`s of the same person that have a usable page with the same text or the same `snap_url`, joined by `;`. A site reused across campaigns shows up here |
| `excluded`, `exclusion_reason` | `True` if a page rule applies; reasons joined by `;`: `empty` (under 50 characters); `parked_or_placeholder` (a directory listing, or under 1,200 characters saying the domain is for sale, parked, expired, suspended or under construction); `spam` (platform profile spam, or five or more gambling, drug or loan terms on a page that does not name the candidate); `binary_or_garbled` (NUL bytes, or over 5 percent replacement or control characters); `error_or_challenge` (under 1,500 characters and a bot challenge, access-denied or "page not found" page); `listing_page` (an address that lists other pages: tag, author, category, feed, comments, search, paging, date archive or calendar view); `not_a_page` (a file or system address, not an HTML page: style sheets, scripts, images, documents, `robots.txt`, sitemaps, WordPress admin, login, theme and plugin addresses, CGI scripts and email-protection pages); `not_this_campaign` (the page check labels the page `earlier_campaign`, `other_office` or `not_campaign`) |
| `page_check` | Label of the page check for pages that were usable before it: `this_campaign` (part of this campaign, including earlier rounds of the same race); `unclear` (a generic page that cannot be tied to one election, such as a contact form); `earlier_campaign` (only about another race of the candidate); `other_office` (a campaign or office page for another office); `not_campaign` (not about the candidate's campaign). Labels come from Qwen3.6-27B, except 71 pages that the model dated to the election year or the year before, which Claude Opus decided. A copy with the same text as a labeled page of the same finalist carries that page's label, so an excluded page (for example a listing page) can carry a label; only `excluded` decides. Empty for pages the check did not read |
| `site_check_passed` | `True` if the site passed the site check; for the one site taken over after the election, only pages up to election day |
| `release_usable` | `True` if `in_text_window`, not `excluded`, no `duplicate_of`, and `site_check_passed` |
| `campaign_period` | Period of the campaign at the capture date. For a finalist's page: `first_round` (on or before the first round of a two-round race), `final_round` (after the first round), or `single_round` (the race had one round). For a primary candidate's page: `first_round` (on or before the candidate's primary) or `after_primary`. The first-round dates are in `mayoral_elections.csv`. For pages of finalists outside the population, the value is empty |

## `mayoral_finalist_panel.csv`

One row per eligible finalist with at least one usable page.

| Column | Definition |
|---|---|
| `candidate_election_id` … `official_result_role`, `person_id`, `election_type`, `top_two_vote_share`, `incumbent` | As in `mayoral_finalists.csv` |
| `stage` | As in `mayoral_elections.csv` |
| `population_2020` | As in `mayoral_cities.csv` |
| `n_sites`, `n_pages`, `n_pages_before_election`, `n_pages_after_election` | Websites, usable pages, and usable pages archived before election day and on or after it |
| `first_capture`, `last_capture` | Earliest and latest capture among the usable pages |
| `text` | Text of all usable pages, ordered by website, address depth, address and capture time, joined by `#+#` |
| `n_char`, `n_words`, `n_tags`, `n_clean_tags` | As in the corpus, for the joined text |
| `text_clean` | `text` after the cleaning rule of ICPSR 226001: addresses and digits removed, and only segments of ten or more words with sentence punctuation and a capital letter kept. Digits are removed, including amounts and proposition numbers. Empty for 6 finalists |
| `text_before`, `n_char_before`, `text_clean_before` | As `text`, `n_char` and `text_clean`, for the usable pages archived before election day only; empty if there are none (33 finalists) |
| `text_unique` | `text` with each segment kept once, at its first occurrence (segments compared after collapsing white space). Menus, footers and other text repeated across pages count once |

## `mayoral_ledb_crosswalk.csv`

Links each finalist to the American Local Government Elections Database (ALGED; de Benedictis-Kessner, Lee, Velez and Warshaw; OSF project mv5e6, file `ledb_candidatelevel.csv`, CC BY 4.0), which covers mayoral elections from 1989 to 2021. The file holds identifiers only; join ALGED for its party, incumbency, vote and candidate variables.

| Column | Definition |
|---|---|
| `candidate_election_id` | Finalist |
| `ledb_candid` | ALGED candidate ID of the same person; empty if there is no match |
| `ledb_contest` | ALGED contest ID of the same election |
| `ledb_match` | `name_same_month`: same city, year and election month, and the same name (a first name also matches its quoted nickname or long form, so Jim matches James); `last_name_same_month`: same city, year and month, and the same last name, which only one ALGED candidate in that city and year has; `outside_ledb_years`: election after 2021; `no_ledb_election`: ALGED has no mayoral election for the city in that year; `no_ledb_candidate`: ALGED has the election but not this person |

## `mayoral_primary_elections.csv`

One row per decisive partisan primary: the primary of the party whose nominee won the general election, in a city with partisan mayoral primaries.

| Column | Definition |
|---|---|
| `primary_election_id`, `city_id`, `city_name`, `state_abb`, `year` | The primary |
| `primary_date`, `primary_party` | Date and party of the primary |
| `general_election_id` | The general election that followed; empty for the District of Columbia primary of June 16, 2026, whose general election is after the end of the data |
| `unopposed` | `True` if the primary had one candidate |
| `total_candidates_on_ballot` | Candidates on the primary ballot |
| `source_url`, `second_source_url` | Official result source, and a second source where used |

## `mayoral_primary_candidates.csv`

One row per candidate who placed first or second in a decisive primary.

| Column | Definition |
|---|---|
| `candidate_election_id`, `person_id` | Primary candidate key; person key, as in `mayoral_finalists.csv` (the same person as a finalist has the same `person_id`) |
| `primary_election_id`, `city_id`, `city_name`, `state_abb`, `year`, `election_date`, `primary_party`, `general_election_id` | The primary; `election_date` is the primary date |
| `candidate_name`, `primary_rank`, `votes`, `vote_share` | Name, place (1 or 2), votes and share of all votes in the deciding round of the primary, from the official result; for the ranked-choice primaries of New York 2021 and 2025, the final round (share of continuing ballots), and for the Charlotte 2015 runoff, the runoff |
| `unopposed` | `True` if the primary had one candidate |
| `finalist_candidate_election_id`, `finalist_role_in_general` | The same person's row in `mayoral_finalists.csv` and role in the general election, if the candidate was also a finalist |
| `searched` | `True` if the candidate is not a finalist, so a separate search looked for the website |
| `url_lead_status` | Result of that search: `accepted`, `outside_window` (a website exists, but the archive has no copy within a year of the primary), `not_found`, `incomplete`; empty if the candidate was not searched |
| `campaign_urls` | Known campaign website addresses (JSON list) |
| `text_status` | `text`, `no_url_lead`, `site_not_archived`, `no_usable_pages`, `excluded_site_check`, `check_incomplete`; meanings as in `README.md` |
| `n_sites`, `n_pages_usable`, `n_char_usable` | Websites, usable pages, and characters of the joined text of the primary period |
| `source_url` | Official result source |

## `mayoral_primary_panel.csv`

One row per primary candidate with usable text from the primary period: pages that pass the page rules and the site check, archived in the 365 days up to and including the primary date. Of several copies with the same text or the same address, the copy nearest the primary is kept.

| Column | Definition |
|---|---|
| `candidate_election_id` … `finalist_candidate_election_id` | As in `mayoral_primary_candidates.csv`; `population_2020` as in `mayoral_cities.csv` |
| `text_source` | `own_search` (pages of the website found for the primary candidate) or `finalist_pages` (pages of the same person's finalist websites) |
| `n_sites`, `n_pages`, `first_capture`, `last_capture` | Websites, pages, and earliest and latest capture |
| `text`, `n_char`, `n_words`, `n_tags`, `n_clean_tags`, `text_clean`, `text_unique` | As in `mayoral_finalist_panel.csv`, for the primary-period pages |
