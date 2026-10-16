# Codebook: campaign websites of mayoral finalists, 100 largest US cities, 2010–2026

**Draft for review, October 16, 2026.** See `README.md` for the population, the collection and the checks.

## Keys and links between files

| File | Unique key | Links |
|---|---|---|
| `mayoral_cities.csv` | `city_id` | |
| `mayoral_city_years.csv` | `city_id`, `year` | `city_id` → cities; `election_ids` → elections |
| `mayoral_elections.csv` | `election_id` | `city_id` → cities |
| `mayoral_finalists.csv` | `candidate_election_id` | `election_id` → elections; `city_id` → cities |
| `mayoral_page_corpus.parquet` | `page_id` | `candidate_election_id` → finalists |
| `mayoral_finalist_panel.csv` | `candidate_election_id` | `candidate_election_id` → finalists |

`city_id` is the 2020 Census place GEOID. `election_id` combines `city_id`, the election date and the election type. `candidate_election_id` adds a hash of the candidate's normalized name, so one person has one ID per election. Empty cells mean "not applicable" or "not recorded"; they never mean zero.

## `mayoral_cities.csv`

| Column | Definition |
|---|---|
| `city_id`, `census_geoid` | 2020 Census place GEOID |
| `city_name`, `state_abb` | Census place name and two-letter state |
| `population_2020`, `population_rank_2020` | 2020 Census population and its rank among US incorporated places |
| `roster_place_fips` | Place code in the older candidate roster, when it differs from the GEOID |
| `crosswalk_status` | How the city was matched to the older roster: `exact_geoid`, `explicit_historical_alias`, or `missing_roster_city` (Santa Clarita, where the council selects the mayor) |
| `jurisdiction_note`, `jurisdiction_source_url` | Notes on consolidated city-county governments and similar cases, with a source |
| `source_url` | Census source of the population figure |

## `mayoral_city_years.csv`

| Column | Definition |
|---|---|
| `city_id`, `city_name`, `state_abb`, `year` | City and calendar year (2010–2026; 2026 up to September 14) |
| `status` | `complete`: at least one mayoral election recorded; `no_election`: official sources show no mayoral election that year; `earlier_round_only`: only a first round fell in the period; `indirect_selection`: the council chose the mayor; `unresolved`: the year could not be resolved from official sources |
| `audit_complete` | `True` if the status rests on official sources; `False` for the 51 unresolved city-years |
| `election_ids` | Recorded elections in that year (JSON list) |
| `expected_election_dates` | Election dates from the city's election calendar, where known |
| `source_url`, `audit_note` | Main official source and a note on how the year was resolved |

## `mayoral_elections.csv`

| Column | Definition |
|---|---|
| `election_id`, `city_id`, `city_name`, `state_abb`, `year`, `election_date` | Election identity |
| `election_type` | `regular`, `special`, or `unresolved` |
| `stage` | Round of the election: `general`, `runoff`, `ranked_choice_final`, `primary_nominating`, and others; `single_recorded_mayoral_contest_per_city_year_stage_not_explicit` where the source does not name the round |
| `verification_status` | `verified_decisive`: complete, decisive and contested, part of the population (430); `uncontested`: one candidate (21); `legacy_results_unverified`: results from the older roster only (11); `unresolved`, `unresolved_final_pair`: the result could not be verified (2) |
| `certification_status`, `certification_source_url` | Whether the result was certified, canvassed or verified against an official source, and that source |
| `source_url`, `source_locator`, `verification_note` | Source of the result, where in the source it is, and notes |
| `legacy_contest_ids` | Contest IDs in the older roster, for linkage |

## `mayoral_finalists.csv`

One row per winner or runner-up of a recorded election. Only rows with `eligible` = `True` (finalists of the 430 `verified_decisive` elections) are in the population.

| Column | Definition |
|---|---|
| `candidate_election_id` | Finalist key |
| `person_id` | One ID per person and city: same city, same last name and same first initial after common nicknames are mapped (for example Ed Lee and Edwin M. Lee). Links a person's elections |
| `election_id`, `city_id`, `city_name`, `state_abb`, `year`, `election_date`, `election_type` | The election |
| `candidate_name` | Name as in the official result |
| `official_result_role` | `winner` or `runner_up` in the official result |
| `official_votes`, `result_source_url` | Votes and the official result source |
| `top_two_vote_share` | The finalist's share of the votes of the two finalists; empty if a vote count is missing |
| `total_votes`, `vote_share` | Total votes cast in the election and the finalist's share of them, where the full official result records a total (38 eligible finalists); empty otherwise |
| `incumbent` | `True` if the finalist won the city's previous decisive election in the data; empty for a city's first election in the data (198 eligible finalists), because the earlier mayor is not recorded here; empty for finalists who are not eligible |
| `eligible`, `verification_status` | Population membership (see above) |
| `url_lead` | `True` if a campaign website is known for this finalist |
| `url_lead_basis` | How the website was found and checked: `legacy_url_inventory` (older roster, checked against archived pages); `agent_search_wayback_capture_checked` (agent search, independent agent check); `agent_search_repair_wayback_capture_checked` (repeated agent search where the first could not finish); `domain_sweep_wayback_capture_checked`, `domain_sweep_commoncrawl_capture_checked`, `domain_sweep_after_election_capture_checked` (domain-pattern search in the Wayback Machine or Common Crawl, then an agent check); `checker_proposal_wayback_capture_verified` (site proposed by a checking agent, verified by a third); `legacy_lead_search_wayback_capture_checked`, `legacy_lead_sweep_wayback_capture_checked` (search for finalists whose roster URL had no archived copy near the election); `agent_search_pass4_wayback_capture_checked` (final agent search for finalists still without a site, independent agent check) |
| `campaign_urls` | Known campaign website addresses (JSON list) |
| `text_status` | Status of the website text (eligible finalists only): `text`, `no_url_lead`, `site_not_archived`, `check_incomplete`, `no_usable_pages`, `excluded_site_check`, `site_rejected`; see `README.md` |
| `n_sites`, `n_pages_usable`, `n_char_usable` | Number of websites, usable pages, and characters of the joined usable text |

## `mayoral_page_corpus.parquet`

One row per archived page of a finalist's campaign website. All downloaded pages are included, with flags; `release_usable` marks the pages that enter the panel.

| Column | Definition |
|---|---|
| `page_id` | Page key (one archived capture) |
| `candidate_election_id` | Finalist |
| `target_id`, `batch`, `run` | Collection provenance: the scrape target (one finalist and one website), the collection batch, and the run within it (`production`: first 150 pages; `uncapped`: all further pages; `carymoon_extra`: pages found by an extra archive query for one site) |
| `site_domain` | Registrable domain of the website |
| `original_url` | Address of the page as archived |
| `snap_url` | Wayback Machine address of the archived copy |
| `capture_timestamp` | Archive time, `YYYYMMDDhhmmss` (UTC) |
| `days_from_election` | Days from the election to the capture; negative before the election |
| `in_text_window` | `True` if the capture is within 365 days before or after the election |
| `phase` | `before`, `election_day` or `after`, from `days_from_election` |
| `page_type` | Page type from the address: `homepage`, `issues`, `biography`, `endorsements`, `news`, `action` (donate, volunteer), `constituent_services`, `other` |
| `text` | Visible text, segments joined with `#+#` |
| `n_char`, `n_words` | Characters and words of `text` |
| `n_tags`, `n_clean_tags` | Number of `#+#` segments, and of segments that pass the ICPSR 226001 cleaning rule (ten or more words, sentence punctuation, a capital letter) |
| `text_sha256`, `raw_html_sha256` | SHA-256 of the extracted text and of the archived HTML |
| `names_candidate` | `True` if the page contains the candidate's last name |
| `app_shell` | `True` if the page has fewer than 40 words and asks for JavaScript or shows only a loading message |
| `other_year_only` | `True` if the page names years, but not the election year or the year before |
| `non_latin_text` | `True` if the page is mostly in a non-Latin script (for example Chinese) |
| `duplicate_of` | `page_id` of an earlier page of the same finalist with identical text, or with the same address after removing the scheme, "www.", a trailing slash and the query; empty if none |
| `excluded`, `exclusion_reason` | `True` if a page rule excludes the page: `empty` (under 50 characters), `parked_or_placeholder` (a directory listing, or a page under 1,200 characters saying the domain is for sale, parked, expired or suspended, or under construction), `spam` (platform user-profile spam, or five or more gambling, drug or loan terms on a page that does not name the candidate), `binary_or_garbled` (NUL bytes, or more than 5 percent replacement or control characters), `error_or_challenge` (a page under 1,500 characters that is a bot challenge, an access-denied or a "page not found" page), `listing_page` (an address that lists other pages: tag, author, category, feed, comments, search, paging, date archives without a post, calendar views). Several reasons are joined with `;` |
| `site_check_passed` | `True` if the website passed the site check (and, for one site, the page is from before the domain changed hands) |
| `release_usable` | `True` if `in_text_window`, not `excluded`, no `duplicate_of`, and `site_check_passed` |

## `mayoral_finalist_panel.csv`

One row per eligible finalist with at least one usable page.

| Column | Definition |
|---|---|
| `candidate_election_id` … `official_result_role` | As in `mayoral_finalists.csv` |
| `person_id`, `incumbent` | As in `mayoral_finalists.csv` |
| `n_sites`, `n_pages` | Websites and usable pages |
| `n_pages_before_election` | Usable pages archived before election day |
| `first_capture`, `last_capture` | Earliest and latest capture time among the usable pages |
| `text` | Text of all usable pages, ordered by website, address depth, address and capture time, joined with `#+#` |
| `n_char`, `n_words`, `n_tags`, `n_clean_tags` | As in the corpus, for the joined text |
| `text_clean` | `text` after the ICPSR 226001 cleaning rule: addresses and digits removed, only segments of ten or more words with sentence punctuation and a capital letter kept. Empty for 8 finalists whose pages have no such segment |
