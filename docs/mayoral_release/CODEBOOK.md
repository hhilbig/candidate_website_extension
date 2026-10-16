# Codebook: campaign websites of mayoral finalists, 100 largest US cities, 2010–2026

**Draft for review, October 16, 2026.** `README.md` describes the population, the collection, the checks and the counts.

## Keys

| File | Unique key | Links |
|---|---|---|
| `mayoral_cities.csv` | `city_id` | |
| `mayoral_city_years.csv` | `city_id`, `year` | `city_id` → cities; `election_ids` → elections |
| `mayoral_elections.csv` | `election_id` | `city_id` → cities |
| `mayoral_finalists.csv` | `candidate_election_id` | `election_id` → elections; `city_id` → cities |
| `mayoral_page_corpus.parquet` | `page_id` | `candidate_election_id` → finalists |
| `mayoral_finalist_panel.csv` | `candidate_election_id` | `candidate_election_id` → finalists |

`city_id` is the 2020 Census place GEOID. `election_id` combines `city_id`, the election date and the election type. `candidate_election_id` adds a hash of the candidate's normalized name. `person_id` links the elections of one person in one city. An empty cell means "not applicable" or "not recorded", never zero.

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
| `stage` | Round of the election, for example `general`, `runoff`, `ranked_choice_final` or `primary_nominating`; `single_recorded_mayoral_contest_per_city_year_stage_not_explicit` where the source does not name the round |
| `verification_status` | `verified_decisive`: decided who became mayor, with at least two candidates, so in the population; `uncontested`: one candidate; `legacy_results_unverified`: results only from the older roster; `unresolved`, `unresolved_final_pair`: the result could not be verified |
| `certification_status`, `certification_source_url` | Whether the result was certified, canvassed or checked against an official source, and that source |
| `source_url`, `source_locator`, `verification_note` | Source of the result, the place in the source, and notes |
| `legacy_contest_ids` | Contest IDs in the older roster |

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
| `total_votes`, `vote_share` | Total votes cast and the finalist's share of them; recorded only where the official result gives a total (38 eligible finalists) |
| `incumbent` | `True` if the finalist won the city's previous election in the population; empty for a city's first election in the data (198 eligible finalists) and for finalists outside the population |
| `eligible`, `verification_status` | Population membership; the election's `verification_status` |
| `url_lead` | `True` if a campaign website is known |
| `url_lead_basis` | Source of the website: `legacy_url_inventory` (older candidate roster, checked against archived pages); values starting with `agent_search`, `legacy_lead` or `checker_proposal` (web search by an AI agent); values starting with `domain_sweep` (common campaign domains tested in the Wayback Machine or Common Crawl). Sites from searches were confirmed by a second, independent agent |
| `campaign_urls` | Known campaign website addresses (JSON list) |
| `text_status` | Status of the website text for eligible finalists; values in `README.md` |
| `n_sites`, `n_pages_usable`, `n_char_usable` | Websites, usable pages, and characters of the joined usable text |

## `mayoral_page_corpus.parquet`

One row per downloaded page. Excluded and duplicate pages stay in the file with their flags.

| Column | Definition |
|---|---|
| `page_id` | Page key (one archived capture) |
| `candidate_election_id` | Finalist |
| `target_id` | One finalist and one website |
| `batch`, `run` | Download job that collected the page (`round_01` …; `first_150_pages`, `remaining_pages`, `supplementary_query`). Provenance only |
| `site_domain` | Registrable domain of the website |
| `original_url`, `snap_url` | Address of the page as archived, and the Wayback Machine address of the copy |
| `capture_timestamp` | Archive time, `YYYYMMDDhhmmss` (UTC) |
| `days_from_election`, `phase` | Days from the election to the capture (negative before it), and `before`, `election_day` or `after` |
| `in_text_window` | `True` if the capture is within 365 days of the election |
| `page_type` | Type from the address: `homepage`, `issues`, `biography`, `endorsements`, `news`, `action` (donate, volunteer), `constituent_services`, `other` |
| `text`, `n_char`, `n_words` | Visible text with segments joined by `#+#`, and its characters and words |
| `n_tags`, `n_clean_tags` | Number of segments, and of segments kept by the cleaning rule (see `text_clean`) |
| `text_sha256`, `raw_html_sha256` | SHA-256 of the text and of the archived HTML |
| `names_candidate` | `True` if the page contains the candidate's last name |
| `app_shell` | `True` if the page has fewer than 40 words and asks for JavaScript or shows only a loading message |
| `other_year_only` | `True` if the page names years, but not the election year or the year before |
| `non_latin_text` | `True` if the page is mostly in a non-Latin script |
| `duplicate_of` | `page_id` of an earlier page of the same finalist with the same text, or the same address after removing the scheme, "www.", a trailing slash and the query |
| `excluded`, `exclusion_reason` | `True` if a page rule applies; reasons joined by `;`: `empty` (under 50 characters); `parked_or_placeholder` (a directory listing, or under 1,200 characters saying the domain is for sale, parked, expired, suspended or under construction); `spam` (platform profile spam, or five or more gambling, drug or loan terms on a page that does not name the candidate); `binary_or_garbled` (NUL bytes, or over 5 percent replacement or control characters); `error_or_challenge` (under 1,500 characters and a bot challenge, access-denied or "page not found" page); `listing_page` (an address that lists other pages: tag, author, category, feed, comments, search, paging, date archive or calendar view) |
| `site_check_passed` | `True` if the site passed the site check; for the one site taken over after the election, only pages up to election day |
| `release_usable` | `True` if `in_text_window`, not `excluded`, no `duplicate_of`, and `site_check_passed` |

## `mayoral_finalist_panel.csv`

One row per eligible finalist with at least one usable page.

| Column | Definition |
|---|---|
| `candidate_election_id` … `official_result_role`, `person_id`, `incumbent` | As in `mayoral_finalists.csv` |
| `n_sites`, `n_pages`, `n_pages_before_election` | Websites, usable pages, and usable pages archived before election day |
| `first_capture`, `last_capture` | Earliest and latest capture among the usable pages |
| `text` | Text of all usable pages, ordered by website, address depth, address and capture time, joined by `#+#` |
| `n_char`, `n_words`, `n_tags`, `n_clean_tags` | As in the corpus, for the joined text |
| `text_clean` | `text` after the cleaning rule of ICPSR 226001: addresses and digits removed, and only segments of ten or more words with sentence punctuation and a capital letter kept. Empty for 8 finalists |
