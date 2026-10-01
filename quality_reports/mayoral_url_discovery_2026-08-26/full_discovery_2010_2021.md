# Mayoral URL discovery, full 2010–2021 wave

## Current state

The full search wave produced candidate-owned URL leads for 740 of 2,180 previously unresolved candidate-years (33.9%). Combining these leads with the current unified claim table gives a discovery list covering 1,046 of all 4,817 candidate-years (21.7%). This is a candidate list, not a strict-coverage result: the new Serper, OpenAI, and Archive-It rows remain grade D and pending validation.

The authoritative outputs are:

- [candidate URL claims](../../data/municipal_discovery/screening_2010_2021/full_discovery_2010_2021/combined_candidate_list/candidate_url_claims.csv), which preserves 1,152 independent source claims;
- [candidate-domain list](../../data/municipal_discovery/screening_2010_2021/full_discovery_2010_2021/combined_candidate_list/candidate_domains.csv), which contains 1,102 candidate-domain rows for 1,046 candidate-years; and
- [combined-list audit](../../data/municipal_discovery/screening_2010_2021/full_discovery_2010_2021/combined_candidate_list/audit.json), which records the source counts and strict flags.

## Why this wave was run

The earlier 480-case screen showed that one Google-style query followed by OpenAI web search could find campaign-site candidates at useful rates. This wave tests whether the same approach scales to the unresolved 2010–2021 population while preserving every source result and respecting the approved paid ceiling.

The comparison is candidate-years with at least one candidate-owned URL lead versus candidate-years with no such lead after the same discovery stack.

## Inputs and exclusions

The current unresolved 2010–2021 frame contained 2,663 candidate-years. The production universe excluded 477 cases from the earlier 480-case screen and six additional cases with an active coverage-eligible claim, leaving 2,180 candidate-years. The 483-row reduction is 18.1%, below the project's 20% large-drop threshold. No missing election month was imputed; eight production candidates retain a missing month.

The production universe contains 765 candidate-years from 2010–2013, 690 from 2014–2017, and 725 from 2018–2021.

## Search results

The search stack found more candidates in later elections, but the early-period yield remains material.

| Period | Production candidates | Candidates with a new lead | Yield |
|---|---:|---:|---:|
| 2010–2013 | 765 | 202 | 26.4% |
| 2014–2017 | 690 | 251 | 36.4% |
| 2018–2021 | 725 | 287 | 39.6% |
| Total | 2,180 | 740 | 33.9% |

Serper attempted 1,813 of the planned 2,020 queries before the provider returned `Not enough credits`. It retained 16,904 organic results and produced 57 independent direct-site claims for 45 candidates. Its attempt ledger contains 1,798 found responses, five no-results, eight retryable request errors, and two terminal credit errors. The 207 unqueried cases and every Serper case without a direct-site hit flowed into OpenAI.

OpenAI web search attempted all 2,135 residual candidates. It produced structured source sets for 2,130 candidates, four no-results, and one transient error. The transient error and one malformed assistant response were retried; both retries parsed successfully and neither contained a campaign URL. Extraction produced 734 candidate-domain targets for 695 candidates. Serper and OpenAI candidate counts do not overlap because direct Serper owners were excluded from the OpenAI manifest.

The OpenAI run used an estimated $30.5683, and the two repairs used an estimated $0.0341. The combined estimated cost is $30.6024, below the approved $36 ceiling. The estimate uses the provider-reported tool calls and token usage; it is not a billing-statement observation.

## Archive-It expansion

The public Archive-It collection-catalog endpoint failed after bounded retries, so the existing 753-collection catalog was not overwritten or interpreted as empty. The public seed endpoint remained available. A high-precision scan of seven unused U.S. election collections inspected 6,806 seeds and produced one eligible lead: Cat Brooks's 2018 Oakland campaign site. OpenAI independently found the same URL, so Archive-It adds a separate source claim but no additional candidate-year.

The seed filter removed 6,805 of 6,806 records (99.99%). This large drop is expected: most records concern federal or state offices, or lack an exact candidate-name match plus a city or mayoral signal. Every seed remains in the seed audit with its disposition.

## Interpretation and next step

The discovery strategy scales. A 33.9% new-candidate yield is high enough to stop broad discovery temporarily and move the 740 new candidate-years into the separate archive-scraping and temporal-validation pipeline. The list should not be reported as validated historical coverage. Under the current unified evidence rules, only 32 candidate-years in the combined list already carry strict custom-domain and exact-URL flags; the new 740 remain pending.

The next production step is to append the 740 candidates to the Wayback scrape queue, while preserving source claims and candidate-domain groupings. Additional discovery should focus on the 1,440 production candidates with no new lead rather than repeat searches for the 740 found here.

## Archive-scrape handoff

The 740 newly discovered candidate-years are frozen as 783 candidate-domain targets. The target builder recovered all 792 independent new discovery claims and grouped them without discarding separate source support. The frozen batch expands each target into URL variants, producing 3,096 planned archive-inventory attempts. It has no missing election months.

This batch is disjoint by candidate-year and registrable domain from all three earlier acquisition batches: the active 151-candidate batch, the completed 100-case pilot, and the already queued 121-target residual expansion. The new batch is third in the droplet queue. It waits for the existing residual-expansion wrapper to finish, then checks disk space before starting. The scraper starts only if at least 2.5 GB remains free; otherwise it exits without scraping and the Slack watcher sends an `ACTION NEEDED` alert. Successful completion also triggers a Slack notification.

The authoritative acquisition inputs are [the source targets](../../data/municipal_archive_acquisition/full_discovery_740_20260826/source_targets.csv), [the frozen targets](../../data/municipal_archive_acquisition/full_discovery_740_20260826/frozen/targets.csv), and [the frozen manifest](../../data/municipal_archive_acquisition/full_discovery_740_20260826/frozen/manifest.json). The frozen-target SHA-256 is `7c82582c7ddf33afa80fa53ac861254503f8d320e456308dad03468fef103132`.

## Reproducibility

The production runner is [run_mayoral_discovery_full_2010_2021.py](../../scripts/run_mayoral_discovery_full_2010_2021.py). The OpenAI extractor is [extract_openai_search_residual_leads.py](../../scripts/extract_openai_search_residual_leads.py). The Archive-It scanner is [discover_archiveit_cross_collection_2010_2021.py](../../scripts/discover_archiveit_cross_collection_2010_2021.py). The combined list is built by [build_mayoral_discovery_candidate_list_2010_2021.py](../../scripts/build_mayoral_discovery_candidate_list_2010_2021.py).

The focused final suite passed 67 tests. Key SHA-256 values are:

- production universe: `843f4556a54fe2deb75f4e84863ff05370d88127ca176ca8cd6296f4daccaae2`;
- Serper request ledger: `9386532cceeaedea3a295a60673f68de3c8842d7d6109c8fb0cb5f7eafdaabaf`;
- OpenAI candidate targets: `c64b6f60b699fec176ad4f4a6bee2023d609545872bd1037e35a83d199040b14`;
- combined candidate URL claims: `d4960040f6a0683e705e7ab2aa6735af630370c4c72536ee34465143197980d9`; and
- combined candidate-domain list: `227c88bca608d0a5cb72dcd7f431ff787b5e4a0a19982b16c37bb77278611d88`.

## Definitions

- **Candidate-year:** one candidate in one mayoral election year.
- **Discovery lead:** a source-supported candidate-owned URL candidate that still requires identity and temporal validation.
- **Strict coverage:** an accepted grade A or B URL under the current evidence rules.
- **Candidate-domain row:** one candidate-year paired with one registrable domain, with all independent source claims retained separately.
