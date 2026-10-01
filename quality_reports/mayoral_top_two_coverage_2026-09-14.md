# Top-two mayoral website coverage

The coverage build works, but the election universe remains incomplete.
It currently includes 25 verified decisive elections, with 50 eligible candidate-election records.
These records do not provide a representative estimate for all 100 cities during 2010–2026.

We compare recorded campaign URLs, attributed period captures, and saved text with the officially reported winner and runner-up in each verified election.

## Fixed definitions

The city list uses April 1, 2020 Census populations and January 1, 2020 geographic definitions.
The period runs from January 1, 2010, through September 14, 2026.
Only completed decisive elections enter the contested-election denominator.
The build imports official result roles; it does not derive candidate roles from first-round vote totals.
Uncontested elections and unresolved final pairs remain separate.

The national list contains 100 cities, with 99 matches to the existing roster.
Santa Clarita enters this list; Hialeah falls outside it.
Louisville, Indianapolis, and Honolulu need explicit geographic crosswalks.
The city file preserves these definitions, Census identifiers, population values, and jurisdiction notes.
The Census inputs are frozen in the [city manifest](../data/municipal_discovery/top100_coverage/inputs/cities_manifest.json).
Sources: [Census incorporated places](https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/tigerWMS_Census2020/MapServer/26) and [Census designated places](https://tigerweb.geo.census.gov/arcgis/rest/services/Census2020/tigerWMS_Census2020/MapServer/28).

Santa Clarita's council selects its mayor. Its 2026 disposition records indirect selection.
Earlier years remain open until historical sources support them. [Official council description](https://santaclarita.gov/city-council/)

## Current result and limits

The verified subset has URL leads for 31 of 50 candidate-election records.
Fifteen of its 25 elections have leads for both candidates.
Four records have attributed period captures; three also have hash-verified saved text.
Unresolved attribution and uninspected inventories do not establish archive absence.
These counts describe only the verified subset; they must not replace the earlier provisional coverage estimate.

The outputs retain 306 unverified legacy contest records and six unresolved official-source review records.
Some source-review records can describe the same election as an unresolved legacy record.
The result queue contains 318 records to reconcile, including six dates identified by calendar sources without verified results.
These records do not establish 318 distinct missing elections.
The calendar table contains all 1,700 city-years; 96 are audited and 1,604 remain open.
A verified election result does not prove that no special election occurred in the same year.
Full 2010–2026 coverage remains unavailable until this calendar work is complete.

The initial official-source files contained 12 verified historical elections and seven verified recent elections.
Separate Boston, New York, Seattle, and Los Angeles inputs now extend those records.
San Diego and Baltimore calendar files add official election dates and no-election dispositions.
San Diego 2013 has an earlier-round-only status because its special primary preceded the February 2014 runoff.
The [official mayoral history](https://www.sandiego.gov/sites/default/files/mayorresults.pdf) records this sequence.
Historical Santa Clarita fiscal-year descriptions remain supporting evidence, not complete calendar-year audits.
New York's 2025 certification and Los Angeles's decisive 2017 election are resolved; the superseded unresolved records remain in a separate output.
Boston 2025 remains unresolved because aggregate write-in votes prevent a confirmed runner-up selection.
The explicit build configuration is `config/mayoral_top_two_build.json`. Complete calendars must enumerate expected election dates. Missing results generate queue records rather than disappearing from coverage checks.
They also preserve unresolved source parses, certification questions, and final-pair questions.
The [historical audit](../data/municipal_discovery/top100_coverage/inputs/historical_audit.json) records source conflicts without changing the original roster.
The [recent source register](../data/municipal_discovery/top100_coverage/inputs/recent_sources.json) records the inspected result and certification sources.

## Evidence rules

- A URL lead is a retained potential campaign website. Social, fundraising, and officeholder-profile types do not count.
- Archive discovery requires campaign attribution and an exact-URL capture within 365 calendar days before or after the election.
- Successful metadata counts even when replay fails.
- Downloaded content additionally requires hash-verified nonempty raw and text files.
- Campaign-content review remains a separate field. Saved text alone does not establish substantive campaign content.
- Exact matching preserves scheme, `www`, path case, and query. The build unwraps archive URLs before comparison.
- Missing archive evidence remains unknown. It is not a confirmed negative.

The saved-evidence exporter reads only listed existing ledgers and files.
It checks decompressed file hashes and uses actual inventory or effective replay timestamps.
Its [manifest](../data/municipal_discovery/top100_coverage/inputs/saved_capture_evidence.manifest.json) identifies every inspected input and its limits.
It sends no requests to archive services.
The final export contains 11,548 evidence rows across 551 URL records and verifies 6,093 file hashes.
The concurrent 22-URL inventory contributes 147 exact matches from 359 index rows.
The other 212 index rows differ in scheme, host, or path and remain preserved upstream.
This exclusion exceeds 20 percent and follows the declared exact-URL rule.

The missing-period-capture queue distinguishes existing metadata needing attribution review from inventory gaps. Neither category automatically authorizes another archive request.

## Denominator changes and the earlier benchmark

The approved date scope selects 2,788 of the original 4,817 candidate-year records.
The national city crosswalk then selects 1,245 records across 322 existing contests.
Both reductions exceed 20 percent. They follow the approved scope, and all upstream records remain intact.

The earlier conversation reported 371 URL leads among 652 candidate records and both URLs for 136 of 326 recorded contests.
That calculation used the largest cities within the existing roster and included unresolved election rounds.
The selected row keys were not frozen as an artifact. This build retains those reported counts but does not independently reproduce them.
The current denominator uses official result roles and remains incomplete. Its difference from the earlier denominator is not a change in discovery yield.

## Outputs and reproduction

The authoritative [summary](../data/municipal_discovery/top100_coverage/outputs/summary.json) states the denominator and its incomplete status.
The [candidate table](../data/municipal_discovery/top100_coverage/outputs/candidate_coverage.csv) retains source URLs, official result roles, identity links, and evidence references.
The output directory also contains city and year summaries, all city-year states, and separate missing-result, missing-URL, and missing-capture queues.

| Module | Inputs | Outputs |
|---|---|---|
| City build | Frozen Census responses and legacy roster | National city list and geographic crosswalk |
| Saved-evidence export | Explicit existing inventories, fetch ledgers, and files | Exact-target evidence and file-hash manifest |
| Coverage build | City list, official result records, reviewed URLs, and saved evidence | Election roster, coverage facts, summaries, and queues |

Run the offline coverage build from the repository root:

```sh
PYTHONPATH=. .venv/bin/python scripts/build_mayoral_top_two_coverage.py
```

Use `--output-dir` for a separate rebuild. The default build makes no network requests.
The [output manifest](../data/municipal_discovery/top100_coverage/outputs/manifest.json) freezes all controlling inputs and output hashes.

## Remaining work and costs

Complete official election-calendar verification for every city-year, including special elections.
Resolve the outstanding legacy results and extend recent results beyond the initial recovered cases.
This task owns election calendars, result verification, coverage, and the missing-website queues. The separate Mayoral website discovery and archive collection task owns attribution reviews and collection. Import its reviewed outputs without repeating its work.
The full election-roster and coverage goal remains active.
Do not infer no election or no website from missing records.

The [source estimates](../data/municipal_discovery/top100_coverage/outputs/source_attempt_estimates.csv) separate official indexes, result documents, public URL sources, and archive metadata.
They use explicit planning assumptions for requests and analyst time, not measured yield or a full acquisition quote.
Public-source API fees are zero in this proposal. Analyst cost requires an approved hourly rate.
New elections will expand these estimates. No paid search, purchase, outreach, or new collection batch followed from this build.

## Verification

Focused tests cover identifier uniqueness, source-role imports, date boundaries, special elections, unresolved certification, missing values, and duplicate evidence.
They also cover exact URL matching, malformed URLs, archive metadata without replay, and raw/text evidence requirements.
All 74 focused tests passed, including explicit supersession, missing calendar results, and decisive elections without a runoff. Calendar checks reject omitted verified dates and preserve earlier-round-only years. The previous four-script compilation check passed.
A clean temporary rebuild matched every output hash.
The build validates unique candidate-election keys; its current table has 54 rows, including four ineligible records. All 1,700 city-year keys are unique.
The original roster and reviewed URL table retained their starting hashes.
The full official election audit and independent reproduction of the earlier benchmark remain unfinished.
