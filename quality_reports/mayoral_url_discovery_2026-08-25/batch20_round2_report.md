# Archive-It second 20-collection batch: final report

## Current verdict

The second batch triggers the prespecified rule to stop Archive-It acquisition. It adds one strict candidate-year from 20 collections. Coverage rises from 144 to 145 of 4,817 candidate-years, or from 2.99 to 3.01 percent. The project should now prioritize official municipal records.

## Why this batch was run

The first 20-collection batch added six strict candidate-years and met the rule for one final small test. This second batch asks whether unused Archive-It collections continue to recover election-period mayoral campaign sites after the strongest catalog entries have been processed.

The comparison is between the 144 candidate-years covered before this batch and candidates in one uncovered contest targeted by each selected collection. A candidate-year is one named candidate in one election year. A strict URL requires a human-approved grade-A or grade-B custom domain.

## Inputs and method

The frozen [batch configuration](../../config/archiveit_mayoral_batch20_round2.json) names 20 unused public collections and 20 contest targets containing 66 collection-candidate rows. The [target audit](batch20_round2_target_audit.json) confirms that every target is in the discovery scope and that none of the collection IDs appeared in an earlier batch.

Only collection `7977`, *Mayoral Campaign 2016*, is an explicit mayoral campaign archive. The other 19 are the best remaining local-politics, local-news, official-government, or civic collections associated with an uncovered city-year. They test the remaining catalog rather than constituting 19 equally strong election sources.

The run froze one public Partner API seed payload per collection, matched seed URLs to candidates in the configured contest, and checked matched sites in election-period Archive-It captures. It made no Serper request and incurred no paid API or license cost.

## Result

The remaining Archive-It catalog has almost no marginal yield for this project. One of 20 collections produced a candidate URL. The other 19 produced no candidate match. The single URL was `http://www.sharonformayor.com/` for Sharon Weston Broome in the 2016 Baton Rouge mayor-president election.

The first validator pass incorrectly assigned grade C because its mayoral-content detector recognized “for mayor” but not “Mayor-President.” The archived page title says “Sharon Weston Broome - Mayor President 2016,” its metadata calls the site “Sharon for Mayor-President,” and the page names Baton Rouge. The validator now recognizes spaced and hyphenated mayor-president titles, and a regression test covers this case. The corrected validator assigns grade A.

Hanno reviewed the one frozen case in chat and approved the identity match, temporal match, custom-domain classification, and grade A. The [canonical audit result](batch20_round2_audit/archiveit_mayoral_batch20_round2_audit_20260825_chat_results.json) records that decision. The batch therefore adds one strict candidate-year.

## Decision rule and implication

The rule fixed before this batch was:

1. Continue Archive-It if the batch adds at least ten strict candidate-years with at least 95 percent audited precision.
2. Run one additional small batch if it adds four through nine strict candidate-years with at least 95 percent audited precision.
3. Stop or deprioritize Archive-It if it adds no more than three strict candidate-years, falls below 90 percent precision, or exceeds the cost ceiling.

The one reviewed proposal was accepted, so observed precision is 100 percent. Yield, not precision, fails the gate: one addition is below the stop threshold of three. The final decision is `drop_archiveit_no_more_than_three_additions`.

Archive-It remains useful when a known election-specific collection appears, but broad catalog promotion should stop. The next active source channel is official municipal records, beginning with the 20 early-period cities in the existing priority table.

## Glossary

- **Candidate-year:** one named candidate in one election year.
- **Strict URL:** a human-approved grade-A or grade-B URL with an eligible campaign-site type.
- **Partner API:** Archive-It's public endpoint for collection and seed metadata.
- **Marginal addition:** a candidate-year that was not covered before the batch.
