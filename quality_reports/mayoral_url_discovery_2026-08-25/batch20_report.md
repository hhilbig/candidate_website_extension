# Archive-It 20-collection batch: final report

## Current verdict

The batch passes the rule for one more small Archive-It batch. It adds six strict candidate-years under the prespecified sampled-audit rule, and the three sampled positive claims had 100 percent observed precision and a 100 percent strict-confirmation rate. Coverage rises from 138 to 144 of 4,817 candidate-years, or from 2.86 to 2.99 percent.

## Why this batch was run

The public Archive-It catalog contains hundreds of possible local-election collections, but catalog metadata is noisy. This batch tests whether a prioritized set of 20 collections can recover new election-period mayoral campaign sites without search credits or paid data.

The comparison is between the 138 candidate-years covered before this batch and uncovered candidates in the city-year targeted by each selected collection. A candidate-year is a named candidate in one election year. A strict proposal requires an election-period capture that identifies the candidate, mayoral campaign, and city.

## Inputs and method

The frozen [batch configuration](../../config/archiveit_mayoral_batch20.json) names exactly 20 public collections and 56 target candidate rows. Candidate rows can appear under more than one collection, so 56 is not a unique-candidate denominator. The run downloaded and froze one public Partner API seed payload per collection. It used no Serper requests and incurred no paid API or license cost.

The adapter matched collection seeds to candidates in the specified city-year. The temporal validator then checked the collection captures against the 365-day headline window, the wider sensitivity window, and the two-year domain-reuse window. The [state manifest](batch20_state_manifest.json) records byte counts and SHA-256 hashes for the configuration, 20 raw payloads, extracted evidence, validation, audit records, and final evaluation.

## Discovery result

The batch found some genuine signal, but most selected collections produced no candidate match. Seven of 20 collections produced 13 claims for 12 candidate-years. Nine candidate-years were uncovered before the batch. Five collections produced the six new machine-validated strict proposals.

The six proposed additions are Jolie Justus (Kansas City, 2019), Dustin Canter (Washington, 2018), Muriel Bowser (Washington, 2018), Kelly Girtz (Athens-Clarke, 2018), Andy Schor (Lansing, 2021), and Sharon Weston Broome (Baton Rouge, 2020). Jamie Giellis also passed validation but was already covered before the batch.

Six other claims remain non-strict. The main unresolved cases are Lorena Gonzalez and Kathie Dunbar, whose captured pages did not confirm a mayoral campaign, and Steve Carter, whose collection had no qualifying capture. These remain leads rather than additions.

The discovery yield is six new strict candidate-years from 20 collections under the sampled-audit rule. This is not a candidate-level rate because the collections target overlapping and differently sized candidate sets. The [collection evaluation](batch20_evaluation/collection_evaluation.csv) therefore reports each collection separately.

## Human audit and decision rule

The audit contained six cases: three randomly sampled strict proposals, both ambiguous campaign-content cases, and one sampled non-strict case. Hanno reviewed the same frozen cases in chat and explicitly approved the proposed coding. The canonical [chat audit result](batch20_audit/archiveit_mayoral_batch20_audit_20260825_chat_results.json) preserves that approval and the claim-level answers.

The final batch decision uses the rule agreed before review:

1. Continue the Archive-It channel if the batch adds at least ten new strict candidate-years and audited precision is at least 95 percent.
2. Stop or deprioritize it if the batch adds no more than three strict candidate-years or audited precision falls below 90 percent.
3. Run one further small batch if it adds four through nine strict candidate-years and audited precision is at least 95 percent.

All three sampled strict proposals were accepted as grade-A custom domains. Kathie Dunbar and Steve Carter were accepted as grade-C source-supported URLs and do not count as strict coverage. Lorena Gonzalez's city-council officeholder site was rejected. The sampled non-strict case produced no false negative.

The three directly reviewed strict claims and the three unsampled grade-A claims accepted by the prespecified 95-percent audit rule are separated in the [claim disposition file](batch20_evaluation/batch_claim_dispositions.csv). This prevents the inferred cases from being mislabeled as individually human-coded. The final decision is `run_one_more_small_archiveit_batch`.

The directly reviewed strict additions are Jolie Justus, Muriel Bowser, and Andy Schor. The rule-inferred additions are Dustin Canter, Kelly Girtz, and Sharon Weston Broome.

## Implication and caveat

Archive-It remains worth one additional small, targeted batch, but not full acquisition. The audit examined only three proposed strict positives, so 100 percent is the observed sample precision rather than a precise population estimate. The next batch should again target explicit election collections or narrowly scoped local-politics archives and should use the same temporal validator and audit rule.

## Glossary

- **Claim:** one independent source attribution of one exact URL to a candidate-year.
- **Sampled-audit rule:** when at least 95 percent of sampled proposed positives are accepted and remain strict, the same machine-validated rule is applied to unsampled proposals in the frozen batch.
- **Marginal addition:** a candidate-year that was not covered before this batch.
- **Partner API:** Archive-It's public endpoint for collection and seed metadata.
