# Model-coded validation of the 2026 extraction (first 2026 version)

Date: November 11, 2026.
Coder: Claude Opus, sixteen agents. Each agent coded a part of one batch and did not see the answers of another agent.
Provenance: model-coded. This is not human validation.
Inputs: `validation/release_validation_2026-11-11/`, bundle `build/hive_candidate_extraction/bundle_2026_20261111_v22` (manifest hash `3c2ed612…`).
Numbers: `quality_reports/candidate_extraction_model_coded_validation_2026-11-11.json`.
Procedure: the procedure of September 27, with seed 20260922. The coder brief and tools are in `validation/release_validation_2026-11-11_inputs/`.

## What the check compares

For each route (issues, biography, endorsements), a coder reads 60 extracted records next to the page text and answers one question for each coded field. A coder also reads 20 complete pages and lists each eligible record that the extraction does not hold.

## Verdict

The release rule of September 28 passes for each route: the sample sizes are met and record recall is at least 0.90.
Field accuracy is not better than in September, and it is lower for three fields. The rule publishes these figures and does not gate on them.

| Route | Record recall (rule: 0.90) | Records held | Records missed | Complete pages of 20 |
|---|---|---|---|---|
| Issues | 0.93 | 456 | 36 | 13 |
| Biography | 0.95 | 206 | 10 | 14 |
| Endorsements | 0.93 | 1,424 | 101 | 10 |

2026 has no known-omission list, so that check does not apply (Hanno, November 9).

## Field accuracy, with the September value

| Route | Question | 2026 | September |
|---|---|---|---|
| Issues | Statement present | 0.70 | 0.67 |
| Issues | Claim faithful | 0.90 | 1.00 |
| Issues | Kind correct | 0.75 | 0.88 |
| Issues | Heading correct | 0.87 | 0.98 |
| Issues | Evidence supports statement | 0.88 | 1.00 |
| Biography | Fact present | 0.98 | 1.00 |
| Biography | Value faithful | 0.95 | 0.98 |
| Biography | Field correct | 0.77 | 0.93 |
| Biography | Timing correct | 0.92 | 0.87 |
| Biography | Relationship correct | 1.00 | 1.00 |
| Biography | Evidence supports fact | 0.88 | 0.92 |
| Endorsements | Endorser present | 1.00 | 1.00 |
| Endorsements | Name correct | 0.98 | 0.97 |
| Endorsements | Entity type and category correct | 0.75 | 0.67 |
| Endorsements | Title, affiliation, jurisdiction correct | 0.52 | 0.82 |
| Endorsements | Evidence supports identity | 0.92 | 0.78 |
| Endorsements | Record and context establish endorsement | 0.93 | 0.85 |

Each value has 60 records, so a difference of less than about 0.10 is within sample error.

## Defects that the coders found

- **Issues: text with no position.** 18 of 60 records are link titles, voting instructions, statistics, or statements of an opponent. This is the September defect, at the same rate.
- **Issues: kind.** A future commitment gets `current_action`, and a clear preference gets `unclear`. The `current_action` and `unclear` subtypes have 1 and 0 correct records of 10.
- **Biography: field.** 14 of 60 records have the wrong field. Most are in `public_service_experience`: acts in an elected office, campaign work, and events that are not biography (1 correct record of 10 in that subtype).
- **Endorsements: fields from the adjacent list entry.** The model gives an endorser the title of the entry above or below. This is the September defect. It affects `title`, `affiliation`, `jurisdiction`, and through them `endorser_category`. Version 4.0 does not publish these four fields.
- **Endorsements: recall on long lists.** One page accounts for 68 of the 101 missed records: a list of 72 state legislators of which the extraction holds 4.

## Implication for the release

The released columns of version 4.0 exclude `timing`, `endorser_category`, `title`, `affiliation`, and `jurisdiction`. The 2026 check supports the same exclusions.
Two released columns are weaker in 2026 than in September: `statement_kind` (0.75) and the biography `field` (0.77). The README states both figures. Whether these two columns stay in the 2026 rows is Hanno's decision.

## Limits

One model coded each item, and no second coder checked it. Coders disagree on borderline cases, for example whether a list of program parts is one record or several.
