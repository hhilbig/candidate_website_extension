# Model-coded validation of the version 2.2 extraction release

Date: September 28, 2026.
Coder: Claude Opus 5, seven agents, one for each batch.
Provenance: model-coded. This is not human validation. The release gate still needs human answers.
Inputs: `validation/release_validation_2026-09-27/`, bundle `build/hive_candidate_extraction/bundle_20260924_v22`.
Numbers: `quality_reports/candidate_extraction_model_coded_validation_2026-09-27.json`.
Previous pass: `candidate_extraction_model_coded_validation_2026-09-22.md`, on the version 2.1 bundle.

## Verdict

The extraction still does not meet the approved criteria on any route.
The known-omission check passes: 11 of 11.

| Route | Record accuracy, need 0.90 | Lowest subtype, need 0.80 | Complete-page recall, need 0.90 |
|---|---|---|---|
| Issues | 0.58 | 0.00 | 0.50 |
| Biography | 0.73 | 0.70 | 0.45 |
| Endorsements | 0.48 | 0.00 | 0.60 |

The alignment repair recovered 33,061 records and it did improve recall at the record
level. It did not fix any precision defect, because the defects are about which text the
model selects, not about how the aligner anchors it.

## Read the two passes carefully: only part is comparable

The seed is unchanged. The sample moved anyway, because 33,061 previously withheld records
became eligible.

| Task | Items | Same sample | Shared |
|---|---|---|---|
| precision issues | 60 | no | 22 |
| precision biography | 60 | no | 52 |
| precision endorsements | 60 | yes | 60 |
| recall issues | 20 | yes | 20 |
| recall biography | 20 | yes | 20 |
| recall endorsements | 20 | yes | 20 |
| known omissions | 15 | no | 13 |

Endorsement precision and all three recall tasks are directly comparable.
Issue precision is not: two thirds of its records differ.

## What the alignment repair bought

Missed records on identical samples:

| Route | September 22 | September 28 | Change |
|---|---|---|---|
| Issues | 82 | 52 | down 37 percent |
| Biography | 36 | 18 | down 50 percent |
| Endorsements | 12 | 13 | no change |

Endorsements did not move, which is correct. The typographic fold recovered records whose
evidence was written with the wrong quotation mark. It could not recover an entry the model
never read.

**The complete-page metric hides this.** A page with one miss counts as incomplete, so
halving the misses on a page changes nothing. Complete-page recall moved from 0.45 to 0.50
on issues and 0.40 to 0.45 on biography. The approved criterion is therefore close to
insensitive to the improvement that actually happened. Report the record-level count as
well.

## What the coding-rule repair bought

Issue precision, failures per question:

| Question | September 22 | September 28 |
|---|---|---|
| Heading correct | 20 | 1 |
| Claim faithful | 5 | 0 |
| Evidence supports statement | 6 | 0 |
| Statement present | 20 | 20 |
| Kind correct | 7 | 7 |

The heading collapse is the repair working. The coder confirmed all 27 null headings on the
window rule, which is the error the September 22 pass made on 20 items.

**The two real defects are unchanged.** That is the honest reading. The pipeline is no
better at what it actually gets wrong.

## The defects that remain

### Issues: the model selects text that states no position

Three shapes produce nearly every failure.

- Press-release link titles and bare issue names. Every `unclear` record in the batch is
  one of these, so that subtype scores 0 of 9.
- Text about someone else. Opponent-attack passages and third-party endorsement quotes,
  9 records, treated as the candidate's own position.
- Future commitments labelled as conduct. "Kay will work tirelessly" and "I am committed
  to" coded `current_action` or `past_action`, which the contract forbids.

Claim faithfulness and evidence support never failed. When a statement exists, the claim and
the span match it.

### Biography: timing, tight spans, and a missing category

- Timing, 8 of 16 failures, wrong in both directions. An ongoing role pushed to `former`,
  and a birthplace pushed to `current`. The model keys on past-tense narration instead of
  the state at the capture date.
- Evidence spans cut too tightly, 5 of 16. The span drops the words that carry the
  relation, leaving a bare "Columbia College" or "Combat Infantry Badge".
- Field errors, 4 of 16, all one kind. The contract allows six fields and no residual
  category, so a fact that fits none is pushed into the nearest one. Party activism became
  `public_service_experience`; a high-school ski team became `occupation`.
  **This one is a contract question for Hanno, not a model defect.**

### Endorsements: one span defect explains most of the score

Two independent coders, working from the page layout rather than from each other, both found
the crossed-entry span defect in **exactly 17 of 60 records**. The span joins one list entry
to its neighbour: it takes the neighbour's descriptor line or quote and then adds this
endorser's name. Three page layouts appear, name-then-descriptor, name-then-quote, and
quote-then-name, and the extraction picks the wrong direction on all three. When the span
crosses, the title, affiliation and jurisdiction usually come from the wrong person too.

Category errors are the other half, 20 records: sitting officeholders labelled
`other_individual`, and `unclear` used where the page states the basis, including a record
whose own name is "International Union of Painters and Allied Trades, AFL-CIO, CLC".

Also found: the model captures ratings and awards as endorsements, which the eligibility
rule excludes. Examples are "85% FreedomWorks", "National Rifle Association A+ Grade" and
"Moms Demand Action Gun Sense Candidate Distinction".

## Two defects outside the extraction

- **Wrong-candidate pages.** Item 2 of the issue batch shows Anthony Brown's Maryland site
  filed under an Alabama candidate. Corpus-wide this affects two hosts, `anthonybrown.com`
  and `kateforcongress.com`, each serving candidates in two states. The official-government
  exclusion does not touch them because they are ordinary campaign domains.
- **An any-link omission gate.** Omission `9463bc0ef49b` counts as recovered while half the
  fact is missing: the page states support for healthcare workers and growth in the
  healthcare industry, and the linked record carries only the industry half.

## One open coding question

The endorsement coder read "title, affiliation and jurisdiction must never be combined into
a generic organization field" as a ban on collapsing them, not as a requirement that
`affiliation` name an organization. It therefore passed records putting a page-stated role
phrase in `affiliation`, such as "House Republican Whip". Under the stricter reading about
seven more records fail. Hanno should say which reading is intended.

## Limits of this measurement

One model coded each batch and no second coder checked it.
The numbers are an interim estimate and they do not replace human validation.
