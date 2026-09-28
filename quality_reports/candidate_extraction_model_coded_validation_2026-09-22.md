# Model-coded validation of the candidate extraction release

Date: September 22, 2026.
Coder: Claude Opus 5, seven separate agents, one agent for each batch.
Provenance: model-coded. This is not human validation. The release gate still needs human answers.
Inputs: `validation/release_validation_2026-09-22/`, bundle `build/hive_candidate_extraction/bundle_20260920`.
Numbers: `quality_reports/candidate_extraction_model_coded_validation_2026-09-22.json`.

## Verdict

The extraction does not meet the approved criteria on any route.
The known-omission check passes. All 11 omissions are confirmed, 11 of 11.

| Route | Record accuracy (need 0.90) | Lowest subtype (need 0.80) | Complete-page recall (need 0.90) |
|---|---|---|---|
| Issues | 0.35 | 0.00 | 0.45 |
| Biography | 0.73 | 0.50 | 0.40 |
| Endorsements | 0.50 | 0.00 | 0.65 |

Record accuracy needs every one of 5 or 6 dimensions to be correct.
Each single dimension scores between 0.67 and 1.00.
The product of the dimensions is therefore much lower than each dimension.

## Two blockers

### Blocker 1: the coding tasks do not state the coding rules

The tasks give five bullets of instruction. No question carries help text.
No task states the allowed values, the null rules, or the heading rule.
The coder must guess the codebook from the model records.

The `source_heading` rule proves the effect. The contract accepts a heading
only when that exact heading occurs once in the page text. It requires null
otherwise. The task never shows this rule. The coder marked 15 items wrong
because a heading stood above the evidence. I checked 20 of those answers.
In 14 of them the heading occurs 0, 2, 3 or 16 times, so the model's null is
correct and the coder is wrong.

Issue record accuracy rises from 0.35 to 0.52 when I drop that dimension.
The same gap will affect Hanno when he codes these batches by hand.

Smallest correction: put the route contract in the task instructions and put
the rule for each dimension in that question's help text. Change
`scripts/build_candidate_extraction_handcode.py` and rebuild the seven tasks.
The bundle, the sample and the frozen identities do not change.

### Blocker 2: low recall comes from withheld records, not from missed content

The aligner drops a model record when its evidence is not an exact substring
of the page. The counts across the corpus are:

| Route | Resolved records | Withheld items | Share withheld |
|---|---|---|---|
| Issues | 269,798 | 40,895 | 13.2% |
| Biography | 103,853 | 10,021 | 8.8% |
| Endorsements | 153,132 | 15,004 | 8.9% |

On the issues route 38,666 of the 40,895 withheld items give the reason
`missing_exact_span`. I tested 4,000 of them against the page text. 68.9%
differ only in punctuation form, for example a straight apostrophe in place
of a curly one. 10.4% differ only in whitespace. 19.1% do not match under any
of those transformations. 77.7% of all withheld issue items match once in the
page after whitespace and punctuation are folded.

The withholding is deliberate. `src/candidate_extraction.py` states that
version 2.1 never normalizes whitespace and punctuation. The pipeline fails
closed and it works as designed. The model is the part that disobeys: the
prompt tells it to copy an exact span, and it silently replaces typographic
characters.

The recall batches show the cost. The page with 17 coded misses has 17
withheld items. The page with 15 coded misses has 17 withheld items.
Every recall page with no miss has no withheld item.

Three options, for Hanno to choose:

1. Accept the loss. Release the data, document the rule, and set a recall
   criterion that the exact-span rule can meet.
2. Extend the alignment search, not the released evidence. Search for the
   span under a small, named set of typographic variants, still demand one
   match, and store the source slice, never the model string. The released
   evidence stays an exact page substring. This changes
   `alignment_spec_sha256` and needs a new alignment pass over the stored
   model outputs. It needs no new inference.
3. Run inference again. This costs the most and it does not guarantee that
   the model copies characters correctly.

Option 2 is the smallest correction that keeps the verbatim guarantee.
I did not start it. It needs approval.

## What the errors look like, by route

### Issues

Per-dimension: statement present 0.67, claim faithful 0.92, kind correct 0.88,
heading correct 0.67, evidence supports statement 0.90.

Heading errors are mostly coder errors, see Blocker 1. The presence errors
look real. The coder rejected a press-release link title that states no
position, a conditional prediction on a beliefs page, and two records about
the opponent's conduct rather than the candidate's position.

Recall: 9 of 20 pages complete, 82 missed records. Six of the nine complete
pages hold no model record at all. Every long page loses something. The model
drops whole sections, for example a complete `Student Debt` section and a
complete `The Opioid Crisis` section.

### Biography

Per-dimension: fact present 1.00, value faithful 0.97, field correct 0.97,
timing correct 0.90, relationship correct 0.97, evidence supports fact 0.92.
This is the strongest route. Timing is the main error: the model labels a
current role as former, and it labels a birthplace as a current relationship.

Recall: 8 of 20 pages complete, 36 missed facts.

### Endorsements

Per-dimension: endorser present 0.98, name correct 0.97, entity and category
correct 0.70, optional fields correct 0.80, record evidence supports identity
0.70, combined support 0.88.

The category errors are real. The coder found a sitting mayor labeled
`other_individual`, a retired television anchor labeled `unclear`, and a bare
name labeled `media_outlet` with no support on the page.

Recall: 13 of 20 pages complete, 12 missed endorsements. Long name lists are
not the problem. The six largest pages hold up to 126 records and four of
them are complete. Eight of the 12 misses are the first or the last item of a
block. Four are endorsements written as prose, not as a list line.

## Known omissions

11 of 11 confirmed. The gate counts an omission as recovered when any linked
record is confirmed.

One linking defect appears, and it does not change the result. The linker
attaches every record whose evidence sits inside the omitted sentence. One
omission has four linked records. Three of them recover other clauses of the
same sentence. This inflates the record-level failure count only.

## Limits of this measurement

One model coded each batch. No second coder checked the answers.
The coder did not have the codebook, see Blocker 1.
The numbers are an interim estimate. They do not replace human validation.

## Findings added after the first write-up, September 22, 2026

### The heading failures are a task defect, confirmed against the segment

The contract evaluates heading uniqueness in the segment the record came from, not in
the parent page the coder sees. I resolved all 20 `heading_correct` failures against
their own segments. In 13 of them the heading occurs zero times or several times in
every segment, so null is correct and the coder is wrong. Several of the other 7
quotes are note text, not a heading. No non-null heading was wrong in the whole batch.

### A real endorsement defect: the evidence span crosses list entries

In 17 of 60 endorsement records the `record_evidence` span joins one entry to its
neighbour. It takes the neighbour's descriptor line or quote and adds this endorser's
name. Three page layouts appear, name-then-descriptor, name-then-quote and
quote-then-name, and the extraction picks the wrong direction on all three. This one
defect produces 17 of the 18 record-evidence failures and 10 of the 12 optional-field
failures, so it explains most of the endorsement score.

Example: a Spanberger 2024 record keeps the honorific in the name, and takes the title,
the affiliation and the quote from the entry above it, so no endorsement language of
its own endorser survives.

### 542 pages are official government sites

2.27 percent of the parent pages sit on a `.gov` host. They cover 26 candidate-years
and produce 7,637 records. Four of the 18 hosts also carry the wrong candidate, for
example `billnelson.senate.gov` filed under a Nebraska candidate named Nelson. A
corpus-wide check found no other linkage defect: 95.5 percent of pages carry a part of
the candidate's name in the host, and only 5 of 2,669 hosts serve two states, of which
3 are the same person who really ran twice.

### Version 2.2 alignment, measured

The typographic fallback recovers 69.7 percent of withheld issue records on 120 real
segments, 2,061 aligned rising to 2,364, with the total still reconciling at 2,496. On
150 segments across issues and biography, 4,324 stored anchors include 361 resolved
through the fallback and zero that differ from their page slice.
