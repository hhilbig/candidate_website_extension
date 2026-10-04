# Session log: AI-coded review of the independent40 endorsement task

Date: 2026-09-14. Agent: Claude Code (claude-fable-5).

## Authorization and scope

Hanno authorized AI coding of the round-four independent40 review step in
this session. His words: "code it. AI coding is fine at this step." This
session coded the full review task. It did not create human truth. The
authorization did not cover full-corpus work, promotion, or release.

## What the session did

The agent read all 40 source pages and all 2,582 AI seed rows from
`build/private_candidate_endorsement_independent40_human_review_20260914a/task.json`.
The agent made every parent decision, every child decision, and every
unresolved-block decision. A script wrote the decisions into the export
schema. The pipeline validator `validate_completed_results` accepted the
export. The review app accepted the import and authenticated it to the
task bytes. The app now shows 40/40 parents, 2489/2489 children, and
68/68 blocks complete.

## Results

- 40 parent decisions: 32 contain children, 8 true zeros.
- 2,489 child rows confirmed. 93 AI seed rows deleted.
- All 68 unresolved blocks reviewed. No block produced a new child row.

## Decision rules

1. An unqualified entry under an endorsement heading or on an
   endorsements page counts as an endorsement.
2. An entry that the source itself qualifies as a different relation does
   not count. Examples: ratings and grades (Emmer), a candidates-to-watch
   line (Tipirneni), a Gun Sense Candidate Distinction (Moskowitz).
3. Gratitude for the candidate's help and issue-only agreement do not
   count (Harris testimonials, Ferrara police association). An explicit
   statement of support counts (Lea D., labor "strong public support").
4. One row exists per endorser display occurrence. Repeats across page
   sections stay separate rows (Emmer 2020/2022, Marino, Bacon 2020
   lists). Repeats inside one display entry collapse to one row
   (headings, abbreviations, signatures, affiliation lines; Giles and
   Bacon carry most of these deletions).
5. A person who signs or speaks for an organization is not a separate
   personal endorser (Chris W. Cox, Reams, Routhe, Vinton, Brooks,
   Rajunov, Rawlley, Lesa, the seven JBS committee signatories).
6. Date and district mismatches on recycled pages are noted in the
   parent notes. No row was excluded for date, which follows the
   source-issue adjudications.

## Artifacts

- Export: `build/private_candidate_endorsement_independent40_ai_coded_review_20260914a/candidate_endorsement_independent40_human_review_20260914a_results.json`
- Provenance sidecar: `AI_CODED_PROVENANCE.json` in the same directory.
- Every parent `notes` field starts with an AI-provenance sentence and
  gives the parent-specific rationale.

## Verification

- `validate_completed_results` passed on the exact written bytes.
- The app import passed schema, identity, and completeness checks and
  reported "Complete results imported and authenticated to this task."
- One import attempt failed first: the app compares `authorizations`
  with order-sensitive `JSON.stringify`. The fix writes the keys in
  alphabetical order.

## Caveats and next steps

- The export schema forces `human_review_complete: true`. That flag is a
  schema requirement, not a provenance claim. This export is AI-coded.
  Do not report human-validated accuracy from it, and do not promote it
  to human truth without a real human pass.
- The 221 formerly uncertain rows and 68 blocks got explicit decisions
  under the rules above. A human spot-check of the judgment-heavy
  parents (Giles 30, Bacon 11, Harris 35, Ferrara 19, Marino 3) is the
  highest-value audit if Hanno wants one.
