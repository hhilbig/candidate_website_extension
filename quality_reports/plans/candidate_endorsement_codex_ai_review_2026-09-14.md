# Plan: Codex review of the endorsement sample

Date: 2026-09-14
Status: Approved by the user request for strong Codex agents.

Codex agents will review the 40 source pages in the current endorsement sample. We compare an independent source-only extraction with a review of the existing AI draft. The parent sources in `build/private_candidate_endorsement_recall40_round4_human_review_20260907b/task.json` control this review. Its SHA-256 is `c92cb24a4c692f839f009b40f4be898e729db669da4c2e11e25fdec53fd195ea`.

One agent will extract endorsers without access to the draft. A second agent will check every draft record against the complete source. The primary agent will resolve disagreements against the source. All reviewers must retain exact name and support spans. They must record uncertain cases.

The output will state `review_status=ai_reviewed` and `human_reviewed=false`. It will retain the source hashes, reviewer identities, and the disagreement decisions. Comparisons with this reference measure agreement with AI labels. They do not establish human-validated accuracy.

The reviewers use the complete displayed text. They exclude offices, honorifics, ranks, credentials, and affiliations from name spans. A program title does not create another personal endorsement. They flag ambiguous heading scope, constituent testimonials, award designations, and domain-only organization links. These cases remain provisional until a human resolves them.

The user selected commercial Codex agents for this review. The original Hive output remains separate. The review uses the existing Codex allowance. It does not use a separately billed API or buy additional credits.

Verification will check all 40 parent identities, source hashes, exact spans, record duplication, and review coverage. Tests will cover invalid spans, changed sources, and incomplete review files. The output will preserve unresolved judgments for later human review.

## Final decisions

The authoritative derived result is `build/private_candidate_endorsement_codex_ai_review_20260914a/ai_reviewed_results_v2.json`. The earlier result and adjudication files remain available. The `adjudicated_v2` files control the final decisions.

Disjoint references within one organization profile remain separate proposed occurrences when the source prints the complete identity. These repeated references remain uncertain. Nested aliases do not add another occurrence. Parent 21 retains the later standalone PACE reference.

Parent 7 excludes the separate list of 18 unions under STRONG PUBLIC SUPPORT. Both agents distinguished public support from explicit endorsements. The output records this exclusion as uncertain for later human judgment.

Parent 35 accepts the explicit HPR Ammunition endorsement and the Arizona Republic Vote Giles statement. The newspaper support includes its recommendation. Uncertainty about ATAC, Lee Ploszaj, and the individuals roster does not cover these explicit organization statements.

Parents with only uncertain records receive `parent_disposition=unclear`. The result contains 1,960 proposed records, including 867 uncertain records. No source pages were removed.
