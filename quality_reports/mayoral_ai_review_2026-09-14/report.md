# GPT-5.6 review of mayoral campaign websites

Two GPT-5.6 Sol agents accepted 41 proposals and marked two unsure.
The review compares 43 automatically cleared proposals with their saved campaign pages.
All 43 decompressed file hashes matched the recorded evidence hashes.
The user authorized these decisions as AI-reviewed research records.
No human approval is recorded.

Matt Hall's 2018 Carlsbad page has campaign metadata but an empty body.
Greg Jamison's 2014 Sioux Falls page has campaign metadata but little visible text.
Both remain unsure. These files do not establish substantive campaign-text acquisition.

The agents checked candidate identity, city, campaign ownership, and election timing.
They read saved primary HTML without script execution.
They did not repeat the full checks of surrounding captures or third-party discovery sources.
The election reference uses day 15 of the recorded month, rather than a verified election day.

## Outputs and provenance

The [validation table](validation.csv) preserves all 120 input rows and every original field.
It adds `ai_reviewed`, `ai_review_model`, `ai_review_date`, `ai_review_decision`, and `ai_review_reason`.
Exactly 43 rows have `ai_reviewed=True`. The other 77 received no review in this pass.
An AI review flag records completion of review; the decision field distinguishes accept, reject, and unsure.
Existing automated eligibility and human approval fields retain their original values.

The [first review](review_a.json) and [second review](review_b.json) preserve evidence excerpts and case-specific findings.
The [summary](summary.json) records input, review, and output hashes.
The input is the September 10 `reuse_resolution/validation.csv` table.
Its SHA-256 is `ff4574d436694bada058d79437ca10b4f31ae32a9115fa4b3b3ae97699fc17ee`.

The consistency check detected six URL-format differences in the initial review records.
The corrected records retain both the exact input URL and the reviewed target.
These include archive wrappers, hostname case, a missing scheme, and Jamison's fragment identifier.
Jamison's fragment limitation remains part of the unsure decision.

The integration and related regression checks passed 103 tests.
The integration script also compiled successfully.
This pass used saved evidence over SSH and session agents, with no separate paid API or new archive downloads.
The accepted decisions can be identified explicitly as AI-reviewed; they are not human-reviewed evidence.
