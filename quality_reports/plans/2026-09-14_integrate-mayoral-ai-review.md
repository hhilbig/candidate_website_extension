# Integrate AI-reviewed mayoral websites

The user approved integration of 41 AI-accepted websites and an audit of their saved text.
The unit is a candidate URL, matched by its existing stable key and candidate-year key.
All 41 websites already exist in the corrected main table as discovery leads.
Add the 43 completed review decisions, including the two unsure cases, without duplicate URLs.
Preserve all original fields and save the integrated main table as a new version.

Input: `ownership_repair_20260910/combined/candidate_urls.csv` under the source-expansion data directory.
Output: `ai_review_20260914/candidate_urls.csv` under the same source-expansion directory.
The review files supply model identity, decisions, evidence paths, capture dates, and file hashes.
Check saved campaign text on the droplet and report missing or unusable files separately.
This audit checks at least one usable campaign page per accepted URL, not completeness of every historical capture.

Test exact joins, duplicate rejection, preservation of original fields, and evidence hashes.
Inspect generated tables and record row counts and disk use.
No new archive downloads are authorized by this step.
