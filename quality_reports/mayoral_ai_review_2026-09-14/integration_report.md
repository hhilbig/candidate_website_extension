# Integrated mayoral AI review and saved text

The main website table now identifies 41 AI-accepted URLs across 40 candidate-years.
All 41 have a saved, hash-verified campaign page with extractable static text.
No new archive download is needed to obtain at least one usable page for each accepted URL.
Matt Hall and Greg Jamison retain their unsure review decisions.

The integration compares all 1,360 corrected URL rows with the 43 completed AI reviews.
Every reviewed URL already existed as a discovery lead.
The output retains all 1,360 rows and every original field, including source claims and human approval values.
It adds model, decision, reason, evidence location, hash, and capture date fields.
All accepted rows have complete keys and evidence references.
No sample rows were dropped or duplicated.

## Current files

The current main table is [candidate_urls.csv](../../data/municipal_discovery/exhaustive_source_expansion_20260908/ai_review_20260914/candidate_urls.csv).
The [integration summary](../../data/municipal_discovery/exhaustive_source_expansion_20260908/ai_review_20260914/integration_summary.json) records input and output hashes.
The [saved-text audit](saved_text_audit.json) records each accepted URL's evidence and text availability.
The frozen corrected input remains available for reproduction.

The integration script is [integrate_mayoral_ai_review.py](../../scripts/integrate_mayoral_ai_review.py).
The audit script is [check_mayoral_ai_saved_text.py](../../scripts/check_mayoral_ai_saved_text.py).
After a source rebuild, reapply the review integration to the new main table.
The script rejects missing keys, mismatched candidates, and conflicting evidence.

## Text audit and limits

The audit read 41 saved files on the droplet and verified every decompressed SHA-256 hash.
The files occupy 790,876 compressed bytes and contain 3,839,101 decompressed bytes.
Static text extraction excludes scripts, style rules, metadata, embedded objects, and explicitly hidden elements.
Campaign relevance comes from the AI review of those same files.
No archived scripts were executed.

`saved_text_available=True` means the verified raw HTML can regenerate nonempty text.
The audit did not check separately persisted text files; that field remains explicitly unknown.
The audit covers one reviewed page per accepted URL, not the complete historical capture inventory.
It therefore establishes no missing-page downloads for this subset, but cannot establish full snapshot completeness.
The task made no archive requests and copied no raw HTML files to the Mac.

The integration, audit, and related regression checks passed 41 tests.
Both scripts compiled successfully.
The integrated table and compact text audit add 1,188,849 bytes locally, plus small code and documentation files.
