# Saved files behind earlier mayoral fetch reports

Only one of 19 previously fetched URLs has matching persisted bytes in the inspected stores.
That match is a 2019 Bill Saffo capture associated with a 2011 candidate-year lead.
It verifies the reported response bytes but does not provide 2011 campaign evidence.
The other 18 URLs have no matching bytes in the inspected stores.

The audit compares 44 historical evidence records for 19 exact URL keys with saved files and response ledgers.
It searches five remote acquisition ledgers, the saved ownership-validation response store, and local HTML files.
The audit inspected 497 local HTML files and hashed 929 remote response files after ledger selection and cache reuse.
It compares raw-byte hashes and three explicit decoded-text hash conventions used by earlier validators.

Two corpus copies match Saffo's reported hash. Both identify the October 18, 2019 capture.
The audit preserves that date and the corpus candidate-year rather than transferring the match into 2011 coverage.
No candidate website or human-approval fields changed.

The [audit table](audit.csv) preserves all 19 URL keys and the matching file provenance.
The [summary](summary.json) lists every inspected store, input hashes, and limitations.
The generating script is [audit_mayoral_priority_files.py](../../scripts/audit_mayoral_priority_files.py).

The negative result is limited to the inspected stores. It does not establish that Wayback lacks captures or that no copy exists elsewhere.
Earlier validator code retained response bodies in memory, so an old fetched status and hash alone cannot establish durable acquisition.
The next archive proposal must retain these historical reports while seeking evidence for the correct election period.
This audit made no archive requests and copied no raw files to the Mac.

The priority-file, acquisition-proposal, and related audit checks passed 16 tests.
Both new scripts compiled successfully. The coordinator verified the saved script hash and all 19 output keys.
