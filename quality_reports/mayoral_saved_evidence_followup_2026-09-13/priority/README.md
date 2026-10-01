# Prior evidence for the 107 priority URLs

Existing local records support further checks before new archive requests. This audit compares 107 current URL rows with prior validation records for the same candidate-year.

Of these rows, 36 match prior requested-URL records with capture metadata. Nineteen also have successful fetch reports and response hashes. One has a prior campaign-support report: Jackie Cherryhomes, Minneapolis, 2013.

These matches concern the requested URL, not necessarily the effective captured URL. Redirects can change the host or path. The output separately records the effective URL after archive-wrapper removal. Effective scheme and `www` variants remain distinct from exact matches.

These counts do not establish current raw-file availability. This audit did not verify saved response bytes. Some captures fall outside the election window. The campaign-support report is a historical proposal, not human approval.

The other 71 rows lack exact-URL capture metadata in the scanned tables. Thirty-one have prior exact-URL validation attempts. Thirty-nine have no exact-URL validation record in these tables. One URL contains user information: `https://info@hill4neworleans.com`. Review that URL before any request.

The next step is to locate and verify the 19 reported responses. Reuse the other capture records before new inventory requests. A local record without capture metadata does not prove archive absence.

The script preserves every matched historical row in `prior_evidence.json`. Repeated attempts and copied results remain separate. Counts summarize current URL rows, not evidence rows. The join requires the exact candidate-year identifier. URL comparison preserves path case and query strings. Scheme and `www` differences remain separate from exact matches.

The initial audit selected local municipal CSV files with capture-level fields. `source_manifest.json` freezes those 26 source tables and the queue. Reruns use only that manifest and reject changed inputs. `summary.json` records the manifest hash. No archive, search, paid-service, or remote-host requests occurred.

Reproduce from the repository root:

```sh
PYTHONPATH=. .venv/bin/python scripts/audit_mayoral_priority_saved_evidence.py
PYTHONPATH=. .venv/bin/python -m pytest tests/test_mayoral_priority_saved_evidence.py -q
```

Three tests passed. They check URL distinctions, changed effective targets, and frozen-input checks. The output contains exactly 107 distinct URL identifiers and 97 candidate-year identifiers. Current discovery and acceptance files remain unchanged.
