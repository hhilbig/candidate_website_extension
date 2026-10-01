# Archive-It adjudication

The Archive-It channel has 33 candidate-domain claims. Hanno completed review on 2026-08-24 and accepted all 33 identity, timing, and URL matches. Fourteen grade-A custom domains count as strict additions; 19 accepted grade-C claims remain non-strict. The app itself prefills no acceptance decision and remains the reproducible coding interface.

## What Hanno codes

Open `mayoral_url_archiveit_adjudication.html` and complete all 33 records. The task compares each Archive-It URL claim with the named candidate, city, mayoral contest, and election period. For each record, code identity match, temporal match, acceptance, URL type, and final evidence grade. Enter a corrected URL when Archive-It metadata identifies the original campaign domain but the claimed URL is an archive-container path.

The task contains all 14 machine-proposed grade-A claims and all 19 non-strict claims. Two of the 19 non-strict claims form a deterministic 10% audit sample, but all 19 remain in the task so the review record is complete.

Only grades A and B count as strict coverage. Grade A requires an election-period capture of the exact URL that identifies the candidate, mayoral campaign, and city. Grade B requires a contemporaneous official source that links the URL plus archived campaign content. A strict custom-domain result must also have `url_type = custom_domain`. Strict exact-URL coverage can additionally include a candidate subdomain or a candidate path on a shared host.

## Save and import

Use **Export results** in the app. Save the completed file as `mayoral_url_archiveit_adjudication_20260824_results.json` in this directory. Then run:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/import_archiveit_mayoral_handcode_results.py \
  quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit/mayoral_url_archiveit_adjudication_20260824_results.json
```

The importer refuses incomplete or mismatched results. It writes
`adjudication.csv`, `adjudicated_claims.csv`, and
`adjudication_summary.json` without modifying the source evidence file.
`adjudicated_claims.csv` is the explicit adapter-evidence input for rebuilding
the unified claim table and rerunning the channel gates.

After import, rebuild the claim table with:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/build_mayoral_evidence_claims.py \
  --adapter-evidence quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit/adjudicated_claims.csv
```

## Assumptions

1. Every input row is a pending candidate-domain claim from the current Archive-It validation output.
2. The machine-proposed grade is context, not an accepted label.
3. The accepted URL is the corrected URL when Hanno enters one; otherwise it is the exact claimed URL.
4. Missing human review is not rejection. The importer stops if any record is incomplete.

## Files

- `task.json`: task specification used to build the app.
- `review_items.csv`: source claims plus deterministic inclusion flags; this is the merge key for imported results.
- `mayoral_url_archiveit_adjudication.html`: self-contained, keyboard-first review app.
- `mayoral_url_archiveit_adjudication_20260824_results.json`: the canonical exported human decisions.
- `adjudication.csv`: one normalized human decision per claim.
- `adjudicated_claims.csv`: adapter-ready reviewed claims used by the unified evidence build.
- `adjudication_summary.json`: strict and accepted-claim counts from the completed review.
