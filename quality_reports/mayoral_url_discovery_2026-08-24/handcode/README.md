# Pilot adjudication

This task checks the ten URL leads from the search pilot and a deterministic 10% sample of negative expansion records. Hanno completed review on 2026-08-24. The pilot contains three strict grade-A URLs, three accepted grade-C leads, four rejected temporally mismatched domains, and no false negative among the three negative audits.

## What Hanno codes

Open `mayoral_url_pilot_adjudication.html`. Complete all 13 records. For each record, code the candidate identity, election timing, URL type, evidence grade, and acceptance decision. The task contains ten positive leads and three negative audits.

Only grades A and B count as strict coverage. A strict custom-domain result must also have `url_type = custom_domain`. A strict exact-URL result can be a custom domain, candidate subdomain, or candidate path on a shared host.

## Save and import

Use **Export results** in the app. Save the file as `mayoral_url_pilot_adjudication_20260824_results.json` in this directory. Then run:

```bash
PYTHONPATH=. .venv-release/bin/python scripts/import_mayoral_url_handcode_results.py \
  quality_reports/mayoral_url_discovery_2026-08-24/handcode/mayoral_url_pilot_adjudication_20260824_results.json
```

The importer refuses incomplete records. It writes `adjudication.json`, an
adapter-ready `adjudicated_claims.csv`, and the raw, post-stratified, and 90%
bootstrap yield estimates in `pilot_metrics.json`. Rebuild the unified claim
table with `adjudicated_claims.csv` as an explicit `--adapter-evidence` input
after coding is complete.

## Assumptions

1. The search pilot contains 120 candidate-years and excludes candidates with an accepted URL at sampling time.
2. The post-stratification uses the six original period-by-profile cells.
3. Missing adjudication is not treated as rejection. The importer stops instead.
4. The negative audit checks extraction errors. It does not estimate the false-negative rate for all search results.

## Files

- `mayoral_url_pilot_adjudication_20260824_results.json`: canonical exported human decisions.
- `adjudication.json`: normalized review records.
- `adjudicated_claims.csv`: adapter-ready reviewed URL claims.
- `pilot_metrics.json`: raw, post-stratified, projected, precision, and bootstrap results.
