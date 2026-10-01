# Session log: mayoral validation flags

The user approved the saved-evidence follow-up. The plan is
`quality_reports/plans/2026-09-10_mayoral-validation-flags.md`.

I corrected narrow name, city and campaign phrase rules. I added a guard for
Archive-It rate-limit HTML returned with HTTP 200. The tests also guard against
résumé-only campaign histories and unrelated “4 mayor” text.

The saved-evidence recheck assessed all 36 target keys. Six cleared their checks.
Six have confirmed later unrelated content. Three contain archive service errors.
Nine lack sufficient surrounding content. Eleven have consistent selected saved
pages but incomplete inventory coverage; one has no qualifying saved side page.

All 92 tests pass. All original fields in the 84 untargeted rows remain unchanged.
All 120 keys and source references match. The copied output hash matches the
remote summary. The parser also rechecked 92 main pages without a regression
among the 37 previously cleared proposals.

Human approval remains pending. No raw pages were copied or newly requested.
Current results and next checks are in
`quality_reports/mayoral_validation_flags_2026-09-10.md`.
