# Plan — Release presentation audit

**Date:** 2026-08-17
**Author:** Codex session
**Status:** done

## Context

The collection and release files are complete, but the project has not yet
been deposited. This audit asks whether a new user can understand, install,
inspect, reproduce, and cite the release without information from prior work
sessions. It also checks whether the files expose stale claims, private paths,
secrets, temporary artifacts, inconsistent counts, or avoidable presentation
problems.

## Recommended approach

Audit the repository without changing release content. Treat the current Git
checkout and the files in `data/deliverable/` as authoritative. Check six
parts:

1. The repository landing page, file structure, licence, citation, and setup.
2. The Dataverse README, codebook, rights statement, and announcement drafts.
3. The schemas, row counts, identifiers, missing values, encodings, file sizes,
   and cross-file consistency of every deliverable.
4. The release scripts, dependency specification, documented commands, and
   reproducibility from a clean environment where practical.
5. The figures and reports for visible layout defects, stale numbers, and weak
   labels or notes.
6. The tracked-file boundary for secrets, caches, machine-specific paths, large
   accidental files, and material that should not ship.

Report findings by release severity: blocker, important, and polish. Give an
exact file and line or a reproducible data check for each finding. Do not edit
files during this audit. After review, propose a separate minimal repair plan.

## Critical files

- `README.md` — repository landing page and setup instructions.
- `SPEC.md` — collection and output specification.
- `docs/RELEASE_README.md` — Dataverse landing document.
- `docs/deliverable_codebook.md` — release data dictionary.
- `docs/release_rights.md` — recorded rights decision and remaining checks.
- `docs/release_announcement.md` — publication text and verified headline counts.
- `data/deliverable/*` — files intended for release.
- `scripts/*`, `src/*`, `requirements.txt`, and `config/config.yaml` — released pipeline and environment.
- `quality_reports/*` — supporting validation reports and figures.

## Reused existing functions

- Existing build and harmonization scripts will supply their own assertions and
  count checks where available.
- Existing tests will be run with their intended test runner if the environment
  supports it. Missing test dependencies will be reported rather than installed.

## Verification

- Confirm that Git status stays unchanged except for this required plan file.
- Recompute headline counts directly from deliverables.
- Compare schemas and keys across release files.
- Search every tracked text file for stale status language, placeholders,
  absolute paths, secrets, and contradictory numbers.
- Run offline tests and documented build checks that do not alter the data.
- Render or inspect publication-facing Markdown and every PDF/PNG figure.
- End with a release verdict and a short, ordered repair list.

## Non-goals

- Do not upload the Dataverse deposit or publish the website.
- Do not contact Rachel Porter or any other person.
- Do not rerun the Wayback collection or add new analyses.
- Do not alter release data, documentation, code, or figures during the audit.
- Do not reconsider the approved rights decision.
