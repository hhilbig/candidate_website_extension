# Top-two mayoral coverage, 2010–2026

Status: approved for implementation on September 14, 2026.

Measure campaign website coverage for officially reported winners and runners-up in the 100 largest cities by 2020 Census population.
Cover completed decisive elections from January 1, 2010, through September 14, 2026.

## Implementation

- Verify the national city list and mayoral jurisdiction crosswalk against official sources.
- Audit every city-year, including special elections and missing election records.
- Retain unresolved stages, certification, ties, and indirect selection as explicit states.
- Report uncontested elections separately.
- Maintain separate candidate-election identifiers and links to existing candidate-year records.
- Report URL leads, attributed period captures, and saved content separately.
- A recorded period capture counts for discovery even when replay fails.
- Reconcile existing evidence before preparing missing-result, missing-URL, and missing-capture queues.
- Prepare attempt and cost estimates. Launch no paid searches, collection jobs, or outreach.

## Deliverables

Create a reproducible script, city list, election roster, candidate coverage table, queues, and report.
Use independent agents for city definitions, historical results, and recent official results.
Record controlling inputs and definitions in `docs/mayoral_url_discovery.md`.

## Verification

Test unique identifiers, decisive-round selection, special and uncontested elections, unresolved records, duplicate evidence, and mixed-case booleans.
Freeze input hashes. Reconcile the previous provisional calculation and disclose all denominator changes.
Do not describe the election universe as complete while any city-year audit remains unresolved.
