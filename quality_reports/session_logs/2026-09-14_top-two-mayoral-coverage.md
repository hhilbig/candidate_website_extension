# Top-two mayoral coverage implementation

The user approved a fixed 2020 Census city list, completed decisive elections, and the January 2010–September 14, 2026 period.
The implementation creates separate coverage outputs and preserves the existing discovery universe.

Three agents handled city geography, historical results, and recent results.
The recent-results agent also exported existing archive evidence through read-only remote checks.
An independent agent added coverage tests and identified URL and identity-link issues, which the main agent corrected.

The pipeline imports official result roles, rather than deriving candidate roles from vote totals.
Unknown election stages and missing calendars remain explicit.
The complete election universe is not yet available.

The main report is `quality_reports/mayoral_top_two_coverage_2026-09-14.md`.
The controlling output manifest is `data/municipal_discovery/top100_coverage/outputs/manifest.json`.
No original input changed. No paid service, outreach, or archive collection job was started.

Initial verification: 69 focused tests passed, four scripts compiled, and a clean temporary build reproduced every output hash.
The final evidence export includes the concurrent 22-URL metadata batch without making new archive requests.
The initial report covered 19 verified elections. The next integration increased this to 25 and verified 63 city-year calendars.
The complete city-year calendar and remaining official results still require work.

The calendar audit now rejects complete calendars that omit verified election dates, including uncontested elections.
Missing expected results create explicit queue records. Superseded source records remain in a separate output.
All 73 relevant tests pass after these changes.
Separate agents now inspect San Diego, Baltimore, and historical Santa Clarita calendar sources.
San Diego and Baltimore audits subsequently closed 33 more city-years, for 96 total.
Six calendar-identified election dates now have explicit missing-result records.
Santa Clarita fiscal-year sources remain provisional evidence rather than full calendar-year checks.
All 74 tests pass after the earlier-round-only calendar distinction.
This task owns election records and coverage. The separate website task owns attribution review and collection.
