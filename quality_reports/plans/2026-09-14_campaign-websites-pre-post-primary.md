# Plan — 2026 campaign websites before and after primaries

**Date:** 2026-09-14
**Status:** approved in the current task

## Context

Compare each 2026 House and Senate candidate's campaign website before and after each primary or runoff. The population includes all same-year FEC Democratic and Republican candidates. Missing websites and unknown ballot participation remain explicit.

## Approved approach

Combine archived and live HTML and text. Use the nearest usable observations within 30 days on each side of each election. Exclude election day and retain actual timestamps and source. Keep separate comparisons for each round, with special and nonpartisan elections identified. Preserve archive inventories and all acquired observations.

Copy live sites immediately, monthly thereafter (every 30 days), plus one targeted copy on the day before and one on the day after each applicable primary or runoff (final user-approved decision, September 15). Follow main campaign content through two internal link levels. Stop 14 days after the general election, extending affected races through later runoffs. Refresh the roster and URLs weekly.

Use the existing droplet and attached volume. Run a 40-candidate pilot, balanced across office and party, before full deployment. Estimate requests, duration, and storage from observed pilot data. Stop for a storage decision if capacity is inadequate. Use no paid APIs or new cloud resources without separate cost approval.

## Verification

Test date boundaries, calendar exceptions, candidate identity, source mixing, retries, and resume behavior. Inspect saved HTML and text. Reconcile queue items to explicit outcomes. Report coverage with the entire roster as denominator. Preserve original release files.

## Records

Implementation and observed results are recorded in the corresponding session log. The separate collection configuration, calendar, roster versions, acquisition ledger, and outputs preserve construction decisions.
