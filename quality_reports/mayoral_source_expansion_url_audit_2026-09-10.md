# Audit of new mayoral URL leads

**Date:** 2026-09-10
**Verdict:** Do not treat the 225 proposed URLs or the reported 87 newly covered candidate-years as validated campaign-website coverage. The alternative-URL rows are strong, but the other two change types contain many news and reference pages.

## Why this audit was run

The source expansion produced 225 new exact URL rows. All remain grade-D discovery leads, but the combined roster counts 87 candidate-years as newly represented by those leads. This audit tests whether the proposed exact URLs are candidate campaign websites rather than pages that merely discuss the candidate.

## What is compared

For each sampled candidate-year and exact URL pair, the audit compares the candidate, mayoral contest, city, and election year with the target URL and the saved source text that linked to it. An accepted row requires evidence that the exact target is the candidate's campaign website. A news article about the candidate does not qualify.

## Design

The audit first linked all 225 proposed URLs to their archived source claims. The linkage recovered all 267 expected claim identifiers exactly once. It then ranked candidate URL identifiers by SHA-256 within each of the three change types and selected 20 rows per type before decisions were coded. The frozen sample contains 60 rows.

The equal allocation estimates precision within each change type. The overall estimate weights those rates by the full counts of 124 new-candidate leads, 50 additional-site leads, and 51 alternative exact URLs. The approximate interval reflects sampling variation only. It does not account for judgment error.

## Results

The source-expansion eligibility rule has low precision for rows that claim a new candidate or an additional site. It performs well when it proposes another exact URL on a site already known for the candidate.

| Change type | Full rows | Sample | Accepted exact URL | Other decision | Verdict |
|---|---:|---:|---:|---:|---|
| New candidate URL lead | 124 | 20 | 6 (30%) | 13 rejected; 1 unresolved | Most sampled rows are not candidate websites. |
| Additional site for known candidate | 50 | 20 | 5 (25%) | 14 rejected; 1 needs a corrected root URL | Most sampled rows are not additional candidate sites. |
| Alternative exact URL for known site | 51 | 20 | 20 (100%) | None | The sampled variants are supported campaign URLs. |

The change-type-weighted exact-URL precision estimate is 44.8 percent. Its approximate 95 percent interval is 33.8 to 55.7 percent. Counting the one recoverable NationBuilder root would not change the conclusion.

Twenty-six of the 27 rejected rows are news articles, government records, or reference pages. Examples include a NOLA.com poll, a CNBC profile of Pete Buttigieg's presidential campaign, and a Portland auditor filing. These pages mention the correct candidate and contest but are not campaign websites.

The error comes from treating a candidate name or article title in a hyperlink as evidence that the hyperlink target is candidate-owned. The current rule also assigns `custom_domain` to many ordinary news domains. The full structural census flags 58 of 225 rows as high risk, but that flag is insufficient: it caught 16 of 27 sample rejects, missed 11 rejects, and also flagged two accepted URLs.

## Ambiguous cases checked online

Ricco Wright ran for Tulsa mayor in 2020, but the saved congressional footnote identifies `riccowright.com` only as his website. It does not connect the domain to his mayoral campaign, so the audit leaves that row unresolved. Contemporary reporting confirms the candidacy but not the URL relationship ([Inc.](https://www.inc.com/emily-canal/juneteenth-trump-rally-tulsa-ricco-wright-mayor.html), [Public Radio Tulsa](https://www.publicradiotulsa.org/local-regional/2020-06-11/tulsa-elections-bynum-draws-7-challengers-for-mayor-2-councilors-unopposed)).

The proposed Benjamin Vazquez domain does not identify his 2016 Santa Ana mayoral site. A contemporaneous report names `benjamin4mayor.com` for that campaign, while the proposed domain is associated with later campaigns ([New Santa Ana](https://newsantaana.com/local-schoolteacher-is-challenging-mayor-pulido-this-november/)). The audit rejects the proposed exact URL for 2016.

The Steve Benjamin site identifies him as Columbia mayor and records his 2013 reelection. The audit accepts the domain as a grade-C claim but does not treat it as strict historical coverage because no election-period capture was checked ([campaign site](https://stevebenjamin.com/about-steve/)).

## Implication

Do not import the 225 rows into the release or use 1,125 candidate-years as validated URL coverage. Preserve the rows as discovery leads. The 51 alternative-URL rows are the strongest next review batch, but the audit decisions remain agent judgments rather than human approvals.

Before another source-expansion export, revise the eligibility logic so it distinguishes candidate-owned websites from news and reference links. Rerun the summary from the archived claim evidence after that repair. The source crawl should remain stopped until the export logic is corrected and storage for the remaining queue is planned.

## Files

- `quality_reports/mayoral_source_expansion_url_audit_2026-09-10/lead_evidence.csv`: all 225 leads with compact saved evidence and structural flags.
- `quality_reports/mayoral_source_expansion_url_audit_2026-09-10/audit_sample.csv`: frozen 60-row sample before decisions.
- `quality_reports/mayoral_source_expansion_url_audit_2026-09-10/audit_decisions.csv`: row-level agent audit decisions and reasons.
- `quality_reports/mayoral_source_expansion_url_audit_2026-09-10/audit_results.json`: weighted precision calculation and stratum counts.
- `scripts/audit_mayoral_source_leads.py`: deterministic audit builder and decision summarizer.

## Limits

The audit did not check Wayback captures for all accepted rows. Accepted rows therefore receive grade C, not strict grade A or B. The sample supports a precision estimate for the 225-row proposal set; it does not adjudicate every row. No audit decision was imported into the roster or release.
