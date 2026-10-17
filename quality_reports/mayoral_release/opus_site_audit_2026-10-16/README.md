# Opus audit of the mayoral site check, October 16, 2026

The site check (`data/municipal_discovery/top100_coverage/release_site_check/site_check.csv`) decides whether each credited website is the finalist's campaign site for that election. Qwen3.6-27B on Hive passed 640 sites alone; Sonnet agents decided 82 (68 passes, 14 exclusions). Claude Opus agents, blind to the earlier decisions, audited it in three steps.

1. **Random sample of 50 sites** (36 Qwen passes, 4 Sonnet passes, 10 Sonnet exclusions; key `answer_key.json`, verdicts `sample_verdicts.json`). Opus agreed with all 36 Qwen passes (95 percent upper bound on the error rate about 8 percent) and disagreed with 5 of 14 Sonnet decisions.
2. **All remaining 68 Sonnet decisions** (`review_key.json`, `agent_site_verdicts.json`), with Wayback access. 3 disagreements; one site (Lincoln 2011, Beutler) has no archived HTML page and stays excluded.
3. **Disagreements settled** by a further Opus agent in the archive (`adjudication.json`, `adjudication_2.json`).

Result: 6 decisions changed in `site_check.csv` (source `opus_audit_20261016`): Pulido (Santa Ana 2012, 2016), Healy (Jersey City 2013) and Wright (Baltimore 2024) pass; Fraim (Norfolk 2014) passes through `mayorfraim.com` (step 4), while `fraimformayor.com` holds only 2010 content and stays excluded; Lamm (Lincoln 2019, `cyndiformayor.com`) is excluded as not her campaign site. Her own site was `cyndiforthecity.com`, which is not in the corpus. Van Duyne (Irving 2014) and Brown (Buffalo 2017) stay passed; their stored pages show only the earlier race.

This is a model audit, not human validation.

4. **Downloaded sites missing from the target map** (`unmapped_key.json`, `unmapped_site_verdicts.json`). 63 downloaded targets with text were not credited because the target map keeps only the finalist's current campaign domain. 36 repeat a domain already credited; 3 were rejected earlier (`roster_url_rejections.csv`). Opus checked the other 24: 9 pass, 15 fail (spam, an attack site, a state senate site, another person named Healy, sites frozen from an earlier race). The passing ones enter the corpus through `release_site_check/extra_target_map.json`. Opus also passed David D. King's site (Milwaukee 2024, `king_verdict.json`; metadata only, no campaign text) and Lamm's own site `cyndiforthecity.com`, downloaded in batch `mayoral_stage3y_20261016`.

Note: stage 3 target IDs derive from the finalist alone and can repeat across batches with different domains; `site_check.csv` rows apply to the batch credited in the target map.
