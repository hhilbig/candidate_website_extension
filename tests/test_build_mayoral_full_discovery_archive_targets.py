from __future__ import annotations

import json

import pandas as pd

from scripts import build_mayoral_full_discovery_archive_targets as builder


def test_real_targets_recover_every_new_claim_and_candidate() -> None:
    claims = pd.read_csv(builder.DEFAULT_CLAIMS, keep_default_na=False, low_memory=False)
    targets, audit = builder.build_targets(claims)
    assert len(targets) == 783
    assert audit["candidate_years"] == 740
    assert audit["source_claims_recovered"] == 792
    assert audit["missing_election_month_targets"] == 0
    assert targets.target_id.is_unique
    assert not targets.duplicated(["candidate_year_id", "registrable_domain"]).any()


def test_grouping_preserves_independent_claim_ids() -> None:
    rows = []
    for claim_id, source, url in (
        ("claim-a", "serper_direct_result", "https://example.org/"),
        ("claim-b", "openai_web_search", "https://example.org/about"),
    ):
        rows.append({
            "discovery_claim_id": claim_id, "source_channel": source,
            "candidate_year_id": "candidate-2018", "candidate_contest_id": "contest-candidate",
            "contest": "contest", "full_name": "Candidate Name", "geo_name": "city",
            "state_abb": "CA", "year": 2018, "month": 11,
            "registrable_domain": "example.org", "site_root_url": "https://example.org/",
            "exact_normalized_url": url, "url_type": "custom_domain",
        })
    targets, audit = builder.build_targets(pd.DataFrame(rows))
    assert len(targets) == 1
    assert targets.iloc[0].campaign_claim_count == 2
    assert set(json.loads(targets.iloc[0].campaign_claim_ids_json)) == {"claim-a", "claim-b"}
    assert audit["exact_new_claim_key_agreement"] is True
