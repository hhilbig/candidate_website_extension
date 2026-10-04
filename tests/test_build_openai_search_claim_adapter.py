from __future__ import annotations

import pandas as pd

import scripts.build_openai_search_claim_adapter as adapter


def test_adapter_preserves_only_eligible_claims_as_pending_d() -> None:
    claims = pd.DataFrame([
        {
            "url_claim_id": "c1", "assistant_output_id": "o1", "candidate_year_id": "x",
            "candidate_contest_id": "contest__x", "full_name": "Candidate", "geo_name": "City",
            "state_abb": "CA", "year": 2020, "month": 11, "raw_url": "https://x.test/",
            "exact_normalized_url": "https://x.test/", "site_root_url": "https://x.test/",
            "registrable_domain": "x.test", "url_type": "custom_domain",
            "eligible_candidate_site_lead": True, "parser_version": "parser-v1",
        },
        {
            "url_claim_id": "c2", "assistant_output_id": "o1", "candidate_year_id": "x",
            "candidate_contest_id": "contest__x", "full_name": "Candidate", "geo_name": "City",
            "state_abb": "CA", "year": 2020, "month": 11, "raw_url": "https://news.test/",
            "exact_normalized_url": "https://news.test/", "site_root_url": "https://news.test/",
            "registrable_domain": "news.test", "url_type": "custom_domain",
            "eligible_candidate_site_lead": False, "parser_version": "parser-v1",
        },
    ])
    outputs = pd.DataFrame([{"assistant_output_id": "o1", "notes": "Candidate campaign site"}])
    rows, audit = adapter.build_adapter(
        claims, outputs, source_id="openai_search", source_date="2026-08-26"
    )
    assert rows.claim_id.tolist() == ["c1"]
    assert rows.iloc[0].evidence_grade == "D"
    assert rows.iloc[0].review_status == "pending"
    assert rows.iloc[0].acceptance == ""
    assert audit["excluded_fraction"] == 0.5
    assert audit["exact_eligible_claim_key_agreement"]
    assert audit["strict_claims_emitted"] == 0
