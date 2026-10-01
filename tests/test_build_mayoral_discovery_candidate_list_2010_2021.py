from __future__ import annotations

import pandas as pd

from scripts import build_mayoral_discovery_candidate_list_2010_2021 as combined


def test_real_build_preserves_sources_and_expected_candidate_union() -> None:
    claims = combined.build_claims(
        pd.read_csv(combined.DEFAULT_EXISTING, keep_default_na=False, low_memory=False),
        pd.read_csv(combined.DEFAULT_SERPER, keep_default_na=False, low_memory=False),
        pd.read_csv(combined.DEFAULT_OPENAI, keep_default_na=False, low_memory=False),
        pd.read_csv(combined.DEFAULT_OPENAI_RETRY, keep_default_na=False, low_memory=False),
        pd.read_csv(combined.DEFAULT_ARCHIVEIT, keep_default_na=False, low_memory=False),
    )
    assert claims.discovery_claim_id.is_unique
    assert claims.candidate_year_id.nunique() == 1046
    counts = claims.groupby("source_channel").candidate_year_id.nunique().to_dict()
    assert counts["existing_unified_claim"] == 306
    assert counts["serper_direct_result"] == 45
    assert counts["openai_web_search"] == 695
    assert counts["archive_it_cross_collection"] == 1
    # Archive-It adds independent provenance for Cat Brooks, not a fake extra candidate.
    cat = claims.loc[claims.candidate_year_id.eq("71381__2018")]
    assert set(cat.source_channel) == {"openai_web_search", "archive_it_cross_collection"}


def test_summary_groups_only_candidate_domain_not_independent_claims() -> None:
    claims = pd.DataFrame([
        {
            **{column: "" for column in combined.CLAIM_COLUMNS},
            "discovery_claim_id": "a", "candidate_year_id": "c1",
            "candidate_contest_id": "cc1", "registrable_domain": "example.org",
            "site_root_url": "https://example.org/", "exact_normalized_url": "https://example.org/",
            "source_channel": "one", "url_type": "custom_domain",
            "strict_custom_domain": False, "strict_exact_url": False,
        },
        {
            **{column: "" for column in combined.CLAIM_COLUMNS},
            "discovery_claim_id": "b", "candidate_year_id": "c1",
            "candidate_contest_id": "cc1", "registrable_domain": "example.org",
            "site_root_url": "https://example.org/", "exact_normalized_url": "https://example.org/about",
            "source_channel": "two", "url_type": "custom_domain",
            "strict_custom_domain": True, "strict_exact_url": True,
        },
    ])
    summary = combined.summarize(claims)
    assert len(summary) == 1
    assert summary.iloc[0].source_claim_count == 2
    assert bool(summary.iloc[0].has_strict_custom_domain)
