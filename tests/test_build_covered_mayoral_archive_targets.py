from __future__ import annotations

import pandas as pd
import pytest

import scripts.build_covered_mayoral_archive_targets as build


def _roster() -> pd.DataFrame:
    return pd.DataFrame([
        {"covered": True, "candidate_year_id": "c1", "candidate_contest_id": "x1",
         "contest": "contest 1", "full_name": "One Candidate", "geo_name": "City",
         "state_abb": "CA", "year": 2021, "month": 11, "period": "2018_2021"},
        {"covered": True, "candidate_year_id": "c2", "candidate_contest_id": "x2",
         "contest": "contest 2", "full_name": "Two Candidate", "geo_name": "City",
         "state_abb": "CA", "year": 2015, "month": 5, "period": "2014_2017"},
    ])


def _claims() -> pd.DataFrame:
    return pd.DataFrame([
        {"claim_id": "a", "candidate_year_id": "c1", "claim_origin": "legacy_baseline",
         "strict_custom_domain": False, "exact_normalized_url": "https://one.example/issues",
         "site_root_url": "https://one.example/", "registrable_domain": "one.example",
         "url_type": "custom_domain", "source_profile_url": "https://source/a"},
        {"claim_id": "b", "candidate_year_id": "c1", "claim_origin": "legacy_baseline",
         "strict_custom_domain": False, "exact_normalized_url": "https://other.example/",
         "site_root_url": "https://other.example/", "registrable_domain": "other.example",
         "url_type": "custom_domain", "source_profile_url": "https://source/b"},
        {"claim_id": "c", "candidate_year_id": "c2", "claim_origin": "new",
         "strict_custom_domain": True, "exact_normalized_url": "http://two.nationbuilder.com",
         "site_root_url": "http://two.nationbuilder.com/", "registrable_domain": "nationbuilder.com",
         "url_type": "candidate_subdomain_or_shared_host_path", "source_profile_url": "https://source/c"},
    ])


def test_builds_candidate_domain_targets_and_preserves_shared_host() -> None:
    targets, audit = build.build_targets(
        _roster(), _claims(), [], expected_candidate_years=2, expected_targets=3
    )
    assert len(targets) == 3
    assert targets.candidate_year_id.nunique() == 2
    assert audit["candidate_years_with_two_domains"] == 1
    shared = targets.loc[targets.registrable_domain.eq("nationbuilder.com")].iloc[0]
    assert shared.site_root_url == "http://two.nationbuilder.com/"


def test_rejects_covered_candidate_without_url() -> None:
    with pytest.raises(build.CoveredTargetError, match="lack URL claims"):
        build.build_targets(
            _roster(), _claims().loc[lambda d: d.candidate_year_id.eq("c1")], [],
            expected_candidate_years=2, expected_targets=2,
        )
