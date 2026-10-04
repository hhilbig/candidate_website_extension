from __future__ import annotations

import pandas as pd
import pytest

from scripts.apply_search_lead_adjudication import (
    DECISION_COLUMNS,
    SearchLeadAdjudicationError,
    apply_decisions,
)


def _validation() -> pd.DataFrame:
    return pd.DataFrame(
        [
            {
                "url_claim_id": "claim-1",
                "validation_key": "key-1",
                "validation_hash": "a" * 64,
                "candidate_year_id": "candidate__2019",
                "candidate_contest_id": "contest__2019",
                "raw_url": "https://Candidate.example/Mayor",
                "exact_normalized_url": "https://Candidate.example/Mayor",
                "url_type": "custom_domain",
                "capture_effective_timestamp": "20191101120000",
                "capture_effective_url": "https://web.archive.org/example",
                "evidence_text": "Candidate for Mayor of City",
                "candidate_identity_match": True,
                "mayoral_campaign_match": True,
                "city_match": True,
                "domain_reuse_status": "same_candidate_only",
                "proposed_evidence_grade": "A",
                "strict_coverage_eligible_result": True,
                "temporal_validation_status": "grade_a_content_confirmed_pending_review",
            },
            {
                "url_claim_id": "claim-c",
                "validation_key": "key-c",
                "validation_hash": "c" * 64,
                "candidate_year_id": "candidate__2020",
                "candidate_contest_id": "contest__2020",
                "raw_url": "https://lead.example/",
                "exact_normalized_url": "https://lead.example/",
                "url_type": "custom_domain",
                "capture_effective_timestamp": "",
                "capture_effective_url": "",
                "evidence_text": "lead",
                "candidate_identity_match": False,
                "mayoral_campaign_match": False,
                "city_match": False,
                "domain_reuse_status": "not_inspected",
                "proposed_evidence_grade": "C",
                "strict_coverage_eligible_result": False,
                "temporal_validation_status": "no_archive_capture",
            },
        ]
    )


def _decisions(claim_id: str = "claim-1", decision: str = "approved") -> pd.DataFrame:
    return pd.DataFrame(
        [[claim_id, decision, "Hanno Hilbig", "2026-08-25", "direct_chat", "Approve all six"]],
        columns=DECISION_COLUMNS,
    )


def test_approved_grade_a_becomes_adapter_evidence() -> None:
    evidence, audit = apply_decisions(_validation(), _decisions())
    assert evidence["claim_id"].tolist() == ["claim-1"]
    assert evidence.iloc[0].review_status == "approved"
    assert bool(evidence.iloc[0].acceptance)
    assert bool(evidence.iloc[0].identity_match)
    assert bool(evidence.iloc[0].temporal_match)
    assert evidence.iloc[0].source_snapshot_timestamp == "20191101120000"
    assert audit["exact_proposed_decision_id_agreement"] is True
    assert audit["approved_rows"] == 1


def test_decisions_must_equal_exact_proposed_a_set() -> None:
    with pytest.raises(SearchLeadAdjudicationError, match="decision IDs"):
        apply_decisions(_validation(), _decisions("claim-c"))


def test_approval_cannot_bypass_content_gate() -> None:
    validation = _validation()
    validation.loc[0, "city_match"] = False
    with pytest.raises(SearchLeadAdjudicationError, match="content gates"):
        apply_decisions(validation, _decisions())
