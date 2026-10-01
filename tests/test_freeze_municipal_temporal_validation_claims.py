from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

from scripts.freeze_municipal_temporal_validation_claims import (
    ACQUISITION_ROOT,
    DEFAULT_CONTRACT_DIR,
    DEFAULT_MERGED_DIR,
    ELIGIBLE_URL_TYPES,
    FROZEN_CLAIM_COLUMNS,
    KEY_SEPARATOR,
    VALIDATION_METHOD_VERSION,
    MunicipalValidationFreezeError,
    build_frozen_claim_frames,
    freeze_claims,
    sha256_file,
)


def _real_inputs() -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    evidence = pd.read_csv(
        DEFAULT_MERGED_DIR / "evidence.csv", keep_default_na=False
    )
    roster = pd.read_csv(
        DEFAULT_CONTRACT_DIR / "acquisition_candidate_targets.csv",
        keep_default_na=False,
    )
    decisions = pd.read_csv(
        DEFAULT_MERGED_DIR / "identity_decisions.csv", keep_default_na=False
    )
    return evidence, roster, decisions


def test_real_freeze_preserves_all_claims_and_selects_exact_url_types() -> None:
    evidence, roster, decisions = _real_inputs()
    all_claims, eligible, summary = build_frozen_claim_frames(
        evidence,
        roster,
        decisions,
        expected_all_claims=40,
        expected_coverage_url_type_claims=22,
        expected_identity_supported_claims=39,
        expected_eligible_claims=21,
    )

    assert all_claims.columns.tolist() == FROZEN_CLAIM_COLUMNS
    assert len(all_claims) == 40
    assert len(eligible) == 21
    assert int(all_claims["coverage_url_type_eligible"].sum()) == 22
    assert int(all_claims["identity_gate_pass"].sum()) == 39
    assert all_claims["identity_decision_hash"].str.fullmatch(r"[0-9a-f]{64}").all()
    assert set(all_claims["identity_attribution_status"]) == {
        "identity_supported",
        "match_unresolved",
    }
    assert eligible["url_type"].value_counts().to_dict() == {
        "custom_domain": 20,
        "shared_host_path": 1,
    }
    assert set(eligible["url_type"]) <= ELIGIBLE_URL_TYPES
    assert set(all_claims) >= {
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "contest",
        "context_hash",
    }
    assert all_claims["validation_method_version"].eq(
        VALIDATION_METHOD_VERSION
    ).all()
    assert summary["large_filter_audit"] == {
        "triggered": True,
        "before": 40,
        "after": 21,
        "dropped": 19,
        "drop_fraction": 0.475,
        "rule": "Network validation requires both a coverage-eligible URL type and an identity-supported source-to-roster match. All excluded claims remain in all_claims.csv.",
    }

    for row in all_claims.itertuples(index=False):
        key = KEY_SEPARATOR.join(
            [row.evidence_id, row.candidate_year_id, row.exact_normalized_url]
        )
        digest = hashlib.sha256(key.encode("utf-8")).hexdigest()
        assert row.validation_key == key
        assert row.evidence_hash == digest
        assert row.validation_shard_id == int(digest, 16) % 2


def test_freeze_hashes_key_evidence_record_and_context_separately() -> None:
    evidence, roster, decisions = _real_inputs()
    baseline, _, _ = build_frozen_claim_frames(evidence, roster, decisions)
    evidence_id = evidence.iloc[0]["evidence_id"]
    baseline_row = baseline.loc[baseline["evidence_id"].eq(evidence_id)].iloc[0]

    changed_evidence = evidence.copy()
    changed_evidence.loc[0, "evidence_text"] += " corrected"
    evidence_result, _, _ = build_frozen_claim_frames(
        changed_evidence, roster, decisions
    )
    evidence_row = evidence_result.loc[evidence_result["evidence_id"].eq(evidence_id)].iloc[0]
    assert evidence_row["validation_key"] == baseline_row["validation_key"]
    assert evidence_row["evidence_hash"] == baseline_row["evidence_hash"]
    assert evidence_row["evidence_record_hash"] != baseline_row["evidence_record_hash"]
    assert evidence_row["context_hash"] == baseline_row["context_hash"]

    changed_roster = roster.copy()
    target_id = evidence.iloc[0]["target_id"]
    candidate_id = evidence.iloc[0]["candidate_year_id"]
    roster_mask = changed_roster["target_id"].eq(target_id) & changed_roster[
        "candidate_year_id"
    ].eq(candidate_id)
    changed_roster.loc[roster_mask, "partition_hostname"] = "corrected.example"
    context_result, _, _ = build_frozen_claim_frames(
        evidence, changed_roster, decisions
    )
    context_row = context_result.loc[context_result["evidence_id"].eq(evidence_id)].iloc[0]
    assert context_row["evidence_hash"] == baseline_row["evidence_hash"]
    assert context_row["evidence_record_hash"] == baseline_row["evidence_record_hash"]
    assert context_row["context_hash"] != baseline_row["context_hash"]


def test_independent_identity_decision_controls_gate_and_has_own_hash() -> None:
    evidence, roster, decisions = _real_inputs()
    baseline, _, _ = build_frozen_claim_frames(evidence, roster, decisions)
    supported = baseline.loc[
        baseline["identity_attribution_status"].eq("identity_supported")
        & baseline["coverage_url_type_eligible"]
    ].iloc[0]
    changed = decisions.copy()
    mask = changed["evidence_id"].eq(supported["evidence_id"])
    changed.loc[mask, "identity_attribution_status"] = "match_unresolved"
    changed.loc[mask, "identity_attribution_reason"] = "synthetic_unresolved_fixture"
    changed.loc[mask, "required_attempt_status"] = "match_unresolved"
    result, eligible, _ = build_frozen_claim_frames(evidence, roster, changed)
    row = result.loc[result["evidence_id"].eq(supported["evidence_id"])].iloc[0]

    assert row["evidence_hash"] == supported["evidence_hash"]
    assert row["evidence_record_hash"] == supported["evidence_record_hash"]
    assert row["context_hash"] == supported["context_hash"]
    assert row["identity_decision_hash"] != supported["identity_decision_hash"]
    assert not row["identity_gate_pass"]
    assert row["evidence_id"] not in set(eligible["evidence_id"])


def test_freeze_rejects_missing_context_decision_and_duplicate_validation_key() -> None:
    evidence, roster, decisions = _real_inputs()
    first = evidence.iloc[0]
    missing_context = roster.loc[
        ~(
            roster["target_id"].eq(first["target_id"])
            & roster["candidate_year_id"].eq(first["candidate_year_id"])
        )
    ].copy()
    with pytest.raises(MunicipalValidationFreezeError, match="absent"):
        build_frozen_claim_frames(evidence, missing_context, decisions)

    missing_decision = decisions.iloc[1:].copy()
    with pytest.raises(MunicipalValidationFreezeError, match="evidence IDs differ"):
        build_frozen_claim_frames(evidence, roster, missing_decision)

    duplicate = pd.concat([evidence, evidence.iloc[[0]]], ignore_index=True)
    with pytest.raises(MunicipalValidationFreezeError, match="Duplicate evidence_id"):
        build_frozen_claim_frames(duplicate, roster, decisions)


def test_real_directory_freeze_is_deterministic_and_records_input_audit(
    tmp_path: Path,
) -> None:
    output_dir = tmp_path / "frozen"
    first = freeze_claims(output_dir=output_dir)
    first_hashes = {
        name: sha256_file(output_dir / name)
        for name in ["all_claims.csv", "eligible_claims.csv", "manifest.json"]
    }
    second = freeze_claims(output_dir=output_dir)
    second_hashes = {
        name: sha256_file(output_dir / name)
        for name in ["all_claims.csv", "eligible_claims.csv", "manifest.json"]
    }

    assert first == second
    assert first_hashes == second_hashes
    assert first["acquisition_audit"]["verdict"] == "pass"
    assert first["acquisition_audit"]["evidence_rows"] == 40
    assert first["outputs"]["all_claims"]["rows"] == 40
    assert first["outputs"]["eligible_claims"]["rows"] == 21
    assert len(first["inputs"]) == 44
    assert first["merged_acquisition_audit"] == {
        "verdict": "pass",
        "manifest_sha256": "42915f6f0a1781c618d63d7fef50e4b5f32e2941eec14ca2facd113626e6c610",
        "evidence_sha256": "a8914cf48b330b005631d5e394f529bd4c66f18be2fda762cb8a71cb034e20ca",
        "identity_decisions_sha256": "2184c7c5c8ba5a94fb24d4c5cb7f677241a64c4207305b9f5c4b4417fbcafb7d",
        "identity_supported": 39,
        "match_unresolved": 1,
        "reject_identity": 0,
        "exact_shard_evidence_union": True,
        "exact_identity_decision_keys": True,
    }
    assert first["identity_decisions_source"]["sha256"] == (
        "2184c7c5c8ba5a94fb24d4c5cb7f677241a64c4207305b9f5c4b4417fbcafb7d"
    )
    persisted = json.loads((output_dir / "manifest.json").read_text(encoding="utf-8"))
    assert persisted == first
