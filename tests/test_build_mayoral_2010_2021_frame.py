import json

import pandas as pd
import pytest

from scripts.build_mayoral_2010_2021_frame import (
    FrameBuildError,
    build_current_frame,
    write_outputs,
)


def _universe() -> pd.DataFrame:
    rows = []
    for i in range(4817):
        year = 2010 + (i % 12)
        rows.append({
            "candidate_year_id": f"c{i}", "in_discovery_scope": True, "year": year,
            "month": None if i == 4 else 11, "geo_name": "city", "state_abb": "CA",
            "population_2020": 200000, "ballotpedia_url": None,
            "stage_scope_status": "verified_general", "election_stage_status": "verified_general",
            "contest_n_candidates": 2, "contest": f"contest{i // 2}",
        })
    rows.append({**rows[0], "candidate_year_id": "out", "in_discovery_scope": False})
    return pd.DataFrame(rows)


def test_current_frame_separates_direct_and_inferred_coverage():
    claims = pd.DataFrame([
        {"candidate_year_id": "c0", "claim_origin": "legacy_baseline", "strict_custom_domain": False},
        {"candidate_year_id": "c0", "claim_origin": "adapter", "strict_custom_domain": True},
        {"candidate_year_id": "c1", "claim_origin": "adapter", "strict_custom_domain": True},
        {"candidate_year_id": "c9", "claim_origin": "adapter", "strict_custom_domain": False},
    ])
    disposition = pd.DataFrame([
        {"candidate_year_id": "c2", "strict_under_batch_audit_rule": True, "batch_disposition": "accepted_under_sampled_audit_rule"},
        {"candidate_year_id": "c1", "strict_under_batch_audit_rule": True, "batch_disposition": "human_accepted"},
    ])
    roster, unresolved, breakdown, audit = build_current_frame(_universe(), claims, [disposition])
    assert len(roster) == 4817
    assert audit["legacy_source_backed_candidate_years"] == 1
    assert audit["direct_new_strict_candidate_years"] == 2
    assert audit["legacy_and_direct_overlap_candidate_years"] == 1
    assert audit["directly_supported_combined_candidate_years"] == 2
    assert audit["sampled_audit_inferred_marginal_candidate_years"] == 1
    assert audit["covered_candidate_years"] == 3
    assert len(unresolved) == 4814
    assert audit["unresolved_missing_election_month"] == 1
    assert set(breakdown["dimension"]) >= {"period", "region", "contest_size_bin"}


def test_current_frame_rejects_claim_key_outside_scope():
    claims = pd.DataFrame([
        {"candidate_year_id": "out", "claim_origin": "adapter", "strict_custom_domain": True},
    ])
    with pytest.raises(FrameBuildError, match="absent"):
        build_current_frame(_universe(), claims, [])


def test_manifest_uses_dataframe_rows_when_csv_fields_contain_newlines(tmp_path):
    roster = pd.DataFrame([{"candidate_year_id": "a", "note": "line one\nline two"}])
    unresolved = roster.copy()
    breakdown = pd.DataFrame([{"dimension": "period", "value": "2010_2013", "unresolved_n": 1}])
    write_outputs(roster, unresolved, breakdown, {"ok": True}, tmp_path)
    manifest = json.loads((tmp_path / "frame_manifest.json").read_text())
    rows = {item["name"]: item["rows"] for item in manifest["files"]}
    assert rows == {"coverage_roster": 1, "unresolved_frame": 1, "breakdown": 1, "audit": None}
