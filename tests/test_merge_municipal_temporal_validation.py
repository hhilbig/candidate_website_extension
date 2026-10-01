from __future__ import annotations

import calendar
import json
from datetime import date, timedelta
from pathlib import Path

import pandas as pd
import pytest

from scripts.freeze_municipal_temporal_validation_claims import (
    ACQUISITION_ROOT,
    DEFAULT_CONTRACT_DIR,
    DEFAULT_MERGED_DIR,
    FREEZE_CONTRACT_VERSION,
    VALIDATION_METHOD_VERSION,
    build_frozen_claim_frames,
    freeze_claims,
)
from scripts.merge_municipal_temporal_validation import (
    RESULT_COLUMNS,
    MunicipalTemporalMergeError,
    audit_and_merge_frames,
    merge_validation_shards,
)


def _shift_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + months
    year, month_zero = divmod(absolute, 12)
    month = month_zero + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _frozen() -> pd.DataFrame:
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
    _, eligible, _ = build_frozen_claim_frames(
        evidence,
        roster,
        decisions,
        expected_all_claims=40,
        expected_coverage_url_type_claims=22,
        expected_identity_supported_claims=39,
        expected_eligible_claims=21,
    )
    return eligible


def _no_capture_results(frozen: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for claim in frozen.to_dict(orient="records"):
        reference = date(int(claim["year"]), int(claim["month"]), 15)
        row = {column: "" for column in RESULT_COLUMNS}
        for column in [
            "freeze_contract_version",
            "validation_method_version",
            "validation_key",
            "evidence_hash",
            "evidence_record_hash",
            "context_hash",
            "identity_decision_hash",
            "validation_shard_id",
            "validation_shard_count",
            "evidence_id",
            "source_id",
            "target_id",
            "candidate_year_id",
            "contest",
            "full_name",
            "geo_name",
            "state_abb",
            "year",
            "month",
            "url_type",
            "raw_url",
            "exact_normalized_url",
        ]:
            row[column] = claim[column]
        row.update(
            {
                "archive_query_target": claim["exact_normalized_url"],
                "archive_cdx_url": "https://web.archive.org/cdx/search/cdx?url=example",
                "archive_query_status": "checked",
                "archive_capture_count": 0,
                "eligible_capture_count": 0,
                "election_reference_date": reference.isoformat(),
                "headline_window_days": 365,
                "sensitivity_window_start": _shift_months(reference, -18).isoformat(),
                "sensitivity_window_end": (reference + timedelta(days=120)).isoformat(),
                "reuse_window_start": _shift_months(reference, -24).isoformat(),
                "reuse_window_end": _shift_months(reference, 24).isoformat(),
                "within_headline_window": False,
                "within_sensitivity_window": False,
                "capture_fetch_status": "not_attempted_no_capture",
                "candidate_identity_match": False,
                "mayoral_campaign_match": False,
                "city_match": False,
                "reuse_captures_before": 0,
                "reuse_captures_after": 0,
                "reuse_probe_count": 0,
                "reuse_conflict_count": 0,
                "domain_reuse_status": "not_inspected_no_capture",
                "proposed_evidence_grade": "C",
                "strict_coverage_eligible": False,
                "temporal_validation_status": "no_public_wayback_capture",
                "validation_reason": "no_public_wayback_capture",
                "validated_on": "2026-08-25",
            }
        )
        rows.append(row)
    return pd.DataFrame(rows, columns=RESULT_COLUMNS)


def _shards(results: pd.DataFrame) -> dict[int, pd.DataFrame]:
    return {
        shard_id: results.loc[results["validation_shard_id"].eq(shard_id)].copy()
        for shard_id in (0, 1)
    }


def _make_first_grade_a(results: pd.DataFrame) -> pd.DataFrame:
    changed = results.copy()
    index = changed.index[0]
    reference = date.fromisoformat(changed.loc[index, "election_reference_date"])
    timestamp = reference.strftime("%Y%m%d") + "120000"
    changed.loc[index, [
        "archive_capture_count",
        "eligible_capture_count",
        "nearest_capture_days_signed",
        "nearest_capture_days_absolute",
        "reuse_captures_before",
        "reuse_captures_after",
        "reuse_probe_count",
        "reuse_conflict_count",
    ]] = [1, 1, 0, 0, 0, 0, 0, 0]
    changed.loc[index, [
        "nearest_capture_timestamp",
        "nearest_capture_original_url",
        "capture_replay_url",
        "capture_fetch_status",
        "capture_content_sha256",
        "content_signal_summary",
        "domain_reuse_status",
        "proposed_evidence_grade",
        "temporal_validation_status",
        "validation_reason",
    ]] = [
        timestamp,
        changed.loc[index, "exact_normalized_url"],
        "https://web.archive.org/web/20131115120000id_/http://example.org/",
        "fetched",
        "a" * 64,
        "candidate=true;mayoral=true;city=true",
        "no_additional_capture",
        "A",
        "grade_a_content_confirmed_pending_review",
        "grade_a_exact_url_election_period_content_confirmed",
    ]
    for column in [
        "within_headline_window",
        "within_sensitivity_window",
        "candidate_identity_match",
        "mayoral_campaign_match",
        "city_match",
        "strict_coverage_eligible",
    ]:
        changed.loc[index, column] = True
    return changed


def test_exact_two_shard_union_passes_and_emits_audit() -> None:
    frozen = _frozen()
    results = _no_capture_results(frozen)
    merged, audit = audit_and_merge_frames(frozen, _shards(results))

    assert len(merged) == 21
    assert audit["verdict"] == "pass"
    assert audit["exact_key_union"] is True
    assert audit["duplicate_keys"] == 0
    assert audit["missing_keys"] == 0
    assert audit["unexpected_keys"] == 0
    assert audit["stale_rows"] == 0
    assert audit["expected_shard_counts"] == {"0": 11, "1": 10}
    assert audit["actual_shard_counts"] == {"0": 11, "1": 10}
    assert audit["proposed_evidence_grade_counts"] == {"C": 21}


@pytest.mark.parametrize("failure", ["duplicate", "missing", "unexpected"])
def test_merge_rejects_duplicate_missing_and_unexpected_keys(failure: str) -> None:
    frozen = _frozen()
    results = _no_capture_results(frozen)
    if failure == "duplicate":
        results = pd.concat([results, results.iloc[[0]]], ignore_index=True)
    elif failure == "missing":
        results = results.iloc[1:].copy()
    else:
        results.loc[0, "validation_key"] = "unexpected-key"
    with pytest.raises(MunicipalTemporalMergeError):
        audit_and_merge_frames(frozen, _shards(results))


@pytest.mark.parametrize(
    "column,bad_value",
    [
        ("validation_method_version", "old_method"),
        ("evidence_hash", "0" * 64),
        ("evidence_record_hash", "1" * 64),
        ("context_hash", "2" * 64),
        ("identity_decision_hash", "3" * 64),
        ("url_type", "social"),
    ],
)
def test_merge_rejects_stale_method_hash_context_and_url_type(
    column: str, bad_value: str
) -> None:
    frozen = _frozen()
    results = _no_capture_results(frozen)
    results.loc[0, column] = bad_value
    with pytest.raises(MunicipalTemporalMergeError, match="Stale|stale"):
        audit_and_merge_frames(frozen, _shards(results))


def test_merge_rejects_wrong_modulo_shard() -> None:
    frozen = _frozen()
    results = _no_capture_results(frozen)
    shards = _shards(results)
    moved = shards[0].iloc[[0]].copy()
    shards[0] = shards[0].iloc[1:].copy()
    shards[1] = pd.concat([shards[1], moved], ignore_index=True)
    with pytest.raises(MunicipalTemporalMergeError, match="wrong-shard"):
        audit_and_merge_frames(frozen, shards)


@pytest.mark.parametrize(
    "mutation",
    ["identity", "mayoral", "city", "timing", "reuse"],
)
def test_merge_recomputes_identity_timing_city_mayoral_and_reuse_decisions(
    mutation: str,
) -> None:
    frozen = _frozen()
    results = _make_first_grade_a(_no_capture_results(frozen))
    index = results.index[0]
    if mutation == "identity":
        results.loc[index, "candidate_identity_match"] = False
    elif mutation == "mayoral":
        results.loc[index, "mayoral_campaign_match"] = False
    elif mutation == "city":
        results.loc[index, "city_match"] = False
    elif mutation == "timing":
        results.loc[index, "within_headline_window"] = False
    else:
        results.loc[index, [
            "domain_reuse_status", "reuse_probe_count", "reuse_conflict_count"
        ]] = ["conflicting_content_detected", 1, 1]
    with pytest.raises(MunicipalTemporalMergeError, match="inconsistent"):
        audit_and_merge_frames(frozen, _shards(results))


def test_offline_cli_merge_checks_manifest_and_writes_nontrivial_outputs(
    tmp_path: Path,
) -> None:
    frozen_dir = tmp_path / "frozen"
    manifest = freeze_claims(output_dir=frozen_dir)
    frozen = pd.read_csv(frozen_dir / "eligible_claims.csv", keep_default_na=False)
    results = _no_capture_results(frozen)
    shard_dir = tmp_path / "shards"
    shard_dir.mkdir()
    for shard_id, frame in _shards(results).items():
        frame.to_csv(shard_dir / f"validation_shard_{shard_id}.csv", index=False)
    merged_path = tmp_path / "merged.csv"
    audit_path = tmp_path / "audit.json"
    audit = merge_validation_shards(
        frozen_claims_path=frozen_dir / "eligible_claims.csv",
        freeze_manifest_path=frozen_dir / "manifest.json",
        shard_dir=shard_dir,
        merged_output_path=merged_path,
        audit_output_path=audit_path,
    )

    assert manifest["summary"]["coverage_url_type_claims"] == 22
    assert audit["merged_claims"] == 21
    assert audit["output"]["rows"] == 21
    assert merged_path.stat().st_size > 1000
    assert audit_path.stat().st_size > 1000
    assert json.loads(audit_path.read_text(encoding="utf-8")) == audit


def test_cli_merge_rejects_stale_extra_shard_file(tmp_path: Path) -> None:
    frozen_dir = tmp_path / "frozen"
    freeze_claims(output_dir=frozen_dir)
    frozen = pd.read_csv(frozen_dir / "eligible_claims.csv", keep_default_na=False)
    results = _no_capture_results(frozen)
    shard_dir = tmp_path / "shards"
    shard_dir.mkdir()
    for shard_id, frame in _shards(results).items():
        frame.to_csv(shard_dir / f"validation_shard_{shard_id}.csv", index=False)
    (shard_dir / "stale.csv").write_text("old\n", encoding="utf-8")
    with pytest.raises(MunicipalTemporalMergeError, match="unexpected"):
        merge_validation_shards(
            frozen_claims_path=frozen_dir / "eligible_claims.csv",
            freeze_manifest_path=frozen_dir / "manifest.json",
            shard_dir=shard_dir,
            merged_output_path=tmp_path / "merged.csv",
            audit_output_path=tmp_path / "audit.json",
        )
