from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd
import pytest

import scripts.merge_search_lead_temporal_validation as merger
from scripts.merge_search_lead_temporal_validation import (
    SELECTION_PROVENANCE_COLUMNS,
)
from scripts.validate_search_lead_wayback_available_content import (
    FROZEN_LEAD_COLUMNS,
    VALIDATION_COLUMNS,
    VALIDATION_METHOD_VERSION,
    validation_hash,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
TEMPORAL_ROOT = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/search_source_expansion/temporal_validation"
)
FROZEN_PATH = TEMPORAL_ROOT / "frozen/eligible_leads.csv"
FROZEN_MANIFEST_PATH = TEMPORAL_ROOT / "frozen/manifest.json"
CLEAN_SHARD_PATHS = {
    0: TEMPORAL_ROOT / "shards/validation_shard_0_clean.csv",
    1: TEMPORAL_ROOT / "shards/validation_shard_1_clean.csv",
}


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _frames() -> dict[int, pd.DataFrame]:
    return {
        shard_id: pd.read_csv(path, keep_default_na=False, low_memory=False)
        for shard_id, path in CLEAN_SHARD_PATHS.items()
    }


def _write_shards(
    directory: Path, frames: dict[int, pd.DataFrame]
) -> dict[int, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    paths: dict[int, Path] = {}
    for shard_id, frame in frames.items():
        path = directory / f"validation_shard_{shard_id}_clean.csv"
        frame.to_csv(path, index=False, lineterminator="\n")
        paths[shard_id] = path
    return paths


def _outputs(tmp_path: Path) -> tuple[Path, Path, Path]:
    return (
        tmp_path / "merged/search_lead_temporal_validation.csv",
        tmp_path / "merged/audit.json",
        tmp_path / "merged/manifest.json",
    )


def _provenance_output(tmp_path: Path) -> Path:
    return tmp_path / "merged/row_selection_provenance.csv"


def _merge(
    shard_paths: dict[int, Path],
    tmp_path: Path,
    *,
    expected_hashes: dict[int, str] | None = None,
    retry_paths: list[Path] | tuple[Path, ...] = (),
    selection_provenance_output: Path | None = None,
    expected_retry_hashes: dict[Path, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, object], dict[str, object]]:
    merged_output, audit_output, manifest_output = _outputs(tmp_path)
    return merger.merge_clean_shards(
        shard_paths,
        FROZEN_PATH,
        FROZEN_MANIFEST_PATH,
        merged_output,
        audit_output,
        manifest_output,
        expected_shard_hashes=expected_hashes,
        retry_paths=retry_paths,
        selection_provenance_output=selection_provenance_output,
        expected_retry_hashes=expected_retry_hashes,
    )


def _all_clean_rows() -> pd.DataFrame:
    return pd.concat(_frames().values(), ignore_index=True)


def _retry_row_for_target(
    target: pd.Series,
    result_donor: pd.Series,
    *,
    validated_on: str | None = None,
) -> pd.DataFrame:
    """Create a result with target provenance and a donor's validation outcome."""
    row = result_donor.copy()
    row.loc[FROZEN_LEAD_COLUMNS] = target.loc[FROZEN_LEAD_COLUMNS]
    if validated_on is not None:
        row.loc["validated_on"] = validated_on
    return row.to_frame().T.reindex(columns=VALIDATION_COLUMNS)


def _write_retry(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n")
    return path


def test_exact_clean_shard_union_preserves_four_pending_grade_a_rows_and_outputs(
    tmp_path: Path,
) -> None:
    expected_hashes = {
        shard_id: _sha256(path) for shard_id, path in CLEAN_SHARD_PATHS.items()
    }
    merged, audit, manifest = _merge(
        CLEAN_SHARD_PATHS, tmp_path, expected_hashes=expected_hashes
    )
    frozen = pd.read_csv(FROZEN_PATH, keep_default_na=False)

    assert merged.columns.tolist() == VALIDATION_COLUMNS
    assert len(merged) == len(frozen) == 41
    assert set(merged.validation_key) == set(frozen.validation_key)
    assert not merged.validation_key.duplicated().any()
    for shard_id in (0, 1):
        expected = set(
            frozen.loc[
                frozen.validation_shard_id.astype(int).eq(shard_id),
                "validation_key",
            ]
        )
        actual = set(
            merged.loc[
                merged.validation_shard_id.astype(int).eq(shard_id),
                "validation_key",
            ]
        )
        assert actual == expected

    grade_a = merged.loc[merged.proposed_evidence_grade.eq("A")]
    assert len(grade_a) == 4
    assert grade_a.strict_coverage_eligible_result.astype(bool).all()
    assert grade_a.capture_fetch_status.str.startswith("fetched").all()
    assert grade_a.candidate_identity_match.astype(bool).all()
    assert grade_a.mayoral_campaign_match.astype(bool).all()
    assert grade_a.city_match.astype(bool).all()
    assert grade_a.within_headline_window.astype(bool).all()
    assert grade_a.temporal_validation_status.eq(
        "grade_a_content_confirmed_pending_review"
    ).all()
    assert grade_a.validation_reason.eq(
        "grade_a_exact_url_election_period_content_confirmed"
    ).all()
    assert grade_a.review_status.eq("pending").all()
    assert grade_a.acceptance.eq("").all()

    merged_output, audit_output, manifest_output = _outputs(tmp_path)
    provenance_output = _provenance_output(tmp_path)
    assert merged_output.stat().st_size > 1000
    assert audit_output.stat().st_size > 100
    assert manifest_output.stat().st_size > 100
    assert provenance_output.stat().st_size > 100
    written = pd.read_csv(merged_output, keep_default_na=False)
    assert written.columns.tolist() == VALIDATION_COLUMNS
    assert set(written.validation_key) == set(merged.validation_key)
    assert json.loads(audit_output.read_text(encoding="utf-8")) == audit
    assert json.loads(manifest_output.read_text(encoding="utf-8")) == manifest
    provenance = pd.read_csv(provenance_output, keep_default_na=False)
    assert len(provenance) == 41
    assert provenance["validation_key"].nunique() == 41
    assert provenance["selected"].astype(bool).all()


@pytest.mark.parametrize(
    "bad_paths",
    [
        {0: TEMPORAL_ROOT / "shards/validation_shard_0.csv", 1: CLEAN_SHARD_PATHS[1]},
        {0: CLEAN_SHARD_PATHS[0]},
        {0: CLEAN_SHARD_PATHS[0], 1: CLEAN_SHARD_PATHS[1], 2: CLEAN_SHARD_PATHS[1]},
    ],
)
def test_merge_requires_exact_two_clean_shard_names_and_mapping(
    tmp_path: Path, bad_paths: dict[int, Path]
) -> None:
    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(bad_paths, tmp_path)


@pytest.mark.parametrize("failure", ["duplicate", "missing", "unexpected"])
def test_merge_rejects_duplicate_missing_and_unexpected_key_union(
    tmp_path: Path, failure: str
) -> None:
    frames = _frames()
    if failure == "duplicate":
        frames[0] = pd.concat([frames[0], frames[0].iloc[[0]]], ignore_index=True)
    elif failure == "missing":
        frames[0] = frames[0].iloc[1:].copy()
    else:
        index = frames[0].index[0]
        frames[0].loc[index, "url_claim_id"] = "unexpected-url-claim"
        key = "\x1f".join(
            [
                "unexpected-url-claim",
                str(frames[0].loc[index, "candidate_year_id"]),
                str(frames[0].loc[index, "exact_normalized_url"]),
            ]
        )
        frames[0].loc[index, "validation_key"] = key
        frames[0].loc[index, "validation_hash"] = validation_hash(key)
        frames[0].loc[index, "validation_shard_id"] = int(
            validation_hash(key), 16
        ) % 2
    paths = _write_shards(tmp_path / "shards", frames)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(paths, tmp_path / "outputs")


@pytest.mark.parametrize(
    "column,bad_value",
    [
        ("validation_method_version", "old-method"),
        ("lead_record_hash", "0" * 64),
        ("candidate_context_hash", "1" * 64),
        ("validation_hash", "2" * 64),
        ("validation_shard_id", 1),
        ("exact_normalized_url", "https://stale.example/changed"),
    ],
)
def test_merge_rejects_stale_echo_method_hash_and_shard_rows(
    tmp_path: Path, column: str, bad_value: object
) -> None:
    frames = _frames()
    frames[0].loc[frames[0].index[0], column] = bad_value
    paths = _write_shards(tmp_path / "shards", frames)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(paths, tmp_path / "outputs")


def test_merge_rejects_schema_drift_and_preserves_existing_outputs_atomically(
    tmp_path: Path,
) -> None:
    frames = _frames()
    frames[0] = frames[0].drop(columns=["capture_content_sha256"])
    paths = _write_shards(tmp_path / "shards", frames)
    merged_output, audit_output, manifest_output = _outputs(tmp_path / "outputs")
    provenance_output = _provenance_output(tmp_path / "outputs")
    for path, value in (
        (merged_output, b"old-merged"),
        (audit_output, b"old-audit"),
        (manifest_output, b"old-manifest"),
        (provenance_output, b"old-provenance"),
    ):
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(value)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(paths, tmp_path / "outputs")

    assert merged_output.read_bytes() == b"old-merged"
    assert audit_output.read_bytes() == b"old-audit"
    assert manifest_output.read_bytes() == b"old-manifest"
    assert provenance_output.read_bytes() == b"old-provenance"


def test_expected_clean_shard_hashes_are_enforced(tmp_path: Path) -> None:
    bad_hashes = {0: "0" * 64, 1: _sha256(CLEAN_SHARD_PATHS[1])}
    with pytest.raises(merger.SearchLeadTemporalMergeError, match="SHA|hash"):
        _merge(CLEAN_SHARD_PATHS, tmp_path, expected_hashes=bad_hashes)


@pytest.mark.parametrize(
    "column,bad_value",
    [
        ("candidate_identity_match", False),
        ("mayoral_campaign_match", False),
        ("city_match", False),
        ("within_headline_window", False),
        ("strict_coverage_eligible_result", False),
        ("temporal_validation_status", "content_validation_unresolved"),
        ("review_status", "accepted"),
        ("acceptance", "accept"),
    ],
)
def test_merge_recomputes_grade_a_strict_decision_consistency(
    tmp_path: Path, column: str, bad_value: object
) -> None:
    frames = _frames()
    shard_id = next(
        shard
        for shard, frame in frames.items()
        if frame.proposed_evidence_grade.eq("A").any()
    )
    index = frames[shard_id].index[
        frames[shard_id].proposed_evidence_grade.eq("A")
    ][0]
    frames[shard_id].loc[index, column] = bad_value
    paths = _write_shards(tmp_path / "shards", frames)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(paths, tmp_path / "outputs")


def test_partial_retry_selects_better_known_key_and_records_every_source_row(
    tmp_path: Path,
) -> None:
    rows = _all_clean_rows()
    target = rows.loc[
        rows.temporal_validation_status.eq("request_error_retryable")
    ].iloc[0]
    fetched_c = rows.loc[
        rows.proposed_evidence_grade.eq("C")
        & rows.capture_fetch_status.str.startswith("fetched")
        & rows.temporal_validation_status.eq("content_validation_unresolved")
    ].iloc[0]
    retry = _write_retry(
        tmp_path / "retries/retry_pass_1.csv",
        _retry_row_for_target(
            target,
            fetched_c,
            validated_on="2026-08-26T01:00:00Z",
        ),
    )

    merged, audit, manifest = _merge(
        CLEAN_SHARD_PATHS,
        tmp_path,
        expected_hashes={key: _sha256(path) for key, path in CLEAN_SHARD_PATHS.items()},
        retry_paths=[retry],
        expected_retry_hashes={retry: _sha256(retry)},
    )

    assert merged.columns.tolist() == VALIDATION_COLUMNS
    assert len(merged) == 41
    selected = merged.loc[merged.validation_key.eq(target.validation_key)].iloc[0]
    assert selected.temporal_validation_status == "content_validation_unresolved"
    assert selected.capture_fetch_status.startswith("fetched")
    assert selected.validated_on == "2026-08-26T01:00:00Z"

    provenance_path = _provenance_output(tmp_path)
    provenance = pd.read_csv(provenance_path, keep_default_na=False)
    assert provenance.columns.tolist() == SELECTION_PROVENANCE_COLUMNS
    assert len(provenance) == 42
    candidates = provenance.loc[provenance.validation_key.eq(target.validation_key)]
    assert len(candidates) == 2
    chosen = candidates.loc[
        candidates.selected.astype(str).str.casefold().eq("true")
    ]
    assert len(chosen) == 1
    assert int(chosen.iloc[0].retry_pass_index) == 1
    assert int(chosen.iloc[0].selection_rank) == 3
    assert chosen.iloc[0].selection_reason
    assert chosen.iloc[0].source_sha256 == _sha256(retry)
    assert str(chosen.iloc[0].source_path).endswith(retry.name)
    assert audit["selection_provenance"]["rows"] == 42
    assert manifest["outputs"]["row_selection_provenance"]["rows"] == 42


def test_retry_ranking_and_later_pass_tie_break_are_deterministic(
    tmp_path: Path,
) -> None:
    rows = _all_clean_rows()
    base_a = rows.loc[rows.proposed_evidence_grade.eq("A")].iloc[0]
    retryable = rows.loc[
        rows.temporal_validation_status.eq("request_error_retryable")
    ].reset_index(drop=True)
    target_fetched = retryable.iloc[0]
    target_equal = retryable.iloc[1]
    target_no_capture = retryable.iloc[2]
    fetched_c = rows.loc[
        rows.proposed_evidence_grade.eq("C")
        & rows.capture_fetch_status.str.startswith("fetched")
        & rows.temporal_validation_status.eq("content_validation_unresolved")
    ].iloc[0]
    fetched_outside_c = rows.loc[
        rows.proposed_evidence_grade.eq("C")
        & rows.capture_fetch_status.str.startswith("fetched")
        & rows.temporal_validation_status.eq(
            "outside_headline_and_sensitivity_windows"
        )
    ].iloc[0]
    no_capture_c = rows.loc[
        rows.temporal_validation_status.eq("no_archive_capture")
    ].iloc[0]
    request_error_c = retryable.iloc[3]

    pass_1 = pd.concat(
        [
            _retry_row_for_target(base_a, fetched_c),
            _retry_row_for_target(target_fetched, no_capture_c),
            _retry_row_for_target(
                target_equal,
                fetched_c,
                validated_on="2026-08-26T02:00:00Z",
            ),
            _retry_row_for_target(target_no_capture, no_capture_c),
        ],
        ignore_index=True,
    ).reindex(columns=VALIDATION_COLUMNS)
    pass_2 = pd.concat(
        [
            _retry_row_for_target(base_a, request_error_c),
            _retry_row_for_target(
                target_fetched,
                fetched_outside_c,
                validated_on="2026-08-27T02:00:00Z",
            ),
            _retry_row_for_target(
                target_equal,
                fetched_c,
                validated_on="2026-08-28T02:00:00Z",
            ),
            _retry_row_for_target(target_no_capture, request_error_c),
        ],
        ignore_index=True,
    ).reindex(columns=VALIDATION_COLUMNS)
    retry_1 = _write_retry(tmp_path / "retries/retry_pass_1.csv", pass_1)
    retry_2 = _write_retry(tmp_path / "retries/retry_pass_2.csv", pass_2)

    merged, _audit, _manifest = _merge(
        CLEAN_SHARD_PATHS,
        tmp_path,
        expected_hashes={key: _sha256(path) for key, path in CLEAN_SHARD_PATHS.items()},
        retry_paths=[retry_1, retry_2],
        expected_retry_hashes={
            retry_1: _sha256(retry_1),
            retry_2: _sha256(retry_2),
        },
    )
    by_key = merged.set_index("validation_key")

    assert by_key.loc[base_a.validation_key, "proposed_evidence_grade"] == "A"
    assert (
        by_key.loc[target_fetched.validation_key, "temporal_validation_status"]
        == "outside_headline_and_sensitivity_windows"
    )
    assert (
        by_key.loc[target_fetched.validation_key, "validated_on"]
        == "2026-08-27T02:00:00Z"
    )
    assert (
        by_key.loc[target_equal.validation_key, "validated_on"]
        == "2026-08-28T02:00:00Z"
    )
    assert (
        by_key.loc[target_no_capture.validation_key, "temporal_validation_status"]
        == "no_archive_capture"
    )

    provenance = pd.read_csv(_provenance_output(tmp_path), keep_default_na=False)
    expected_pass = {
        base_a.validation_key: 0,
        target_fetched.validation_key: 2,
        target_equal.validation_key: 2,
        target_no_capture.validation_key: 1,
    }
    for key, pass_index in expected_pass.items():
        candidates = provenance.loc[provenance.validation_key.eq(key)]
        chosen = candidates.loc[
            candidates.selected.astype(str).str.casefold().eq("true")
        ]
        assert len(candidates) == 3
        assert len(chosen) == 1
        assert int(chosen.iloc[0].retry_pass_index) == pass_index


def test_retry_may_add_grade_a_but_cannot_displace_original_four(
    tmp_path: Path,
) -> None:
    rows = _all_clean_rows()
    original_a_keys = set(
        rows.loc[rows.proposed_evidence_grade.eq("A"), "validation_key"]
    )
    target = rows.loc[
        rows.temporal_validation_status.eq("request_error_retryable")
    ].iloc[0]
    a_donor = rows.loc[rows.proposed_evidence_grade.eq("A")].iloc[0]
    retry = _write_retry(
        tmp_path / "retries/retry_adds_a.csv",
        _retry_row_for_target(target, a_donor),
    )

    merged, audit, _manifest = _merge(
        CLEAN_SHARD_PATHS,
        tmp_path,
        expected_hashes={key: _sha256(path) for key, path in CLEAN_SHARD_PATHS.items()},
        retry_paths=[retry],
        expected_retry_hashes={retry: _sha256(retry)},
    )

    final_a_keys = set(
        merged.loc[merged.proposed_evidence_grade.eq("A"), "validation_key"]
    )
    assert original_a_keys < final_a_keys
    assert target.validation_key in final_a_keys
    assert len(final_a_keys) == 5
    assert audit["pending_proposed_a_rows"] == 5


@pytest.mark.parametrize(
    "failure",
    ["unexpected_key", "schema", "method", "shard", "frozen_echo", "duplicate"],
)
def test_retry_rows_fail_closed_on_contract_drift(
    tmp_path: Path, failure: str
) -> None:
    rows = _all_clean_rows()
    target = rows.loc[
        rows.temporal_validation_status.eq("request_error_retryable")
    ].iloc[0]
    donor = rows.loc[
        rows.proposed_evidence_grade.eq("C")
        & rows.capture_fetch_status.str.startswith("fetched")
    ].iloc[0]
    retry_frame = _retry_row_for_target(target, donor)
    if failure == "unexpected_key":
        key = "unexpected-claim\x1funexpected-year\x1fhttps://example.test/"
        retry_frame.loc[0, "validation_key"] = key
        retry_frame.loc[0, "validation_hash"] = validation_hash(key)
    elif failure == "schema":
        retry_frame = retry_frame.drop(columns=["error"])
    elif failure == "method":
        retry_frame.loc[0, "validation_method_version"] = "old-method"
    elif failure == "shard":
        retry_frame.loc[0, "validation_shard_id"] = 1 - int(
            target.validation_shard_id
        )
    elif failure == "frozen_echo":
        retry_frame.loc[0, "exact_normalized_url"] = "https://stale.example/"
    else:
        retry_frame = pd.concat([retry_frame, retry_frame], ignore_index=True)
    retry = _write_retry(tmp_path / "retries/retry_bad.csv", retry_frame)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(
            CLEAN_SHARD_PATHS,
            tmp_path,
            expected_hashes={
                key: _sha256(path) for key, path in CLEAN_SHARD_PATHS.items()
            },
            retry_paths=[retry],
            expected_retry_hashes={retry: _sha256(retry)},
        )


def test_retry_hashes_are_enforced(tmp_path: Path) -> None:
    rows = _all_clean_rows()
    target = rows.loc[
        rows.temporal_validation_status.eq("request_error_retryable")
    ].iloc[0]
    retry = _write_retry(
        tmp_path / "retries/retry_hash.csv",
        _retry_row_for_target(target, target),
    )

    with pytest.raises(merger.SearchLeadTemporalMergeError, match="SHA|hash"):
        _merge(
            CLEAN_SHARD_PATHS,
            tmp_path,
            expected_hashes={
                key: _sha256(path) for key, path in CLEAN_SHARD_PATHS.items()
            },
            retry_paths=[retry],
            expected_retry_hashes={retry: "0" * 64},
        )


def test_retry_cannot_fill_a_missing_base_clean_shard_key(tmp_path: Path) -> None:
    frames = _frames()
    missing = frames[0].iloc[[0]].copy()
    frames[0] = frames[0].iloc[1:].copy()
    shard_paths = _write_shards(tmp_path / "base_shards", frames)
    retry = _write_retry(tmp_path / "retries/retry_missing_base.csv", missing)

    with pytest.raises(merger.SearchLeadTemporalMergeError):
        _merge(
            shard_paths,
            tmp_path,
            expected_hashes={key: _sha256(path) for key, path in shard_paths.items()},
            retry_paths=[retry],
            expected_retry_hashes={retry: _sha256(retry)},
        )


def test_validation_columns_and_frozen_prefix_remain_exact() -> None:
    assert VALIDATION_COLUMNS[: len(FROZEN_LEAD_COLUMNS)] == FROZEN_LEAD_COLUMNS
    assert len(VALIDATION_COLUMNS) == len(set(VALIDATION_COLUMNS))
    assert VALIDATION_METHOD_VERSION == "search_lead_wayback_available_content_v1"
