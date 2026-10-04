#!/usr/bin/env python3
"""Strictly audit and merge the two frozen clean search-lead validation shards.

This module is deliberately offline.  It accepts two explicitly named clean
shard files, verifies them against the frozen lead universe, and writes the
merged CSV, audit, and completion manifest without inspecting ambient shard
files.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import tempfile
from pathlib import Path
from typing import Mapping, Sequence
from urllib.parse import urlsplit

import pandas as pd

from scripts.validate_search_lead_wayback_available_content import (
    DEFAULT_FROZEN_LEADS,
    DEFAULT_FROZEN_MANIFEST,
    DEFAULT_SHARD_DIR,
    FROZEN_LEAD_COLUMNS,
    VALIDATION_COLUMNS,
    VALIDATION_METHOD_VERSION,
    load_frozen_targets,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = DEFAULT_SHARD_DIR.parent / "merged"
DEFAULT_MERGED_OUTPUT = DEFAULT_OUTPUT_DIR / "search_lead_temporal_validation.csv"
DEFAULT_AUDIT_OUTPUT = DEFAULT_OUTPUT_DIR / "merge_audit.json"
DEFAULT_MANIFEST_OUTPUT = DEFAULT_OUTPUT_DIR / "manifest.json"
DEFAULT_SELECTION_PROVENANCE_OUTPUT = (
    DEFAULT_OUTPUT_DIR / "row_selection_provenance.csv"
)

MERGE_AUDIT_VERSION = "search-lead-temporal-validation-merge-audit-v1"
MERGE_MANIFEST_VERSION = "search-lead-temporal-validation-merge-manifest-v1"
EXPECTED_SHARDS = (0, 1)
EXPECTED_SHARD_COUNT = 2
EXPECTED_MERGED_ROWS = 41
EXPECTED_PENDING_A_ROWS = 4
EXPECTED_CLEAN_SHARD_SHA256 = {
    0: "fb59c21377b74c1921547b340e5efe0a84576765df4680fb26168363bce6e72b",
    1: "3f0ea4eceff4538d68c020b2a9af9fab16b483f7608739acbce1b0f4ea6629a4",
}
HEX64 = re.compile(r"[0-9a-f]{64}")
SELECTION_CONTRACT_VERSION = "search-lead-temporal-row-selection-v1"
SELECTION_PROVENANCE_COLUMNS = [
    "selection_contract_version",
    "validation_key",
    "validation_hash",
    "url_claim_id",
    "candidate_year_id",
    "exact_normalized_url",
    "source_kind",
    "retry_pass_index",
    "source_label",
    "source_path",
    "source_sha256",
    "source_row_number",
    "rank_category",
    "selection_rank",
    "selected",
    "selection_reason",
]


class SearchLeadTemporalMergeError(ValueError):
    """Raised before publication when a clean shard violates the contract."""


def _text(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip()


def _as_bool(value: object, label: str) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).casefold()
    if token in {"true", "1", "yes"}:
        return True
    if token in {"false", "0", "no", ""}:
        return False
    raise SearchLeadTemporalMergeError(f"{label} has invalid boolean {value!r}")


def _as_int(value: object, label: str) -> int:
    token = _text(value)
    if not re.fullmatch(r"-?\d+", token):
        raise SearchLeadTemporalMergeError(f"{label} has invalid integer {value!r}")
    return int(token)


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _canonical_json_bytes(value: object) -> bytes:
    return (
        json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_exact_columns(frame: pd.DataFrame, expected: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != expected:
        raise SearchLeadTemporalMergeError(
            f"{label} column contract drift; "
            f"missing={sorted(set(expected) - set(actual))}, "
            f"unexpected={sorted(set(actual) - set(expected))}, "
            f"order_matches={actual == expected}"
        )


def _read_stable_csv(path: Path, label: str) -> tuple[pd.DataFrame, bytes, str]:
    """Read one immutable byte snapshot and reject an observed concurrent edit."""
    if not path.is_file():
        raise SearchLeadTemporalMergeError(f"{label} does not exist: {path}")
    before = path.stat()
    raw = path.read_bytes()
    after = path.stat()
    before_token = (before.st_dev, before.st_ino, before.st_size, before.st_mtime_ns)
    after_token = (after.st_dev, after.st_ino, after.st_size, after.st_mtime_ns)
    if before_token != after_token or len(raw) != after.st_size:
        raise SearchLeadTemporalMergeError(
            f"{label} changed concurrently while being read: {path}"
        )
    try:
        # String dtype preserves the exact CSV cell representation.  In
        # particular, an election month must not become 11.0 merely because a
        # different shard contains a blank month.
        frame = pd.read_csv(
            io.BytesIO(raw),
            keep_default_na=False,
            low_memory=False,
            dtype=str,
        )
    except Exception as error:
        raise SearchLeadTemporalMergeError(f"Cannot parse {label}: {error}") from error
    return frame, raw, _sha256_bytes(raw)


def _validate_clean_path(shard_id: int, path: Path) -> None:
    expected_name = f"validation_shard_{shard_id}_clean.csv"
    if path.name != expected_name:
        raise SearchLeadTemporalMergeError(
            f"Shard {shard_id} must be passed as explicitly named {expected_name}; "
            f"refusing local/concurrent shard file {path.name!r}"
        )


def _echo_value(column: str, value: object) -> str:
    token = _text(value)
    if column == "month" and token:
        # The frozen cross-shard CSV contains one blank month, so pandas
        # serialized populated months as e.g. ``11.0``.  The validator emits
        # the semantically identical integer month ``11``.
        if re.fullmatch(r"\d+(?:\.0+)?", token):
            return str(int(float(token)))
    return token


def _validate_static_echoes(
    shard: pd.DataFrame,
    frozen: pd.DataFrame,
    *,
    shard_id: int,
) -> None:
    expected = {
        _text(row["validation_key"]): row for row in frozen.to_dict("records")
    }
    actual_keys = shard["validation_key"].map(_text)
    if actual_keys.duplicated().any():
        duplicates = sorted(actual_keys[actual_keys.duplicated(keep=False)].unique())
        raise SearchLeadTemporalMergeError(
            f"Clean shard {shard_id} contains duplicate validation keys: {duplicates}"
        )
    actual = set(actual_keys)
    expected_keys = set(expected)
    if actual != expected_keys or len(shard) != len(expected_keys):
        raise SearchLeadTemporalMergeError(
            f"Clean shard {shard_id}/frozen key mismatch: "
            f"missing={sorted(expected_keys - actual)}, "
            f"unexpected={sorted(actual - expected_keys)}, "
            f"duplicate_rows={len(shard) - len(actual)}"
        )
    for row in shard.to_dict("records"):
        key = _text(row["validation_key"])
        frozen_row = expected[key]
        stale = [
            column
            for column in FROZEN_LEAD_COLUMNS
            if _echo_value(column, row[column])
            != _echo_value(column, frozen_row[column])
        ]
        if stale:
            raise SearchLeadTemporalMergeError(
                f"Clean shard {shard_id} has stale frozen echoes for {key!r}: {stale}"
            )
        if _text(row["validation_method_version"]) != VALIDATION_METHOD_VERSION:
            raise SearchLeadTemporalMergeError(
                f"Clean shard {shard_id} has method-version drift for {key!r}"
            )
        if _as_int(row["validation_shard_id"], "validation_shard_id") != shard_id:
            raise SearchLeadTemporalMergeError(
                f"Clean shard {shard_id} carries the wrong shard id for {key!r}"
            )
        if (
            _as_int(row["validation_shard_count"], "validation_shard_count")
            != EXPECTED_SHARD_COUNT
        ):
            raise SearchLeadTemporalMergeError(
                f"Clean shard {shard_id} carries the wrong shard count for {key!r}"
            )


def _validate_retry_echoes(
    retry: pd.DataFrame,
    frozen: pd.DataFrame,
    *,
    label: str,
) -> None:
    expected = {
        _text(row["validation_key"]): row for row in frozen.to_dict("records")
    }
    actual_keys = retry["validation_key"].map(_text)
    if actual_keys.duplicated().any():
        duplicates = sorted(actual_keys[actual_keys.duplicated(keep=False)].unique())
        raise SearchLeadTemporalMergeError(
            f"{label} contains duplicate validation keys: {duplicates}"
        )
    unexpected = set(actual_keys) - set(expected)
    if unexpected:
        raise SearchLeadTemporalMergeError(
            f"{label} contains keys outside the frozen universe: {sorted(unexpected)}"
        )
    for row in retry.to_dict("records"):
        key = _text(row["validation_key"])
        frozen_row = expected[key]
        stale = [
            column
            for column in FROZEN_LEAD_COLUMNS
            if _echo_value(column, row[column])
            != _echo_value(column, frozen_row[column])
        ]
        if stale:
            raise SearchLeadTemporalMergeError(
                f"{label} has conflicting frozen echoes for {key!r}: {stale}"
            )
        if _text(row["validation_method_version"]) != VALIDATION_METHOD_VERSION:
            raise SearchLeadTemporalMergeError(
                f"{label} has method-version drift for {key!r}"
            )
        if _as_int(row["validation_shard_id"], "validation_shard_id") != _as_int(
            frozen_row["validation_shard_id"], "validation_shard_id"
        ):
            raise SearchLeadTemporalMergeError(
                f"{label} has a wrong shard assignment for {key!r}"
            )
        if (
            _as_int(row["validation_shard_count"], "validation_shard_count")
            != EXPECTED_SHARD_COUNT
        ):
            raise SearchLeadTemporalMergeError(
                f"{label} has a wrong shard count for {key!r}"
            )


def _validate_decisions(
    merged: pd.DataFrame,
    *,
    expected_pending_a_rows: int | None = None,
) -> list[str]:
    allowed_grades = {"A", "C"}
    grades = set(merged["proposed_evidence_grade"].map(_text))
    if not grades <= allowed_grades:
        raise SearchLeadTemporalMergeError(
            f"Unexpected proposed evidence grades: {sorted(grades - allowed_grades)}"
        )
    pending_a_keys: list[str] = []
    for row in merged.to_dict("records"):
        key = _text(row["validation_key"])
        grade = _text(row["proposed_evidence_grade"])
        strict = _as_bool(
            row["strict_coverage_eligible_result"],
            "strict_coverage_eligible_result",
        )
        retryable = _as_bool(row["retryable"], "retryable")
        status = _text(row["temporal_validation_status"])
        is_pending_a = status == "grade_a_content_confirmed_pending_review"
        if (grade == "A") != strict or strict != is_pending_a:
            raise SearchLeadTemporalMergeError(
                f"Inconsistent grade/strict/status decision for {key!r}"
            )
        if retryable != (status == "request_error_retryable"):
            raise SearchLeadTemporalMergeError(
                f"Inconsistent retryable/status decision for {key!r}"
            )
        if not strict:
            if grade != "C":
                raise SearchLeadTemporalMergeError(
                    f"Non-strict row must remain grade C for {key!r}"
                )
            continue
        required_true = (
            "candidate_identity_match",
            "mayoral_campaign_match",
            "city_match",
            "within_headline_window",
        )
        if not all(_as_bool(row[column], column) for column in required_true):
            raise SearchLeadTemporalMergeError(
                f"Proposed A lacks a required content/timing signal for {key!r}"
            )
        if not _text(row["capture_fetch_status"]).startswith("fetched"):
            raise SearchLeadTemporalMergeError(
                f"Proposed A lacks fetched archived content for {key!r}"
            )
        content_bytes = _as_int(row["capture_content_bytes"], "capture_content_bytes")
        if content_bytes <= 0 or not HEX64.fullmatch(
            _text(row["capture_content_sha256"])
        ):
            raise SearchLeadTemporalMergeError(
                f"Proposed A lacks valid archived-content provenance for {key!r}"
            )
        if _text(row["timing_capture_basis"]) != "effective_replay_capture":
            raise SearchLeadTemporalMergeError(
                f"Proposed A timing is not based on the effective replay for {key!r}"
            )
        effective_url = urlsplit(_text(row["capture_effective_url"]))
        if effective_url.scheme not in {"http", "https"} or not effective_url.hostname:
            raise SearchLeadTemporalMergeError(
                f"Proposed A lacks a valid effective replay URL for {key!r}"
            )
        if not re.fullmatch(r"\d{14}", _text(row["capture_effective_timestamp"])):
            raise SearchLeadTemporalMergeError(
                f"Proposed A lacks a valid effective replay timestamp for {key!r}"
            )
        if _text(row["domain_reuse_status"]) != "same_candidate_only":
            raise SearchLeadTemporalMergeError(
                f"Proposed A has incomplete or conflicting reuse evidence for {key!r}"
            )
        if _text(row["validation_reason"]) != (
            "grade_a_exact_url_election_period_content_confirmed"
        ):
            raise SearchLeadTemporalMergeError(
                f"Proposed A has an inconsistent validation reason for {key!r}"
            )
        if _text(row["review_status"]).casefold() != "pending" or _text(
            row["acceptance"]
        ):
            raise SearchLeadTemporalMergeError(
                f"Proposed A must remain pending and unaccepted for {key!r}"
            )
        if retryable:
            raise SearchLeadTemporalMergeError(
                f"Proposed A cannot be retryable for {key!r}"
            )
        pending_a_keys.append(key)
    if (
        expected_pending_a_rows is not None
        and len(pending_a_keys) != expected_pending_a_rows
    ):
        raise SearchLeadTemporalMergeError(
            f"Expected {expected_pending_a_rows} pending proposed A rows, "
            f"found {len(pending_a_keys)}"
        )
    return sorted(pending_a_keys)


def _selection_rank(row: Mapping[str, object]) -> tuple[int, str]:
    """Return the approved deterministic best-evidence class for one row."""
    grade = _text(row["proposed_evidence_grade"])
    strict = _as_bool(
        row["strict_coverage_eligible_result"],
        "strict_coverage_eligible_result",
    )
    retryable = _as_bool(row["retryable"], "retryable")
    status = _text(row["temporal_validation_status"])
    fetch_status = _text(row["capture_fetch_status"])
    if (
        grade == "A"
        and strict
        and status == "grade_a_content_confirmed_pending_review"
    ):
        return 4, "strict_a_content_confirmed_pending_review"
    if grade == "C" and not retryable and fetch_status.startswith("fetched"):
        return 3, "terminal_fetched_c"
    if grade == "C" and not retryable and status in {
        "no_archive_capture",
        "outside_headline_365_within_sensitivity",
        "outside_headline_and_sensitivity_windows",
        "content_validation_unresolved",
        "missing_election_month",
    }:
        return 2, "terminal_nonfetched_c"
    if grade == "C" and retryable and status == "request_error_retryable":
        return 1, "request_error_retryable"
    raise SearchLeadTemporalMergeError(
        "Validation row does not fit an approved merge-ranking category for "
        f"{_text(row['validation_key'])!r}"
    )


def _counts(series: pd.Series) -> dict[str, int]:
    return {
        str(key): int(value)
        for key, value in series.map(_text).value_counts().sort_index().items()
    }


def _csv_bytes(frame: pd.DataFrame) -> bytes:
    return frame.to_csv(index=False, lineterminator="\n").encode("utf-8")


def _write_atomic_bundle(outputs: Mapping[Path, bytes], *, manifest_path: Path) -> None:
    resolved = [path.resolve() for path in outputs]
    if len(set(resolved)) != len(resolved):
        raise SearchLeadTemporalMergeError("Merge output paths must be distinct")
    temporary: dict[Path, Path] = {}
    try:
        for path, payload in outputs.items():
            path.parent.mkdir(parents=True, exist_ok=True)
            with tempfile.NamedTemporaryFile(
                mode="wb", dir=path.parent, delete=False
            ) as handle:
                handle.write(payload)
                handle.flush()
                os.fsync(handle.fileno())
                temporary[path] = Path(handle.name)
        for path in outputs:
            if path == manifest_path:
                continue
            os.replace(temporary[path], path)
        os.replace(temporary[manifest_path], manifest_path)
    finally:
        for path in temporary.values():
            if path.exists():
                path.unlink()


def merge_clean_shards(
    shard_paths: Mapping[int, Path | str],
    frozen_path: Path | str,
    frozen_manifest_path: Path | str,
    merged_output: Path | str,
    audit_output: Path | str,
    manifest_output: Path | str,
    *,
    expected_shard_hashes: Mapping[int, str] | None = None,
    expected_merged_rows: int = EXPECTED_MERGED_ROWS,
    expected_pending_a_rows: int | None = EXPECTED_PENDING_A_ROWS,
    retry_paths: Sequence[Path | str] = (),
    selection_provenance_output: Path | str | None = None,
    expected_retry_hashes: Mapping[Path | str, str] | None = None,
) -> tuple[pd.DataFrame, dict[str, object], dict[str, object]]:
    """Merge clean base shards plus explicit hash-pinned partial retry passes."""
    if set(shard_paths) != set(EXPECTED_SHARDS):
        raise SearchLeadTemporalMergeError(
            f"Exactly clean shard paths {EXPECTED_SHARDS} are required; "
            f"received {sorted(shard_paths)}"
        )
    expected_hashes = dict(
        EXPECTED_CLEAN_SHARD_SHA256
        if expected_shard_hashes is None
        else expected_shard_hashes
    )
    if set(expected_hashes) != set(EXPECTED_SHARDS) or any(
        not HEX64.fullmatch(_text(value)) for value in expected_hashes.values()
    ):
        raise SearchLeadTemporalMergeError(
            "Expected clean-shard hashes must contain two full SHA-256 values"
        )
    frozen_path = Path(frozen_path)
    frozen_manifest_path = Path(frozen_manifest_path)
    merged_output = Path(merged_output)
    audit_output = Path(audit_output)
    manifest_output = Path(manifest_output)
    selection_provenance_output = Path(
        selection_provenance_output
        if selection_provenance_output is not None
        else merged_output.parent / "row_selection_provenance.csv"
    )
    clean_paths = {shard_id: Path(path) for shard_id, path in shard_paths.items()}
    retry_path_list = [Path(path) for path in retry_paths]
    retry_resolved = [path.resolve() for path in retry_path_list]
    if len(set(retry_resolved)) != len(retry_resolved):
        raise SearchLeadTemporalMergeError("Retry paths must be unique")
    for path in retry_path_list:
        if re.fullmatch(r"validation_shard_[01](?:_clean)?\.csv", path.name):
            raise SearchLeadTemporalMergeError(
                f"Retry input must be an explicitly distinct retry pass, not {path.name!r}"
            )
    if retry_path_list and expected_retry_hashes is None:
        raise SearchLeadTemporalMergeError(
            "Every retry pass requires an explicit expected SHA-256"
        )
    normalized_retry_hashes = {
        Path(path).resolve(): _text(digest)
        for path, digest in (expected_retry_hashes or {}).items()
    }
    if set(normalized_retry_hashes) != set(retry_resolved) or any(
        not HEX64.fullmatch(value) for value in normalized_retry_hashes.values()
    ):
        raise SearchLeadTemporalMergeError(
            "Expected retry hashes must match the explicit retry paths exactly"
        )
    for shard_id, path in clean_paths.items():
        _validate_clean_path(shard_id, path)
    all_inputs = {
        frozen_path.resolve(),
        frozen_manifest_path.resolve(),
        *(path.resolve() for path in clean_paths.values()),
        *retry_resolved,
    }
    if any(
        output.resolve() in all_inputs
        for output in (
            merged_output,
            audit_output,
            manifest_output,
            selection_provenance_output,
        )
    ):
        raise SearchLeadTemporalMergeError("Merge outputs may not overwrite inputs")

    # The public loader performs the authoritative manifest/hash/gate checks.
    # Re-read one stable string-typed snapshot for byte-faithful echo checks.
    for shard_id in EXPECTED_SHARDS:
        load_frozen_targets(frozen_path, frozen_manifest_path, shard_id)
    frozen_raw, _frozen_raw_bytes, frozen_stable_sha256 = _read_stable_csv(
        frozen_path, "frozen eligible leads"
    )
    if frozen_stable_sha256 != _sha256_file(frozen_path):
        raise SearchLeadTemporalMergeError(
            "Frozen eligible leads changed between contract validation and merge"
        )
    _require_exact_columns(
        frozen_raw, FROZEN_LEAD_COLUMNS, "frozen eligible leads"
    )
    frozen_by_shard = {
        shard_id: frozen_raw.loc[
            frozen_raw["validation_shard_id"].eq(str(shard_id))
        ].reset_index(drop=True)
        for shard_id in EXPECTED_SHARDS
    }
    if expected_merged_rows <= 0:
        raise SearchLeadTemporalMergeError("expected_merged_rows must be positive")
    if sum(len(frame) for frame in frozen_by_shard.values()) != expected_merged_rows:
        raise SearchLeadTemporalMergeError(
            f"Frozen universe must contain exactly {expected_merged_rows} rows"
        )

    shard_frames: dict[int, pd.DataFrame] = {}
    shard_records: list[dict[str, object]] = []
    for shard_id in EXPECTED_SHARDS:
        path = clean_paths[shard_id]
        frame, raw, digest = _read_stable_csv(path, f"clean shard {shard_id}")
        if digest != _text(expected_hashes[shard_id]):
            raise SearchLeadTemporalMergeError(
                f"Clean shard {shard_id} SHA-256 mismatch: "
                f"expected={expected_hashes[shard_id]}, actual={digest}"
            )
        _require_exact_columns(frame, VALIDATION_COLUMNS, f"clean shard {shard_id}")
        _validate_static_echoes(frame, frozen_by_shard[shard_id], shard_id=shard_id)
        shard_frames[shard_id] = frame
        shard_records.append(
            {
                "shard_id": shard_id,
                "path": _display_path(path),
                "sha256": digest,
                "bytes": len(raw),
                "rows": int(len(frame)),
                "unique_validation_keys": int(frame["validation_key"].nunique()),
                "exact_frozen_key_agreement": True,
            }
        )

    base_merged = pd.concat(
        [shard_frames[shard_id] for shard_id in EXPECTED_SHARDS],
        ignore_index=True,
    ).reindex(columns=VALIDATION_COLUMNS)
    if base_merged["validation_key"].map(_text).duplicated().any():
        raise SearchLeadTemporalMergeError("Clean shards overlap on validation keys")
    frozen_all = pd.concat(
        [frozen_by_shard[shard_id] for shard_id in EXPECTED_SHARDS],
        ignore_index=True,
    )
    expected_keys = set(frozen_all["validation_key"].map(_text))
    actual_keys = set(base_merged["validation_key"].map(_text))
    if actual_keys != expected_keys or len(base_merged) != expected_merged_rows:
        raise SearchLeadTemporalMergeError(
            "Merged clean-shard keys do not equal the exact frozen universe"
        )
    base_pending_a_keys = _validate_decisions(
        base_merged, expected_pending_a_rows=expected_pending_a_rows
    )

    retry_frames: list[pd.DataFrame] = []
    retry_records: list[dict[str, object]] = []
    for pass_index, path in enumerate(retry_path_list, start=1):
        label = f"retry pass {pass_index} ({path.name})"
        frame, raw, digest = _read_stable_csv(path, label)
        expected_digest = normalized_retry_hashes[path.resolve()]
        if digest != expected_digest:
            raise SearchLeadTemporalMergeError(
                f"{label} SHA-256 mismatch: expected={expected_digest}, actual={digest}"
            )
        _require_exact_columns(frame, VALIDATION_COLUMNS, label)
        if frame.empty:
            raise SearchLeadTemporalMergeError(f"{label} is empty")
        _validate_retry_echoes(frame, frozen_all, label=label)
        _validate_decisions(frame)
        retry_frames.append(frame)
        retry_records.append(
            {
                "pass_index": pass_index,
                "label": f"retry_{pass_index:03d}_{path.stem}",
                "path": _display_path(path),
                "sha256": digest,
                "bytes": len(raw),
                "rows": int(len(frame)),
                "unique_validation_keys": int(frame["validation_key"].nunique()),
                "keys_subset_of_frozen_universe": True,
            }
        )

    candidates: list[dict[str, object]] = []

    def add_candidates(
        frame: pd.DataFrame,
        *,
        source_kind: str,
        source_pass_index: int,
        source_label: str,
        source_path: Path,
        source_sha256: str,
    ) -> None:
        for offset, row in enumerate(frame.to_dict("records"), start=2):
            priority, category = _selection_rank(row)
            candidates.append(
                {
                    "row": row,
                    "source_kind": source_kind,
                    "source_pass_index": source_pass_index,
                    "source_label": source_label,
                    "source_path": _display_path(source_path),
                    "source_sha256": source_sha256,
                    "source_row_number": offset,
                    "rank_category": category,
                    "rank_priority": priority,
                }
            )

    for shard_id in EXPECTED_SHARDS:
        add_candidates(
            shard_frames[shard_id],
            source_kind="base_clean_shard",
            source_pass_index=0,
            source_label=f"base_clean_shard_{shard_id}",
            source_path=clean_paths[shard_id],
            source_sha256=_text(expected_hashes[shard_id]),
        )
    for pass_index, (path, frame, record) in enumerate(
        zip(retry_path_list, retry_frames, retry_records), start=1
    ):
        add_candidates(
            frame,
            source_kind="retry_pass",
            source_pass_index=pass_index,
            source_label=_text(record["label"]),
            source_path=path,
            source_sha256=_text(record["sha256"]),
        )

    candidates_by_key: dict[str, list[dict[str, object]]] = {}
    for candidate in candidates:
        key = _text(candidate["row"]["validation_key"])  # type: ignore[index]
        candidates_by_key.setdefault(key, []).append(candidate)
    selected_by_key: dict[str, dict[str, object]] = {}
    provenance_rows: list[dict[str, object]] = []
    for key in sorted(expected_keys):
        key_candidates = candidates_by_key.get(key, [])
        if not key_candidates:
            raise SearchLeadTemporalMergeError(
                f"No validation candidate row remains for frozen key {key!r}"
            )
        selected = max(
            key_candidates,
            key=lambda candidate: (
                int(candidate["rank_priority"]),
                int(candidate["source_pass_index"]),
                _text(candidate["source_sha256"]),
                int(candidate["source_row_number"]),
            ),
        )
        selected_by_key[key] = selected
        for candidate in key_candidates:
            is_selected = candidate is selected
            candidate_row = candidate["row"]
            provenance_rows.append(
                {
                    "selection_contract_version": SELECTION_CONTRACT_VERSION,
                    "validation_key": key,
                    "validation_hash": _text(candidate_row["validation_hash"]),
                    "url_claim_id": _text(candidate_row["url_claim_id"]),
                    "candidate_year_id": _text(candidate_row["candidate_year_id"]),
                    "exact_normalized_url": _text(
                        candidate_row["exact_normalized_url"]
                    ),
                    "source_kind": candidate["source_kind"],
                    "retry_pass_index": candidate["source_pass_index"],
                    "source_label": candidate["source_label"],
                    "source_path": candidate["source_path"],
                    "source_sha256": candidate["source_sha256"],
                    "source_row_number": candidate["source_row_number"],
                    "rank_category": candidate["rank_category"],
                    "selection_rank": candidate["rank_priority"],
                    "selected": is_selected,
                    "selection_reason": (
                        "selected_highest_rank_then_latest_explicit_pass"
                        if is_selected
                        else (
                            "not_selected_lower_rank"
                            if int(candidate["rank_priority"])
                            < int(selected["rank_priority"])
                            else "not_selected_earlier_equal_rank_pass"
                        )
                    ),
                }
            )
    merged = pd.DataFrame(
        [selected_by_key[key]["row"] for key in sorted(selected_by_key)]
    ).reindex(columns=VALIDATION_COLUMNS)
    pending_a_keys = _validate_decisions(merged)
    all_candidate_a_keys = sorted(
        {
            _text(candidate["row"]["validation_key"])
            for candidate in candidates
            if _text(candidate["row"]["proposed_evidence_grade"]) == "A"
            and _as_bool(
                candidate["row"]["strict_coverage_eligible_result"],
                "strict_coverage_eligible_result",
            )
        }
    )
    if not set(base_pending_a_keys) <= set(pending_a_keys):
        raise SearchLeadTemporalMergeError(
            "A retry selection would downgrade a base proposed A row"
        )
    if not set(all_candidate_a_keys) <= set(pending_a_keys):
        raise SearchLeadTemporalMergeError(
            "Best-row selection would downgrade a proposed A from an input pass"
        )
    retry_added_a_keys = sorted(set(pending_a_keys) - set(base_pending_a_keys))
    selection_provenance = pd.DataFrame(provenance_rows).reindex(
        columns=SELECTION_PROVENANCE_COLUMNS
    )
    if len(selection_provenance) != len(candidates):
        raise SearchLeadTemporalMergeError(
            "Selection provenance does not account for every base/retry candidate row"
        )
    selected_flags = selection_provenance["selected"].map(
        lambda value: _as_bool(value, "selected")
    )
    if int(selected_flags.sum()) != expected_merged_rows:
        raise SearchLeadTemporalMergeError(
            "Selection provenance must identify exactly one row per frozen key"
        )
    selection_provenance = selection_provenance.sort_values(
        ["validation_hash", "retry_pass_index", "source_label", "source_row_number"],
        kind="stable",
    ).reset_index(drop=True)
    selected_flags = selection_provenance["selected"].map(
        lambda value: _as_bool(value, "selected")
    )
    selected_per_key = (
        selection_provenance.assign(_selected=selected_flags)
        .groupby("validation_key", sort=False)["_selected"]
        .sum()
    )
    if not selected_per_key.eq(1).all() or len(selected_per_key) != expected_merged_rows:
        raise SearchLeadTemporalMergeError(
            "Selection provenance must select exactly one row for every frozen key"
        )
    merged = merged.sort_values(
        ["validation_hash", "url_claim_id"], kind="stable"
    ).reset_index(drop=True)
    merged_bytes = _csv_bytes(merged)
    selection_provenance_bytes = _csv_bytes(selection_provenance)
    merged_sha256 = _sha256_bytes(merged_bytes)
    frozen_sha256 = _sha256_file(frozen_path)
    frozen_manifest_sha256 = _sha256_file(frozen_manifest_path)
    retryable_count = int(
        merged["retryable"].map(lambda value: _as_bool(value, "retryable")).sum()
    )
    retryable_fraction = retryable_count / len(merged)
    selected_provenance = selection_provenance.loc[selected_flags].copy()
    selected_retry_count = int(
        selected_provenance["source_kind"].eq("retry_pass").sum()
    )
    base_priority = {
        _text(candidate["row"]["validation_key"]): int(candidate["rank_priority"])
        for candidate in candidates
        if candidate["source_kind"] == "base_clean_shard"
    }
    improved_by_retry_count = sum(
        1
        for key, candidate in selected_by_key.items()
        if candidate["source_kind"] == "retry_pass"
        and int(candidate["rank_priority"]) > base_priority[key]
    )
    large_transition_report = {
        "stage": "frozen_eligible_leads_to_nonretryable_validation",
        "before_rows": int(len(merged)),
        "after_rows": int(len(merged) - retryable_count),
        "retryable_rows": retryable_count,
        "retryable_fraction": retryable_fraction,
        "exceeds_20_percent": retryable_fraction > 0.20,
        "rows_dropped": 0,
        "explanation": (
            "All rows are preserved. Retryable rows lack a terminal content/reuse "
            "decision because public archive requests failed."
        ),
    }
    audit: dict[str, object] = {
        "audit_contract_version": MERGE_AUDIT_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "frozen": {
            "eligible_leads_path": _display_path(frozen_path),
            "eligible_leads_sha256": frozen_sha256,
            "manifest_path": _display_path(frozen_manifest_path),
            "manifest_sha256": frozen_manifest_sha256,
            "rows": int(len(frozen_all)),
            "unique_validation_keys": int(frozen_all["validation_key"].nunique()),
        },
        "clean_shards": shard_records,
        "retry_passes": retry_records,
        "merged": {
            "path": _display_path(merged_output),
            "sha256": merged_sha256,
            "bytes": len(merged_bytes),
            "rows": int(len(merged)),
            "unique_validation_keys": int(merged["validation_key"].nunique()),
            "exact_frozen_key_agreement": True,
            "duplicate_validation_keys": 0,
        },
        "availability_status_counts": _counts(merged["availability_status"]),
        "capture_fetch_status_counts": _counts(merged["capture_fetch_status"]),
        "temporal_validation_status_counts": _counts(
            merged["temporal_validation_status"]
        ),
        "validation_reason_counts": _counts(merged["validation_reason"]),
        "proposed_evidence_grade_counts": _counts(
            merged["proposed_evidence_grade"]
        ),
        "retryable_rows": retryable_count,
        "strict_coverage_eligible_rows": len(pending_a_keys),
        "pending_proposed_a_rows": len(pending_a_keys),
        "pending_proposed_a_validation_keys": pending_a_keys,
        "base_pending_proposed_a_rows": len(base_pending_a_keys),
        "base_pending_proposed_a_validation_keys": base_pending_a_keys,
        "all_input_proposed_a_rows_by_key": len(all_candidate_a_keys),
        "all_input_proposed_a_validation_keys": all_candidate_a_keys,
        "retry_added_pending_proposed_a_rows": len(retry_added_a_keys),
        "retry_added_pending_proposed_a_validation_keys": retry_added_a_keys,
        "no_proposed_a_downgraded": True,
        "selection_provenance": {
            "path": _display_path(selection_provenance_output),
            "sha256": _sha256_bytes(selection_provenance_bytes),
            "bytes": len(selection_provenance_bytes),
            "rows": int(len(selection_provenance)),
            "selected_rows": int(selected_flags.sum()),
            "unselected_rows": int(len(selection_provenance) - selected_flags.sum()),
            "selected_retry_rows": selected_retry_count,
            "rank_improvements_from_retry": improved_by_retry_count,
            "all_candidate_rows_accounted_for": True,
        },
        "accepted_rows": 0,
        "large_transition_report": large_transition_report,
        "all_rows_preserved": True,
        "offline_merge": True,
    }
    audit_bytes = _canonical_json_bytes(audit)
    manifest: dict[str, object] = {
        "manifest_contract_version": MERGE_MANIFEST_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "frozen_eligible_leads_sha256": frozen_sha256,
        "frozen_manifest_sha256": frozen_manifest_sha256,
        "clean_shards": shard_records,
        "retry_passes": retry_records,
        "outputs": {
            "merged_validation": {
                "path": _display_path(merged_output),
                "sha256": merged_sha256,
                "bytes": len(merged_bytes),
                "rows": int(len(merged)),
            },
            "audit": {
                "path": _display_path(audit_output),
                "sha256": _sha256_bytes(audit_bytes),
                "bytes": len(audit_bytes),
            },
            "row_selection_provenance": {
                "path": _display_path(selection_provenance_output),
                "sha256": _sha256_bytes(selection_provenance_bytes),
                "bytes": len(selection_provenance_bytes),
                "rows": int(len(selection_provenance)),
            },
        },
        "exact_frozen_key_agreement": True,
        "pending_proposed_a_rows": len(pending_a_keys),
        "retry_added_pending_proposed_a_rows": len(retry_added_a_keys),
        "no_proposed_a_downgraded": True,
        "accepted_rows": 0,
        "completion_marker": "complete",
    }
    manifest_bytes = _canonical_json_bytes(manifest)
    _write_atomic_bundle(
        {
            merged_output: merged_bytes,
            audit_output: audit_bytes,
            selection_provenance_output: selection_provenance_bytes,
            manifest_output: manifest_bytes,
        },
        manifest_path=manifest_output,
    )
    return merged, audit, manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--shard-0-clean", type=Path, required=True)
    parser.add_argument("--shard-1-clean", type=Path, required=True)
    parser.add_argument("--shard-0-sha256")
    parser.add_argument("--shard-1-sha256")
    parser.add_argument("--expected-merged-rows", type=int, default=EXPECTED_MERGED_ROWS)
    parser.add_argument("--expected-pending-a-rows", type=int, default=EXPECTED_PENDING_A_ROWS)
    parser.add_argument(
        "--retry-clean",
        type=Path,
        action="append",
        default=[],
        help="Explicit partial retry CSV, repeat in pass order.",
    )
    parser.add_argument(
        "--retry-sha256",
        action="append",
        default=[],
        help="Full SHA-256 for each --retry-clean, in the same order.",
    )
    parser.add_argument("--frozen-leads", type=Path, default=DEFAULT_FROZEN_LEADS)
    parser.add_argument(
        "--frozen-manifest", type=Path, default=DEFAULT_FROZEN_MANIFEST
    )
    parser.add_argument("--merged-output", type=Path, default=DEFAULT_MERGED_OUTPUT)
    parser.add_argument("--audit-output", type=Path, default=DEFAULT_AUDIT_OUTPUT)
    parser.add_argument(
        "--manifest-output", type=Path, default=DEFAULT_MANIFEST_OUTPUT
    )
    parser.add_argument(
        "--selection-provenance-output",
        type=Path,
        default=DEFAULT_SELECTION_PROVENANCE_OUTPUT,
    )
    args = parser.parse_args()
    if len(args.retry_clean) != len(args.retry_sha256):
        parser.error("Each --retry-clean requires one ordered --retry-sha256")
    if bool(args.shard_0_sha256) != bool(args.shard_1_sha256):
        parser.error("Both clean-shard SHA-256 values must be supplied together")
    return args


def main() -> None:
    args = parse_args()
    merged, audit, _manifest = merge_clean_shards(
        {0: args.shard_0_clean, 1: args.shard_1_clean},
        args.frozen_leads,
        args.frozen_manifest,
        args.merged_output,
        args.audit_output,
        args.manifest_output,
        expected_shard_hashes=(
            {0: args.shard_0_sha256, 1: args.shard_1_sha256}
            if args.shard_0_sha256
            else None
        ),
        expected_merged_rows=args.expected_merged_rows,
        expected_pending_a_rows=args.expected_pending_a_rows,
        retry_paths=args.retry_clean,
        expected_retry_hashes={
            path: digest
            for path, digest in zip(args.retry_clean, args.retry_sha256)
        },
        selection_provenance_output=args.selection_provenance_output,
    )
    print(
        json.dumps(
            {
                "rows": int(len(merged)),
                "exact_frozen_key_agreement": audit["merged"][
                    "exact_frozen_key_agreement"
                ],
                "pending_proposed_a_rows": audit["pending_proposed_a_rows"],
                "retryable_rows": audit["retryable_rows"],
                "merged_output": _display_path(args.merged_output),
                "audit_output": _display_path(args.audit_output),
                "manifest_output": _display_path(args.manifest_output),
                "selection_provenance_output": _display_path(
                    args.selection_provenance_output
                ),
                "retry_passes": len(args.retry_clean),
            },
            indent=2,
            sort_keys=True,
        )
    )


if __name__ == "__main__":
    main()
