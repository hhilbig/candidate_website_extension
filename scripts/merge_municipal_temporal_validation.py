#!/usr/bin/env python3
"""Audit and merge frozen municipal public-Wayback validation shards offline."""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import tempfile
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Mapping
from urllib.parse import urlsplit

import pandas as pd

from scripts.freeze_municipal_temporal_validation_claims import (
    DEFAULT_OUTPUT_DIR as DEFAULT_FROZEN_DIR,
    ELIGIBLE_URL_TYPES,
    FREEZE_CONTRACT_VERSION,
    FROZEN_CLAIM_COLUMNS,
    VALIDATION_METHOD_VERSION,
    VALIDATION_SHARD_COUNT,
    sha256_file,
    validation_evidence_hash,
    validation_key,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATION_ROOT = DEFAULT_FROZEN_DIR.parent
DEFAULT_SHARD_DIR = VALIDATION_ROOT / "shards"
DEFAULT_OUTPUT_DIR = VALIDATION_ROOT / "merged"
DEFAULT_MERGED_OUTPUT = DEFAULT_OUTPUT_DIR / "municipal_curated_wayback_validation.csv"
DEFAULT_AUDIT_OUTPUT = DEFAULT_OUTPUT_DIR / "merge_audit.json"

MERGE_AUDIT_VERSION = "municipal-curated-wayback-merge-audit-v1"
HEADLINE_WINDOW_DAYS = 365
SENSITIVITY_MONTHS_BEFORE = 18
SENSITIVITY_DAYS_AFTER = 120
REUSE_MONTHS_EACH_SIDE = 24

RESULT_COLUMNS = [
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
    "archive_query_target",
    "archive_cdx_url",
    "archive_query_status",
    "archive_capture_count",
    "eligible_capture_count",
    "election_reference_date",
    "headline_window_days",
    "sensitivity_window_start",
    "sensitivity_window_end",
    "reuse_window_start",
    "reuse_window_end",
    "nearest_capture_timestamp",
    "nearest_capture_original_url",
    "nearest_capture_days_signed",
    "nearest_capture_days_absolute",
    "within_headline_window",
    "within_sensitivity_window",
    "capture_replay_url",
    "capture_fetch_status",
    "capture_content_sha256",
    "candidate_identity_match",
    "mayoral_campaign_match",
    "city_match",
    "content_signal_summary",
    "reuse_captures_before",
    "reuse_captures_after",
    "reuse_probe_timestamps",
    "reuse_probe_count",
    "reuse_conflict_count",
    "domain_reuse_status",
    "proposed_evidence_grade",
    "strict_coverage_eligible",
    "temporal_validation_status",
    "validation_reason",
    "error",
    "validated_on",
]

KEY_COLUMNS = ["evidence_id", "candidate_year_id", "exact_normalized_url"]
STATIC_ECHO_COLUMNS = [
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
]

ARCHIVE_QUERY_STATUSES = {
    "checked",
    "request_error",
    "not_attempted_missing_election_month",
}
CAPTURE_FETCH_STATUSES = {
    "fetched",
    "request_error",
    "not_attempted_query_error",
    "not_attempted_no_capture",
    "not_attempted_missing_election_month",
}
TEMPORAL_VALIDATION_STATUSES = {
    "grade_a_content_confirmed_pending_review",
    "content_validation_unresolved",
    "no_public_wayback_capture",
    "missing_election_month",
    "request_error_retryable",
    "outside_headline_365_within_sensitivity",
    "outside_headline_and_sensitivity_windows",
}
DOMAIN_REUSE_STATUSES = {
    "not_inspected_missing_election_month",
    "not_inspected_query_error",
    "not_inspected_no_capture",
    "not_inspected_capture_error",
    "same_candidate_only",
    "conflicting_content_detected",
    "no_distinct_content_to_probe",
    "no_additional_capture",
    "request_error",
}
VALIDATION_REASONS = {
    "grade_a_exact_url_election_period_content_confirmed",
    "missing_election_month_no_imputation",
    "archive_query_request_error_retryable",
    "no_public_wayback_capture",
    "capture_request_error_retryable",
    "outside_headline_365_within_sensitivity",
    "outside_headline_and_sensitivity_windows",
    "candidate_identity_not_confirmed",
    "mayoral_campaign_not_confirmed",
    "city_not_confirmed",
    "reuse_inspection_request_error_retryable",
    "domain_reuse_conflict",
}


class MunicipalTemporalMergeError(ValueError):
    """Raised before output when validation results violate the frozen contract."""


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
    raise MunicipalTemporalMergeError(f"{label} has invalid boolean {value!r}")


def _as_int(value: object, label: str, *, allow_blank: bool = False) -> int | None:
    token = _text(value)
    if not token and allow_blank:
        return None
    if not re.fullmatch(r"-?\d+", token):
        raise MunicipalTemporalMergeError(f"{label} has invalid integer {value!r}")
    return int(token)


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != columns:
        raise MunicipalTemporalMergeError(
            f"{label} columns differ from the result contract; "
            f"missing={sorted(set(columns) - set(actual))}, "
            f"unexpected={sorted(set(actual) - set(columns))}, "
            f"order_matches={actual == columns}"
        )


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _shift_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + months
    year, month_zero = divmod(absolute, 12)
    month = month_zero + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _iso_date(value: object, label: str, *, allow_blank: bool = False) -> date | None:
    token = _text(value)
    if not token and allow_blank:
        return None
    try:
        return date.fromisoformat(token)
    except ValueError as error:
        raise MunicipalTemporalMergeError(f"{label} has invalid ISO date {token!r}") from error


def _capture_date(timestamp: object, label: str) -> date:
    token = _text(timestamp)
    if not re.fullmatch(r"\d{14}", token):
        raise MunicipalTemporalMergeError(f"{label} must be a 14-digit timestamp")
    try:
        return datetime.strptime(token, "%Y%m%d%H%M%S").date()
    except ValueError as error:
        raise MunicipalTemporalMergeError(f"{label} has invalid timestamp {token!r}") from error


def _is_http_url(value: object) -> bool:
    parsed = urlsplit(_text(value))
    return parsed.scheme in {"http", "https"} and bool(parsed.hostname)


def validate_frozen_claims(frozen: pd.DataFrame) -> pd.DataFrame:
    """Verify the eligible freeze itself before trusting its shard assignments."""
    _require_exact_columns(frozen, FROZEN_CLAIM_COLUMNS, "frozen eligible claims")
    if frozen.empty:
        raise MunicipalTemporalMergeError("Frozen eligible claims are empty")
    if frozen.duplicated(KEY_COLUMNS).any() or frozen["validation_key"].duplicated().any():
        raise MunicipalTemporalMergeError("Frozen eligible claims contain duplicate keys")
    if not frozen["freeze_contract_version"].map(_text).eq(FREEZE_CONTRACT_VERSION).all():
        raise MunicipalTemporalMergeError("Frozen claims use an unexpected freeze contract")
    if not frozen["validation_method_version"].map(_text).eq(
        VALIDATION_METHOD_VERSION
    ).all():
        raise MunicipalTemporalMergeError("Frozen claims use an old validation method")
    for row in frozen.itertuples(index=False):
        key = validation_key(row.evidence_id, row.candidate_year_id, row.exact_normalized_url)
        digest = validation_evidence_hash(key)
        if row.validation_key != key or row.evidence_hash != digest:
            raise MunicipalTemporalMergeError(f"Frozen key/hash drift for {row.evidence_id}")
        if int(row.validation_shard_count) != VALIDATION_SHARD_COUNT:
            raise MunicipalTemporalMergeError("Frozen validation_shard_count drift")
        if int(row.validation_shard_id) != int(digest, 16) % VALIDATION_SHARD_COUNT:
            raise MunicipalTemporalMergeError(f"Frozen wrong-shard key {row.evidence_id}")
        if not re.fullmatch(r"[0-9a-f]{64}", _text(row.evidence_record_hash)):
            raise MunicipalTemporalMergeError(f"Invalid evidence-record hash {row.evidence_id}")
        if not re.fullmatch(r"[0-9a-f]{64}", _text(row.context_hash)):
            raise MunicipalTemporalMergeError(f"Invalid context hash {row.evidence_id}")
        if not re.fullmatch(r"[0-9a-f]{64}", _text(row.identity_decision_hash)):
            raise MunicipalTemporalMergeError(
                f"Invalid identity-decision hash {row.evidence_id}"
            )
        if row.url_type not in ELIGIBLE_URL_TYPES:
            raise MunicipalTemporalMergeError(f"Ineligible URL type {row.url_type}")
        for flag in [
            "coverage_url_type_eligible",
            "identity_gate_pass",
            "eligible_for_temporal_validation",
        ]:
            if not _as_bool(getattr(row, flag), f"{row.evidence_id}.{flag}"):
                raise MunicipalTemporalMergeError(
                    f"Frozen eligible claim {row.evidence_id} fails {flag}"
                )
    return frozen.copy()


def _expected_decision(row: Mapping[str, object]) -> tuple[str, str, str, bool]:
    query = _text(row["archive_query_status"])
    fetch = _text(row["capture_fetch_status"])
    reuse = _text(row["domain_reuse_status"])
    nearest = _text(row["nearest_capture_timestamp"])
    headline = _as_bool(row["within_headline_window"], "within_headline_window")
    sensitivity = _as_bool(
        row["within_sensitivity_window"], "within_sensitivity_window"
    )
    identity = _as_bool(row["candidate_identity_match"], "candidate_identity_match")
    mayoral = _as_bool(row["mayoral_campaign_match"], "mayoral_campaign_match")
    city = _as_bool(row["city_match"], "city_match")

    if query == "not_attempted_missing_election_month":
        return (
            "missing_election_month_no_imputation",
            "missing_election_month",
            "C",
            False,
        )
    if query == "request_error":
        return (
            "archive_query_request_error_retryable",
            "request_error_retryable",
            "C",
            False,
        )
    if not nearest:
        return ("no_public_wayback_capture", "no_public_wayback_capture", "C", False)
    if fetch == "request_error":
        return (
            "capture_request_error_retryable",
            "request_error_retryable",
            "C",
            False,
        )
    if not headline:
        reason = (
            "outside_headline_365_within_sensitivity"
            if sensitivity
            else "outside_headline_and_sensitivity_windows"
        )
        return (reason, reason, "C", False)
    if not identity:
        return (
            "candidate_identity_not_confirmed",
            "content_validation_unresolved",
            "C",
            False,
        )
    if not mayoral:
        return (
            "mayoral_campaign_not_confirmed",
            "content_validation_unresolved",
            "C",
            False,
        )
    if not city:
        return (
            "city_not_confirmed",
            "content_validation_unresolved",
            "C",
            False,
        )
    if reuse == "request_error":
        return (
            "reuse_inspection_request_error_retryable",
            "request_error_retryable",
            "C",
            False,
        )
    if reuse == "conflicting_content_detected":
        return (
            "domain_reuse_conflict",
            "content_validation_unresolved",
            "C",
            False,
        )
    return (
        "grade_a_exact_url_election_period_content_confirmed",
        "grade_a_content_confirmed_pending_review",
        "A",
        True,
    )


def validate_result_decision(row: Mapping[str, object]) -> None:
    """Recompute timing, content, and reuse decisions for one result row."""
    evidence_id = _text(row["evidence_id"])
    prefix = f"result {evidence_id}"
    query = _text(row["archive_query_status"])
    fetch = _text(row["capture_fetch_status"])
    temporal = _text(row["temporal_validation_status"])
    reuse = _text(row["domain_reuse_status"])
    reason = _text(row["validation_reason"])
    if query not in ARCHIVE_QUERY_STATUSES:
        raise MunicipalTemporalMergeError(f"{prefix} has invalid archive_query_status")
    if fetch not in CAPTURE_FETCH_STATUSES:
        raise MunicipalTemporalMergeError(f"{prefix} has invalid capture_fetch_status")
    if temporal not in TEMPORAL_VALIDATION_STATUSES:
        raise MunicipalTemporalMergeError(f"{prefix} has invalid temporal_validation_status")
    if reuse not in DOMAIN_REUSE_STATUSES:
        raise MunicipalTemporalMergeError(f"{prefix} has invalid domain_reuse_status")
    if reason not in VALIDATION_REASONS:
        raise MunicipalTemporalMergeError(f"{prefix} has invalid validation_reason")
    if _text(row["proposed_evidence_grade"]) not in {"A", "C"}:
        raise MunicipalTemporalMergeError(f"{prefix} may emit only evidence grade A or C")
    if _as_int(row["headline_window_days"], f"{prefix}.headline_window_days") != 365:
        raise MunicipalTemporalMergeError(f"{prefix} changed the 365-day headline window")
    for column in [
        "archive_capture_count",
        "eligible_capture_count",
        "reuse_captures_before",
        "reuse_captures_after",
        "reuse_probe_count",
        "reuse_conflict_count",
    ]:
        value = _as_int(row[column], f"{prefix}.{column}")
        if value is None or value < 0:
            raise MunicipalTemporalMergeError(f"{prefix}.{column} must be nonnegative")
    archive_n = int(_as_int(row["archive_capture_count"], f"{prefix}.archive_capture_count"))
    eligible_n = int(_as_int(row["eligible_capture_count"], f"{prefix}.eligible_capture_count"))
    probe_n = int(_as_int(row["reuse_probe_count"], f"{prefix}.reuse_probe_count"))
    conflict_n = int(_as_int(row["reuse_conflict_count"], f"{prefix}.reuse_conflict_count"))
    if eligible_n > archive_n:
        raise MunicipalTemporalMergeError(f"{prefix} has more eligible than total captures")
    if conflict_n > probe_n:
        raise MunicipalTemporalMergeError(f"{prefix} has more conflicts than reuse probes")

    month = _as_int(row["month"], f"{prefix}.month", allow_blank=True)
    year = _as_int(row["year"], f"{prefix}.year")
    if month is None:
        if query != "not_attempted_missing_election_month":
            raise MunicipalTemporalMergeError(f"{prefix} missing month was not recorded explicitly")
        for column in [
            "election_reference_date",
            "sensitivity_window_start",
            "sensitivity_window_end",
            "reuse_window_start",
            "reuse_window_end",
            "nearest_capture_timestamp",
        ]:
            if _text(row[column]):
                raise MunicipalTemporalMergeError(f"{prefix} imputed timing field {column}")
    else:
        if not 1 <= month <= 12:
            raise MunicipalTemporalMergeError(f"{prefix} has invalid election month")
        reference = date(int(year), month, 15)
        expected_dates = {
            "election_reference_date": reference,
            "sensitivity_window_start": _shift_months(
                reference, -SENSITIVITY_MONTHS_BEFORE
            ),
            "sensitivity_window_end": reference + timedelta(days=SENSITIVITY_DAYS_AFTER),
            "reuse_window_start": _shift_months(reference, -REUSE_MONTHS_EACH_SIDE),
            "reuse_window_end": _shift_months(reference, REUSE_MONTHS_EACH_SIDE),
        }
        for column, expected in expected_dates.items():
            actual = _iso_date(row[column], f"{prefix}.{column}")
            if actual != expected:
                raise MunicipalTemporalMergeError(
                    f"{prefix}.{column} drifted: expected {expected}, found {actual}"
                )
        if query == "not_attempted_missing_election_month":
            raise MunicipalTemporalMergeError(f"{prefix} records a month but skipped for missing month")

    if _text(row["archive_query_target"]) != _text(row["exact_normalized_url"]):
        raise MunicipalTemporalMergeError(f"{prefix} did not query the exact frozen URL")
    if query != "not_attempted_missing_election_month" and not _is_http_url(
        row["archive_cdx_url"]
    ):
        raise MunicipalTemporalMergeError(f"{prefix} lacks a valid public CDX query URL")

    nearest = _text(row["nearest_capture_timestamp"])
    headline = _as_bool(row["within_headline_window"], f"{prefix}.within_headline_window")
    sensitivity = _as_bool(
        row["within_sensitivity_window"], f"{prefix}.within_sensitivity_window"
    )
    if nearest:
        if eligible_n == 0:
            raise MunicipalTemporalMergeError(f"{prefix} selects a capture from zero eligible captures")
        captured = _capture_date(nearest, f"{prefix}.nearest_capture_timestamp")
        reference = _iso_date(row["election_reference_date"], f"{prefix}.election_reference_date")
        assert reference is not None
        signed = (captured - reference).days
        absolute = abs(signed)
        if _as_int(row["nearest_capture_days_signed"], f"{prefix}.nearest_capture_days_signed") != signed:
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent signed capture distance")
        if _as_int(row["nearest_capture_days_absolute"], f"{prefix}.nearest_capture_days_absolute") != absolute:
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent absolute capture distance")
        if headline != (absolute <= HEADLINE_WINDOW_DAYS):
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent headline-window decision")
        sensitivity_start = _iso_date(
            row["sensitivity_window_start"], f"{prefix}.sensitivity_window_start"
        )
        sensitivity_end = _iso_date(
            row["sensitivity_window_end"], f"{prefix}.sensitivity_window_end"
        )
        assert sensitivity_start is not None and sensitivity_end is not None
        if sensitivity != (sensitivity_start <= captured <= sensitivity_end):
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent sensitivity decision")
        for column in ["nearest_capture_original_url", "capture_replay_url"]:
            if not _is_http_url(row[column]):
                raise MunicipalTemporalMergeError(f"{prefix} lacks {column}")
    else:
        for column in [
            "nearest_capture_days_signed",
            "nearest_capture_days_absolute",
            "nearest_capture_original_url",
            "capture_replay_url",
        ]:
            if _text(row[column]):
                raise MunicipalTemporalMergeError(f"{prefix} has {column} without a capture")
        if headline or sensitivity:
            raise MunicipalTemporalMergeError(f"{prefix} has window decisions without a capture")

    content_flags = [
        _as_bool(row[column], f"{prefix}.{column}")
        for column in [
            "candidate_identity_match",
            "mayoral_campaign_match",
            "city_match",
        ]
    ]
    if fetch == "fetched":
        if not nearest:
            raise MunicipalTemporalMergeError(f"{prefix} fetched content without a capture")
        if not re.fullmatch(r"[0-9a-f]{64}", _text(row["capture_content_sha256"])):
            raise MunicipalTemporalMergeError(f"{prefix} fetched content lacks SHA-256")
        if not _text(row["content_signal_summary"]):
            raise MunicipalTemporalMergeError(f"{prefix} fetched content lacks signal summary")
    else:
        if any(content_flags) or _text(row["capture_content_sha256"]):
            raise MunicipalTemporalMergeError(f"{prefix} records content signals without fetched content")

    if query == "request_error":
        if fetch != "not_attempted_query_error" or reuse != "not_inspected_query_error":
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent query-error statuses")
    if query == "not_attempted_missing_election_month":
        if (
            fetch != "not_attempted_missing_election_month"
            or reuse != "not_inspected_missing_election_month"
        ):
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent missing-month statuses")
    if query == "checked" and not nearest:
        if fetch != "not_attempted_no_capture" or reuse != "not_inspected_no_capture":
            raise MunicipalTemporalMergeError(f"{prefix} has inconsistent no-capture statuses")
    if fetch == "request_error" and reuse != "not_inspected_capture_error":
        raise MunicipalTemporalMergeError(f"{prefix} has inconsistent capture-error reuse status")
    if fetch == "fetched" and reuse.startswith("not_inspected_"):
        raise MunicipalTemporalMergeError(f"{prefix} did not inspect reuse after fetching content")
    if reuse == "conflicting_content_detected" and conflict_n == 0:
        raise MunicipalTemporalMergeError(f"{prefix} claims reuse conflict without a conflict")
    if reuse == "same_candidate_only" and (probe_n == 0 or conflict_n != 0):
        raise MunicipalTemporalMergeError(f"{prefix} has inconsistent same-candidate probes")
    if reuse in {"no_distinct_content_to_probe", "no_additional_capture"} and (
        probe_n != 0 or conflict_n != 0
    ):
        raise MunicipalTemporalMergeError(f"{prefix} has probes under a no-probe reuse status")

    needs_error = query == "request_error" or fetch == "request_error" or reuse == "request_error"
    if needs_error and not _text(row["error"]):
        raise MunicipalTemporalMergeError(f"{prefix} request error lacks error text")
    if not needs_error and _text(row["error"]):
        raise MunicipalTemporalMergeError(f"{prefix} has error text without a request error")
    _iso_date(row["validated_on"], f"{prefix}.validated_on")

    expected_reason, expected_status, expected_grade, expected_strict = _expected_decision(row)
    actual = (
        reason,
        temporal,
        _text(row["proposed_evidence_grade"]),
        _as_bool(row["strict_coverage_eligible"], f"{prefix}.strict_coverage_eligible"),
    )
    expected = (expected_reason, expected_status, expected_grade, expected_strict)
    if actual != expected:
        raise MunicipalTemporalMergeError(
            f"{prefix} decision tuple is inconsistent; expected={expected}, actual={actual}"
        )


def audit_and_merge_frames(
    frozen: pd.DataFrame,
    shard_frames: Mapping[int, pd.DataFrame],
    *,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Fail closed unless shard union and every decision exactly match the freeze."""
    if validation_shard_count != VALIDATION_SHARD_COUNT:
        raise MunicipalTemporalMergeError(
            f"Current contract requires {VALIDATION_SHARD_COUNT} validation shards"
        )
    frozen = validate_frozen_claims(frozen)
    if set(shard_frames) != set(range(validation_shard_count)):
        raise MunicipalTemporalMergeError(
            f"Validation shard IDs differ; expected={list(range(validation_shard_count))}, "
            f"actual={sorted(shard_frames)}"
        )
    normalized_frames: list[pd.DataFrame] = []
    for shard_id in range(validation_shard_count):
        frame = shard_frames[shard_id].copy()
        _require_exact_columns(frame, RESULT_COLUMNS, f"validation shard {shard_id}")
        if len(frame):
            actual_shards = {
                int(_as_int(value, f"shard {shard_id}.validation_shard_id"))
                for value in frame["validation_shard_id"]
            }
            if actual_shards != {shard_id}:
                raise MunicipalTemporalMergeError(
                    f"Validation shard {shard_id} contains wrong-shard rows {sorted(actual_shards)}"
                )
        normalized_frames.append(frame)
    combined = pd.concat(normalized_frames, ignore_index=True)
    if combined.duplicated(KEY_COLUMNS).any() or combined["validation_key"].duplicated().any():
        duplicated = combined.loc[
            combined.duplicated(KEY_COLUMNS, keep=False), KEY_COLUMNS
        ].astype(str).agg("|".join, axis=1).tolist()
        raise MunicipalTemporalMergeError(f"Duplicate validation result keys: {duplicated}")

    expected_keys = set(frozen["validation_key"].astype(str))
    actual_keys = set(combined["validation_key"].astype(str))
    missing = sorted(expected_keys - actual_keys)
    unexpected = sorted(actual_keys - expected_keys)
    if missing or unexpected:
        raise MunicipalTemporalMergeError(
            f"Validation key union mismatch; missing={missing}, unexpected={unexpected}"
        )

    frozen_lookup = frozen.set_index("validation_key", drop=False)
    stale: list[str] = []
    for row in combined.to_dict(orient="records"):
        key = _text(row["validation_key"])
        expected = frozen_lookup.loc[key]
        for column in STATIC_ECHO_COLUMNS:
            if _text(row[column]) != _text(expected[column]):
                stale.append(f"{key}:{column}")
        if _text(row["validation_method_version"]) != VALIDATION_METHOD_VERSION:
            stale.append(f"{key}:validation_method_version")
        validate_result_decision(row)
    if stale:
        raise MunicipalTemporalMergeError(
            f"Stale or changed validation rows: {sorted(set(stale))}"
        )

    merged = combined.sort_values(["evidence_hash", "evidence_id"], kind="stable").reset_index(
        drop=True
    )
    expected_shard_counts = {
        str(int(key)): int(value)
        for key, value in frozen["validation_shard_id"].value_counts().sort_index().items()
    }
    actual_shard_counts = {
        str(int(key)): int(value)
        for key, value in merged["validation_shard_id"].value_counts().sort_index().items()
    }
    if actual_shard_counts != expected_shard_counts:
        raise MunicipalTemporalMergeError(
            f"Per-shard counts drifted; expected={expected_shard_counts}, actual={actual_shard_counts}"
        )
    retryable = (
        merged["archive_query_status"].eq("request_error")
        | merged["capture_fetch_status"].eq("request_error")
        | merged["domain_reuse_status"].eq("request_error")
        | merged["temporal_validation_status"].eq("request_error_retryable")
    )
    audit: dict[str, object] = {
        "audit_version": MERGE_AUDIT_VERSION,
        "verdict": "pass",
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "validation_shard_count": validation_shard_count,
        "expected_claims": int(len(frozen)),
        "merged_claims": int(len(merged)),
        "exact_key_union": True,
        "duplicate_keys": 0,
        "missing_keys": 0,
        "unexpected_keys": 0,
        "stale_rows": 0,
        "expected_shard_counts": expected_shard_counts,
        "actual_shard_counts": actual_shard_counts,
        "archive_query_status_counts": {
            str(key): int(value)
            for key, value in merged["archive_query_status"].value_counts().sort_index().items()
        },
        "capture_fetch_status_counts": {
            str(key): int(value)
            for key, value in merged["capture_fetch_status"].value_counts().sort_index().items()
        },
        "temporal_validation_status_counts": {
            str(key): int(value)
            for key, value in merged["temporal_validation_status"].value_counts().sort_index().items()
        },
        "domain_reuse_status_counts": {
            str(key): int(value)
            for key, value in merged["domain_reuse_status"].value_counts().sort_index().items()
        },
        "proposed_evidence_grade_counts": {
            str(key): int(value)
            for key, value in merged["proposed_evidence_grade"].value_counts().sort_index().items()
        },
        "strict_coverage_eligible": int(
            merged["strict_coverage_eligible"].map(
                lambda value: _as_bool(value, "strict_coverage_eligible")
            ).sum()
        ),
        "retryable_claims": int(retryable.sum()),
    }
    return merged, audit


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
    os.replace(temporary, path)


def _atomic_json(payload: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def merge_validation_shards(
    *,
    frozen_claims_path: Path = DEFAULT_FROZEN_DIR / "eligible_claims.csv",
    freeze_manifest_path: Path = DEFAULT_FROZEN_DIR / "manifest.json",
    shard_dir: Path = DEFAULT_SHARD_DIR,
    merged_output_path: Path = DEFAULT_MERGED_OUTPUT,
    audit_output_path: Path = DEFAULT_AUDIT_OUTPUT,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
) -> dict[str, object]:
    """Load, validate, merge, and atomically write the current two-shard result."""
    frozen_claims_path = frozen_claims_path.resolve()
    freeze_manifest_path = freeze_manifest_path.resolve()
    shard_dir = shard_dir.resolve()
    merged_output_path = merged_output_path.resolve()
    audit_output_path = audit_output_path.resolve()
    if not frozen_claims_path.is_file() or not freeze_manifest_path.is_file():
        raise MunicipalTemporalMergeError("Frozen claims or freeze manifest is missing")
    manifest = json.loads(freeze_manifest_path.read_text(encoding="utf-8"))
    if manifest.get("freeze_contract_version") != FREEZE_CONTRACT_VERSION:
        raise MunicipalTemporalMergeError("Freeze manifest has the wrong contract version")
    if manifest.get("validation_method_version") != VALIDATION_METHOD_VERSION:
        raise MunicipalTemporalMergeError("Freeze manifest has an old method version")
    eligible_manifest = manifest.get("outputs", {}).get("eligible_claims", {})
    if eligible_manifest.get("sha256") != sha256_file(frozen_claims_path):
        raise MunicipalTemporalMergeError("Frozen eligible claims differ from their manifest hash")

    frozen = pd.read_csv(frozen_claims_path, keep_default_na=False)
    if int(eligible_manifest.get("rows", -1)) != len(frozen):
        raise MunicipalTemporalMergeError("Frozen eligible row count differs from its manifest")
    expected_names = {
        f"validation_shard_{shard_id}.csv" for shard_id in range(validation_shard_count)
    }
    if not shard_dir.is_dir():
        raise MunicipalTemporalMergeError(f"Validation shard directory is missing: {shard_dir}")
    actual_entries = {path.name for path in shard_dir.iterdir()}
    if actual_entries != expected_names:
        raise MunicipalTemporalMergeError(
            f"Validation shard directory differs; missing={sorted(expected_names-actual_entries)}, "
            f"unexpected={sorted(actual_entries-expected_names)}"
        )
    shard_paths = {
        shard_id: shard_dir / f"validation_shard_{shard_id}.csv"
        for shard_id in range(validation_shard_count)
    }
    shard_frames = {
        shard_id: pd.read_csv(path, keep_default_na=False)
        for shard_id, path in shard_paths.items()
    }
    merged, audit = audit_and_merge_frames(
        frozen, shard_frames, validation_shard_count=validation_shard_count
    )
    _atomic_csv(merged, merged_output_path)
    audit.update(
        {
            "inputs": {
                "frozen_claims": {
                    "path": _display_path(frozen_claims_path),
                    "bytes": frozen_claims_path.stat().st_size,
                    "sha256": sha256_file(frozen_claims_path),
                    "rows": int(len(frozen)),
                },
                "freeze_manifest": {
                    "path": _display_path(freeze_manifest_path),
                    "bytes": freeze_manifest_path.stat().st_size,
                    "sha256": sha256_file(freeze_manifest_path),
                },
                "validation_shards": [
                    {
                        "shard_id": shard_id,
                        "path": _display_path(path),
                        "bytes": path.stat().st_size,
                        "sha256": sha256_file(path),
                        "rows": int(len(shard_frames[shard_id])),
                    }
                    for shard_id, path in sorted(shard_paths.items())
                ],
            },
            "output": {
                "path": _display_path(merged_output_path),
                "bytes": merged_output_path.stat().st_size,
                "sha256": sha256_file(merged_output_path),
                "rows": int(len(merged)),
            },
            "freeze_filter_audit": manifest.get("summary", {}).get(
                "large_filter_audit", {}
            ),
        }
    )
    _atomic_json(audit, audit_output_path)
    return audit


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--frozen-claims", type=Path, default=DEFAULT_FROZEN_DIR / "eligible_claims.csv"
    )
    parser.add_argument(
        "--freeze-manifest", type=Path, default=DEFAULT_FROZEN_DIR / "manifest.json"
    )
    parser.add_argument("--shard-dir", type=Path, default=DEFAULT_SHARD_DIR)
    parser.add_argument("--merged-output", type=Path, default=DEFAULT_MERGED_OUTPUT)
    parser.add_argument("--audit-output", type=Path, default=DEFAULT_AUDIT_OUTPUT)
    parser.add_argument(
        "--validation-shard-count", type=int, default=VALIDATION_SHARD_COUNT
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    audit = merge_validation_shards(
        frozen_claims_path=args.frozen_claims,
        freeze_manifest_path=args.freeze_manifest,
        shard_dir=args.shard_dir,
        merged_output_path=args.merged_output,
        audit_output_path=args.audit_output,
        validation_shard_count=args.validation_shard_count,
    )
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
