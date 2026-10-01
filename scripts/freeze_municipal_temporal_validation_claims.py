#!/usr/bin/env python3
"""Freeze audited municipal URL claims for deterministic Wayback validation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from typing import Iterable, Mapping

import pandas as pd

from scripts.build_municipal_curated_acquisition_contract import (
    CANDIDATE_TARGET_COLUMNS,
    EVIDENCE_COLUMNS,
    audit_post_shard_directories,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
ACQUISITION_ROOT = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_acquisition"
)
DEFAULT_CONTRACT_DIR = ACQUISITION_ROOT / "audit"
DEFAULT_MERGED_DIR = ACQUISITION_ROOT / "merged"
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_wayback_validation/frozen"
)

FREEZE_CONTRACT_VERSION = "municipal-curated-wayback-freeze-v1"
VALIDATION_METHOD_VERSION = "public_wayback_exact_url_content_v4"
VALIDATION_SHARD_COUNT = 2
KEY_SEPARATOR = "\x1f"
ELIGIBLE_URL_TYPES = frozenset(
    {"custom_domain", "candidate_subdomain", "shared_host_path"}
)

DEFAULT_EXPECTED_ALL_CLAIMS = 40
DEFAULT_EXPECTED_COVERAGE_URL_TYPE_CLAIMS = 22
DEFAULT_EXPECTED_IDENTITY_SUPPORTED_CLAIMS = 39
DEFAULT_EXPECTED_ELIGIBLE_CLAIMS = 21

IDENTITY_SUPPORTED_STATUS = "identity_supported"
IDENTITY_ATTRIBUTION_STATUSES = frozenset(
    {IDENTITY_SUPPORTED_STATUS, "match_unresolved", "reject_identity"}
)

IDENTITY_DECISION_COLUMNS = [
    "evidence_id",
    "target_id",
    "source_id",
    "contest",
    "candidate_year_id",
    "assigned_candidate_name",
    "exact_normalized_url",
    "context_id",
    "source_locator",
    "association_kind",
    "context_candidate_name",
    "assigned_name_score",
    "best_roster_candidate_year_id",
    "best_roster_candidate_name",
    "best_roster_score",
    "runner_up_roster_score",
    "identity_attribution_status",
    "identity_attribution_reason",
    "required_attempt_status",
    "claim_approved_by_this_audit",
]

FROZEN_CLAIM_COLUMNS = [
    "freeze_contract_version",
    "validation_method_version",
    "validation_key",
    "evidence_hash",
    "evidence_record_hash",
    "context_hash",
    "identity_decision_hash",
    "validation_shard_id",
    "validation_shard_count",
    "coverage_url_type_eligible",
    "identity_gate_pass",
    "identity_attribution_status",
    "identity_attribution_reason",
    "eligible_for_temporal_validation",
    "eligibility_reason",
    "acquisition_contract_version",
    "evidence_id",
    "attempt_key",
    "target_id",
    "source_id",
    "parse_group_id",
    "acquisition_shard_id",
    "partition_hostname",
    "candidate_year_id",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "contest",
    "in_candidate_screen",
    "in_complete_contest_screen",
    "raw_url",
    "exact_normalized_url",
    "site_root_url",
    "registrable_domain",
    "url_type",
    "source_url",
    "source_date",
    "source_file_id",
    "source_file_sha256",
    "source_snapshot_timestamp",
    "evidence_text",
    "expected_website_field",
    "parser_id",
    "parser_version",
    "identity_match",
    "temporal_match",
    "evidence_grade",
    "review_status",
]


class MunicipalValidationFreezeError(ValueError):
    """Raised when acquisition evidence cannot be frozen without ambiguity."""


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
    raise MunicipalValidationFreezeError(f"{label} has invalid boolean {value!r}")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validation_key(
    evidence_id: object, candidate_year_id: object, exact_normalized_url: object
) -> str:
    """Return the exact three-field validation key agreed with the validator."""
    values = [_text(evidence_id), _text(candidate_year_id), _text(exact_normalized_url)]
    if any(not value for value in values):
        raise MunicipalValidationFreezeError("Validation key fields must all be nonempty")
    return KEY_SEPARATOR.join(values)


def validation_evidence_hash(key: str) -> str:
    """Hash the validation key; this hash owns the modulo-two shard assignment."""
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


def canonical_record_hash(row: Mapping[str, object], columns: Iterable[str]) -> str:
    payload = {column: _text(row[column]) for column in columns}
    encoded = json.dumps(
        payload, ensure_ascii=False, separators=(",", ":"), sort_keys=True
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != columns:
        raise MunicipalValidationFreezeError(
            f"{label} columns differ from the frozen contract; "
            f"missing={sorted(set(columns) - set(actual))}, "
            f"unexpected={sorted(set(actual) - set(columns))}, "
            f"order_matches={actual == columns}"
        )


def build_frozen_claim_frames(
    evidence: pd.DataFrame,
    candidate_roster: pd.DataFrame,
    identity_decisions: pd.DataFrame,
    *,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
    expected_all_claims: int | None = None,
    expected_coverage_url_type_claims: int | None = None,
    expected_identity_supported_claims: int | None = None,
    expected_eligible_claims: int | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Enrich and freeze acquisition claims without reading or writing external state."""
    if validation_shard_count <= 0:
        raise MunicipalValidationFreezeError("validation_shard_count must be positive")
    _require_exact_columns(evidence, EVIDENCE_COLUMNS, "acquisition evidence")
    _require_exact_columns(
        candidate_roster, CANDIDATE_TARGET_COLUMNS, "authoritative candidate roster"
    )
    _require_exact_columns(
        identity_decisions, IDENTITY_DECISION_COLUMNS, "identity decisions"
    )
    if evidence["evidence_id"].map(_text).eq("").any():
        raise MunicipalValidationFreezeError("Every claim requires a nonempty evidence_id")
    if evidence["evidence_id"].astype(str).duplicated().any():
        raise MunicipalValidationFreezeError("Duplicate evidence_id in acquisition claims")
    key_fields = ["evidence_id", "candidate_year_id", "exact_normalized_url"]
    if evidence.duplicated(key_fields).any():
        raise MunicipalValidationFreezeError("Duplicate three-field validation key")
    if identity_decisions["evidence_id"].map(_text).eq("").any():
        raise MunicipalValidationFreezeError("Every identity decision requires evidence_id")
    if identity_decisions["evidence_id"].astype(str).duplicated().any():
        raise MunicipalValidationFreezeError("Duplicate identity-decision evidence_id")
    evidence_ids = set(evidence["evidence_id"].astype(str))
    decision_ids = set(identity_decisions["evidence_id"].astype(str))
    if evidence_ids != decision_ids:
        raise MunicipalValidationFreezeError(
            "Identity-decision evidence IDs differ from acquisition evidence; "
            f"missing={sorted(evidence_ids-decision_ids)}, "
            f"unexpected={sorted(decision_ids-evidence_ids)}"
        )
    statuses = set(identity_decisions["identity_attribution_status"].map(_text))
    unexpected_statuses = statuses - set(IDENTITY_ATTRIBUTION_STATUSES)
    if unexpected_statuses:
        raise MunicipalValidationFreezeError(
            f"Unknown identity-attribution statuses: {sorted(unexpected_statuses)}"
        )
    if identity_decisions["identity_attribution_reason"].map(_text).eq("").any():
        raise MunicipalValidationFreezeError(
            "Every identity decision requires an attribution reason"
        )
    approved = identity_decisions["claim_approved_by_this_audit"].map(
        lambda value: _as_bool(value, "claim_approved_by_this_audit")
    )
    if approved.any():
        raise MunicipalValidationFreezeError(
            "Identity attribution may not approve URL claims"
        )

    roster_key = ["target_id", "candidate_year_id"]
    if candidate_roster.duplicated(roster_key).any():
        raise MunicipalValidationFreezeError(
            "Authoritative candidate roster has duplicate target/candidate keys"
        )

    evidence_work = evidence.copy()
    evidence_work["evidence_record_hash"] = [
        canonical_record_hash(row, EVIDENCE_COLUMNS)
        for row in evidence_work.to_dict(orient="records")
    ]
    identity_work = identity_decisions.copy()
    identity_work["identity_decision_hash"] = [
        canonical_record_hash(row, IDENTITY_DECISION_COLUMNS)
        for row in identity_work.to_dict(orient="records")
    ]
    roster_work = candidate_roster.copy()
    roster_work["context_hash"] = [
        canonical_record_hash(row, CANDIDATE_TARGET_COLUMNS)
        for row in roster_work.to_dict(orient="records")
    ]

    context_renames = {
        "contract_version": "context_contract_version",
        "attempt_key": "context_attempt_key",
        "source_id": "context_source_id",
        "shard_id": "context_acquisition_shard_id",
        "contest": "context_contest",
    }
    roster_work = roster_work.rename(columns=context_renames)
    context_columns = roster_key + [
        "context_contract_version",
        "context_attempt_key",
        "context_source_id",
        "context_acquisition_shard_id",
        "partition_hostname",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "context_contest",
        "in_candidate_screen",
        "in_complete_contest_screen",
        "context_hash",
    ]
    merged = evidence_work.merge(
        roster_work[context_columns],
        on=roster_key,
        how="left",
        validate="many_to_one",
        indicator=True,
    )
    unmatched = merged.loc[merged["_merge"].ne("both"), roster_key]
    if len(unmatched):
        raise MunicipalValidationFreezeError(
            "Claims are absent from the authoritative 264-row candidate roster: "
            f"{unmatched.astype(str).agg('|'.join, axis=1).tolist()}"
        )
    merged = merged.drop(columns="_merge")

    decision_renames = {
        "target_id": "decision_target_id",
        "source_id": "decision_source_id",
        "contest": "decision_contest",
        "candidate_year_id": "decision_candidate_year_id",
        "exact_normalized_url": "decision_exact_normalized_url",
    }
    identity_work = identity_work.rename(columns=decision_renames)
    decision_columns = [
        "evidence_id",
        "decision_target_id",
        "decision_source_id",
        "decision_contest",
        "decision_candidate_year_id",
        "assigned_candidate_name",
        "decision_exact_normalized_url",
        "identity_attribution_status",
        "identity_attribution_reason",
        "identity_decision_hash",
    ]
    merged = merged.merge(
        identity_work[decision_columns],
        on="evidence_id",
        how="left",
        validate="one_to_one",
        indicator="identity_merge",
    )
    if not merged["identity_merge"].eq("both").all():
        raise MunicipalValidationFreezeError("Identity-decision merge was not exact")
    merged = merged.drop(columns="identity_merge")

    echo_pairs = [
        ("contract_version", "context_contract_version"),
        ("attempt_key", "context_attempt_key"),
        ("source_id", "context_source_id"),
        ("shard_id", "context_acquisition_shard_id"),
        ("contest", "context_contest"),
    ]
    for evidence_column, context_column in echo_pairs:
        mismatch = merged[evidence_column].map(_text).ne(merged[context_column].map(_text))
        if mismatch.any():
            bad_ids = merged.loc[mismatch, "evidence_id"].astype(str).tolist()
            raise MunicipalValidationFreezeError(
                f"Evidence/context mismatch for {evidence_column}: {bad_ids}"
            )
    decision_echo_pairs = [
        ("target_id", "decision_target_id"),
        ("source_id", "decision_source_id"),
        ("contest", "decision_contest"),
        ("candidate_year_id", "decision_candidate_year_id"),
        ("exact_normalized_url", "decision_exact_normalized_url"),
        ("full_name", "assigned_candidate_name"),
    ]
    for evidence_column, decision_column in decision_echo_pairs:
        mismatch = merged[evidence_column].map(_text).ne(
            merged[decision_column].map(_text)
        )
        if mismatch.any():
            bad_ids = merged.loc[mismatch, "evidence_id"].astype(str).tolist()
            raise MunicipalValidationFreezeError(
                f"Evidence/identity-decision mismatch for {evidence_column}: {bad_ids}"
            )

    merged["validation_key"] = [
        validation_key(row.evidence_id, row.candidate_year_id, row.exact_normalized_url)
        for row in merged.itertuples(index=False)
    ]
    merged["evidence_hash"] = merged["validation_key"].map(validation_evidence_hash)
    merged["validation_shard_id"] = merged["evidence_hash"].map(
        lambda value: int(value, 16) % validation_shard_count
    )
    merged["validation_shard_count"] = validation_shard_count
    merged["coverage_url_type_eligible"] = merged["url_type"].isin(
        ELIGIBLE_URL_TYPES
    )
    merged["identity_gate_pass"] = merged["identity_attribution_status"].eq(
        IDENTITY_SUPPORTED_STATUS
    )
    merged["eligible_for_temporal_validation"] = (
        merged["coverage_url_type_eligible"] & merged["identity_gate_pass"]
    )
    merged["eligibility_reason"] = merged.apply(
        lambda row: (
            "eligible_candidate_website_url_type_and_identity"
            if row["eligible_for_temporal_validation"]
            else (
                f"identity_gate_failed:{row['identity_attribution_status']}"
                if row["coverage_url_type_eligible"]
                else f"ineligible_url_type:{row['url_type']}"
            )
        ),
        axis=1,
    )
    merged["freeze_contract_version"] = FREEZE_CONTRACT_VERSION
    merged["validation_method_version"] = VALIDATION_METHOD_VERSION
    merged = merged.rename(
        columns={
            "contract_version": "acquisition_contract_version",
            "shard_id": "acquisition_shard_id",
        }
    )

    redundant_context_columns = [
        "context_contract_version",
        "context_attempt_key",
        "context_source_id",
        "context_acquisition_shard_id",
        "context_contest",
        "decision_target_id",
        "decision_source_id",
        "decision_contest",
        "decision_candidate_year_id",
        "decision_exact_normalized_url",
        "assigned_candidate_name",
    ]
    merged = merged.drop(columns=redundant_context_columns)
    all_claims = merged[FROZEN_CLAIM_COLUMNS].sort_values(
        ["evidence_hash", "evidence_id"], kind="stable"
    ).reset_index(drop=True)
    _require_exact_columns(all_claims, FROZEN_CLAIM_COLUMNS, "frozen all-claims output")

    if all_claims["validation_key"].duplicated().any():
        raise MunicipalValidationFreezeError("Frozen validation keys are not unique")
    if all_claims["evidence_hash"].duplicated().any():
        raise MunicipalValidationFreezeError("Frozen evidence hashes are not unique")
    expected_shards = all_claims["evidence_hash"].map(
        lambda value: int(value, 16) % validation_shard_count
    )
    if not all_claims["validation_shard_id"].astype(int).equals(expected_shards.astype(int)):
        raise MunicipalValidationFreezeError("Frozen validation shard assignment drifted")

    eligible = all_claims.loc[
        all_claims["eligible_for_temporal_validation"].astype(bool)
    ].reset_index(drop=True)
    if not set(eligible["url_type"]).issubset(ELIGIBLE_URL_TYPES):
        raise MunicipalValidationFreezeError("Ineligible URL type entered validation claims")
    coverage_type_claims = all_claims.loc[all_claims["coverage_url_type_eligible"]]
    if set(coverage_type_claims["url_type"]) - set(ELIGIBLE_URL_TYPES):
        raise MunicipalValidationFreezeError("Ineligible URL type passed the coverage-type gate")
    if set(
        all_claims.loc[~all_claims["coverage_url_type_eligible"], "url_type"]
    ) & set(ELIGIBLE_URL_TYPES):
        raise MunicipalValidationFreezeError("Eligible URL type failed the coverage-type gate")
    if not eligible["identity_gate_pass"].all():
        raise MunicipalValidationFreezeError("Identity-unresolved claim entered validation claims")

    if expected_all_claims is not None and len(all_claims) != expected_all_claims:
        raise MunicipalValidationFreezeError(
            f"Expected {expected_all_claims} all claims, found {len(all_claims)}"
        )
    if (
        expected_coverage_url_type_claims is not None
        and len(coverage_type_claims) != expected_coverage_url_type_claims
    ):
        raise MunicipalValidationFreezeError(
            f"Expected {expected_coverage_url_type_claims} coverage URL-type claims, "
            f"found {len(coverage_type_claims)}"
        )
    identity_supported_n = int(all_claims["identity_gate_pass"].sum())
    if (
        expected_identity_supported_claims is not None
        and identity_supported_n != expected_identity_supported_claims
    ):
        raise MunicipalValidationFreezeError(
            f"Expected {expected_identity_supported_claims} identity-supported claims, "
            f"found {identity_supported_n}"
        )
    if expected_eligible_claims is not None and len(eligible) != expected_eligible_claims:
        raise MunicipalValidationFreezeError(
            f"Expected {expected_eligible_claims} eligible claims, found {len(eligible)}"
        )

    excluded_n = len(all_claims) - len(eligible)
    summary = {
        "all_claims": int(len(all_claims)),
        "coverage_url_type_claims": int(len(coverage_type_claims)),
        "identity_gate_pass_claims": identity_supported_n,
        "identity_gate_failed_claims": int((~all_claims["identity_gate_pass"]).sum()),
        "eligible_claims": int(len(eligible)),
        "excluded_claims": int(excluded_n),
        "excluded_fraction": excluded_n / len(all_claims) if len(all_claims) else 0.0,
        "url_type_counts": {
            str(key): int(value)
            for key, value in all_claims["url_type"].value_counts().sort_index().items()
        },
        "coverage_url_type_counts": {
            str(key): int(value)
            for key, value in coverage_type_claims["url_type"].value_counts().sort_index().items()
        },
        "eligible_url_type_counts": {
            str(key): int(value)
            for key, value in eligible["url_type"].value_counts().sort_index().items()
        },
        "validation_shard_counts": {
            str(int(key)): int(value)
            for key, value in eligible["validation_shard_id"].value_counts().sort_index().items()
        },
        "excluded_url_type_counts": {
            str(key): int(value)
            for key, value in all_claims.loc[
                ~all_claims["coverage_url_type_eligible"], "url_type"
            ].value_counts().sort_index().items()
        },
        "identity_gate_failure_counts": {
            str(key): int(value)
            for key, value in all_claims.loc[
                ~all_claims["identity_gate_pass"], "identity_attribution_status"
            ].value_counts().sort_index().items()
        },
        "large_filter_audit": {
            "triggered": bool(len(all_claims) and excluded_n / len(all_claims) > 0.20),
            "before": int(len(all_claims)),
            "after": int(len(eligible)),
            "dropped": int(excluded_n),
            "drop_fraction": excluded_n / len(all_claims) if len(all_claims) else 0.0,
            "rule": "Network validation requires both a coverage-eligible URL type and an identity-supported source-to-roster match. All excluded claims remain in all_claims.csv.",
        },
    }
    return all_claims, eligible, summary


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT))
    except ValueError:
        return str(resolved)


def _file_record(path: Path) -> dict[str, object]:
    return {
        "path": _display_path(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def _manifest_repo_path(value: object) -> Path:
    token = _text(value)
    path = (REPO_ROOT / token).resolve()
    try:
        path.relative_to(REPO_ROOT)
    except ValueError as error:
        raise MunicipalValidationFreezeError(
            f"Merged manifest path escapes the repository: {token!r}"
        ) from error
    return path


def load_audited_merged_inputs(
    merged_dir: Path,
    shard_evidence: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object], list[Path]]:
    """Verify the independent merged bundle and exact agreement with shard claims."""
    merged_dir = merged_dir.resolve()
    expected_names = {
        "attempts.csv",
        "downloaded_files.csv",
        "evidence.csv",
        "source_contexts.csv",
        "identity_decisions.csv",
        "audit.json",
        "manifest.json",
    }
    if not merged_dir.is_dir():
        raise MunicipalValidationFreezeError(f"Merged acquisition directory is missing: {merged_dir}")
    actual_names = {path.name for path in merged_dir.iterdir()}
    if actual_names != expected_names:
        raise MunicipalValidationFreezeError(
            "Merged acquisition directory differs from its exact interface; "
            f"missing={sorted(expected_names-actual_names)}, "
            f"unexpected={sorted(actual_names-expected_names)}"
        )
    manifest_path = merged_dir / "manifest.json"
    audit_path = merged_dir / "audit.json"
    evidence_path = merged_dir / "evidence.csv"
    decisions_path = merged_dir / "identity_decisions.csv"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != "municipal-curated-merged-manifest-v1":
        raise MunicipalValidationFreezeError("Merged acquisition manifest has a wrong schema")

    for section in ["inputs", "outputs"]:
        records = manifest.get(section)
        if not isinstance(records, list) or not records:
            raise MunicipalValidationFreezeError(f"Merged manifest lacks {section}")
        for record in records:
            path = _manifest_repo_path(record.get("path"))
            if not path.is_file():
                raise MunicipalValidationFreezeError(f"Merged manifest file is missing: {path}")
            if int(record.get("bytes", -1)) != path.stat().st_size:
                raise MunicipalValidationFreezeError(f"Merged manifest byte drift: {path}")
            if _text(record.get("sha256")) != sha256_file(path):
                raise MunicipalValidationFreezeError(f"Merged manifest hash drift: {path}")
    output_paths = {_manifest_repo_path(row.get("path")) for row in manifest["outputs"]}
    expected_output_paths = {
        merged_dir / name for name in expected_names if name != "manifest.json"
    }
    if output_paths != expected_output_paths:
        raise MunicipalValidationFreezeError(
            "Merged manifest output set differs from the merged directory"
        )

    audit = json.loads(audit_path.read_text(encoding="utf-8"))
    if audit.get("schema_version") != "municipal-curated-merged-audit-v1":
        raise MunicipalValidationFreezeError("Merged acquisition audit has a wrong schema")
    structural = audit.get("structural_audit", {})
    identity = audit.get("identity_attribution", {})
    key_checks = audit.get("key_checks", {})
    if structural.get("verdict") != "pass" or int(structural.get("evidence_rows", -1)) != 40:
        raise MunicipalValidationFreezeError("Merged structural acquisition audit did not pass")
    if not all(
        key_checks.get(key) is True
        for key in [
            "attempt_keys_exact",
            "evidence_context_keys_exact",
            "evidence_decision_keys_exact",
        ]
    ):
        raise MunicipalValidationFreezeError("Merged identity key audit did not pass")
    if (
        int(identity.get("evidence_rows", -1)) != 40
        or int(identity.get("identity_supported", -1)) != 39
        or int(identity.get("match_unresolved", -1)) != 1
        or int(identity.get("reject_identity", -1)) != 0
    ):
        raise MunicipalValidationFreezeError("Merged identity decision counts drifted")

    merged_evidence = pd.read_csv(evidence_path, keep_default_na=False)
    decisions = pd.read_csv(decisions_path, keep_default_na=False)
    _require_exact_columns(merged_evidence, EVIDENCE_COLUMNS, "merged acquisition evidence")
    _require_exact_columns(decisions, IDENTITY_DECISION_COLUMNS, "merged identity decisions")
    shard_hashes = {
        _text(row["evidence_id"]): canonical_record_hash(row, EVIDENCE_COLUMNS)
        for row in shard_evidence.to_dict(orient="records")
    }
    merged_hashes = {
        _text(row["evidence_id"]): canonical_record_hash(row, EVIDENCE_COLUMNS)
        for row in merged_evidence.to_dict(orient="records")
    }
    if shard_hashes != merged_hashes:
        raise MunicipalValidationFreezeError(
            "Merged evidence rows differ from the exact two-shard evidence union"
        )
    merged_audit = {
        "verdict": "pass",
        "manifest_sha256": sha256_file(manifest_path),
        "evidence_sha256": sha256_file(evidence_path),
        "identity_decisions_sha256": sha256_file(decisions_path),
        "identity_supported": int(identity["identity_supported"]),
        "match_unresolved": int(identity["match_unresolved"]),
        "reject_identity": int(identity["reject_identity"]),
        "exact_shard_evidence_union": True,
        "exact_identity_decision_keys": True,
    }
    return (
        merged_evidence,
        decisions,
        merged_audit,
        sorted(path for path in merged_dir.iterdir() if path.is_file()),
    )


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


def freeze_claims(
    *,
    acquisition_root: Path = ACQUISITION_ROOT,
    contract_dir: Path = DEFAULT_CONTRACT_DIR,
    merged_dir: Path = DEFAULT_MERGED_DIR,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    validation_shard_count: int = VALIDATION_SHARD_COUNT,
    expected_all_claims: int | None = DEFAULT_EXPECTED_ALL_CLAIMS,
    expected_coverage_url_type_claims: int | None = DEFAULT_EXPECTED_COVERAGE_URL_TYPE_CLAIMS,
    expected_identity_supported_claims: int | None = DEFAULT_EXPECTED_IDENTITY_SUPPORTED_CLAIMS,
    expected_eligible_claims: int | None = DEFAULT_EXPECTED_ELIGIBLE_CLAIMS,
) -> dict[str, object]:
    """Audit both acquisition shards and atomically freeze all and eligible claims."""
    acquisition_root = acquisition_root.resolve()
    contract_dir = contract_dir.resolve()
    merged_dir = merged_dir.resolve()
    output_dir = output_dir.resolve()
    contract_path = contract_dir / "acquisition_targets.csv"
    roster_path = contract_dir / "acquisition_candidate_targets.csv"
    parser_path = contract_dir / "parser_support_matrix.csv"
    shard_roots = {
        shard_id: acquisition_root / f"shard_{shard_id}"
        for shard_id in range(validation_shard_count)
    }
    required = [contract_path, roster_path, parser_path]
    for shard_root in shard_roots.values():
        required.extend(
            [
                shard_root / "attempts.csv",
                shard_root / "downloaded_files.csv",
                shard_root / "evidence.csv",
            ]
        )
    missing = [str(path) for path in required if not path.is_file()]
    if missing:
        raise MunicipalValidationFreezeError(f"Missing freeze inputs: {missing}")

    targets = pd.read_csv(contract_path, keep_default_na=False)
    roster = pd.read_csv(roster_path, keep_default_na=False)
    parser_matrix = pd.read_csv(parser_path, keep_default_na=False)
    acquisition_audit = audit_post_shard_directories(
        targets, roster, parser_matrix, shard_roots
    )
    if acquisition_audit.get("verdict") != "pass":
        raise MunicipalValidationFreezeError("Acquisition post-shard audit did not pass")

    shard_evidence = pd.concat(
        [
            pd.read_csv(shard_roots[shard_id] / "evidence.csv", keep_default_na=False)
            for shard_id in sorted(shard_roots)
        ],
        ignore_index=True,
    )
    evidence, identity_decisions, merged_acquisition_audit, merged_input_paths = (
        load_audited_merged_inputs(merged_dir, shard_evidence)
    )
    all_claims, eligible_claims, summary = build_frozen_claim_frames(
        evidence,
        roster,
        identity_decisions,
        validation_shard_count=validation_shard_count,
        expected_all_claims=expected_all_claims,
        expected_coverage_url_type_claims=expected_coverage_url_type_claims,
        expected_identity_supported_claims=expected_identity_supported_claims,
        expected_eligible_claims=expected_eligible_claims,
    )

    all_path = output_dir / "all_claims.csv"
    eligible_path = output_dir / "eligible_claims.csv"
    manifest_path = output_dir / "manifest.json"
    _atomic_csv(all_claims, all_path)
    _atomic_csv(eligible_claims, eligible_path)

    input_paths = [contract_path, roster_path, parser_path]
    for shard_root in shard_roots.values():
        input_paths.extend(
            sorted(path for path in shard_root.rglob("*") if path.is_file())
        )
    input_paths.extend(merged_input_paths)
    manifest: dict[str, object] = {
        "manifest_version": "municipal-curated-wayback-freeze-manifest-v1",
        "freeze_contract_version": FREEZE_CONTRACT_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "validation_key_fields": [
            "evidence_id",
            "candidate_year_id",
            "exact_normalized_url",
        ],
        "validation_key_separator": "U+001F",
        "evidence_hash_definition": "sha256(utf8(evidence_id + U+001F + candidate_year_id + U+001F + exact_normalized_url))",
        "evidence_record_hash_definition": "sha256(canonical JSON of the exact acquisition evidence row)",
        "context_hash_definition": "sha256(canonical JSON of the exact authoritative candidate-target row)",
        "identity_decision_hash_definition": "sha256(canonical JSON of the exact independent merged identity-decision row)",
        "validation_shard_definition": f"int(evidence_hash, 16) modulo {validation_shard_count}",
        "validation_shard_count": validation_shard_count,
        "eligible_url_types": sorted(ELIGIBLE_URL_TYPES),
        "identity_gate_pass_status": IDENTITY_SUPPORTED_STATUS,
        "frozen_claim_columns": FROZEN_CLAIM_COLUMNS,
        "acquisition_audit": acquisition_audit,
        "merged_acquisition_audit": merged_acquisition_audit,
        "identity_decisions_source": {
            **_file_record(merged_dir / "identity_decisions.csv"),
            "rows": int(len(identity_decisions)),
        },
        "merged_acquisition_manifest": _file_record(merged_dir / "manifest.json"),
        "summary": summary,
        "inputs": [_file_record(path) for path in sorted(set(input_paths))],
        "outputs": {
            "all_claims": {**_file_record(all_path), "rows": int(len(all_claims))},
            "eligible_claims": {
                **_file_record(eligible_path),
                "rows": int(len(eligible_claims)),
            },
        },
    }
    _atomic_json(manifest, manifest_path)
    return manifest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--acquisition-root", type=Path, default=ACQUISITION_ROOT)
    parser.add_argument("--contract-dir", type=Path, default=DEFAULT_CONTRACT_DIR)
    parser.add_argument("--merged-dir", type=Path, default=DEFAULT_MERGED_DIR)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument(
        "--expected-all-claims", type=int, default=DEFAULT_EXPECTED_ALL_CLAIMS
    )
    parser.add_argument(
        "--expected-coverage-url-type-claims",
        type=int,
        default=DEFAULT_EXPECTED_COVERAGE_URL_TYPE_CLAIMS,
    )
    parser.add_argument(
        "--expected-identity-supported-claims",
        type=int,
        default=DEFAULT_EXPECTED_IDENTITY_SUPPORTED_CLAIMS,
    )
    parser.add_argument(
        "--expected-eligible-claims",
        type=int,
        default=DEFAULT_EXPECTED_ELIGIBLE_CLAIMS,
    )
    parser.add_argument(
        "--validation-shard-count", type=int, default=VALIDATION_SHARD_COUNT
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    manifest = freeze_claims(
        acquisition_root=args.acquisition_root,
        contract_dir=args.contract_dir,
        merged_dir=args.merged_dir,
        output_dir=args.output_dir,
        validation_shard_count=args.validation_shard_count,
        expected_all_claims=args.expected_all_claims,
        expected_coverage_url_type_claims=args.expected_coverage_url_type_claims,
        expected_identity_supported_claims=args.expected_identity_supported_claims,
        expected_eligible_claims=args.expected_eligible_claims,
    )
    print(json.dumps(manifest["summary"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
