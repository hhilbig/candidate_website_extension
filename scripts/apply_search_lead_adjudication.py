#!/usr/bin/env python3
"""Apply explicit human decisions to merged search-lead archive validation.

The merged validator output remains immutable.  Decisions are keyed by the
stable URL-claim ID and must cover the exact set of proposed grade-A rows.
The emitted adapter evidence can be consumed by build_mayoral_evidence_claims.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import tempfile
from pathlib import Path

import pandas as pd


SOURCE_ID = "serper_search_source_expansion_2010_2021"
PARSER_VERSION = "search_lead_adjudication_v1"
DECISION_COLUMNS = [
    "url_claim_id",
    "decision",
    "reviewer",
    "reviewed_on",
    "decision_source",
    "decision_note",
]
ADAPTER_COLUMNS = [
    "claim_id",
    "source_id",
    "parser_version",
    "candidate_year_id",
    "candidate_contest_id",
    "raw_url",
    "exact_normalized_url",
    "url_type",
    "source_date",
    "source_snapshot_timestamp",
    "source_profile_url",
    "url_source",
    "evidence_text",
    "discovery_method",
    "identity_match",
    "temporal_match",
    "evidence_grade",
    "review_status",
    "acceptance",
    "validation_key",
    "validation_hash",
]


class SearchLeadAdjudicationError(ValueError):
    """Adjudication inputs do not identify the frozen proposed claims exactly."""


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame.columns.tolist() != columns:
        raise SearchLeadAdjudicationError(
            f"{label} columns differ: expected {columns}, got {frame.columns.tolist()}"
        )


def apply_decisions(
    validation: pd.DataFrame, decisions: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, object]]:
    required_validation = {
        "url_claim_id",
        "validation_key",
        "validation_hash",
        "candidate_year_id",
        "candidate_contest_id",
        "raw_url",
        "exact_normalized_url",
        "url_type",
        "capture_effective_timestamp",
        "capture_effective_url",
        "evidence_text",
        "candidate_identity_match",
        "mayoral_campaign_match",
        "city_match",
        "domain_reuse_status",
        "proposed_evidence_grade",
        "strict_coverage_eligible_result",
        "temporal_validation_status",
    }
    missing = sorted(required_validation - set(validation.columns))
    if missing:
        raise SearchLeadAdjudicationError(
            f"validation is missing required columns: {missing}"
        )
    _require_exact_columns(decisions, DECISION_COLUMNS, "decisions")

    proposed = validation.loc[
        validation["proposed_evidence_grade"].map(_text).eq("A")
    ].copy()
    if proposed.empty:
        raise SearchLeadAdjudicationError("validation has no proposed grade-A rows")
    for label, frame in (("proposed claims", proposed), ("decisions", decisions)):
        ids = frame["url_claim_id"].map(_text)
        if ids.eq("").any() or ids.duplicated().any():
            raise SearchLeadAdjudicationError(
                f"{label} has missing or duplicate url_claim_id values"
            )
    proposed_ids = set(proposed["url_claim_id"].map(_text))
    decision_ids = set(decisions["url_claim_id"].map(_text))
    if proposed_ids != decision_ids:
        raise SearchLeadAdjudicationError(
            "decision IDs do not equal the proposed grade-A IDs; "
            f"missing={sorted(proposed_ids - decision_ids)}, "
            f"unexpected={sorted(decision_ids - proposed_ids)}"
        )

    normalized_decisions = decisions.copy()
    normalized_decisions["decision"] = normalized_decisions["decision"].map(
        lambda value: _text(value).casefold()
    )
    invalid = ~normalized_decisions["decision"].isin({"approved", "rejected"})
    if invalid.any():
        raise SearchLeadAdjudicationError(
            "decisions must be either approved or rejected"
        )
    for column in ("reviewer", "reviewed_on", "decision_source"):
        if normalized_decisions[column].map(_text).eq("").any():
            raise SearchLeadAdjudicationError(f"decisions has blank {column}")

    joined = proposed.merge(
        normalized_decisions,
        on="url_claim_id",
        how="left",
        validate="one_to_one",
    )
    content_ok = (
        joined["candidate_identity_match"].astype(str).str.casefold().eq("true")
        & joined["mayoral_campaign_match"].astype(str).str.casefold().eq("true")
        & joined["city_match"].astype(str).str.casefold().eq("true")
        & joined["domain_reuse_status"].map(_text).eq("same_candidate_only")
        & joined["strict_coverage_eligible_result"]
        .astype(str)
        .str.casefold()
        .eq("true")
    )
    approved = joined["decision"].eq("approved")
    if (approved & ~content_ok).any():
        bad = joined.loc[approved & ~content_ok, "url_claim_id"].tolist()
        raise SearchLeadAdjudicationError(
            f"approved claims do not satisfy the grade-A content gates: {bad}"
        )

    evidence = pd.DataFrame(
        {
            "claim_id": joined["url_claim_id"].map(_text),
            "source_id": SOURCE_ID,
            "parser_version": PARSER_VERSION,
            "candidate_year_id": joined["candidate_year_id"].map(_text),
            "candidate_contest_id": joined["candidate_contest_id"].map(_text),
            "raw_url": joined["raw_url"].map(_text),
            "exact_normalized_url": joined["exact_normalized_url"].map(_text),
            "url_type": joined["url_type"].map(_text),
            "source_date": joined["capture_effective_timestamp"].map(
                lambda value: _text(value)[:8]
            ),
            "source_snapshot_timestamp": joined["capture_effective_timestamp"].map(
                _text
            ),
            "source_profile_url": joined["capture_effective_url"].map(_text),
            "url_source": "serper_result_or_source_page_plus_archived_campaign_content",
            "evidence_text": joined["evidence_text"].map(_text),
            "discovery_method": "serper_one_query_source_expansion_wayback_validation",
            "identity_match": content_ok,
            "temporal_match": content_ok,
            "evidence_grade": joined["proposed_evidence_grade"].map(_text),
            "review_status": joined["decision"],
            "acceptance": approved,
            "validation_key": joined["validation_key"].map(_text),
            "validation_hash": joined["validation_hash"].map(_text),
        },
        columns=ADAPTER_COLUMNS,
    ).sort_values(["candidate_year_id", "claim_id"]).reset_index(drop=True)

    audit = {
        "contract_version": PARSER_VERSION,
        "source_id": SOURCE_ID,
        "validation_rows": int(len(validation)),
        "proposed_grade_a_rows": int(len(proposed)),
        "decision_rows": int(len(decisions)),
        "approved_rows": int(approved.sum()),
        "rejected_rows": int((~approved).sum()),
        "strict_adapter_rows": int(
            (evidence["acceptance"] & evidence["evidence_grade"].isin({"A", "B"})).sum()
        ),
        "exact_proposed_decision_id_agreement": True,
    }
    return evidence, audit


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temp_path = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
    temp_path.replace(path)


def _atomic_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temp_path = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temp_path.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--validation", type=Path, required=True)
    parser.add_argument("--decisions", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args()

    evidence, audit = apply_decisions(
        pd.read_csv(args.validation, keep_default_na=False, low_memory=False),
        pd.read_csv(args.decisions, keep_default_na=False),
    )
    _atomic_csv(evidence, args.output)
    audit.update(
        {
            "validation_file": str(args.validation),
            "validation_sha256": _sha256(args.validation),
            "decision_file": str(args.decisions),
            "decision_sha256": _sha256(args.decisions),
            "output_file": str(args.output),
            "output_rows": int(len(evidence)),
        }
    )
    _atomic_json(audit, args.audit_output)
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
