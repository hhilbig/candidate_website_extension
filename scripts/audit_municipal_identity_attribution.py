#!/usr/bin/env python3
"""Audit candidate attribution for municipal-source URL evidence.

This module is deliberately claim-agnostic.  ``identity_supported`` means only
that one source association supports assigning the URL to the named candidate;
it never accepts a URL claim or assigns an evidence grade.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Iterable

import pandas as pd
from rapidfuzz import fuzz

from scripts.extract_municipal_mayoral_urls import normalize_exact_url
from src.mayoral_discovery import normalize_person_name


SOURCE_CONTEXT_COLUMNS = [
    "context_id",
    "evidence_id",
    "source_id",
    "source_file_id",
    "exact_normalized_url",
    "source_locator",
    "association_kind",
    "context_candidate_name",
    "context_text",
    "nearest_distance",
    "runner_up_distance",
    "explicit_separate_evidence",
    "separate_evidence_note",
]

DECISION_COLUMNS = [
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

DIRECT_ASSOCIATIONS = {
    "direct_anchor_text",
    "same_row_field",
    "manual_exact_context",
}
NEAREST_ASSOCIATIONS = {
    "pdf_annotation_nearest_name",
    "pdf_visible_url_nearest_name",
}
PAGE_ASSOCIATIONS = {"page_context"}
ASSOCIATION_KINDS = DIRECT_ASSOCIATIONS | NEAREST_ASSOCIATIONS | PAGE_ASSOCIATIONS
NEAREST_DISTANCE_MARGIN = 12.0
SUPPORTED_NAME_SCORE = 94.0
SUPPORTED_NAME_MARGIN = 8.0
DIFFERENT_PERSON_SCORE = 70.0


class IdentityAttributionError(ValueError):
    """Raised when attribution inputs violate the offline audit contract."""


@dataclass(frozen=True)
class _NameAssessment:
    status: str
    reason: str
    assigned_score: float
    best_id: str
    best_name: str
    best_score: float
    runner_up_score: float


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def _bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).casefold()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n", ""}:
        return False
    raise IdentityAttributionError(f"Invalid boolean value {value!r}")


def _required_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(frame.columns))
    if missing:
        raise IdentityAttributionError(f"{label} is missing columns: {missing}")


def _json_string_list(value: object, label: str) -> list[str]:
    try:
        result = json.loads(_text(value))
    except json.JSONDecodeError as error:
        raise IdentityAttributionError(f"{label} is not valid JSON") from error
    if not isinstance(result, list) or any(not isinstance(item, str) for item in result):
        raise IdentityAttributionError(f"{label} must be a JSON string array")
    if len(result) != len(set(result)):
        raise IdentityAttributionError(f"{label} contains duplicate candidate IDs")
    return result


def validate_full_contest_rosters(
    targets: pd.DataFrame, candidate_roster: pd.DataFrame
) -> dict[str, int]:
    """Require an exact full-contest roster for every source target."""
    _required_columns(
        targets,
        [
            "target_id", "source_id", "contest", "unresolved_candidate_n",
            "acquisition_candidate_ids_json",
        ],
        "target contract",
    )
    _required_columns(
        candidate_roster,
        ["target_id", "source_id", "contest", "candidate_year_id", "full_name"],
        "candidate roster",
    )
    if targets["target_id"].astype(str).duplicated().any():
        raise IdentityAttributionError("Target contract has duplicate target_id values")
    if candidate_roster.duplicated(["target_id", "candidate_year_id"]).any():
        raise IdentityAttributionError("Candidate roster has duplicate target/candidate keys")
    expected_targets = set(targets["target_id"].astype(str))
    actual_targets = set(candidate_roster["target_id"].astype(str))
    if actual_targets != expected_targets:
        raise IdentityAttributionError(
            "Candidate roster target mismatch; "
            f"missing={sorted(expected_targets-actual_targets)}, "
            f"unexpected={sorted(actual_targets-expected_targets)}"
        )

    roster_by_target = {
        str(target_id): group.copy()
        for target_id, group in candidate_roster.groupby("target_id", sort=False)
    }
    for target in targets.itertuples(index=False):
        target_id = str(target.target_id)
        roster = roster_by_target[target_id]
        expected_ids = set(
            _json_string_list(
                target.acquisition_candidate_ids_json,
                f"{target_id}.acquisition_candidate_ids_json",
            )
        )
        actual_ids = set(roster["candidate_year_id"].astype(str))
        if expected_ids != actual_ids or len(roster) != int(target.unresolved_candidate_n):
            raise IdentityAttributionError(
                f"Full-contest roster drift for {target_id}: "
                f"declared={int(target.unresolved_candidate_n)}, actual={len(roster)}, "
                f"missing={sorted(expected_ids-actual_ids)}, unexpected={sorted(actual_ids-expected_ids)}"
            )
        if set(roster["source_id"].astype(str)) != {str(target.source_id)}:
            raise IdentityAttributionError(f"Roster source mismatch for {target_id}")
        if set(roster["contest"].astype(str)) != {str(target.contest)}:
            raise IdentityAttributionError(f"Roster contest mismatch for {target_id}")
        if roster["full_name"].map(_text).eq("").any():
            raise IdentityAttributionError(f"Roster contains a blank candidate name for {target_id}")
    return {
        "targets": int(len(targets)),
        "candidate_source_rows": int(len(candidate_roster)),
        "unique_candidate_years": int(candidate_roster["candidate_year_id"].nunique()),
    }


def _name_score(source_name: str, roster_name: str) -> float:
    source = normalize_person_name(source_name)
    roster = normalize_person_name(roster_name)
    if not source or not roster:
        return 0.0
    if source == roster:
        return 100.0
    source_tokens = source.split()
    roster_tokens = roster.split()
    if len(source_tokens) >= 2 and len(roster_tokens) >= 2:
        if (source_tokens[0], source_tokens[-1]) == (roster_tokens[0], roster_tokens[-1]):
            return 96.0
    # Official lists often substitute a quoted nickname for a legal first name
    # ("Ole Savior" versus "Oloveuse 'Ole' Savior") or move a quoted nickname
    # around middle initials ("Abdul M. Rahaman 'The Rock'"). Require two
    # substantive shared tokens and a first/last/nickname anchor before using
    # order-insensitive matching. A shared surname alone therefore cannot turn
    # Troy Benjegerdes into roster candidate Tony Benjegerdes.
    source_set = set(source_tokens) - {"the"}
    roster_set = set(roster_tokens) - {"the"}
    shared = source_set & roster_set
    anchored = (
        source_tokens[0] == roster_tokens[0]
        or source_tokens[-1] == roster_tokens[-1]
        or source_tokens[0] in roster_set
        or roster_tokens[0] in source_set
    )
    token_score = (
        float(fuzz.token_set_ratio(source, roster))
        if len(shared) >= 2 and anchored
        else 0.0
    )
    return max(float(fuzz.ratio(source, roster)), token_score)


def _assess_name(
    assigned_id: str,
    assigned_name: str,
    context_name: str,
    roster: pd.DataFrame,
) -> _NameAssessment:
    scored = [
        (
            str(row.candidate_year_id),
            str(row.full_name),
            _name_score(context_name, str(row.full_name)),
        )
        for row in roster.itertuples(index=False)
    ]
    scored.sort(key=lambda item: (-item[2], item[0]))
    best_id, best_name, best_score = scored[0]
    runner_up = scored[1][2] if len(scored) > 1 else 0.0
    assigned_score = next(score for candidate_id, _, score in scored if candidate_id == assigned_id)
    exact_context = normalize_person_name(context_name)
    exact_ids = {
        str(row.candidate_year_id)
        for row in roster.itertuples(index=False)
        if normalize_person_name(row.full_name) == exact_context and exact_context
    }
    if len(exact_ids) > 1:
        return _NameAssessment(
            "match_unresolved", "duplicate_normalized_name_in_roster", assigned_score,
            best_id, best_name, best_score, runner_up,
        )
    if exact_ids:
        exact_id = next(iter(exact_ids))
        if exact_id == assigned_id:
            return _NameAssessment(
                "identity_supported", "context_exactly_names_assigned_candidate",
                assigned_score, best_id, best_name, best_score, runner_up,
            )
        return _NameAssessment(
            "reject_identity", "context_names_different_contest_candidate",
            assigned_score, best_id, best_name, best_score, runner_up,
        )
    gap = best_score - runner_up
    if best_score >= SUPPORTED_NAME_SCORE and gap < SUPPORTED_NAME_MARGIN:
        return _NameAssessment(
            "match_unresolved", "nearest_roster_name_ambiguous", assigned_score,
            best_id, best_name, best_score, runner_up,
        )
    if best_score >= SUPPORTED_NAME_SCORE and best_id != assigned_id:
        return _NameAssessment(
            "reject_identity", "context_names_different_contest_candidate",
            assigned_score, best_id, best_name, best_score, runner_up,
        )
    if assigned_score >= SUPPORTED_NAME_SCORE and gap >= SUPPORTED_NAME_MARGIN:
        return _NameAssessment(
            "identity_supported", "context_uniquely_names_assigned_candidate",
            assigned_score, best_id, best_name, best_score, runner_up,
        )
    if len(exact_context.split()) >= 2 and assigned_score < DIFFERENT_PERSON_SCORE:
        return _NameAssessment(
            "reject_identity", "context_names_different_person",
            assigned_score, best_id, best_name, best_score, runner_up,
        )
    return _NameAssessment(
        "match_unresolved", "context_name_not_unique_enough", assigned_score,
        best_id, best_name, best_score, runner_up,
    )


def _url_identity_key(value: object) -> str:
    exact = normalize_exact_url(_text(value))
    if not exact:
        raise IdentityAttributionError(f"Invalid exact evidence URL: {value!r}")
    # normalize_exact_url already lowercases the host and removes fragments.  Treat
    # HTTP and HTTPS as one identity assignment while retaining path case.
    return exact.split("://", 1)[-1].rstrip("/")


def audit_identity_attribution(
    targets: pd.DataFrame,
    candidate_roster: pd.DataFrame,
    evidence: pd.DataFrame,
    source_contexts: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, object]]:
    """Return evidence-level attribution decisions without approving any claim."""
    roster_summary = validate_full_contest_rosters(targets, candidate_roster)
    _required_columns(
        evidence,
        [
            "evidence_id", "target_id", "source_id", "source_file_id", "contest",
            "candidate_year_id", "exact_normalized_url",
        ],
        "evidence",
    )
    if source_contexts.columns.tolist() != SOURCE_CONTEXT_COLUMNS:
        raise IdentityAttributionError(
            "source context schema mismatch; "
            f"expected={SOURCE_CONTEXT_COLUMNS}, actual={source_contexts.columns.tolist()}"
        )
    if evidence["evidence_id"].astype(str).duplicated().any():
        raise IdentityAttributionError("Evidence has duplicate evidence_id values")
    if source_contexts["context_id"].astype(str).duplicated().any():
        raise IdentityAttributionError("Source contexts have duplicate context_id values")
    unexpected_contexts = set(source_contexts["evidence_id"].astype(str)) - set(
        evidence["evidence_id"].astype(str)
    )
    if unexpected_contexts:
        raise IdentityAttributionError(
            f"Source contexts reference unexpected evidence IDs: {sorted(unexpected_contexts)}"
        )
    invalid_kinds = set(source_contexts["association_kind"].astype(str)) - ASSOCIATION_KINDS
    if invalid_kinds:
        raise IdentityAttributionError(f"Unsupported association kinds: {sorted(invalid_kinds)}")

    target_lookup = targets.set_index("target_id")
    roster_groups = {
        str(target_id): group.reset_index(drop=True)
        for target_id, group in candidate_roster.groupby("target_id", sort=False)
    }
    context_groups = {
        str(evidence_id): group.reset_index(drop=True)
        for evidence_id, group in source_contexts.groupby("evidence_id", sort=False)
    }
    rows: list[dict[str, object]] = []
    context_meta: dict[str, dict[str, object]] = {}
    for item in evidence.itertuples(index=False):
        evidence_id = str(item.evidence_id)
        target_id = str(item.target_id)
        if target_id not in target_lookup.index:
            raise IdentityAttributionError(f"Evidence {evidence_id} has unexpected target_id")
        target = target_lookup.loc[target_id]
        roster = roster_groups[target_id]
        assigned = roster.loc[
            roster["candidate_year_id"].astype(str).eq(str(item.candidate_year_id))
        ]
        if len(assigned) != 1:
            raise IdentityAttributionError(
                f"Evidence {evidence_id} candidate is not unique in its full contest roster"
            )
        if str(item.source_id) != str(target.source_id) or str(item.contest) != str(target.contest):
            raise IdentityAttributionError(f"Evidence {evidence_id} source/contest differs from target")
        assigned_name = str(assigned.iloc[0]["full_name"])
        contexts = context_groups.get(evidence_id, pd.DataFrame(columns=SOURCE_CONTEXT_COLUMNS))
        if len(contexts) != 1:
            status = "match_unresolved"
            reason = "missing_source_context" if contexts.empty else "multiple_source_contexts"
            assessment = _NameAssessment(status, reason, 0.0, "", "", 0.0, 0.0)
            context = {column: "" for column in SOURCE_CONTEXT_COLUMNS}
        else:
            context = contexts.iloc[0].to_dict()
            for column in [
                "context_id", "source_id", "source_file_id", "exact_normalized_url",
                "source_locator", "association_kind", "context_text",
            ]:
                if not _text(context[column]):
                    raise IdentityAttributionError(f"Context for {evidence_id} lacks {column}")
            for column in ["source_id", "source_file_id"]:
                if _text(context[column]) != _text(getattr(item, column)):
                    raise IdentityAttributionError(
                        f"Context for {evidence_id} has incorrect {column}"
                    )
            if _url_identity_key(context["exact_normalized_url"]) != _url_identity_key(
                item.exact_normalized_url
            ):
                raise IdentityAttributionError(f"Context URL differs for {evidence_id}")
            kind = str(context["association_kind"])
            context_name = _text(context["context_candidate_name"])
            if context_name and normalize_person_name(context_name) not in normalize_person_name(
                context["context_text"]
            ):
                raise IdentityAttributionError(
                    f"Context candidate name is absent from context_text for {evidence_id}"
                )
            if kind in PAGE_ASSOCIATIONS:
                assessment = _NameAssessment(
                    "match_unresolved", "page_level_association_ambiguous", 0.0,
                    "", "", 0.0, 0.0,
                )
            elif not context_name:
                assessment = _NameAssessment(
                    "match_unresolved", "source_context_has_no_candidate_name", 0.0,
                    "", "", 0.0, 0.0,
                )
            elif kind in NEAREST_ASSOCIATIONS:
                try:
                    nearest = float(context["nearest_distance"])
                    runner_up_distance = float(context["runner_up_distance"])
                except (TypeError, ValueError):
                    nearest = runner_up_distance = -1.0
                if (
                    nearest < 0
                    or runner_up_distance < nearest
                    or runner_up_distance - nearest < NEAREST_DISTANCE_MARGIN
                ):
                    assessment = _NameAssessment(
                        "match_unresolved", "nearest_name_geometry_ambiguous", 0.0,
                        "", "", 0.0, 0.0,
                    )
                else:
                    assessment = _assess_name(
                        str(item.candidate_year_id), assigned_name, context_name, roster
                    )
            else:
                assessment = _assess_name(
                    str(item.candidate_year_id), assigned_name, context_name, roster
                )
        required_attempt_status = {
            "identity_supported": "unchanged_pending_claim_review",
            "reject_identity": "must_not_be_found",
            "match_unresolved": "match_unresolved",
        }[assessment.status]
        rows.append(
            {
                "evidence_id": evidence_id,
                "target_id": target_id,
                "source_id": str(item.source_id),
                "contest": str(item.contest),
                "candidate_year_id": str(item.candidate_year_id),
                "assigned_candidate_name": assigned_name,
                "exact_normalized_url": str(item.exact_normalized_url),
                "context_id": _text(context["context_id"]),
                "source_locator": _text(context["source_locator"]),
                "association_kind": _text(context["association_kind"]),
                "context_candidate_name": _text(context["context_candidate_name"]),
                "assigned_name_score": round(assessment.assigned_score, 3),
                "best_roster_candidate_year_id": assessment.best_id,
                "best_roster_candidate_name": assessment.best_name,
                "best_roster_score": round(assessment.best_score, 3),
                "runner_up_roster_score": round(assessment.runner_up_score, 3),
                "identity_attribution_status": assessment.status,
                "identity_attribution_reason": assessment.reason,
                "required_attempt_status": required_attempt_status,
                "claim_approved_by_this_audit": False,
            }
        )
        context_meta[evidence_id] = context

    decisions = pd.DataFrame(rows, columns=DECISION_COLUMNS)
    if len(decisions):
        decisions["_url_key"] = decisions["exact_normalized_url"].map(_url_identity_key)
        for _, group in decisions.groupby(["source_id", "contest", "_url_key"], sort=False):
            if group["candidate_year_id"].nunique() <= 1:
                continue
            distinct_evidence = True
            context_ids: set[str] = set()
            locators: set[str] = set()
            for row in group.itertuples(index=False):
                meta = context_meta[str(row.evidence_id)]
                context_id = _text(meta.get("context_id"))
                locator = _text(meta.get("source_locator"))
                note = _text(meta.get("separate_evidence_note"))
                explicit = _bool(meta.get("explicit_separate_evidence"))
                if not explicit or not note or not context_id or not locator:
                    distinct_evidence = False
                context_ids.add(context_id)
                locators.add(locator)
            if len(context_ids) != len(group) or len(locators) != len(group):
                distinct_evidence = False
            if not distinct_evidence:
                index = group.index
                decisions.loc[index, "identity_attribution_status"] = "reject_identity"
                decisions.loc[index, "identity_attribution_reason"] = (
                    "url_assigned_multiple_candidates_without_explicit_separate_evidence"
                )
                decisions.loc[index, "required_attempt_status"] = "must_not_be_found"
        decisions = decisions.drop(columns="_url_key")

    status_counts = decisions["identity_attribution_status"].value_counts().to_dict()
    summary = {
        "schema_version": "municipal-identity-attribution-audit-v1",
        "scope": "candidate attribution only; no URL claim is approved",
        **roster_summary,
        "evidence_rows": int(len(decisions)),
        "identity_supported": int(status_counts.get("identity_supported", 0)),
        "reject_identity": int(status_counts.get("reject_identity", 0)),
        "match_unresolved": int(status_counts.get("match_unresolved", 0)),
        "claim_approved_rows": 0,
    }
    return decisions, summary


def require_resolved_identity_attribution(decisions: pd.DataFrame) -> None:
    """Fail closed if any evidence is rejected or unresolved."""
    _required_columns(
        decisions,
        ["evidence_id", "identity_attribution_status"],
        "identity decisions",
    )
    blocked = decisions.loc[
        ~decisions["identity_attribution_status"].eq("identity_supported")
    ]
    if len(blocked):
        counts = blocked["identity_attribution_status"].value_counts().to_dict()
        raise IdentityAttributionError(
            f"Identity attribution gate failed for {len(blocked)} evidence rows: {counts}"
        )
