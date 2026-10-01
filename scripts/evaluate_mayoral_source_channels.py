#!/usr/bin/env python3
"""Evaluate mayoral URL source channels on the blinded candidate screen.

The evaluator counts only accepted grade A or B claims as strict evidence.  It
keeps custom-domain coverage separate from exact-URL coverage.  The evaluator
also preserves pending claims, so an incomplete review cannot pass a gate.
"""

from __future__ import annotations

import argparse
import json
import math
import tempfile
from pathlib import Path
from statistics import NormalDist
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCREEN = (
    REPO_ROOT / "data/municipal_discovery/screening/candidate_screening_sample.csv"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "quality_reports/mayoral_source_channel_evaluation"

SCREEN_REQUIRED = {"candidate_year_id", "year", "population_weight"}
CLAIM_REQUIRED = {
    "candidate_year_id",
    "source_id",
    "evidence_grade",
    "review_status",
    "acceptance",
    "url_type",
}
STRICT_GRADES = {"A", "B"}
CUSTOM_URL_TYPES = {"custom_domain", "candidate_owned_custom_domain"}
EXACT_URL_TYPES = CUSTOM_URL_TYPES | {
    "candidate_subdomain",
    "shared_host_path",
    "candidate_subdomain_or_shared_host_path",
}
PENDING_REVIEW_STATUSES = {
    "",
    "pending",
    "pending_review",
    "unreviewed",
    "needs_review",
    "match_unresolved",
}
ADJUDICATED_REVIEW_STATUSES = {
    "accepted",
    "approved",
    "rejected",
    "declined",
    "reviewed",
    "adjudicated",
    "complete",
}


class ChannelEvaluationError(ValueError):
    """The channel inputs cannot support a valid evaluation."""


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(frame.columns.tolist()))
    if missing:
        raise ChannelEvaluationError(f"{label} is missing required columns: {missing}")


def _clean_ids(series: pd.Series, label: str) -> pd.Series:
    result = series.astype("string").str.strip()
    invalid = result.isna() | result.eq("")
    if invalid.any():
        raise ChannelEvaluationError(f"{label} has {int(invalid.sum())} missing IDs")
    return result


def _as_acceptance(series: pd.Series) -> pd.Series:
    true_values = {"1", "true", "t", "yes", "y", "accepted", "accept"}
    false_values = {"0", "false", "f", "no", "n", "rejected", "reject"}

    def convert(value: object) -> object:
        if isinstance(value, bool):
            return value
        if pd.isna(value):
            return pd.NA
        normalized = str(value).strip().casefold()
        if normalized == "":
            return pd.NA
        if normalized in true_values:
            return True
        if normalized in false_values:
            return False
        raise ChannelEvaluationError(f"acceptance has an invalid value: {value!r}")

    return series.map(convert).astype("boolean")


def _normalize_token(value: object) -> str:
    if pd.isna(value):
        return ""
    return str(value).strip().casefold().replace("-", "_").replace(" ", "_")


def _explicit_match(values: pd.Series) -> pd.Series:
    """Require a positive match when a match field is supplied."""
    tokens = values.map(_normalize_token)
    negative_parts = {"false", "no", "mismatch", "unresolved", "unknown", "outside", "conflict"}
    return tokens.ne("") & ~tokens.map(
        lambda token: bool(set(token.split("_")) & negative_parts)
    )


def _drop_audit(
    stage: str,
    before: int,
    after: int,
    *,
    note: str,
) -> dict[str, object]:
    if before < 0 or after < 0 or after > before:
        raise ChannelEvaluationError(f"Invalid drop counts at {stage}: {before} -> {after}")
    dropped = before - after
    share = dropped / before if before else 0.0
    if share > 0.20 and not note.strip():
        raise ChannelEvaluationError(
            f"Filter {stage} drops {share:.1%} without an explicit report"
        )
    return {
        "stage": stage,
        "before_n": int(before),
        "after_n": int(after),
        "dropped_n": int(dropped),
        "drop_share": float(share),
        "large_drop": bool(share > 0.20),
        "note": note,
    }


def prepare_screen(
    screen: pd.DataFrame,
    *,
    expected_screen_size: int | None = 480,
    expected_early_size: int | None = 120,
    early_year_start: int = 2000,
    early_year_end: int = 2007,
) -> pd.DataFrame:
    """Validate the blinded screen and its design weights."""
    _require_columns(screen, SCREEN_REQUIRED, "candidate screen")
    result = screen.copy()
    result["candidate_year_id"] = _clean_ids(
        result["candidate_year_id"], "candidate screen"
    )
    if result["candidate_year_id"].duplicated().any():
        raise ChannelEvaluationError("candidate screen candidate_year_id must be unique")
    result["year"] = pd.to_numeric(result["year"], errors="raise").astype(int)
    result["population_weight"] = pd.to_numeric(
        result["population_weight"], errors="raise"
    ).astype(float)
    if (~result["population_weight"].map(math.isfinite)).any():
        raise ChannelEvaluationError("candidate screen weights must be finite")
    if result["population_weight"].le(0).any():
        raise ChannelEvaluationError("candidate screen weights must be positive")
    if expected_screen_size is not None and len(result) != expected_screen_size:
        raise ChannelEvaluationError(
            f"Candidate screen has {len(result)} rows; expected {expected_screen_size}"
        )
    early_n = int(result["year"].between(early_year_start, early_year_end).sum())
    if expected_early_size is not None and early_n != expected_early_size:
        raise ChannelEvaluationError(
            f"Candidate screen has {early_n} early-period rows; expected {expected_early_size}"
        )
    return result


def prepare_claims(
    claims: pd.DataFrame,
    screen: pd.DataFrame,
    *,
    outside_screen_report: str = "",
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """Validate claims and add strict-evidence indicators."""
    _require_columns(claims, CLAIM_REQUIRED, "claim data")
    result = claims.copy()
    result["candidate_year_id"] = _clean_ids(result["candidate_year_id"], "claim data")
    result["source_id"] = _clean_ids(result["source_id"], "claim data source_id")
    result["_claim_order"] = range(len(result))

    screen_ids = set(screen["candidate_year_id"].astype(str))
    before = len(result)
    result = result.loc[result["candidate_year_id"].isin(screen_ids)].copy()
    audit = [
        _drop_audit(
            "restrict_claims_to_candidate_screen",
            before,
            len(result),
            note=outside_screen_report,
        )
    ]
    if "claim_id" in result.columns:
        claim_ids = result["claim_id"].astype("string").str.strip()
        present_claim_ids = claim_ids.notna() & claim_ids.ne("")
        if not present_claim_ids.all():
            raise ChannelEvaluationError("claim_id cannot be missing when the column exists")
        duplicate_claim_ids = claim_ids.loc[present_claim_ids].duplicated(keep=False)
        if duplicate_claim_ids.any():
            examples = claim_ids.loc[present_claim_ids].loc[duplicate_claim_ids].head(5)
            raise ChannelEvaluationError(
                f"claim_id must be unique; examples: {examples.tolist()}"
            )
    elif result.duplicated(sorted(CLAIM_REQUIRED), keep=False).any():
        raise ChannelEvaluationError(
            "Claim data has exact duplicate rows and no claim_id for deduplication"
        )

    result["evidence_grade"] = result["evidence_grade"].astype("string").str.strip().str.upper()
    invalid_grades = ~result["evidence_grade"].isin({"A", "B", "C", "D"})
    if invalid_grades.any():
        values = sorted(result.loc[invalid_grades, "evidence_grade"].dropna().unique())
        raise ChannelEvaluationError(f"claim data has invalid evidence grades: {values}")

    result["review_status"] = result["review_status"].map(_normalize_token)
    valid_statuses = PENDING_REVIEW_STATUSES | ADJUDICATED_REVIEW_STATUSES
    invalid_status = ~result["review_status"].isin(valid_statuses)
    if invalid_status.any():
        values = sorted(result.loc[invalid_status, "review_status"].unique())
        raise ChannelEvaluationError(f"claim data has invalid review statuses: {values}")
    result["is_adjudicated"] = result["review_status"].isin(
        ADJUDICATED_REVIEW_STATUSES
    )
    result["acceptance"] = _as_acceptance(result["acceptance"])
    missing_adjudicated_acceptance = result["is_adjudicated"] & result[
        "acceptance"
    ].isna()
    if missing_adjudicated_acceptance.any():
        raise ChannelEvaluationError("An adjudicated claim has no acceptance value")
    result["acceptance"] = result["acceptance"].fillna(False).astype(bool)
    accepted_status = result["review_status"].isin({"accepted", "approved"})
    rejected_status = result["review_status"].isin({"rejected", "declined"})
    if (accepted_status & ~result["acceptance"]).any():
        raise ChannelEvaluationError("accepted review_status conflicts with acceptance")
    if (rejected_status & result["acceptance"]).any():
        raise ChannelEvaluationError("rejected review_status conflicts with acceptance")

    result["url_type"] = result["url_type"].map(_normalize_token)
    identity_ok = (
        _explicit_match(result["identity_match"])
        if "identity_match" in result.columns
        else pd.Series(True, index=result.index)
    )
    temporal_ok = (
        _explicit_match(result["temporal_match"])
        if "temporal_match" in result.columns
        else pd.Series(True, index=result.index)
    )
    accepted_strict = (
        result["is_adjudicated"]
        & result["acceptance"]
        & result["evidence_grade"].isin(STRICT_GRADES)
        & identity_ok
        & temporal_ok
    )
    result["accepted_target_url"] = (
        result["is_adjudicated"]
        & result["acceptance"]
        & identity_ok
        & temporal_ok
        & result["url_type"].isin(EXACT_URL_TYPES)
    )
    result["strict_exact_url"] = accepted_strict & result["url_type"].isin(
        EXACT_URL_TYPES
    )
    result["strict_custom_domain"] = result["strict_exact_url"] & result[
        "url_type"
    ].isin(CUSTOM_URL_TYPES)
    return result.sort_values("_claim_order").reset_index(drop=True), audit


def load_claim_files(paths: Iterable[Path]) -> pd.DataFrame:
    parts: list[pd.DataFrame] = []
    for path in paths:
        part = pd.read_csv(path, dtype="string")
        part["claim_file"] = str(path)
        parts.append(part)
    if not parts:
        raise ChannelEvaluationError("At least one claim file is required")
    return pd.concat(parts, ignore_index=True)


def _channel_design(
    metadata: dict[str, object], source_ids: set[str]
) -> tuple[list[str], dict[str, dict[str, object]], dict[str, str]]:
    raw_channels = metadata.get("channels")
    if not isinstance(raw_channels, dict) or not raw_channels:
        raise ChannelEvaluationError("cost metadata must contain a nonempty channels object")

    designs: dict[str, dict[str, object]] = {}
    source_to_channel: dict[str, str] = {}
    for channel_id, raw in raw_channels.items():
        channel = str(channel_id).strip()
        if not channel:
            raise ChannelEvaluationError("channel IDs cannot be empty")
        if not isinstance(raw, dict):
            raise ChannelEvaluationError(f"Channel {channel} metadata must be an object")
        source_list = raw.get("source_ids", [channel])
        if not isinstance(source_list, list) or not source_list:
            raise ChannelEvaluationError(f"Channel {channel} must have source_ids")
        normalized_sources = [str(value).strip() for value in source_list]
        if any(not value for value in normalized_sources):
            raise ChannelEvaluationError(f"Channel {channel} has an empty source_id")
        for source_id in normalized_sources:
            previous = source_to_channel.get(source_id)
            if previous is not None:
                raise ChannelEvaluationError(
                    f"source_id {source_id} belongs to channels {previous} and {channel}"
                )
            source_to_channel[source_id] = channel
        analyst_minutes = float(raw.get("analyst_minutes", 0.0))
        license_cost = float(raw.get("license_cost", 0.0))
        if not math.isfinite(analyst_minutes) or analyst_minutes < 0:
            raise ChannelEvaluationError(f"Channel {channel} has invalid analyst_minutes")
        if not math.isfinite(license_cost) or license_cost < 0:
            raise ChannelEvaluationError(f"Channel {channel} has invalid license_cost")
        designs[channel] = {
            "source_ids": normalized_sources,
            "analyst_minutes": analyst_minutes,
            "license_cost": license_cost,
        }

    missing = sorted(source_ids - set(source_to_channel))
    if missing:
        raise ChannelEvaluationError(
            f"cost metadata does not map claim source_ids: {missing}"
        )
    raw_order = metadata.get("channel_order", list(designs))
    if not isinstance(raw_order, list):
        raise ChannelEvaluationError("channel_order must be a list")
    order = [str(value) for value in raw_order]
    if len(order) != len(set(order)) or set(order) != set(designs):
        raise ChannelEvaluationError("channel_order must list every channel exactly once")
    return order, designs, source_to_channel


def _weighted_projection(
    screen: pd.DataFrame,
    positive_ids: set[str],
) -> dict[str, float]:
    outcome = screen["candidate_year_id"].isin(positive_ids).astype(float)
    weights = screen["population_weight"].astype(float)
    total_weight = float(weights.sum())
    projected = float((weights * outcome).sum())
    rate = projected / total_weight if total_weight else 0.0
    effective_n = float(weights.sum() ** 2 / (weights.pow(2).sum()))
    z_value = NormalDist().inv_cdf(0.90)
    denominator = 1.0 + z_value**2 / effective_n
    center = rate + z_value**2 / (2.0 * effective_n)
    radius = z_value * math.sqrt(
        rate * (1.0 - rate) / effective_n
        + z_value**2 / (4.0 * effective_n**2)
    )
    lower_rate = (
        0.0 if projected == 0.0
        else max(0.0, (center - radius) / denominator)
    )
    return {
        "weighted_rate": float(rate),
        "projected_population_additions": projected,
        "one_sided_90_lower_rate": float(lower_rate),
        "one_sided_90_lower_additions": float(lower_rate * total_weight),
        "represented_population": total_weight,
        "effective_sample_size": effective_n,
    }


def _decision(
    *,
    marginal_exact_n: int,
    marginal_early_exact_n: int,
    precision: float | None,
    cost_per_accepted: float | None,
    pending_n: int,
) -> tuple[str, list[str]]:
    if pending_n:
        return "pending_adjudication", [
            f"{pending_n} claim(s) still require adjudication."
        ]

    drop_reasons: list[str] = []
    if marginal_exact_n <= 3:
        drop_reasons.append("The channel adds no more than three strict URLs.")
    if precision is None or precision < 0.90:
        drop_reasons.append("Adjudicated precision is below 90 percent.")
    if cost_per_accepted is not None and cost_per_accepted > 250.0:
        drop_reasons.append("Cost exceeds $250 per accepted URL.")
    if drop_reasons:
        return "drop", drop_reasons

    yield_pass = marginal_exact_n >= 10 or marginal_early_exact_n >= 5
    precision_pass = precision is not None and precision >= 0.95
    cost_pass = cost_per_accepted is not None and cost_per_accepted < 100.0
    if yield_pass and precision_pass and cost_pass:
        reasons = [
            "The channel passes the strict URL yield gate.",
            "Adjudicated precision is at least 95 percent.",
            "Cost is below $100 per accepted URL.",
        ]
        return "continue", reasons
    return "hold", [
        "The channel is between the approved continue and drop thresholds."
    ]


def _acquisition_gate(
    screen: pd.DataFrame,
    claims: pd.DataFrame,
    channel_rows: list[dict[str, object]],
    metadata: dict[str, object],
) -> dict[str, object]:
    settings = metadata.get("acquisition_gate", {})
    if not isinstance(settings, dict):
        raise ChannelEvaluationError("acquisition_gate must be an object")
    universe_n = int(settings.get("universe_candidate_years", 4817))
    existing_primary = int(settings.get("existing_primary_strict_candidate_years", 0))
    if universe_n <= 0 or not 0 <= existing_primary <= universe_n:
        raise ChannelEvaluationError("acquisition gate has invalid universe/baseline counts")

    continued = {
        str(row["channel_id"])
        for row in channel_rows if row["decision"] == "continue"
    }
    pending_n = sum(int(row["pending_claim_n"]) for row in channel_rows)
    selected = claims.loc[claims["channel_id"].isin(continued)].copy()
    primary_ids = set(
        selected.loc[selected["strict_custom_domain"], "candidate_year_id"].astype(str)
    )
    projection = _weighted_projection(screen, primary_ids)
    projected_total = existing_primary + projection["projected_population_additions"]
    lower_total = existing_primary + projection["one_sided_90_lower_additions"]
    projected_rate = projected_total / universe_n
    lower_rate = lower_total / universe_n

    adjudicated_n = int(selected["is_adjudicated"].sum())
    accepted_target_n = int(selected["accepted_target_url"].sum())
    precision = accepted_target_n / adjudicated_n if adjudicated_n else None
    projected_cost = settings.get("projected_production_cost")
    approved_ceiling = settings.get("approved_production_cost_ceiling")
    cost_ready = projected_cost is not None and approved_ceiling is not None
    if cost_ready:
        projected_cost = float(projected_cost)
        approved_ceiling = float(approved_ceiling)
        if (
            not math.isfinite(projected_cost)
            or not math.isfinite(approved_ceiling)
            or projected_cost < 0
            or approved_ceiling < 0
        ):
            raise ChannelEvaluationError("acquisition gate has invalid production cost")
    cost_pass = bool(cost_ready and projected_cost <= approved_ceiling)
    coverage_pass = projected_rate >= 0.15
    lower_bound_pass = lower_rate >= 0.13
    precision_pass = precision is not None and precision >= 0.95

    if pending_n:
        decision = "pending_adjudication"
    elif not continued:
        decision = "do_not_proceed"
    elif not cost_ready:
        decision = "pending_cost_approval"
    elif continued and coverage_pass and lower_bound_pass and precision_pass and cost_pass:
        decision = "proceed_to_full_acquisition"
    else:
        decision = "do_not_proceed"
    return {
        "decision": decision,
        "continued_channels": sorted(continued),
        "universe_candidate_years": universe_n,
        "existing_primary_strict_candidate_years": existing_primary,
        "projected_primary_candidate_years": float(projected_total),
        "projected_primary_coverage_rate": float(projected_rate),
        "one_sided_90_lower_primary_candidate_years": float(lower_total),
        "one_sided_90_lower_primary_coverage_rate": float(lower_rate),
        "adjudicated_precision": precision,
        "projected_production_cost": projected_cost,
        "approved_production_cost_ceiling": approved_ceiling,
        "coverage_15_percent_pass": coverage_pass,
        "lower_bound_13_percent_pass": lower_bound_pass,
        "precision_95_percent_pass": precision_pass,
        "production_cost_pass": cost_pass,
    }


def evaluate_source_channels(
    screen: pd.DataFrame,
    claims: pd.DataFrame,
    cost_metadata: dict[str, object],
    *,
    expected_screen_size: int | None = 480,
    expected_early_size: int | None = 120,
    early_year_start: int = 2000,
    early_year_end: int = 2007,
) -> dict[str, object]:
    """Calculate channel yields, projections, precision, costs, and gates."""
    if early_year_end < early_year_start:
        raise ChannelEvaluationError("early_year_end must be at least early_year_start")
    prepared_screen = prepare_screen(
        screen,
        expected_screen_size=expected_screen_size,
        expected_early_size=expected_early_size,
        early_year_start=early_year_start,
        early_year_end=early_year_end,
    )
    drop_reports = cost_metadata.get("drop_reports", {})
    if not isinstance(drop_reports, dict):
        raise ChannelEvaluationError("drop_reports must be an object")
    prepared_claims, audit = prepare_claims(
        claims,
        prepared_screen,
        outside_screen_report=str(
            drop_reports.get("restrict_claims_to_candidate_screen", "")
        ),
    )
    source_ids = set(prepared_claims["source_id"].astype(str))
    order, designs, source_to_channel = _channel_design(cost_metadata, source_ids)
    prepared_claims["channel_id"] = prepared_claims["source_id"].map(source_to_channel)

    seen_exact: set[str] = set()
    seen_custom: set[str] = set()
    rows: list[dict[str, object]] = []
    for channel_index, channel_id in enumerate(order, start=1):
        group = prepared_claims.loc[prepared_claims["channel_id"].eq(channel_id)].copy()
        exact_ids = set(
            group.loc[group["strict_exact_url"], "candidate_year_id"].astype(str)
        )
        custom_ids = set(
            group.loc[group["strict_custom_domain"], "candidate_year_id"].astype(str)
        )
        marginal_exact = exact_ids - seen_exact
        marginal_custom = custom_ids - seen_custom
        cumulative_exact = seen_exact | exact_ids
        cumulative_custom = seen_custom | custom_ids
        early_ids = set(
            prepared_screen.loc[
                prepared_screen["year"].between(early_year_start, early_year_end),
                "candidate_year_id",
            ].astype(str)
        )

        adjudicated_n = int(group["is_adjudicated"].sum())
        strict_claim_n = int(group["strict_exact_url"].sum())
        accepted_target_n = int(group["accepted_target_url"].sum())
        precision = accepted_target_n / adjudicated_n if adjudicated_n else None
        pending_n = int((~group["is_adjudicated"]).sum())
        analyst_minutes = float(designs[channel_id]["analyst_minutes"])
        license_cost = float(designs[channel_id]["license_cost"])
        cost_per_accepted = (
            license_cost / len(marginal_exact) if marginal_exact else None
        )
        minutes_per_accepted = (
            analyst_minutes / len(marginal_exact) if marginal_exact else None
        )
        decision, reasons = _decision(
            marginal_exact_n=len(marginal_exact),
            marginal_early_exact_n=len(marginal_exact & early_ids),
            precision=precision,
            cost_per_accepted=cost_per_accepted,
            pending_n=pending_n,
        )
        row: dict[str, object] = {
            "channel_order": channel_index,
            "channel_id": channel_id,
            "source_ids": list(designs[channel_id]["source_ids"]),
            "claim_n": int(len(group)),
            "adjudicated_claim_n": adjudicated_n,
            "pending_claim_n": pending_n,
            "accepted_target_claim_n": accepted_target_n,
            "strict_claim_n": strict_claim_n,
            "adjudicated_precision": precision,
            "gross_primary_n": len(custom_ids),
            "marginal_primary_n": len(marginal_custom),
            "cumulative_primary_n": len(cumulative_custom),
            "gross_exact_n": len(exact_ids),
            "marginal_exact_n": len(marginal_exact),
            "marginal_early_exact_n": len(marginal_exact & early_ids),
            "cumulative_exact_n": len(cumulative_exact),
            "analyst_minutes": analyst_minutes,
            "license_cost": license_cost,
            "analyst_minutes_per_accepted_url": minutes_per_accepted,
            "cost_per_accepted_url": cost_per_accepted,
            "decision": decision,
            "decision_reasons": reasons,
        }
        row["gross_primary_projection"] = _weighted_projection(
            prepared_screen, custom_ids
        )
        row["marginal_primary_projection"] = _weighted_projection(
            prepared_screen, marginal_custom
        )
        row["cumulative_primary_projection"] = _weighted_projection(
            prepared_screen, cumulative_custom
        )
        row["gross_exact_projection"] = _weighted_projection(
            prepared_screen, exact_ids
        )
        row["marginal_exact_projection"] = _weighted_projection(
            prepared_screen, marginal_exact
        )
        row["cumulative_exact_projection"] = _weighted_projection(
            prepared_screen, cumulative_exact
        )
        row["primary_projection"] = row["cumulative_primary_projection"]
        row["exact_projection"] = row["cumulative_exact_projection"]
        rows.append(row)
        seen_exact = cumulative_exact
        seen_custom = cumulative_custom

    all_adjudicated_n = int(prepared_claims["is_adjudicated"].sum())
    all_accepted_target_n = int(prepared_claims["accepted_target_url"].sum())
    stack_precision = (
        all_accepted_target_n / all_adjudicated_n if all_adjudicated_n else None
    )
    acquisition_gate = _acquisition_gate(
        prepared_screen, prepared_claims, rows, cost_metadata
    )
    return {
        "schema_version": 1,
        "gate_basis": "marginal_strict_exact_url_candidate_years",
        "strict_rule": (
            "accepted grade A or B claim with positive identity and temporal "
            "matches and an eligible campaign URL type"
        ),
        "screen": {
            "candidate_years": int(len(prepared_screen)),
            "early_candidate_years": int(
                prepared_screen["year"].between(early_year_start, early_year_end).sum()
            ),
            "early_year_start": early_year_start,
            "early_year_end": early_year_end,
            "represented_population": float(prepared_screen["population_weight"].sum()),
        },
        "channels": rows,
        "stack": {
            "strict_primary_candidate_years": len(seen_custom),
            "strict_exact_candidate_years": len(seen_exact),
            "adjudicated_claim_n": all_adjudicated_n,
            "accepted_target_claim_n": all_accepted_target_n,
            "pending_claim_n": int((~prepared_claims["is_adjudicated"]).sum()),
            "adjudicated_precision": stack_precision,
            "primary_projection": _weighted_projection(prepared_screen, seen_custom),
            "exact_projection": _weighted_projection(prepared_screen, seen_exact),
            "acquisition_gate": acquisition_gate,
        },
        "drop_audit": audit,
    }


def _json_ready(value: object) -> object:
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {key: _json_ready(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_ready(item) for item in value]
    return value


def write_evaluation(result: dict[str, object], output_dir: Path) -> dict[str, Path]:
    """Write a JSON report, channel table, and filter audit."""
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "report": output_dir / "source_channel_evaluation.json",
        "channels": output_dir / "source_channel_metrics.csv",
        "drop_audit": output_dir / "source_channel_drop_audit.csv",
    }
    ready = _json_ready(result)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", dir=output_dir, delete=False, encoding="utf-8"
    ) as handle:
        temporary = Path(handle.name)
        json.dump(ready, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temporary.replace(paths["report"])

    channel_rows = []
    for row in ready["channels"]:
        flat = {key: value for key, value in row.items() if not isinstance(value, dict)}
        flat["source_ids"] = json.dumps(flat["source_ids"], sort_keys=True)
        flat["decision_reasons"] = json.dumps(flat["decision_reasons"])
        for projection_name, projection in row.items():
            if not projection_name.endswith("_projection") or not isinstance(
                projection, dict
            ):
                continue
            for key, value in projection.items():
                flat[f"{projection_name}_{key}"] = value
        channel_rows.append(flat)
    pd.DataFrame(channel_rows).to_csv(paths["channels"], index=False)
    pd.DataFrame(ready["drop_audit"]).to_csv(paths["drop_audit"], index=False)
    return paths


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--screen", type=Path, default=DEFAULT_SCREEN)
    parser.add_argument("--claims", type=Path, action="append", required=True)
    parser.add_argument("--costs", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--expected-screen-size", type=int, default=480)
    parser.add_argument("--expected-early-size", type=int, default=120)
    parser.add_argument("--early-year-start", type=int, default=2000)
    parser.add_argument("--early-year-end", type=int, default=2007)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    screen = pd.read_csv(args.screen, dtype={"candidate_year_id": "string"})
    claims = load_claim_files(args.claims)
    cost_metadata = json.loads(args.costs.read_text(encoding="utf-8"))
    result = evaluate_source_channels(
        screen,
        claims,
        cost_metadata,
        expected_screen_size=args.expected_screen_size,
        expected_early_size=args.expected_early_size,
        early_year_start=args.early_year_start,
        early_year_end=args.early_year_end,
    )
    paths = write_evaluation(result, args.output_dir)
    print(json.dumps({key: str(path) for key, path in paths.items()}, sort_keys=True))


if __name__ == "__main__":
    main()
