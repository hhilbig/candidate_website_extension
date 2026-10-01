#!/usr/bin/env python3
"""Apply the sampled Archive-It audit rule without mislabeling inferred claims as human-coded."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


STRICT_TYPES = {"custom_domain", "candidate_owned_custom_domain"}


def _bool_series(series: pd.Series) -> pd.Series:
    return series.astype("boolean").fillna(False).astype(bool)


def apply_rule(
    evidence: pd.DataFrame,
    validation: pd.DataFrame,
    adjudication: pd.DataFrame,
    audit_manifest: dict,
    existing_claims: pd.DataFrame,
) -> tuple[pd.DataFrame, dict]:
    if set(evidence["claim_id"].astype(str)) != set(validation["claim_id"].astype(str)):
        raise ValueError("Evidence and validation claim IDs disagree")
    if set(adjudication["claim_id"].astype(str)) != set(map(str, audit_manifest["selected_claim_ids"])):
        raise ValueError("Adjudication and audit manifest claim IDs disagree")
    if len(adjudication) != int(audit_manifest["audit_items"]):
        raise ValueError("Adjudication row count disagrees with the audit manifest")

    if {"candidate_year_id", "covered"}.issubset(existing_claims.columns):
        covered_before = set(
            existing_claims.loc[
                _bool_series(existing_claims["covered"]), "candidate_year_id",
            ].astype(str)
        )
    else:
        covered_before = set(
            existing_claims.loc[
                existing_claims["claim_origin"].eq("legacy_baseline")
                | _bool_series(existing_claims["strict_custom_domain"]),
                "candidate_year_id",
            ].astype(str)
        )
    validation_columns = [
        "claim_id", "proposed_evidence_grade", "strict_coverage_eligible",
        "validation_reason",
    ]
    base = evidence.drop(
        columns=[column for column in validation_columns[1:] if column in evidence.columns]
    ).merge(validation[validation_columns], on="claim_id", validate="one_to_one")
    base["covered_before_batch"] = base["candidate_year_id"].astype(str).isin(covered_before)
    base["marginal_before_batch"] = ~base["covered_before_batch"]
    base["machine_proposed_strict"] = (
        base["proposed_evidence_grade"].isin(["A", "B"])
        & _bool_series(base["strict_coverage_eligible"])
    )

    audit_columns = [
        "claim_id", "audit_inclusion_reason", "accepted_claim", "strict_identity",
        "strict_temporal", "strict_custom_domain", "coded_evidence_grade", "coded_url_type",
    ]
    missing = set(audit_columns) - set(adjudication.columns)
    if missing:
        raise ValueError(f"Adjudication missing columns: {sorted(missing)}")
    sampled = adjudication[
        adjudication["audit_inclusion_reason"].eq("sampled_marginal_grade_a")
    ]
    if len(sampled) != int(audit_manifest["sampled_marginal_grade_a"]):
        raise ValueError("Sampled-positive audit count disagrees with the manifest")
    if len(sampled):
        precision = float(_bool_series(sampled["accepted_claim"]).mean())
        strict_confirmation = float(
            _bool_series(sampled["strict_custom_domain"]).mean()
        )
        extrapolate = precision >= 0.95 and strict_confirmation >= 0.95
    else:
        precision = None
        strict_confirmation = None
        extrapolate = False

    result = base.merge(adjudication[audit_columns], on="claim_id", how="left", validate="one_to_one")
    result["batch_disposition"] = "not_selected_for_audit"
    result["acceptance_basis"] = "none"
    result["strict_under_batch_audit_rule"] = False

    reviewed = result["accepted_claim"].notna()
    accepted = reviewed & _bool_series(result["accepted_claim"])
    rejected = reviewed & ~_bool_series(result["accepted_claim"])
    result.loc[accepted, "batch_disposition"] = "human_accepted"
    result.loc[accepted, "acceptance_basis"] = "in_chat_explicit_approval"
    result.loc[rejected, "batch_disposition"] = "human_rejected"
    result.loc[rejected, "acceptance_basis"] = "in_chat_explicit_rejection"
    result.loc[
        accepted & _bool_series(result["strict_custom_domain"]),
        "strict_under_batch_audit_rule",
    ] = True

    inferred = (
        ~reviewed
        & result["marginal_before_batch"]
        & result["machine_proposed_strict"]
    )
    if extrapolate:
        result.loc[inferred, "batch_disposition"] = "accepted_under_sampled_audit_rule"
        result.loc[inferred, "acceptance_basis"] = "sampled_audit_95_percent_rule"
        result.loc[inferred, "strict_under_batch_audit_rule"] = True
    else:
        result.loc[inferred, "batch_disposition"] = "pending_failed_audit_extrapolation_gate"

    marginal_strict_ids = set(result.loc[
        result["marginal_before_batch"] & result["strict_under_batch_audit_rule"],
        "candidate_year_id",
    ].astype(str))
    summary = {
        "rule_version": "archiveit-sampled-audit-rule-v1",
        "audit_items": int(len(adjudication)),
        "sampled_positive_precision": precision,
        "sampled_strict_confirmation_rate": strict_confirmation,
        "extrapolation_gate_passed": extrapolate,
        "directly_human_accepted_strict_candidate_years": int(
            result.loc[
                result["batch_disposition"].eq("human_accepted")
                & result["strict_under_batch_audit_rule"],
                "candidate_year_id",
            ].nunique()
        ),
        "inferred_strict_candidate_years": int(
            result.loc[
                result["batch_disposition"].eq("accepted_under_sampled_audit_rule"),
                "candidate_year_id",
            ].nunique()
        ),
        "marginal_strict_candidate_years": len(marginal_strict_ids),
        "marginal_strict_candidate_year_ids": sorted(marginal_strict_ids),
    }
    return result.sort_values("claim_id").reset_index(drop=True), summary


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--validation", required=True)
    parser.add_argument("--adjudication", required=True)
    parser.add_argument("--audit-manifest", required=True)
    parser.add_argument("--existing-claims", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    disposition, summary = apply_rule(
        pd.read_csv(args.evidence, low_memory=False),
        pd.read_csv(args.validation, low_memory=False),
        pd.read_csv(args.adjudication, low_memory=False),
        json.loads(Path(args.audit_manifest).read_text(encoding="utf-8")),
        pd.read_csv(args.existing_claims, low_memory=False),
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    disposition.to_csv(output / "batch_claim_dispositions.csv", index=False)
    (output / "batch_claim_disposition_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
