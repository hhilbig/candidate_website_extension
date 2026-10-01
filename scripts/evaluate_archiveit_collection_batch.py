#!/usr/bin/env python3
"""Evaluate marginal yield and the continuation gate for an Archive-It batch."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _covered_ids(claims: pd.DataFrame) -> set[str]:
    if "covered" in claims.columns:
        covered = claims["covered"].astype("boolean").fillna(False)
        return set(claims.loc[covered, "candidate_year_id"].astype(str))
    legacy = claims["claim_origin"].eq("legacy_baseline")
    strict = claims["strict_custom_domain"].fillna(False).astype(bool)
    return set(claims.loc[legacy | strict, "candidate_year_id"].astype(str))


def evaluate_batch(
    config: dict,
    evidence: pd.DataFrame,
    attempts: pd.DataFrame,
    validation: pd.DataFrame,
    existing_claims: pd.DataFrame,
    audit_manifest: dict,
    audit_adjudication: pd.DataFrame | None = None,
) -> tuple[dict, pd.DataFrame]:
    collections = config.get("collections", [])
    universe_candidate_years = int(config.get("universe_candidate_years", 0))
    coverage_before = int(config.get("covered_candidate_years_before", -1))
    if universe_candidate_years <= 0 or not 0 <= coverage_before <= universe_candidate_years:
        raise ValueError("Batch config requires valid universe and pre-batch coverage counts")
    if len(collections) != 20:
        raise ValueError(f"Batch evaluation requires 20 configured collections, found {len(collections)}")
    configured_ids = {int(row["collection_id"]) for row in collections}
    if len(configured_ids) != 20:
        raise ValueError("Batch collection IDs must be unique")
    if len(attempts) != 20 or set(pd.to_numeric(attempts["collection_id"]).astype(int)) != configured_ids:
        raise ValueError("Batch must have one source-attempt row for every configured collection")
    if attempts.duplicated(["source_id", "contest_id"]).any():
        raise ValueError("Batch source attempts contain duplicate source/contest keys")
    if set(evidence["claim_id"].astype(str)) != set(validation["claim_id"].astype(str)):
        raise ValueError("Batch evidence and temporal validation claim IDs disagree")

    validation_fields = [
        "claim_id", "proposed_evidence_grade", "strict_coverage_eligible",
        "validation_reason",
    ]
    # The adapter emits provisional validation fields too.  The temporal
    # validator is authoritative for the batch evaluation, so remove any
    # same-named adapter fields instead of letting pandas silently suffix them.
    evidence_for_merge = evidence.drop(
        columns=[column for column in validation_fields[1:] if column in evidence.columns],
    )
    claims = evidence_for_merge.merge(
        validation[validation_fields], on="claim_id", how="left", validate="one_to_one",
    )
    covered = _covered_ids(existing_claims)
    claims["covered_before_batch"] = claims["candidate_year_id"].astype(str).isin(covered)
    claims["marginal_before_batch"] = ~claims["covered_before_batch"]
    claims["proposed_strict"] = (
        claims["proposed_evidence_grade"].isin(["A", "B"])
        & claims["strict_coverage_eligible"].fillna(False).astype(bool)
    )
    marginal_proposed_ids = set(
        claims.loc[
            claims["marginal_before_batch"] & claims["proposed_strict"],
            "candidate_year_id",
        ].astype(str)
    )

    per_collection = pd.DataFrame(collections)[[
        "collection_id", "collection_name", "priority_tier", "selection_reason",
        "uncovered_candidates_before",
    ]].copy()
    attempt_summary = attempts[[
        "collection_id", "source_status", "mayoral_seeds_considered",
        "candidate_matches", "eligible_url_leads",
    ]].copy()
    attempt_summary["collection_id"] = pd.to_numeric(attempt_summary["collection_id"]).astype(int)
    per_collection = per_collection.merge(
        attempt_summary, on="collection_id", how="left", validate="one_to_one",
    )
    grouped = claims.groupby(pd.to_numeric(claims["collection_id"]).astype(int)).agg(
        evidence_claims=("claim_id", "size"),
        candidate_years=("candidate_year_id", "nunique"),
        proposed_strict_claims=("proposed_strict", "sum"),
    )
    marginal_grouped = claims.loc[
        claims["marginal_before_batch"] & claims["proposed_strict"]
    ].groupby(pd.to_numeric(
        claims.loc[claims["marginal_before_batch"] & claims["proposed_strict"], "collection_id"]
    ).astype(int))["candidate_year_id"].nunique()
    grouped["marginal_proposed_strict_candidate_years"] = marginal_grouped
    per_collection = per_collection.merge(
        grouped.fillna(0), left_on="collection_id", right_index=True, how="left",
    ).fillna({
        "evidence_claims": 0, "candidate_years": 0, "proposed_strict_claims": 0,
        "marginal_proposed_strict_candidate_years": 0,
    })

    audit_status = "pending"
    audited_positive_precision = None
    sampled_strict_confirmation_rate = None
    ambiguous_strict_additions = 0
    sampled_negative_false_negatives = 0
    decision = "pending_sampled_human_audit"
    final_marginal_strict = None
    if audit_adjudication is not None:
        audit_status = "complete"
        required = {
            "claim_id", "audit_inclusion_reason", "candidate_year_id",
            "accepted_claim", "strict_custom_domain",
        }
        missing = required - set(audit_adjudication.columns)
        if missing:
            raise ValueError(f"Audit adjudication missing columns: {sorted(missing)}")
        if len(audit_adjudication) != int(audit_manifest["audit_items"]):
            raise ValueError("Audit adjudication row count does not match the audit manifest")
        if set(audit_adjudication["claim_id"].astype(str)) != set(
            map(str, audit_manifest["selected_claim_ids"])
        ):
            raise ValueError("Audit adjudication claim IDs do not match the audit manifest")
        expected_reason_counts = {
            "sampled_marginal_grade_a": int(audit_manifest["sampled_marginal_grade_a"]),
            "ambiguous_campaign_content": int(audit_manifest["ambiguous_campaign_content"]),
            "sampled_marginal_non_strict": int(audit_manifest["sampled_marginal_non_strict"]),
        }
        observed_reason_counts = audit_adjudication["audit_inclusion_reason"].value_counts()
        unexpected_reasons = set(map(str, observed_reason_counts.index)) - set(expected_reason_counts)
        if unexpected_reasons:
            raise ValueError(
                f"Audit adjudication has unexpected inclusion reasons: {sorted(unexpected_reasons)}"
            )
        actual_reason_counts = {
            reason: int(observed_reason_counts.get(reason, 0))
            for reason in expected_reason_counts
        }
        if actual_reason_counts != expected_reason_counts:
            raise ValueError("Audit adjudication inclusion reasons do not match the audit manifest")
        sampled_positive = audit_adjudication[
            audit_adjudication["audit_inclusion_reason"].eq("sampled_marginal_grade_a")
        ]
        audited_positive_precision = float(
            sampled_positive["accepted_claim"].fillna(False).astype(bool).mean()
        ) if len(sampled_positive) else None
        sampled_strict_confirmation_rate = float(
            sampled_positive["strict_custom_domain"].fillna(False).astype(bool).mean()
        ) if len(sampled_positive) else None
        ambiguous = audit_adjudication[
            audit_adjudication["audit_inclusion_reason"].eq("ambiguous_campaign_content")
            & audit_adjudication["strict_custom_domain"].fillna(False).astype(bool)
        ]
        ambiguous_strict_additions = int(ambiguous["candidate_year_id"].nunique())
        negative = audit_adjudication[
            audit_adjudication["audit_inclusion_reason"].eq("sampled_marginal_non_strict")
        ]
        sampled_negative_false_negatives = int(
            negative["strict_custom_domain"].fillna(False).astype(bool).sum()
        )
        if (
            audited_positive_precision is not None
            and audited_positive_precision >= 0.95
            and sampled_strict_confirmation_rate is not None
            and sampled_strict_confirmation_rate >= 0.95
        ):
            final_marginal_strict = len(marginal_proposed_ids) + ambiguous_strict_additions
        else:
            final_marginal_strict = int(
                audit_adjudication["strict_custom_domain"].fillna(False).astype(bool).sum()
            )
        if audited_positive_precision is not None and audited_positive_precision < 0.90:
            decision = "drop_archiveit_precision_below_90_percent"
        elif final_marginal_strict <= 3:
            decision = "drop_archiveit_no_more_than_three_additions"
        elif final_marginal_strict >= 10 and audited_positive_precision is not None and audited_positive_precision >= 0.95:
            decision = "continue_archiveit"
        elif 4 <= final_marginal_strict <= 9 and audited_positive_precision is not None and audited_positive_precision >= 0.95:
            decision = "run_one_more_small_archiveit_batch"
        else:
            decision = "hold_archiveit"

    summary = {
        "evaluation_version": "archiveit-collection-batch-v1",
        "batch_id": config.get("batch_id", ""),
        "collections": 20,
        "target_candidate_rows_with_collection_overlap": int(
            sum(int(row["uncovered_candidates_before"]) for row in collections)
        ),
        "source_attempts": int(len(attempts)),
        "attempt_statuses": {
            str(key): int(value)
            for key, value in attempts["source_status"].value_counts().sort_index().items()
        },
        "evidence_claims": int(len(claims)),
        "candidate_years_with_claims": int(claims["candidate_year_id"].nunique()),
        "marginal_candidate_years_with_claims": int(
            claims.loc[claims["marginal_before_batch"], "candidate_year_id"].nunique()
        ),
        "proposed_strict_candidate_years": int(
            claims.loc[claims["proposed_strict"], "candidate_year_id"].nunique()
        ),
        "marginal_proposed_strict_candidate_years": len(marginal_proposed_ids),
        "universe_candidate_years": universe_candidate_years,
        "covered_candidate_years_before": coverage_before,
        "coverage_before_percent": 100 * coverage_before / universe_candidate_years,
        "collections_with_any_claim": int((per_collection["candidate_years"] > 0).sum()),
        "collections_with_marginal_proposed_strict": int(
            (per_collection["marginal_proposed_strict_candidate_years"] > 0).sum()
        ),
        "public_api_or_license_cost_usd": 0.0,
        "serper_requests": 0,
        "audit_status": audit_status,
        "audit_items": int(audit_manifest["audit_items"]),
        "audited_positive_precision": audited_positive_precision,
        "sampled_strict_confirmation_rate": sampled_strict_confirmation_rate,
        "ambiguous_strict_additions": ambiguous_strict_additions,
        "sampled_negative_false_negatives": sampled_negative_false_negatives,
        "final_marginal_strict_candidate_years": final_marginal_strict,
        "covered_candidate_years_after_under_audit_rule": (
            coverage_before + final_marginal_strict
            if final_marginal_strict is not None else None
        ),
        "coverage_after_percent_under_audit_rule": (
            100 * (coverage_before + final_marginal_strict) / universe_candidate_years
            if final_marginal_strict is not None else None
        ),
        "decision": decision,
    }
    return summary, per_collection.sort_values("collection_id").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--evidence", required=True)
    parser.add_argument("--attempts", required=True)
    parser.add_argument("--validation", required=True)
    parser.add_argument("--existing-claims", required=True)
    parser.add_argument("--audit-manifest", required=True)
    parser.add_argument("--audit-adjudication")
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    adjudication = (
        pd.read_csv(args.audit_adjudication, low_memory=False)
        if args.audit_adjudication else None
    )
    summary, collections = evaluate_batch(
        json.loads(Path(args.config).read_text()),
        pd.read_csv(args.evidence, low_memory=False),
        pd.read_csv(args.attempts, low_memory=False),
        pd.read_csv(args.validation, low_memory=False),
        pd.read_csv(args.existing_claims, low_memory=False),
        json.loads(Path(args.audit_manifest).read_text()),
        audit_adjudication=adjudication,
    )
    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output / "batch_evaluation.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8",
    )
    collections.to_csv(output / "collection_evaluation.csv", index=False)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
