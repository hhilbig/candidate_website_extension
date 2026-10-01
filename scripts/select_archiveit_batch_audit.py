#!/usr/bin/env python3
"""Select a deterministic, marginal-coverage audit from an Archive-It batch."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
from pathlib import Path

import pandas as pd


AMBIGUOUS_REASONS = {"mayoral_campaign_not_confirmed"}


def _rank(seed: int, claim_id: object) -> str:
    return hashlib.sha256(f"{seed}\x1f{claim_id}".encode()).hexdigest()


def covered_candidate_years(claims: pd.DataFrame) -> set[str]:
    if {"candidate_year_id", "covered"}.issubset(claims.columns):
        covered = claims["covered"].astype("boolean").fillna(False)
        return set(claims.loc[covered, "candidate_year_id"].astype(str))
    required = {"candidate_year_id", "claim_origin", "strict_custom_domain"}
    missing = required - set(claims.columns)
    if missing:
        raise ValueError(f"Existing claim table missing columns: {sorted(missing)}")
    legacy = claims["claim_origin"].eq("legacy_baseline")
    strict = claims["strict_custom_domain"].fillna(False).astype(bool)
    return set(claims.loc[legacy | strict, "candidate_year_id"].astype(str))


def select_audit(
    review: pd.DataFrame,
    existing_claims: pd.DataFrame,
    seed: int = 20260825,
    positive_audit_n: int = 3,
    negative_share: float = 0.10,
) -> tuple[pd.DataFrame, dict]:
    required = {
        "claim_id", "candidate_year_id", "evidence_grade",
        "archive_validation_reason", "review_status",
    }
    missing = required - set(review.columns)
    if missing:
        raise ValueError(f"Archive-It review file missing columns: {sorted(missing)}")
    if review["claim_id"].isna().any() or review["claim_id"].duplicated().any():
        raise ValueError("Archive-It batch claims need unique, nonmissing claim IDs")
    if not review["review_status"].fillna("").eq("pending").all():
        raise ValueError("Archive-It batch audit input must be pending")
    if positive_audit_n < 0 or not 0 <= negative_share <= 1:
        raise ValueError("Invalid audit sample parameters")

    rows = review.copy()
    covered = covered_candidate_years(existing_claims)
    rows["covered_before_batch"] = rows["candidate_year_id"].astype(str).isin(covered)
    rows["marginal_before_batch"] = ~rows["covered_before_batch"]
    rows["audit_inclusion_reason"] = ""
    rows["_rank"] = rows["claim_id"].map(lambda value: _rank(seed, value))

    positive_pool = rows.loc[
        rows["marginal_before_batch"] & rows["evidence_grade"].eq("A")
    ].sort_values(["_rank", "claim_id"])
    positive_ids = set(positive_pool.head(positive_audit_n)["claim_id"])

    ambiguous = rows.loc[
        rows["marginal_before_batch"]
        & rows["archive_validation_reason"].isin(AMBIGUOUS_REASONS)
    ]
    ambiguous_ids = set(ambiguous["claim_id"])

    negative_pool = rows.loc[
        rows["marginal_before_batch"]
        & ~rows["evidence_grade"].isin(["A", "B"])
        & ~rows["claim_id"].isin(ambiguous_ids)
    ].sort_values(["_rank", "claim_id"])
    negative_n = math.ceil(len(negative_pool) * negative_share) if len(negative_pool) else 0
    negative_ids = set(negative_pool.head(negative_n)["claim_id"])

    rows.loc[rows["claim_id"].isin(positive_ids), "audit_inclusion_reason"] = (
        "sampled_marginal_grade_a"
    )
    rows.loc[rows["claim_id"].isin(ambiguous_ids), "audit_inclusion_reason"] = (
        "ambiguous_campaign_content"
    )
    rows.loc[rows["claim_id"].isin(negative_ids), "audit_inclusion_reason"] = (
        "sampled_marginal_non_strict"
    )
    selected_ids = positive_ids | ambiguous_ids | negative_ids
    selected = rows.loc[rows["claim_id"].isin(selected_ids)].copy()
    selected = selected.sort_values(
        ["audit_inclusion_reason", "collection_id", "candidate_year_id", "claim_id"]
    ).drop(columns="_rank").reset_index(drop=True)

    manifest = {
        "audit_version": "archiveit-batch-audit-v1",
        "seed": seed,
        "batch_claims": int(len(rows)),
        "batch_candidate_years": int(rows["candidate_year_id"].nunique()),
        "marginal_candidate_years": int(
            rows.loc[rows["marginal_before_batch"], "candidate_year_id"].nunique()
        ),
        "marginal_grade_a_candidate_years": int(
            positive_pool["candidate_year_id"].nunique()
        ),
        "sampled_marginal_grade_a": len(positive_ids),
        "ambiguous_campaign_content": len(ambiguous_ids),
        "non_strict_sampling_frame": int(len(negative_pool)),
        "sampled_marginal_non_strict": len(negative_ids),
        "audit_items": int(len(selected)),
        "selected_claim_ids": selected["claim_id"].astype(str).tolist(),
    }
    return selected, manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review", required=True)
    parser.add_argument("--existing-claims", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--manifest-output", required=True)
    parser.add_argument("--seed", type=int, default=20260825)
    parser.add_argument("--positive-audit-n", type=int, default=3)
    parser.add_argument("--negative-share", type=float, default=0.10)
    args = parser.parse_args()
    selected, manifest = select_audit(
        pd.read_csv(args.review, low_memory=False),
        pd.read_csv(args.existing_claims, low_memory=False),
        seed=args.seed,
        positive_audit_n=args.positive_audit_n,
        negative_share=args.negative_share,
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    selected.to_csv(output, index=False)
    manifest_path = Path(args.manifest_output)
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(manifest, indent=2))


if __name__ == "__main__":
    main()
