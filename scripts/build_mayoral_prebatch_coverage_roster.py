#!/usr/bin/env python3
"""Build a candidate-year coverage roster from claims and prior batch dispositions."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


def build_roster(claims: pd.DataFrame, dispositions: pd.DataFrame) -> pd.DataFrame:
    required_claims = {"candidate_year_id", "claim_origin", "strict_custom_domain"}
    required_dispositions = {
        "candidate_year_id", "strict_under_batch_audit_rule", "batch_disposition",
    }
    if missing := required_claims - set(claims.columns):
        raise ValueError(f"Claims missing columns: {sorted(missing)}")
    if missing := required_dispositions - set(dispositions.columns):
        raise ValueError(f"Dispositions missing columns: {sorted(missing)}")
    baseline = claims[
        claims["claim_origin"].eq("legacy_baseline")
        | claims["strict_custom_domain"].astype("boolean").fillna(False)
    ][["candidate_year_id", "claim_origin"]].copy()
    baseline["coverage_basis"] = baseline["claim_origin"].map(
        {"legacy_baseline": "legacy_source_backed"}
    ).fillna("directly_adjudicated_strict")
    inferred = dispositions[
        dispositions["strict_under_batch_audit_rule"].astype("boolean").fillna(False)
    ][["candidate_year_id", "batch_disposition"]].copy()
    inferred["coverage_basis"] = inferred["batch_disposition"].map({
        "human_accepted": "directly_adjudicated_strict",
        "accepted_under_sampled_audit_rule": "sampled_audit_rule_inferred",
    })
    if inferred["coverage_basis"].isna().any():
        raise ValueError("A strict disposition has no recognized coverage basis")
    inferred = inferred.rename(columns={"batch_disposition": "claim_origin"})
    combined = pd.concat([baseline, inferred], ignore_index=True)
    priority = {
        "legacy_source_backed": 1,
        "directly_adjudicated_strict": 2,
        "sampled_audit_rule_inferred": 3,
    }
    combined["priority"] = combined["coverage_basis"].map(priority)
    combined = combined.sort_values(["candidate_year_id", "priority"])
    roster = combined.groupby("candidate_year_id", as_index=False).last()
    roster["covered"] = True
    return roster[["candidate_year_id", "covered", "coverage_basis"]].sort_values(
        "candidate_year_id"
    ).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", required=True)
    parser.add_argument("--dispositions", required=True)
    parser.add_argument("--output", required=True)
    args = parser.parse_args()
    roster = build_roster(
        pd.read_csv(args.claims, low_memory=False),
        pd.read_csv(args.dispositions, low_memory=False),
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    roster.to_csv(output, index=False)
    print(roster["coverage_basis"].value_counts().sort_index().to_json(indent=2))


if __name__ == "__main__":
    main()
