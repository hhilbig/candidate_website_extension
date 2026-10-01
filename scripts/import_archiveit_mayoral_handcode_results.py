#!/usr/bin/env python3
"""Import Archive-It handcoding results without changing the source evidence file."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
TASK_ID = "mayoral_url_archiveit_adjudication_20260824"
REQUIRED_ANSWERS = {
    "identity_match",
    "temporal_match",
    "accept_url",
    "url_type",
    "evidence_grade",
}
STRICT_GRADES = {"A", "B"}
STRICT_EXACT_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}


def import_results(
    results: dict,
    review_items: pd.DataFrame,
    expected_task_id: str = TASK_ID,
) -> pd.DataFrame:
    """Validate a complete export and return claim-level coded outcomes."""

    if results.get("format") != "handcode-results-v1":
        raise ValueError("Results must use handcode-results-v1")
    if results.get("task_id") != expected_task_id:
        raise ValueError(f"Results must have task_id {expected_task_id}")
    responses = results.get("responses", [])
    if len(responses) != len(review_items):
        raise ValueError(
            f"Result count {len(responses)} does not match review item count {len(review_items)}"
        )

    answer_rows = []
    for response in responses:
        answers = response.get("answers", {})
        missing = REQUIRED_ANSWERS - set(answers)
        if response.get("status") != "complete" or missing:
            raise ValueError(
                f"Incomplete response {response.get('item_id')}: {sorted(missing)}"
            )
        answer_rows.append({
            "item_id": response.get("item_id"),
            **{f"coded_{key}": value for key, value in answers.items()},
        })
    answers = pd.DataFrame(answer_rows)
    if answers["item_id"].isna().any() or answers["item_id"].duplicated().any():
        raise ValueError("Handcoding results contain missing or duplicate item IDs")

    merged = review_items.merge(answers, on="item_id", how="left", validate="one_to_one")
    if merged["coded_accept_url"].isna().any():
        raise ValueError("Handcoding results do not cover every review item")
    corrected = merged.get("coded_corrected_url", pd.Series("", index=merged.index))
    merged["accepted_url"] = corrected.fillna("").astype(str).str.strip()
    merged.loc[merged["accepted_url"].eq(""), "accepted_url"] = merged["candidate_url"]
    merged["accepted_claim"] = merged["coded_accept_url"].eq("yes")
    merged["strict_identity"] = merged["coded_identity_match"].eq("yes")
    merged["strict_temporal"] = merged["coded_temporal_match"].eq("yes")
    merged["strict_grade"] = merged["coded_evidence_grade"].isin(STRICT_GRADES)
    merged["strict_exact_url"] = (
        merged["accepted_claim"]
        & merged["strict_identity"]
        & merged["strict_temporal"]
        & merged["strict_grade"]
        & merged["coded_url_type"].isin(STRICT_EXACT_TYPES)
    )
    merged["strict_custom_domain"] = (
        merged["strict_exact_url"] & merged["coded_url_type"].eq("custom_domain")
    )
    return merged


def result_summary(adjudicated: pd.DataFrame) -> dict:
    return {
        "metric_version": "archiveit-handcode-v1",
        "claims_reviewed": int(len(adjudicated)),
        "accepted_claims": int(adjudicated["accepted_claim"].sum()),
        "strict_exact_url_claims": int(adjudicated["strict_exact_url"].sum()),
        "strict_custom_domain_claims": int(adjudicated["strict_custom_domain"].sum()),
        "strict_exact_candidate_years": int(
            adjudicated.loc[adjudicated["strict_exact_url"], "candidate_year_id"].nunique()
        ),
        "strict_custom_domain_candidate_years": int(
            adjudicated.loc[
                adjudicated["strict_custom_domain"], "candidate_year_id"
            ].nunique()
        ),
    }


def build_adjudicated_claims(adjudicated: pd.DataFrame) -> pd.DataFrame:
    """Return an adapter-evidence overlay consumable by the claim builder."""
    result = adjudicated.copy()
    result["raw_url"] = result["accepted_url"]
    result["exact_normalized_url"] = result["accepted_url"]
    result["evidence_url"] = result["accepted_url"]
    result["url_type"] = result["coded_url_type"]
    result["identity_match"] = result["coded_identity_match"]
    result["temporal_match"] = result["coded_temporal_match"]
    result["evidence_grade"] = (
        result["coded_evidence_grade"].astype(str).str.upper().replace({"REJECT": "D"})
    )
    result["acceptance"] = result["accepted_claim"].astype(bool)
    result["review_status"] = result["accepted_claim"].map(
        {True: "accepted", False: "rejected"}
    )
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results")
    parser.add_argument(
        "--review-items",
        default=(
            "quality_reports/mayoral_url_discovery_2026-08-24/"
            "handcode_archiveit/review_items.csv"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=(
            "quality_reports/mayoral_url_discovery_2026-08-24/handcode_archiveit"
        ),
    )
    parser.add_argument("--task-id", default=TASK_ID)
    args = parser.parse_args()

    results = json.loads((REPO_ROOT / args.results).read_text(encoding="utf-8"))
    adjudicated = import_results(
        results,
        pd.read_csv(REPO_ROOT / args.review_items, low_memory=False),
        expected_task_id=args.task_id,
    )
    output = REPO_ROOT / args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    adjudicated.to_csv(output / "adjudication.csv", index=False)
    build_adjudicated_claims(adjudicated).to_csv(
        output / "adjudicated_claims.csv", index=False
    )
    summary = result_summary(adjudicated)
    (output / "adjudication_summary.json").write_text(
        json.dumps(summary, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
