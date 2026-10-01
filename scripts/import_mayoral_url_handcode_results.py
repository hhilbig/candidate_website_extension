#!/usr/bin/env python3
"""Import mayoral URL handcoding results and estimate pilot yield."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import numpy as np
import pandas as pd

from src.mayoral_discovery import canonical_http_url


REPO_ROOT = Path(__file__).resolve().parents[1]
STRICT_GRADES = {"A", "B"}
STRICT_EXACT_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
REQUIRED_ANSWERS = {
    "identity_match", "temporal_match", "accept_url", "url_type", "evidence_grade"
}


def import_results(results: dict, review_items: pd.DataFrame) -> pd.DataFrame:
    if results.get("format") != "handcode-results-v1":
        raise ValueError("Results must use handcode-results-v1")
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
            raise ValueError(f"Incomplete response {response.get('item_id')}: {sorted(missing)}")
        answer_rows.append({"item_id": response["item_id"], **answers})
    answer_frame = pd.DataFrame(answer_rows)
    if answer_frame["item_id"].duplicated().any():
        raise ValueError("Handcoding results contain duplicate item IDs")
    merged = review_items.merge(answer_frame, on="item_id", how="left", validate="one_to_one")
    if merged["accept_url"].isna().any():
        raise ValueError("Handcoding results do not cover every review item")
    merged["accepted_claim"] = merged["accept_url"].eq("yes")
    merged["strict_identity"] = merged["identity_match"].eq("yes")
    merged["strict_temporal"] = merged["temporal_match"].eq("yes")
    merged["strict_grade"] = merged["evidence_grade"].isin(STRICT_GRADES)
    merged["strict_exact_url"] = (
        merged["accepted_claim"]
        & merged["strict_identity"]
        & merged["strict_temporal"]
        & merged["strict_grade"]
        & merged["url_type"].isin(STRICT_EXACT_TYPES)
    )
    merged["strict_custom_domain"] = (
        merged["strict_exact_url"] & merged["url_type"].eq("custom_domain")
    )
    return merged


def build_adjudicated_claims(adjudicated: pd.DataFrame) -> pd.DataFrame:
    """Return reviewed URL leads in the unified adapter-evidence contract."""
    result = adjudicated.copy()
    corrected = result.get("corrected_url", pd.Series("", index=result.index))
    corrected = corrected.fillna("").astype(str).str.strip()
    original = result.get("candidate_url", pd.Series("", index=result.index))
    original = original.fillna("").astype(str).str.strip()
    result["accepted_url"] = corrected.where(corrected.ne(""), original)
    invalid_acceptance = result["accepted_claim"] & result["accepted_url"].eq("")
    if invalid_acceptance.any():
        ids = result.loc[invalid_acceptance, "item_id"].astype(str).tolist()
        raise ValueError(f"Accepted handcode rows have no URL: {ids[:5]}")
    result = result.loc[result["accepted_url"].ne("")].copy()
    result["exact_normalized_url"] = result["accepted_url"].map(
        lambda value: canonical_http_url(value)
        or (
            canonical_http_url(f"http://{value}")
            if str(value).casefold().startswith("www.")
            else ""
        )
    )
    invalid_url = result["exact_normalized_url"].eq("")
    if invalid_url.any():
        ids = result.loc[invalid_url, "item_id"].astype(str).tolist()
        raise ValueError(f"Handcode rows have no valid HTTP(S) URL: {ids[:5]}")
    result["claim_id"] = result["item_id"].astype(str).map(
        lambda value: "search_pilot_claim_" + hashlib.sha256(
            value.encode("utf-8")
        ).hexdigest()[:24]
    )
    result["source_id"] = "serper_search_pilot"
    result["parser_version"] = "mayoral_search_pilot_handcode_v1"
    result["raw_url"] = result["accepted_url"]
    result["evidence_url"] = result["exact_normalized_url"]
    result["review_status"] = result["accepted_claim"].map(
        {True: "accepted", False: "rejected"}
    )
    result["acceptance"] = result["accepted_claim"].astype(bool)
    result["source_profile_url"] = result.get(
        "evidence_source_url", pd.Series("", index=result.index)
    )
    result["discovery_method"] = "search_pilot_human_adjudication"
    result["evidence_text"] = result.get(
        "result_snippet", pd.Series("", index=result.index)
    ).fillna("")
    return result


def _stratified_bootstrap(
    sample: pd.DataFrame,
    population: pd.DataFrame,
    outcome: str,
    repetitions: int,
    seed: int,
) -> np.ndarray:
    rng = np.random.default_rng(seed)
    keys = ["period_stratum", "profile_stratum"]
    population_counts = population.groupby(keys).size()
    total = int(population_counts.sum())
    estimates = np.zeros(repetitions)
    groups = {key: frame for key, frame in sample.groupby(keys)}
    for draw in range(repetitions):
        projected = 0.0
        for key, count in population_counts.items():
            values = groups[key][outcome].astype(float).to_numpy()
            resampled = rng.choice(values, size=len(values), replace=True)
            projected += count * float(resampled.mean())
        estimates[draw] = projected / total
    return estimates


def pilot_metrics(
    adjudicated: pd.DataFrame,
    query_manifest: pd.DataFrame,
    candidate_summary: pd.DataFrame,
    repetitions: int = 10_000,
    seed: int = 20260824,
) -> dict:
    sample = query_manifest.sort_values("query_variant").drop_duplicates("candidate_year_id")[
        ["candidate_year_id", "period_stratum", "profile_stratum"]
    ].copy()
    candidate_outcomes = adjudicated.groupby("candidate_year_id").agg(
        strict_custom_domain=("strict_custom_domain", "max"),
        strict_exact_url=("strict_exact_url", "max"),
    ).reset_index()
    sample = sample.merge(candidate_outcomes, on="candidate_year_id", how="left")
    outcome_columns = ["strict_custom_domain", "strict_exact_url"]
    sample[outcome_columns] = (
        sample[outcome_columns].astype("boolean").fillna(False).astype(bool)
    )

    unresolved = candidate_summary.loc[
        ~candidate_summary["has_validated_campaign_url"].fillna(False)
    ].copy()
    unresolved["profile_stratum"] = unresolved["ballotpedia_url"].notna().map(
        {True: "profile", False: "no_profile"}
    )
    unresolved["period_stratum"] = "outside_period"
    unresolved.loc[unresolved["year"].between(2000, 2007), "period_stratum"] = "2000_2007"
    unresolved.loc[unresolved["year"].between(2008, 2014), "period_stratum"] = "2008_2014"
    unresolved.loc[unresolved["year"].between(2015, 2021), "period_stratum"] = "2015_2021"
    unresolved = unresolved.loc[unresolved["period_stratum"].ne("outside_period")]

    result = {
        "metric_version": "mayoral-search-pilot-v1",
        "sample_candidate_years": int(len(sample)),
        "unresolved_population": int(len(unresolved)),
        "bootstrap_repetitions": repetitions,
        "bootstrap_seed": seed,
        "adjudication": {},
        "metrics": {},
    }
    lead_origin = adjudicated.get(
        "lead_origin", pd.Series("positive_lead", index=adjudicated.index)
    ).fillna("").astype(str)
    negative_audit = lead_origin.eq("negative_audit")
    accepted = adjudicated.get(
        "accepted_claim", pd.Series(False, index=adjudicated.index)
    ).astype("boolean").fillna(False).astype(bool)
    positive_n = int((~negative_audit).sum())
    accepted_positive_n = int((accepted & ~negative_audit).sum())
    result["adjudication"] = {
        "reviewed_items": int(len(adjudicated)),
        "positive_leads_reviewed": positive_n,
        "accepted_positive_claims": accepted_positive_n,
        "positive_lead_precision": (
            accepted_positive_n / positive_n if positive_n else None
        ),
        "negative_audits_reviewed": int(negative_audit.sum()),
        "negative_audit_false_negatives": int((accepted & negative_audit).sum()),
    }
    for outcome in ("strict_custom_domain", "strict_exact_url"):
        draws = _stratified_bootstrap(sample, unresolved, outcome, repetitions, seed)
        rates = sample.groupby(["period_stratum", "profile_stratum"])[outcome].mean()
        counts = unresolved.groupby(["period_stratum", "profile_stratum"]).size()
        weighted_rate = float((rates * counts).sum() / counts.sum())
        result["metrics"][outcome] = {
            "raw_successes": int(sample[outcome].sum()),
            "raw_rate": float(sample[outcome].mean()),
            "post_stratified_rate": weighted_rate,
            "post_stratified_projected_additions": float(weighted_rate * len(unresolved)),
            "bootstrap_90_percent_interval": [
                float(np.quantile(draws, 0.05)), float(np.quantile(draws, 0.95))
            ],
        }
    return result


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("results")
    parser.add_argument(
        "--review-items",
        default="quality_reports/mayoral_url_discovery_2026-08-24/handcode/review_items.csv",
    )
    parser.add_argument(
        "--query-manifest", default="data/municipal_discovery/search_pilot/query_manifest.csv"
    )
    parser.add_argument(
        "--candidate-summary",
        default="data/municipal_discovery/final/mayoral_candidate_url_summary.csv",
    )
    parser.add_argument(
        "--output-dir", default="quality_reports/mayoral_url_discovery_2026-08-24/handcode"
    )
    args = parser.parse_args()
    results = json.loads((REPO_ROOT / args.results).read_text(encoding="utf-8"))
    adjudicated = import_results(results, pd.read_csv(REPO_ROOT / args.review_items))
    metrics = pilot_metrics(
        adjudicated,
        pd.read_csv(REPO_ROOT / args.query_manifest),
        pd.read_csv(REPO_ROOT / args.candidate_summary, low_memory=False),
    )
    output = REPO_ROOT / args.output_dir
    output.mkdir(parents=True, exist_ok=True)
    claims = build_adjudicated_claims(adjudicated)
    adjudication_records = adjudicated.where(pd.notna(adjudicated), None).to_dict("records")
    (output / "adjudication.json").write_text(
        json.dumps(adjudication_records, indent=2, default=str) + "\n", encoding="utf-8"
    )
    (output / "pilot_metrics.json").write_text(
        json.dumps(metrics, indent=2) + "\n", encoding="utf-8"
    )
    claims.to_csv(output / "adjudicated_claims.csv", index=False)
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
