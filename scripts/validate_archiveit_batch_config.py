#!/usr/bin/env python3
"""Validate that every configured Archive-It target is in scope and uncovered."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

import pandas as pd


def _truthy(series: pd.Series) -> pd.Series:
    return series.astype("string").str.casefold().isin({"1", "true", "yes"})


def validate_config(
    config: dict,
    universe: pd.DataFrame,
    coverage: pd.DataFrame,
    prior_collection_ids: set[int] | None = None,
) -> dict:
    collections = config.get("collections", [])
    if len(collections) != 20:
        raise ValueError(f"Expected 20 collection targets, found {len(collections)}")
    ids = [int(row["collection_id"]) for row in collections]
    if len(ids) != len(set(ids)):
        raise ValueError("Collection target IDs are not unique")
    overlap = sorted(set(ids) & (prior_collection_ids or set()))
    if overlap:
        raise ValueError(f"Collection targets were already used: {overlap}")
    if coverage["candidate_year_id"].astype(str).duplicated().any():
        raise ValueError("Pre-batch coverage roster has duplicate candidate-year IDs")
    covered = set(coverage.loc[_truthy(coverage["covered"]), "candidate_year_id"].astype(str))
    expected_covered = int(config["covered_candidate_years_before"])
    if len(covered) != expected_covered:
        raise ValueError(
            f"Pre-batch coverage is {len(covered)}, config records {expected_covered}"
        )
    scope = _truthy(universe["in_discovery_scope"])
    target_rows = 0
    contest_targets = 0
    for row in collections:
        selected = universe[
            scope
            & universe["geo_name"].astype("string").str.casefold().eq(str(row["geo_name"]).casefold())
            & universe["state_abb"].astype("string").str.upper().eq(str(row["state_abb"]).upper())
            & pd.to_numeric(universe["year"], errors="coerce").between(
                int(row["start_year"]), int(row["end_year"])
            )
        ]
        if selected.empty:
            raise ValueError(f"Collection {row['collection_id']} has no in-scope target")
        contests = selected["contest"].astype(str).nunique()
        if contests != 1:
            raise ValueError(
                f"Collection {row['collection_id']} targets {contests} contests; expected one"
            )
        uncovered = int((~selected["candidate_year_id"].astype(str).isin(covered)).sum())
        declared = int(row["uncovered_candidates_before"])
        if uncovered != declared:
            raise ValueError(
                f"Collection {row['collection_id']} has {uncovered} uncovered candidates, config records {declared}"
            )
        target_rows += uncovered
        contest_targets += contests
    return {
        "batch_id": config.get("batch_id", ""),
        "collections": len(collections),
        "contest_targets": contest_targets,
        "target_candidate_rows_with_collection_overlap": target_rows,
        "covered_candidate_years_before": len(covered),
        "prior_collection_overlap": overlap,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--universe", required=True)
    parser.add_argument("--coverage", required=True)
    parser.add_argument("--prior-config", action="append", default=[])
    parser.add_argument("--output")
    args = parser.parse_args()
    prior_ids: set[int] = set()
    for path in args.prior_config:
        prior = json.loads(Path(path).read_text(encoding="utf-8"))
        prior_ids.update(int(row["collection_id"]) for row in prior.get("collections", []))
    result = validate_config(
        json.loads(Path(args.config).read_text(encoding="utf-8")),
        pd.read_csv(args.universe, low_memory=False),
        pd.read_csv(args.coverage, low_memory=False),
        prior_ids,
    )
    if args.output:
        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
