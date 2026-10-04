#!/usr/bin/env python3
"""Run the approved OpenAI search expansion on the 357 residual screen cases."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

import scripts.run_openai_search_residual_pilot_2010_2021 as pilot


REPO_ROOT = Path(__file__).resolve().parents[1]
SCREEN_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_SCREEN = SCREEN_DIR / "candidate_screening_sample_2010_2021.csv"
DEFAULT_SERPER_ELIGIBLE = (
    SCREEN_DIR / "search_source_expansion/temporal_validation/frozen/eligible_leads.csv"
)
DEFAULT_PILOT_MANIFEST = (
    SCREEN_DIR / "openai_search_residual_pilot/production/candidate_manifest.csv"
)
DEFAULT_OUTPUT_DIR = SCREEN_DIR / "openai_search_residual_expansion/production"
CONTRACT_VERSION = "openai-web-search-residual-expansion-v1"
LEAD_CONTRACT_VERSION = "openai-web-search-residual-expansion-leads-v1"
EXPECTED_ROWS = 357
MAX_REQUESTS = 357
COST_CEILING_USD = 5.50
EXPECTED_PERIOD_COUNTS = {"2010_2013": 118, "2014_2017": 118, "2018_2021": 121}


def build_expansion_manifest(
    screen: pd.DataFrame,
    serper_eligible: pd.DataFrame,
    pilot_manifest: pd.DataFrame,
) -> tuple[pd.DataFrame, dict[str, object]]:
    required = {
        "candidate_year_id", "candidate_contest_id", "full_name", "geo_name",
        "state_abb", "year", "month", "period",
    }
    missing = sorted(required - set(screen.columns))
    if missing:
        raise pilot.OpenAISearchPilotError(f"screen is missing columns: {missing}")
    if len(screen) != 480 or screen["candidate_year_id"].astype(str).duplicated().any():
        raise pilot.OpenAISearchPilotError("screen must contain 480 unique candidate-years")
    if len(pilot_manifest) != 100 or pilot_manifest["candidate_year_id"].astype(str).duplicated().any():
        raise pilot.OpenAISearchPilotError("pilot manifest must contain 100 unique candidate-years")
    if "candidate_year_id" not in serper_eligible:
        raise pilot.OpenAISearchPilotError("Serper eligible leads lack candidate_year_id")
    screen = screen.copy()
    if "sample_order" in screen.columns:
        screen = screen.rename(columns={"sample_order": "parent_sample_order"})
    screen_ids = set(screen["candidate_year_id"].map(pilot._text))
    serper_ids = set(serper_eligible["candidate_year_id"].map(pilot._text)) & screen_ids
    prior_ids = set(pilot_manifest["candidate_year_id"].map(pilot._text))
    if not prior_ids.issubset(screen_ids) or prior_ids & serper_ids:
        raise pilot.OpenAISearchPilotError("pilot exclusions differ from the fixed screen contract")
    selected = screen.loc[
        ~screen["candidate_year_id"].map(pilot._text).isin(serper_ids | prior_ids)
    ].copy()
    period_counts = {
        str(key): int(value) for key, value in selected.groupby("period").size().sort_index().items()
    }
    if len(selected) != EXPECTED_ROWS or period_counts != EXPECTED_PERIOD_COUNTS:
        raise pilot.OpenAISearchPilotError(
            f"expected 357 residual cases with {EXPECTED_PERIOD_COUNTS}; found {period_counts}"
        )
    selected["_order_hash"] = selected["candidate_year_id"].map(
        lambda value: hashlib.sha256(
            f"{CONTRACT_VERSION}\x1f{pilot._text(value)}".encode("utf-8")
        ).hexdigest()
    )
    selected = selected.sort_values(["period", "_order_hash", "candidate_year_id"]).reset_index(drop=True)
    selected.insert(0, "sample_order", range(1, len(selected) + 1))
    selected["residual_period_population"] = selected["period"].map(EXPECTED_PERIOD_COUNTS)
    selected["residual_inclusion_probability"] = 1.0
    selected["residual_population_weight"] = 1.0
    query_fields = selected["full_name"].map(pilot.repair_query_name)
    selected["query_full_name"] = query_fields.map(lambda value: value[0])
    selected["query_name_repaired"] = query_fields.map(lambda value: value[1])
    selected["query_name_repair_reason"] = query_fields.map(lambda value: value[2])
    selected["prompt"] = selected.apply(pilot._prompt, axis=1)
    selected["model"] = pilot.MODEL
    selected["model_snapshot"] = pilot.MODEL_SNAPSHOT
    selected["max_tool_calls"] = pilot.MAX_TOOL_CALLS
    selected["max_output_tokens"] = pilot.MAX_OUTPUT_TOKENS
    selected["search_context_size"] = "low"
    selected["sample_seed"] = pilot.SAMPLE_SEED
    selected["contract_version"] = CONTRACT_VERSION
    selected["attempt_id"] = selected.apply(
        lambda row: pilot._stable_id(
            "openai_search_attempt", CONTRACT_VERSION, row["candidate_year_id"],
            row["prompt"], pilot.MODEL_SNAPSHOT, pilot.MAX_TOOL_CALLS,
            pilot.MAX_OUTPUT_TOKENS, "low",
        ),
        axis=1,
    )
    manifest = selected.reindex(columns=pilot.MANIFEST_COLUMNS)
    if manifest["attempt_id"].duplicated().any():
        raise pilot.OpenAISearchPilotError("expansion attempt IDs are duplicated")
    audit = {
        "contract_version": CONTRACT_VERSION,
        "screen_rows": len(screen),
        "serper_eligible_owner_ids": len(serper_ids),
        "prior_pilot_rows": len(prior_ids),
        "candidate_manifest_rows": len(manifest),
        "period_counts": period_counts,
        "pilot_overlap": int(manifest["candidate_year_id"].isin(prior_ids).sum()),
        "serper_overlap": int(manifest["candidate_year_id"].isin(serper_ids).sum()),
        "model": pilot.MODEL,
        "model_snapshot": pilot.MODEL_SNAPSHOT,
        "max_requests": MAX_REQUESTS,
        "cost_ceiling_usd": COST_CEILING_USD,
        "maximum_next_response_cost_usd": pilot.MAX_NEXT_RESPONSE_COST,
        "stage": "prepared_no_paid_requests",
    }
    return manifest, audit


def prepare(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    manifest, audit = build_expansion_manifest(
        pd.read_csv(DEFAULT_SCREEN, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_SERPER_ELIGIBLE, keep_default_na=False, low_memory=False),
        pd.read_csv(DEFAULT_PILOT_MANIFEST, keep_default_na=False, low_memory=False),
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "candidate_manifest.csv"
    pilot._atomic_csv(manifest, manifest_path)
    pilot._atomic_csv(pd.DataFrame(columns=pilot.LEDGER_COLUMNS), output_dir / "request_ledger.csv")
    pilot._atomic_csv(pd.DataFrame(columns=pilot.SOURCE_COLUMNS), output_dir / "source_urls.csv")
    pilot._atomic_text("", output_dir / "raw_responses.jsonl")
    audit["candidate_manifest_sha256"] = pilot._sha256_file(manifest_path)
    pilot._atomic_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", output_dir / "audit.json")
    return audit


def run(output_dir: Path = DEFAULT_OUTPUT_DIR, *, request_budget: int = MAX_REQUESTS) -> dict[str, object]:
    return pilot.run(
        output_dir,
        request_budget=request_budget,
        max_requests=MAX_REQUESTS,
        expected_rows=EXPECTED_ROWS,
        cost_ceiling_usd=COST_CEILING_USD,
        contract_version=CONTRACT_VERSION,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["prepare", "check-key", "run"])
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--request-budget", type=int, default=MAX_REQUESTS)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(args.output_dir)
    elif args.command == "check-key":
        result = {"openai_api_key_available": bool(pilot.load_openai_api_key())}
    else:
        result = run(args.output_dir, request_budget=args.request_budget)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
