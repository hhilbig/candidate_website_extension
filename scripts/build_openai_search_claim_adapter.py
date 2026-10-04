#!/usr/bin/env python3
"""Translate OpenAI search URL leads into pending unified-claim adapter rows."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
ADAPTER_COLUMNS = [
    "claim_id", "source_id", "parser_version", "source_record_id",
    "candidate_year_id", "candidate_contest_id", "contest_id", "full_name",
    "geo_name", "state_abb", "year", "month", "raw_url",
    "exact_normalized_url", "site_root_url", "registrable_domain", "url_type",
    "source_date", "source_profile_url", "url_source", "evidence_text",
    "discovery_method", "identity_match", "temporal_match", "evidence_grade",
    "review_status", "acceptance",
]


class OpenAIClaimAdapterError(RuntimeError):
    pass


def _truth(value: object) -> bool:
    return str(value).strip().casefold() in {"1", "true", "t", "yes", "y"}


def build_adapter(
    claims: pd.DataFrame,
    outputs: pd.DataFrame,
    *,
    source_id: str,
    source_date: str,
) -> tuple[pd.DataFrame, dict[str, object]]:
    required_claims = {
        "url_claim_id", "assistant_output_id", "candidate_year_id",
        "candidate_contest_id", "full_name", "geo_name", "state_abb", "year",
        "month", "raw_url", "exact_normalized_url", "site_root_url",
        "registrable_domain", "url_type", "eligible_candidate_site_lead",
        "parser_version",
    }
    required_outputs = {"assistant_output_id", "notes"}
    if missing := sorted(required_claims - set(claims.columns)):
        raise OpenAIClaimAdapterError(f"URL claims are missing columns: {missing}")
    if missing := sorted(required_outputs - set(outputs.columns)):
        raise OpenAIClaimAdapterError(f"assistant outputs are missing columns: {missing}")
    if claims["url_claim_id"].duplicated().any() or outputs["assistant_output_id"].duplicated().any():
        raise OpenAIClaimAdapterError("input claim or output keys are duplicated")
    eligible = claims.loc[claims["eligible_candidate_site_lead"].map(_truth)].copy()
    eligible = eligible.merge(
        outputs[["assistant_output_id", "notes"]],
        on="assistant_output_id",
        how="left",
        validate="many_to_one",
    )
    if eligible["notes"].isna().any():
        raise OpenAIClaimAdapterError("eligible claims lack their assistant output")
    rows = pd.DataFrame({
        "claim_id": eligible["url_claim_id"],
        "source_id": source_id,
        "parser_version": eligible["parser_version"],
        "source_record_id": eligible["assistant_output_id"],
        "candidate_year_id": eligible["candidate_year_id"],
        "candidate_contest_id": eligible["candidate_contest_id"],
        "contest_id": eligible["candidate_contest_id"].astype(str).str.rsplit("__", n=1).str[0],
        "full_name": eligible["full_name"],
        "geo_name": eligible["geo_name"],
        "state_abb": eligible["state_abb"],
        "year": eligible["year"],
        "month": eligible["month"],
        "raw_url": eligible["raw_url"],
        "exact_normalized_url": eligible["exact_normalized_url"],
        "site_root_url": eligible["site_root_url"],
        "registrable_domain": eligible["registrable_domain"],
        "url_type": eligible["url_type"],
        "source_date": source_date,
        "source_profile_url": "",
        "url_source": "OpenAI Responses web search structured campaign URL",
        "evidence_text": eligible["notes"].astype(str).str.slice(0, 3000),
        "discovery_method": "openai_responses_web_search_structured_campaign_url",
        "identity_match": "model_explicit_candidate_attribution_pending_independent_validation",
        "temporal_match": "not_assessed",
        "evidence_grade": "D",
        "review_status": "pending",
        "acceptance": "",
    }).reindex(columns=ADAPTER_COLUMNS)
    rows = rows.sort_values(["candidate_year_id", "claim_id"]).reset_index(drop=True)
    if rows["claim_id"].duplicated().any() or len(rows) != len(eligible):
        raise OpenAIClaimAdapterError("adapter output does not preserve eligible claim keys")
    audit = {
        "contract_version": "openai-search-unified-claim-adapter-v1",
        "source_id": source_id,
        "input_url_claim_rows": len(claims),
        "eligible_candidate_site_claim_rows": len(eligible),
        "adapter_claim_rows": len(rows),
        "adapter_candidate_years": int(rows["candidate_year_id"].nunique()),
        "adapter_candidate_domains": int(
            rows[["candidate_year_id", "registrable_domain"]].drop_duplicates().shape[0]
        ),
        "excluded_input_rows": len(claims) - len(rows),
        "excluded_fraction": float((len(claims) - len(rows)) / len(claims)) if len(claims) else 0.0,
        "large_drop_explanation": (
            "The adapter intentionally consumes only campaign URL rows already marked "
            "eligible. Evidence URLs and excluded campaign rows remain in url_claims.csv."
        ),
        "exact_eligible_claim_key_agreement": set(rows["claim_id"]) == set(eligible["url_claim_id"]),
        "strict_claims_emitted": 0,
    }
    return rows, audit


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", type=Path, required=True)
    parser.add_argument("--outputs", type=Path, required=True)
    parser.add_argument("--source-id", required=True)
    parser.add_argument("--source-date", default="2026-08-26")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    rows, audit = build_adapter(
        pd.read_csv(args.claims, keep_default_na=False, low_memory=False),
        pd.read_csv(args.outputs, keep_default_na=False, low_memory=False),
        source_id=args.source_id,
        source_date=args.source_date,
    )
    _atomic_csv(rows, args.output)
    audit.update({
        "output": {
            "path": str(args.output), "rows": len(rows), "bytes": args.output.stat().st_size,
            "sha256": hashlib.sha256(args.output.read_bytes()).hexdigest(),
        }
    })
    audit_path = args.output.with_name(args.output.stem + "_audit.json")
    audit_path.write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
