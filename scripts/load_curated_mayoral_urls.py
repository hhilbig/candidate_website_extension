#!/usr/bin/env python3
"""Validate curated source-backed mayoral URL claims against the ALGEL universe."""

from __future__ import annotations

import argparse
import json
import sys
from datetime import date
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.build_mayoral_url_discovery import EVIDENCE_COLUMNS, write_csv_atomic
from src.mayoral_discovery import canonical_http_url, is_candidate_campaign_host


MATCH_COLUMNS = ["geo_name", "state_abb", "year", "full_name"]


def load_curated_claims(universe: pd.DataFrame, claims: list[dict]) -> pd.DataFrame:
    scoped = universe[universe["in_discovery_scope"]].copy()
    evidence = []
    for claim in claims:
        missing = [column for column in MATCH_COLUMNS if column not in claim]
        if missing:
            raise ValueError(f"Curated claim missing match fields: {missing}")
        matches = scoped
        for column in MATCH_COLUMNS:
            matches = matches[matches[column].eq(claim[column])]
        matches = matches.drop_duplicates("candidate_year_id")
        if len(matches) != 1:
            raise ValueError(
                f"Curated claim must match exactly one candidate-year; "
                f"found {len(matches)} for {[claim[c] for c in MATCH_COLUMNS]}"
            )
        url = canonical_http_url(claim.get("evidence_url", ""))
        if not url or not is_candidate_campaign_host(url):
            raise ValueError(f"Invalid or excluded campaign URL: {claim.get('evidence_url')}")
        required = ["url_source", "source_profile_url", "evidence_text", "discovery_method", "confidence"]
        absent = [column for column in required if not str(claim.get(column, "")).strip()]
        if absent:
            raise ValueError(f"Curated claim missing provenance fields: {absent}")
        matched = matches.iloc[0]
        evidence.append({
            "candidate_year_id": matched["candidate_year_id"],
            "ledb_candid": matched["ledb_candid"],
            "full_name": matched["full_name"],
            "geo_name": matched["geo_name"],
            "state_abb": matched["state_abb"],
            "year": matched["year"],
            "candidate_source": "American Local Government Elections Database",
            "candidate_source_doi": "10.17605/OSF.IO/MV5E6",
            "url_source": claim["url_source"],
            "source_profile_url": claim["source_profile_url"],
            "source_snapshot_timestamp": claim.get("source_snapshot_timestamp", ""),
            "retrieved_on": claim.get("retrieved_on", date.today().isoformat()),
            "evidence_url": url,
            "evidence_text": claim["evidence_text"],
            "discovery_method": claim["discovery_method"],
            "confidence": claim["confidence"],
        })
    return pd.DataFrame(evidence).reindex(columns=EVIDENCE_COLUMNS)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-dir", default="data/municipal_discovery/shard_0")
    parser.add_argument("--claims", default="config/curated_mayoral_url_evidence.json")
    args = parser.parse_args()
    directory = Path(args.input_dir)
    universe = pd.read_csv(directory / "mayoral_candidate_universe.csv", low_memory=False)
    claims = json.loads(Path(args.claims).read_text(encoding="utf-8"))
    evidence = load_curated_claims(universe, claims)
    write_csv_atomic(evidence, directory / "mayoral_url_evidence_curated.csv", EVIDENCE_COLUMNS)
    print({"evidence_rows": len(evidence), "candidate_years": evidence["candidate_year_id"].nunique()})


if __name__ == "__main__":
    main()
