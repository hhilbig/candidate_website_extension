"""Write the input of the v6 URL search: primary candidates who are not yet finalists (approved by Hanno, October 17, 2026).

Input: v6_primaries/primaries.csv, the top two of each decisive partisan primary (one row per candidate).
A candidate is searched when the row is not unopposed and the person is not already a finalist of the general election.
IDs: the primary election is <city_id>__<primary_date>__primary_<party>, and a candidate is that ID plus "__" and the
first 16 hex digits of the SHA-256 of the candidate name. The v6 population build uses the same rule.
Output: url_discovery/v6_primaries_20261017/v6_args.json, one entry per city in the pass 4 format.

Usage:
  python scripts/build_mayoral_v6_primary_search_args.py
"""
from __future__ import annotations

import csv
import hashlib
import json
import re
from collections import defaultdict
from pathlib import Path

BASE = Path("data/municipal_discovery/top100_coverage")
PRIMARIES = BASE / "v6_primaries" / "primaries.csv"
OUT = BASE / "url_discovery" / "v6_primaries_20261017" / "v6_args.json"


def primary_election_id(row: dict) -> str:
    return f"{row['city_id']}__{row['primary_date']}__primary_{row['primary_party'].strip().lower()}"


def candidate_id(row: dict) -> str:
    return primary_election_id(row) + "__" + hashlib.sha256(row["candidate_name"].encode()).hexdigest()[:16]


def main() -> None:
    rows = list(csv.DictReader(PRIMARIES.open()))
    cities = defaultdict(list)
    for row in rows:
        if row.get("unopposed", "").strip().lower() == "true" or row["already_finalist"].strip().lower() == "yes":
            continue
        cities[(row["city_id"], row["city_name"], row["state_abb"])].append({
            "candidate_election_id": candidate_id(row), "candidate_name": row["candidate_name"],
            "role": f"primary_rank_{row['rank']}", "election_date": row["primary_date"], "group": "primary_candidate",
            "primary_party": row["primary_party"], "general_election_id": row["general_election_id"],
            "known_roster_urls": "", "site_check_rejected_targets": [], "earlier_attempts": []})
    out = []
    for (city_id, city_name, state_abb), finalists in sorted(cities.items()):
        slug = re.sub(r"[^a-z]+", "_", re.sub(r" (city|town|metro.*|\(balance\))$", "", city_name.lower())).strip("_")
        out.append({"city_id": city_id, "city_name": city_name, "state_abb": state_abb, "slug": slug,
                    "finalists": sorted(finalists, key=lambda f: (f["election_date"], f["candidate_name"]))})
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1))
    print(json.dumps({"cities": len(out), "candidates": sum(len(c["finalists"]) for c in out)}))


if __name__ == "__main__":
    main()
