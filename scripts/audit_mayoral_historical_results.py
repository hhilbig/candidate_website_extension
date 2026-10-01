"""Audit existing contest records against manually verified official results.

This script does not infer decisive stages or candidate roles from votes.
"""

import argparse
import collections
import csv
import hashlib
import json
from pathlib import Path


def build_audit(roster_path, results_path):
    rows = list(csv.DictReader(roster_path.open()))
    results = json.loads(results_path.read_text())
    ids = {r["candidate_year_id"]: r for r in rows}
    assert len(ids) == len(rows), "Duplicate candidate-year IDs"
    verified = {}
    vote_conflicts = []
    for result in results:
        date = result["election_date"]
        key = (result["roster_place_fips"], date[:4], int(date[5:7]))
        assert key not in verified, "Overlapping verified election records"
        if result["verification_status"] == "verified_decisive":
            assert {c["role"] for c in result["candidates"]} == {"winner", "runner_up"}
        for candidate in result["candidates"]:
            row = ids[candidate["candidate_year_id"]]
            assert row["place_fips"] == key[0] and row["year"] == key[1]
            assert int(float(row["month"])) == key[2]
            if float(row["votes"]) != candidate["votes"]:
                assert candidate.get("roster_vote_conflict") is True, candidate
                assert float(row["votes"]) == candidate["roster_votes"], candidate
                vote_conflicts.append({"candidate_year_id": candidate["candidate_year_id"], "roster_votes": float(row["votes"]), "official_votes": candidate["votes"], "source_url": result["source_url"]})
        verified[key] = result
    groups = collections.defaultdict(list)
    for row in rows:
        if 2010 <= int(row["year"]) <= 2021:
            groups[row["contest"]].append(row)
    contests = []
    for contest, members in sorted(groups.items()):
        row = members[0]
        month = int(float(row["month"])) if row["month"] else None
        result = verified.get((row["place_fips"], row["year"], month))
        stage = row["election_stage_status"]
        status = result["verification_status"] if result else "official_results_not_verified"
        if not result and stage == "single_recorded_mayoral_contest_per_city_year_stage_not_explicit":
            status = "stage_and_official_results_unresolved"
        contests.append({
            "contest": contest,
            "city_name": row["geo_name"], "state_abb": row["state_abb"],
            "roster_place_fips": row["place_fips"], "year": int(row["year"]),
            "month": month, "candidate_record_count": len(members),
            "inherited_stage": stage,
            "inherited_stage_source_url": row["election_stage_source_url"],
            "inherited_election_page_source_url": row["election_page_source_url"],
            "verification_status": status,
            "source_url": result["source_url"] if result else None,
            "election_date": result["election_date"] if result else None,
            "candidate_roles_assigned_from_official_results": status == "verified_decisive",
        })
    return {
        "cutoff": "2026-09-14", "scope": "All existing 2010-2021 roster contests; filter through the independent Census city crosswalk for top-100 coverage.",
        "complete": False,
        "limitations": [
            "Only records with verified_decisive status contain a verified final candidate pair. Other records retain unresolved statuses.",
            "Inherited verified stages often cite Ballotpedia. They are not official-result verification.",
            "An absent city-year does not prove that no mayoral election occurred.",
            "The audit does not infer winners, runners-up, or decisive stages from roster vote totals.",
            "Baltimore 2011 has inconsistent published totals and remains unresolved.",
        ],
        "inputs": [{"path": str(p), "sha256": hashlib.sha256(p.read_bytes()).hexdigest()} for p in [roster_path, results_path]],
        "candidate_records": sum(len(v) for v in groups.values()),
        "vote_conflicts": vote_conflicts,
        "contest_count": len(contests),
        "status_counts": dict(collections.Counter(c["verification_status"] for c in contests)),
        "source_conflicts": [{
            "city_name": "Baltimore", "year": 2011,
            "source_url": "https://msa.maryland.gov/msa/mdmanual/36loc/bcity/elect/general/bcit2011.html",
            "note": "State Archives lists Rawlings-Blake 39,548 and Griffin 6,022. Roster lists 37,970 and 5,826. Obtain certified final returns before promotion.",
        }],
        "contests": contests,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--roster", type=Path, default=Path("data/municipal_discovery/exhaustive_source_expansion_20260908/ownership_repair_20260910/combined/candidate_roster.csv"))
    parser.add_argument("--results", type=Path, default=Path("data/municipal_discovery/top100_coverage/inputs/historical_results.json"))
    parser.add_argument("--output", type=Path, default=Path("data/municipal_discovery/top100_coverage/inputs/historical_audit.json"))
    args = parser.parse_args()
    audit = build_audit(args.roster, args.results)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(audit, indent=2) + "\n")
    print(json.dumps({k: audit[k] for k in ["candidate_records", "contest_count", "status_counts"]}))


if __name__ == "__main__":
    main()
