"""Build the v6 primary population: decisive partisan mayoral primaries and their top two candidates.

Hanno decided on October 17, 2026 that v6 adds the top two of the decisive primary (the primary of the party whose
nominee won the general election, normally Democratic) in the cities with partisan mayoral primaries. Unopposed
primaries stay in the files with a flag. The DC Democratic primary of June 16, 2026 is included; its general
election is after the population cutoff, so it has no general_election_id.

Input:  v6_primaries/primaries.csv (one row per candidate, all rows checked against official results)
        outputs/candidate_coverage.csv (the finalists, to link a primary candidate to the same person's finalist row)
Output: outputs/primary_elections.csv    one row per primary
        outputs/primary_candidates.csv   one row per top-two primary candidate
IDs use the rule of scripts/build_mayoral_v6_primary_search_args.py (shared through its functions).
A primary candidate who is also a finalist of the general election carries that finalist's candidate_election_id
in finalist_candidate_election_id; the person was not searched again, and the release takes the primary-period
text from the finalist's pages.

Usage:
  python scripts/build_mayoral_v6_primaries.py
"""
from __future__ import annotations

import csv
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_mayoral_release import name_parts  # noqa: E402
from scripts.build_mayoral_v6_primary_search_args import candidate_id, primary_election_id  # noqa: E402

BASE = REPO / "data/municipal_discovery/top100_coverage"
PRIMARIES = BASE / "v6_primaries" / "primaries.csv"
COVERAGE = BASE / "outputs" / "candidate_coverage.csv"
ELECTION_COLUMNS = ["primary_election_id", "city_id", "city_name", "state_abb", "year", "primary_date", "primary_party",
                    "general_election_id", "unopposed", "total_candidates_on_ballot", "source_url", "second_source_url"]
CANDIDATE_COLUMNS = ["candidate_election_id", "primary_election_id", "city_id", "city_name", "state_abb", "year",
                     "election_date", "primary_party", "general_election_id", "candidate_name", "primary_rank", "votes",
                     "vote_share", "unopposed", "finalist_candidate_election_id", "finalist_role_in_general",
                     "searched", "source_url"]


def truth(value: str) -> bool:
    return str(value).strip().lower() == "true"


def link_finalists(rows: list[dict], coverage: list[dict]) -> dict[str, str]:
    """candidate_election_id of the finalist row of the same person in the linked general election."""
    by_election = defaultdict(list)
    for person in coverage:
        by_election[person["election_id"]].append(person)
    out = {}
    for row in rows:
        if row["already_finalist"] != "yes":
            continue
        key = name_parts(row["candidate_name"])
        hits = [p for p in by_election[row["general_election_id"]] if name_parts(p["candidate_name"])[1] == key[1]]
        if len(hits) != 1:
            raise ValueError(f"finalist link for {row['candidate_name']} {row['general_election_id']}: {len(hits)} matches")
        out[candidate_id(row)] = hits[0]["candidate_election_id"]
    return out


def build(rows: list[dict], coverage: list[dict]) -> tuple[list[dict], list[dict]]:
    links = link_finalists(rows, coverage)
    elections, candidates = {}, []
    for row in rows:
        pid = primary_election_id(row)
        elections.setdefault(pid, {
            "primary_election_id": pid, "city_id": row["city_id"], "city_name": row["city_name"],
            "state_abb": row["state_abb"], "year": row["primary_date"][:4], "primary_date": row["primary_date"],
            "primary_party": row["primary_party"], "general_election_id": row["general_election_id"],
            "unopposed": str(truth(row["unopposed"])), "total_candidates_on_ballot": row["total_candidates_on_ballot"],
            "source_url": row["source_url"], "second_source_url": row["second_source_url"]})
        cid = candidate_id(row)
        candidates.append({
            "candidate_election_id": cid, "primary_election_id": pid, "city_id": row["city_id"],
            "city_name": row["city_name"], "state_abb": row["state_abb"], "year": row["primary_date"][:4],
            "election_date": row["primary_date"], "primary_party": row["primary_party"],
            "general_election_id": row["general_election_id"], "candidate_name": row["candidate_name"],
            "primary_rank": row["rank"], "votes": row["votes"], "vote_share": row["vote_share"],
            "unopposed": str(truth(row["unopposed"])), "finalist_candidate_election_id": links.get(cid, ""),
            "finalist_role_in_general": row["finalist_role_in_general"] if cid in links else "",
            "searched": str(cid not in links and not truth(row["unopposed"])), "source_url": row["source_url"]})
    ids = [c["candidate_election_id"] for c in candidates]
    if len(ids) != len(set(ids)):
        raise ValueError("duplicate primary candidate ID")
    return sorted(elections.values(), key=lambda e: e["primary_election_id"]), sorted(candidates, key=lambda c: c["candidate_election_id"])


def write(path: Path, rows: list[dict], columns: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def main() -> None:
    rows = list(csv.DictReader(PRIMARIES.open()))
    coverage = list(csv.DictReader(COVERAGE.open()))
    elections, candidates = build(rows, coverage)
    write(BASE / "outputs" / "primary_elections.csv", elections, ELECTION_COLUMNS)
    write(BASE / "outputs" / "primary_candidates.csv", candidates, CANDIDATE_COLUMNS)
    print(json.dumps({"primaries": len(elections), "unopposed_primaries": sum(e["unopposed"] == "True" for e in elections),
                      "cities": len({e["city_id"] for e in elections}), "candidates": len(candidates),
                      "linked_to_finalist": sum(bool(c["finalist_candidate_election_id"]) for c in candidates),
                      "searched": sum(c["searched"] == "True" for c in candidates),
                      "party": dict(Counter(e["primary_party"] for e in elections))}, indent=1))


if __name__ == "__main__":
    main()
