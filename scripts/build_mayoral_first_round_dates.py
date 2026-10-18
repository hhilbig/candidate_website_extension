"""Build the first-round date of each eligible mayoral election (v6 campaign_period flag).

Hanno approved on October 18, 2026: the first-round dates come from the recorded primaries, the notes of the city
calendar files and the archived Ballotpedia election pages (scripts/collect_mayoral_ballotpedia_round_dates.py).
A small agent search covers the elections that these sources leave open; its results enter through --search.
Elections that no source dates keep rounds = unknown and an empty first_round_date.

Sources, in order of precedence:
  primary_file   the general election of a city with a decisive partisan primary (outputs/primary_elections.csv)
  calendar_note  a date in the calendar note, before the election and at most 365 days before it
  ballotpedia    the archived Ballotpedia page of the city that lists the election date as one of its rounds
  agent_search   the search result lists (JSON: election_id, rounds, first_round_date, source_url, note)
rounds:
  two_rounds     an earlier round of the same race took place on first_round_date
  single_round   the election had one round (first_round_date = election_date)
  unknown        no source dates the first round

Usage:
  python scripts/build_mayoral_first_round_dates.py --ballotpedia v6_primaries/ballotpedia_rounds.jsonl [--search ...]
"""
from __future__ import annotations

import argparse
import csv
import glob
import json
import re
from collections import Counter
from datetime import date, datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
BASE = REPO / "data/municipal_discovery/top100_coverage"
OUT = BASE / "outputs" / "first_round_dates.csv"
COLUMNS = ["election_id", "city_id", "city_name", "state_abb", "election_date", "stage", "rounds", "first_round_date",
           "first_round_source", "first_round_source_url", "note"]
MONTHS = "January|February|March|April|May|June|July|August|September|October|November|December"
# Calendar-note dates that are not a first round of the mayoral race (checked by hand on October 18, 2026).
NOTE_EXCLUSIONS = {
    "3977000__2015-11-03__special": "the September 2015 primary listed no mayoral contest, so the special election had one round",
}
STATES = {
    "AL": "Alabama", "AK": "Alaska", "AZ": "Arizona", "AR": "Arkansas", "CA": "California", "CO": "Colorado",
    "CT": "Connecticut", "DC": "District of Columbia", "DE": "Delaware", "FL": "Florida", "GA": "Georgia",
    "HI": "Hawaii", "ID": "Idaho", "IL": "Illinois", "IN": "Indiana", "IA": "Iowa", "KS": "Kansas", "KY": "Kentucky",
    "LA": "Louisiana", "ME": "Maine", "MD": "Maryland", "MA": "Massachusetts", "MI": "Michigan", "MN": "Minnesota",
    "MS": "Mississippi", "MO": "Missouri", "MT": "Montana", "NE": "Nebraska", "NV": "Nevada", "NH": "New Hampshire",
    "NJ": "New Jersey", "NM": "New Mexico", "NY": "New York", "NC": "North Carolina", "ND": "North Dakota",
    "OH": "Ohio", "OK": "Oklahoma", "OR": "Oregon", "PA": "Pennsylvania", "RI": "Rhode Island",
    "SC": "South Carolina", "SD": "South Dakota", "TN": "Tennessee", "TX": "Texas", "UT": "Utah", "VT": "Vermont",
    "VA": "Virginia", "WA": "Washington", "WV": "West Virginia", "WI": "Wisconsin", "WY": "Wyoming",
}


def note_dates(text: str) -> set[str]:
    out = set(re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text))
    for month, day, year in re.findall(rf"({MONTHS}) (\d{{1,2}}),? (\d{{4}})", text):
        out.add(datetime.strptime(f"{month} {day} {year}", "%B %d %Y").strftime("%Y-%m-%d"))
    return out


def within_year_before(day: str, election_date: str) -> bool:
    gap = (date.fromisoformat(election_date) - date.fromisoformat(day)).days
    return 0 < gap <= 365


def from_note(election: dict, calendar: dict) -> dict | None:
    if election["election_id"] in NOTE_EXCLUSIONS:
        return {"rounds": "single_round", "first_round_date": election["election_date"], "first_round_source": "calendar_note",
                "first_round_source_url": calendar.get("source_url", ""), "note": NOTE_EXCLUSIONS[election["election_id"]]}
    earlier = sorted(d for d in note_dates(calendar.get("note", "")) if within_year_before(d, election["election_date"]))
    if not earlier:
        return None
    return {"rounds": "two_rounds", "first_round_date": earlier[0], "first_round_source": "calendar_note",
            "first_round_source_url": calendar.get("source_url", ""), "note": ""}


def from_ballotpedia(election: dict, pages: list[dict]) -> dict | None:
    """The page of the same city and state that lists the election date as a round."""
    suffix = f",_{STATES[election['state_abb']].replace(' ', '_')}_("
    hits = [p for p in pages if p["city_id"] == election["city_id"] and suffix in p["page"]
            and election["election_date"] in {f["date"] for f in p.get("fields", [])}]
    if not hits:
        return None
    page = hits[0]
    earlier = sorted(f["date"] for f in page["fields"] if within_year_before(f["date"], election["election_date"]))
    rounds = "two_rounds" if earlier else "single_round"
    return {"rounds": rounds, "first_round_date": earlier[0] if earlier else election["election_date"],
            "first_round_source": "ballotpedia", "first_round_source_url": page["snapshot"], "note": ""}


def build(elections: list[dict], primaries: list[dict], calendars: dict, pages: list[dict],
          search: dict[str, dict], conflicts: list | None = None) -> list[dict]:
    conflicts = [] if conflicts is None else conflicts
    primary_of = {p["general_election_id"]: p for p in primaries if p["general_election_id"]}
    out = []
    for e in elections:
        base = {k: e[k] for k in ["election_id", "city_id", "city_name", "state_abb", "election_date", "stage"]}
        if e["election_id"] in primary_of:
            p = primary_of[e["election_id"]]
            found = {"rounds": "two_rounds", "first_round_date": p["primary_date"], "first_round_source": "primary_file",
                     "first_round_source_url": p["source_url"], "note": f"decisive {p['primary_party']} primary"}
        else:
            note, page = from_note(e, calendars.get((e["city_id"], int(e["year"])), {})), from_ballotpedia(e, pages)
            if note and page and note["first_round_date"] != page["first_round_date"]:
                conflicts.append((e["election_id"], note["first_round_date"], page["first_round_date"]))
            found = note or page or search.get(e["election_id"])
        found = found or {"rounds": "unknown", "first_round_date": "", "first_round_source": "", "first_round_source_url": "", "note": ""}
        if found["rounds"] == "two_rounds" and not found["first_round_date"] < e["election_date"]:
            raise ValueError(f"{e['election_id']}: first round not before the election")
        out.append({**base, **{k: found.get(k, "") for k in COLUMNS if k not in base}})
    return sorted(out, key=lambda r: r["election_id"])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--ballotpedia", required=True, help="JSON lines from collect_mayoral_ballotpedia_round_dates.py")
    parser.add_argument("--search", nargs="*", default=[], help="JSON result lists of the agent search for the open elections")
    args = parser.parse_args()
    by_id = {e["election_id"]: e for e in csv.DictReader((BASE / "outputs/elections.csv").open())}
    eligible = {c["election_id"] for c in csv.DictReader((BASE / "outputs/candidate_coverage.csv").open())
                if c["eligible"] == "True"}
    calendars = {}
    for path in glob.glob(str(BASE / "inputs" / "*_calendar.json")):
        for row in json.load(open(path)):
            calendars[(row["city_id"], int(row["year"]))] = row
    pages = [json.loads(line) for line in open(args.ballotpedia)]
    pages = [p for p in pages if "fields" in p]
    search = {}
    for path in args.search:
        for r in json.load(open(path)):
            if r["rounds"] in {"two_rounds", "single_round"}:
                search[r["election_id"]] = {**r, "first_round_source": "agent_search",
                                            "first_round_source_url": r["source_url"]}
    conflicts = []
    rows = build([by_id[i] for i in sorted(eligible)], list(csv.DictReader((BASE / "outputs/primary_elections.csv").open())),
                 calendars, pages, search, conflicts)
    with OUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    print(json.dumps({"elections": len(rows), "rounds": dict(Counter(r["rounds"] for r in rows)),
                      "source": dict(Counter(r["first_round_source"] for r in rows)),
                      "note_ballotpedia_conflicts": conflicts}, indent=1))


if __name__ == "__main__":
    main()
