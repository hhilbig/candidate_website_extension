"""Select the finalists for the fourth search pass (approved by Hanno on October 10, 2026) and write its input.

Groups (docs/status.md, approval (b)):
  stage2_only        no URL lead; only the stage 2 search ran, and its WebSearch budget was used up. These are the
                     12 cities that the October 4 repair omitted, plus three finalists whose repairs had no working search.
  check_incomplete   the legacy-lead search of October 9 left the check unfinished (one attempt, no working search).
  excluded           the site check rejected the only known site, and no later search ran (Bill Bell 2011 is
                     recovered by reassignment of billbellformayor.com instead).
  new_finalist       added by the population repair of October 10; never searched.
Output: url_discovery/pass4_20261010/pass4_args.json, one entry per city with its finalists and earlier notes.
"""
from __future__ import annotations

import csv
import glob
import json
import re
from collections import defaultdict
from pathlib import Path

BASE = Path("data/municipal_discovery/top100_coverage")
DISCOVERY = BASE / "url_discovery"
OUT = DISCOVERY / "pass4_20261010" / "pass4_args.json"
OMITTED_CITIES = {"Richmond", "El Paso", "Toledo", "Garland", "Lubbock", "Austin", "Santa Ana", "Scottsdale", "Reno",
                  "Virginia Beach", "Milwaukee", "New Orleans"}
NO_WORKING_REPAIR = {("Burnie Mansfield", "2012"), ("John Wright", "2022"), ("Cynthia Jaquith", "2017")}
RECOVERED_BY_REASSIGNMENT = {("Durham", "2011")}  # Bill Bell 2011
# Eligible finalists in release candidate v1 (before the population repair); every other eligible finalist is new.
V1_FINALISTS = Path("build/mayoral_release_v1_candidate/mayoral_finalists.csv")


def city_name(row: dict) -> str:
    return re.sub(r" (city|town|CDP|municipality|urban county|metro.*|.*balance.*)$", "", row["city_name"])


def slug(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def main() -> None:
    rows = [r for r in csv.DictReader((BASE / "outputs" / "candidate_coverage.csv").open()) if r["eligible"] == "True"]
    history = defaultdict(list)
    for path in sorted(glob.glob(str(DISCOVERY / "**" / "*_url_leads.json"), recursive=True)):
        if "haiku_pilot" in path or "pass4" in path:
            continue
        for lead in json.loads(Path(path).read_text()):
            history[lead.get("candidate_election_id")].append(
                {"file": str(Path(path).relative_to(DISCOVERY)), "status": lead.get("status", ""),
                 "domain": lead.get("registrable_domain", ""), "note": (lead.get("note") or "")[:800]})
    incomplete = {cid for cid, items in history.items()
                  if any(i["file"].startswith("legacy_search_20261009") and i["status"] == "incomplete" for i in items)}
    checks = defaultdict(list)
    for row in csv.DictReader((BASE / "release_site_check" / "site_check.csv").open()):
        checks[row["candidate_election_id"]].append(row)
    excluded = {cid for cid, items in checks.items() if all(i["status"] != "pass" for i in items)}

    before = {r["candidate_election_id"] for r in csv.DictReader(V1_FINALISTS.open()) if r["eligible"] == "True"}
    chosen = {}
    for r in rows:
        cid, key = r["candidate_election_id"], (r["candidate_name"], r["year"])
        has_text = r["downloaded_content"] == "True" and cid not in excluded
        if cid not in before:
            group = "new_finalist"
        elif r["url_lead"] == "False" and (city_name(r) in OMITTED_CITIES or key in NO_WORKING_REPAIR):
            group = "stage2_only"
        elif cid in incomplete and not has_text:
            group = "check_incomplete"
        elif cid in excluded and (city_name(r), r["year"]) not in RECOVERED_BY_REASSIGNMENT:
            group = "excluded"
        else:
            continue
        chosen[cid] = (r, group)

    cities = {}
    for cid, (r, group) in chosen.items():
        name = city_name(r)
        city = cities.setdefault(r["city_id"], {"city_id": r["city_id"], "city_name": name, "state_abb": r["state_abb"],
                                                "slug": slug(name), "finalists": []})
        rejected = sorted({i["target_id"] for i in checks.get(cid, []) if i["status"] != "pass"})
        city["finalists"].append({
            "candidate_election_id": cid, "candidate_name": r["candidate_name"], "role": r["official_result_role"],
            "election_date": r["election_date"], "group": group,
            "known_roster_urls": r["exact_urls"] if group == "new_finalist" else "",
            "site_check_rejected_targets": rejected,
            "earlier_attempts": history.get(cid, [])[-3:]})
    out = sorted(cities.values(), key=lambda c: c["slug"])
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(out, indent=1, ensure_ascii=False) + "\n")
    counts = defaultdict(int)
    for _, group in chosen.values():
        counts[group] += 1
    print(json.dumps({"cities": len(out), "finalists": len(chosen), "groups": counts}, indent=1))


if __name__ == "__main__":
    main()
