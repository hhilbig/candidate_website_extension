"""Merge the targeted population repair (October 10, 2026) into the coverage build inputs.

Input: data/municipal_discovery/top100_coverage/staging/repair_20261010/ with <slug>_calendar.json (one row per
repaired year) and <slug>_results.json (records for those years), written by
scripts/workflows_mayoral_population_repair.js (run wf_2542411c-23b) and checked by its verify agents.

For each city: a staged results record replaces the input record with the same election date, or is added; a
staged calendar row replaces the input row for the same year. A city without input files gets new ones, and they
are registered in config/mayoral_top_two_build.json. Corrections listed in FIXES are applied first.
"""
from __future__ import annotations

import json
from pathlib import Path

BASE = Path("data/municipal_discovery/top100_coverage")
STAGE = BASE / "staging" / "repair_20261010"
INPUTS = BASE / "inputs"
CONFIG = Path("config/mayoral_top_two_build.json")

# Cities whose calendar input has another file name (Santa Clarita: 2026 is in city_year_dispositions.json).
CALENDAR_FILES = {"santa_clarita": "santa_clarita_historical_calendar.json"}

# Corrections after reading the staged records.
FIXES = {
    ("corpus_christi", "2011-05-14"): {
        "names": {"Adame": "Joe Adame"},
        "note": " Winner full name Joe Adame from the city's 2011 general election summary as cited in his biography; "
                "the runner-up's first name is not in any source reached, so only the surname Moon is recorded.",
    },
}


def load(path: Path) -> list:
    return json.loads(path.read_text()) if path.exists() else []


def main() -> None:
    config = json.loads(CONFIG.read_text())
    args = json.loads((STAGE / "repair_args.json").read_text())
    report = {}
    for city in args:
        slug = city["slug"]
        staged_results = load(STAGE / f"{slug}_results.json")
        staged_calendar = load(STAGE / f"{slug}_calendar.json")
        for rec in staged_results:
            fix = FIXES.get((slug, rec["election_date"]))
            if fix:
                for cand in rec["candidates"]:
                    cand["name"] = fix["names"].get(cand["name"], cand["name"])
                rec["verification_note"] += fix["note"]
        results_path = INPUTS / f"{slug}_results.json"
        calendar_path = INPUTS / CALENDAR_FILES.get(slug, f"{slug}_calendar.json")
        results = load(results_path)
        by_date = {r["election_date"]: i for i, r in enumerate(results)}
        added = replaced = 0
        for rec in staged_results:
            if rec["election_date"] in by_date:
                results[by_date[rec["election_date"]]] = rec
                replaced += 1
            else:
                results.append(rec)
                added += 1
        results.sort(key=lambda r: r["election_date"])
        calendar = load(calendar_path)
        by_year = {int(r["year"]): i for i, r in enumerate(calendar)}
        for row in staged_calendar:
            if int(row["year"]) in by_year:
                calendar[by_year[int(row["year"])]] = row
            else:
                calendar.append(row)
        calendar.sort(key=lambda r: int(r["year"]))
        if staged_results or results_path.exists():
            results_path.write_text(json.dumps(results, indent=1, ensure_ascii=False) + "\n")
            if str(results_path) not in config["results"]:
                config["results"].append(str(results_path))
        calendar_path.write_text(json.dumps(calendar, indent=1, ensure_ascii=False) + "\n")
        if str(calendar_path) not in config["city_year_audits"]:
            config["city_year_audits"].append(str(calendar_path))
        report[slug] = {"results_added": added, "results_replaced": replaced, "calendar_rows": len(staged_calendar)}
    CONFIG.write_text(json.dumps(config, indent=2) + "\n")
    print(json.dumps(report, indent=1))


if __name__ == "__main__":
    main()
