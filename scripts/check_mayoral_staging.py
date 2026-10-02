"""Check staged mayoral city files and dry-run the coverage build with them.

Reads data/municipal_discovery/top100_coverage/staging/, validates each city's
calendar and results files against the build's rules, compares any election
that already exists in the registered inputs, and runs
scripts/build_mayoral_top_two_coverage.py on a temporary copy of the build
configuration. It writes nothing into the inputs or the real outputs.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import tempfile
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[1]
BASE = REPO / "data/municipal_discovery/top100_coverage"
STATUSES = {"complete", "no_election", "indirect_selection", "earlier_round_only", "unresolved"}
STAGES = {"general", "runoff", "ranked_choice", "ranked_choice_final", "special", "primary_nominating"}
OFFICIAL_HINTS = (".gov", ".us", "clarityelections.com", "vote.", "elections", "clerk", "boe", "sos.")
NON_OFFICIAL = ("wikipedia.org", "ballotpedia.org", "nytimes.com", "news", "patch.com")


def key(record):
    city = str(record.get("roster_place_fips") or record.get("city_id") or "").lstrip("0")
    return city, record["election_date"], record.get("election_type", "regular")


def check_city(slug, existing):
    problems, notes = [], []
    cal_path, res_path = BASE / "staging" / f"{slug}_calendar.json", BASE / "staging" / f"{slug}_results.json"
    try:
        cal, res = json.loads(cal_path.read_text()), json.loads(res_path.read_text())
    except Exception as exc:  # noqa: BLE001 - report any read failure as a problem
        return {"slug": slug, "problems": [f"unreadable: {exc}"], "notes": [], "duplicates": []}
    years = [e.get("year") for e in cal]
    if sorted(years) != list(range(2010, 2027)):
        problems.append(f"calendar years {sorted(years)}")
    if len({e.get("city_id") for e in cal}) != 1:
        problems.append("calendar has several city_id values")
    dates = Counter()
    for r in res:
        dates[r["election_date"][:4]] += 1
        if r.get("stage") not in STAGES:
            problems.append(f"{r['election_date']} stage {r.get('stage')}")
        if (r.get("stage") == "primary_nominating" and r.get("verification_status") == "verified_decisive"
                and not r.get("decisive_stage_source_url")):
            problems.append(f"{r['election_date']} primary without decisive_stage_source_url")
        roles = Counter(c.get("role") for c in r.get("candidates", []))
        if r.get("verification_status") == "verified_decisive" and roles not in (Counter({"winner": 1, "runner_up": 1}), Counter({"winner": 1})):
            problems.append(f"{r['election_date']} roles {dict(roles)}")
        if any(not isinstance(c.get("votes"), int) for c in r.get("candidates", [])):
            problems.append(f"{r['election_date']} non-integer votes")
        src = (r.get("source_url") or "").lower()
        if any(b in src for b in NON_OFFICIAL[:3]):
            problems.append(f"{r['election_date']} non-official source {src}")
        elif not any(h in src for h in OFFICIAL_HINTS):
            notes.append(f"{r['election_date']} check source domain {src}")
    for e in cal:
        if e.get("status") not in STATUSES:
            problems.append(f"{e.get('year')} status {e.get('status')}")
        expected = e.get("expected_election_dates") or []
        if e.get("status") != "complete" and expected:
            problems.append(f"{e.get('year')} expected dates on a {e.get('status')} year")
        if e.get("status") == "complete":
            have = sorted(r["election_date"] for r in res if r["election_date"].startswith(str(e["year"])))
            if sorted(expected) != have:
                problems.append(f"{e['year']} expected {expected} but results have {have}")
    duplicates = []
    for r in res:
        old = existing.get(key(r))
        if old:
            same = ({c["role"]: (c["name"], c.get("votes")) for c in old.get("candidates", [])}
                    == {c["role"]: (c["name"], c.get("votes")) for c in r.get("candidates", [])})
            duplicates.append({"date": r["election_date"], "old_status": old.get("verification_status"), "same_names_and_votes": same})
    counts = Counter(e.get("status") for e in cal)
    return {"slug": slug, "problems": problems, "notes": notes, "duplicates": duplicates,
            "status_counts": dict(counts), "elections": len(res)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--build", action="store_true", help="also dry-run the coverage build")
    args = parser.parse_args()
    config = json.loads((REPO / "config/mayoral_top_two_build.json").read_text())
    registered = {Path(p).name for p in config["results"] + config["city_year_audits"]}
    existing = {}
    for p in config["results"]:
        for r in json.loads((REPO / p).read_text()):
            existing[key(r)] = r
    slugs = sorted({p.name[: -len("_calendar.json")] for p in (BASE / "staging").glob("*_calendar.json")
                    if (BASE / "staging" / p.name.replace("_calendar", "_results")).exists()})
    pending = [s for s in slugs if f"{s}_calendar.json" not in registered]
    reports = [check_city(s, existing) for s in pending]
    print(json.dumps({"staged_unregistered": len(pending),
                      "with_problems": [r["slug"] for r in reports if r["problems"]],
                      "status_totals": dict(sum((Counter(r.get("status_counts", {})) for r in reports), Counter())),
                      "elections": sum(r.get("elections", 0) for r in reports),
                      "duplicate_disagreements": [(r["slug"], d) for r in reports for d in r["duplicates"]
                                                  if d["old_status"] == "verified_decisive" and not d["same_names_and_votes"]],
                      "reports": reports}, indent=1))
    if args.build:
        with tempfile.TemporaryDirectory() as tmp:
            cfg = json.loads(json.dumps(config))
            for r in reports:
                if r["problems"]:
                    continue
                res = json.loads((BASE / "staging" / f"{r['slug']}_results.json").read_text())
                keep = []
                for rec in res:
                    old = existing.get(key(rec))
                    if old and old.get("verification_status") == "verified_decisive":
                        continue
                    if old:
                        rec["supersedes_verification_status"] = old.get("verification_status")
                    keep.append(rec)
                rp = Path(tmp) / f"{r['slug']}_results.json"
                rp.write_text(json.dumps(keep))
                cfg["results"].append(str(rp))
                cfg["city_year_audits"].append(str(BASE / "staging" / f"{r['slug']}_calendar.json"))
            cp = Path(tmp) / "build.json"
            cp.write_text(json.dumps(cfg))
            out = Path(tmp) / "out"
            run = subprocess.run([sys.executable, str(REPO / "scripts/build_mayoral_top_two_coverage.py"),
                                  "--build-config", str(cp), "--output-dir", str(out)],
                                 cwd=REPO, capture_output=True, text=True)
            if run.returncode:
                print("BUILD FAILED:", run.stderr[-1500:])
            else:
                s = json.loads((out / "summary.json").read_text())
                print("BUILD OK:", json.dumps({k: s[k] for k in ("city_years_audited", "contested_elections")}),
                      json.dumps(s["election_status_counts"]))


if __name__ == "__main__":
    main()
