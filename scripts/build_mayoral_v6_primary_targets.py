"""Merge the v6 primary URL search results and write the download targets of the primary candidates.

Inputs: url_discovery/v6_primaries_20261017/pass1/<slug>_url_leads.json (search of October 17-18, 2026)
        url_discovery/v6_primaries_20261017/rerun/<slug>_url_leads.json (rerun of the incomplete entries; a rerun
        result replaces the pass 1 entry of the same candidate)
        outputs/primary_candidates.csv (scripts/build_mayoral_v6_primaries.py)
Outputs: url_discovery/v6_primaries_20261017/<slug>_url_leads.json (the merged leads, one file per city)
         linkage/v6_primary_url_leads.csv (one row per searched candidate)
         data/municipal_archive_acquisition/<batch>/candidate_site_targets.csv (accepted and outside_window leads)
The targets use the stage 3 format and builder (scripts/build_mayoral_stage3_targets.build_targets), with the
primary date as the election date, so the scrape window is 365 days around the primary.

Usage:
  python scripts/build_mayoral_v6_primary_targets.py --batch mayoral_stage3p_20261018
"""
from __future__ import annotations

import argparse
import json
import sys
from collections import Counter
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_mayoral_stage3_targets import build_targets  # noqa: E402

BASE = REPO / "data/municipal_discovery/top100_coverage"
SEARCH = BASE / "url_discovery" / "v6_primaries_20261017"
CANDIDATES = BASE / "outputs" / "primary_candidates.csv"
LEADS_CSV = BASE / "linkage" / "v6_primary_url_leads.csv"
ACQUISITION = REPO / "data/municipal_archive_acquisition"


def merge(pass1: dict[str, list[dict]], rerun: dict[str, list[dict]]) -> dict[str, list[dict]]:
    out = {}
    for slug, leads in pass1.items():
        newer = {lead["candidate_election_id"]: lead for lead in rerun.get(slug, [])}
        out[slug] = [{**newer[lead["candidate_election_id"]], "search_run": "rerun"} if lead["candidate_election_id"] in newer
                     else {**lead, "search_run": "pass1"} for lead in leads]
        unknown = set(newer) - {lead["candidate_election_id"] for lead in leads}
        if unknown:
            raise ValueError(f"rerun result without a pass 1 entry: {sorted(unknown)[:3]}")
    return out


def read_folder(folder: Path) -> dict[str, list[dict]]:
    return {p.name.removesuffix("_url_leads.json"): json.loads(p.read_text()) for p in sorted(folder.glob("*_url_leads.json"))}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--batch", required=True)
    args = parser.parse_args()
    merged = merge(read_folder(SEARCH / "pass1"), read_folder(SEARCH / "rerun"))
    candidates = pd.read_csv(CANDIDATES, dtype=str, keep_default_na=False)
    searched = set(candidates.loc[candidates["searched"] == "True", "candidate_election_id"])
    leads = [lead for city in merged.values() for lead in city]
    ids = [lead["candidate_election_id"] for lead in leads]
    if len(ids) != len(set(ids)) or set(ids) != searched:
        raise ValueError("the search results do not match the searched primary candidates one to one")
    for slug, city in merged.items():
        (SEARCH / f"{slug}_url_leads.json").write_text(json.dumps(city, indent=1) + "\n")
    table = pd.DataFrame([{
        "candidate_election_id": lead["candidate_election_id"], "candidate_name": lead["candidate_name"],
        "election_date": lead["election_date"], "final_status": lead["status"], "campaign_url": lead.get("campaign_url", ""),
        "registrable_domain": lead.get("registrable_domain", ""), "checked_capture_url": lead.get("checked_capture_url", ""),
        "lead_sources": " | ".join(lead.get("lead_sources", [])), "link_basis": "v6_primary_search",
        "search_run": lead["search_run"], "note": lead.get("note", "")} for lead in leads])
    table = table.sort_values("candidate_election_id")
    table.to_csv(LEADS_CSV, index=False)
    coverage = candidates.rename(columns={"primary_election_id": "election_id"}).assign(candidate_year_id="")
    targets = build_targets(table, coverage)
    out = ACQUISITION / args.batch / "candidate_site_targets.csv"
    out.parent.mkdir(parents=True, exist_ok=True)
    targets.to_csv(out, index=False)
    print(json.dumps({"candidates": len(table), "status": dict(Counter(table["final_status"])),
                      "targets": len(targets), "domains": int(targets["registrable_domain"].nunique()), "output": str(out)}))


if __name__ == "__main__":
    main()
