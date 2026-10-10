"""Combine the legacy-lead search (October 9, 2026) into checked URL leads for the coverage build.

107 finalists had only an old roster URL without an archived copy in the window. For them:
  legacy_search_20261009/<slug>_url_leads.json   agent search results (workflow wf_44cc61ab-187)
  legacy_search_20261009/checker_verdicts.json   independent checker verdicts ("reject" removes, "downgrade"
                                                 makes an accepted lead outside_window)
  legacy_search_20261009/sweep_check_verdicts.json  optional; checked domain-sweep hits (wf_c56a8985-1e3), applied
                                                 where the search result is not accepted
Output: linkage/legacy_search_leads.csv, in the columns of discovered_url_leads.csv plus replaces_legacy=True.
"incomplete" entries are not written; their finalists keep the old roster URL.
"""
from __future__ import annotations

import csv
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from scripts.build_mayoral_url_leads import COLUMNS, capture_timestamp, repair_verdicts  # noqa: E402

BASE = Path("data/municipal_discovery/top100_coverage")
SEARCH = BASE / "url_discovery" / "legacy_search_20261009"
OUTPUT = BASE / "linkage" / "legacy_search_leads.csv"
SEARCH_BASIS = "legacy_lead_search_wayback_capture_checked"
SWEEP_BASIS = "legacy_lead_sweep_wayback_capture_checked"


def main() -> int:
    args = json.loads((SEARCH / "search_args.json").read_text())
    slug_of = {f["candidate_election_id"]: c["slug"] for c in args for f in c["finalists"]}
    finalist = {f["candidate_election_id"]: f for c in args for f in c["finalists"]}
    leads = {c["slug"]: json.loads((SEARCH / f"{c['slug']}_url_leads.json").read_text()) for c in args}
    items, incomplete = repair_verdicts(leads, json.loads((SEARCH / "checker_verdicts.json").read_text()))
    final = {i["candidate_election_id"]: dict(i, link_basis=SEARCH_BASIS) for i in items}
    sweep_path = SEARCH / "sweep_check_verdicts.json"
    sweep_used = 0
    if sweep_path.exists():
        for v in json.loads(sweep_path.read_text()):
            key = v["candidate_election_id"]
            if key not in slug_of:
                raise ValueError(f"sweep verdict for a finalist outside the search: {key}")
            if v["status"] in {"accepted", "outside_window"} and v["campaign_url"] and \
                    final.get(key, {}).get("status") != "accepted":
                final[key] = dict(v, link_basis=SWEEP_BASIS, lead_sources=["domain sweep"])
                sweep_used += 1
    rows = []
    for key, lead in sorted(final.items()):
        status = lead["status"]
        keep = status in {"accepted", "outside_window"} and lead.get("campaign_url")
        rows.append({
            "candidate_election_id": key, "city_slug": slug_of[key], "candidate_name": finalist[key]["candidate_name"],
            "election_date": finalist[key]["election_date"], "discovery_status": status, "checker_severity": "",
            "final_status": status if keep or status == "not_found" else "not_found",
            "campaign_url": lead["campaign_url"] if keep else "",
            "registrable_domain": (lead.get("registrable_domain") or "").split()[0] if keep and lead.get("registrable_domain") else "",
            "first_capture_in_window": lead.get("first_capture_in_window", "") if status == "accepted" else "",
            "checked_capture_timestamp": capture_timestamp(lead.get("checked_capture_url", "")) if status == "accepted" else "",
            "checked_capture_url": lead.get("checked_capture_url", "") if status == "accepted" else "",
            "wayback_window_captures": lead.get("wayback_window_captures", ""),
            "page_evidence": lead.get("page_evidence", "") if status == "accepted" else "",
            "lead_sources": " | ".join(lead.get("lead_sources", [])), "checker_issue": "",
            "discovery_note": lead.get("note", ""), "link_basis": lead["link_basis"] if keep else "",
            "replaces_legacy": "True"})
    with OUTPUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS + ["replaces_legacy"])
        writer.writeheader()
        writer.writerows(rows)
    counts = {}
    for r in rows:
        counts[r["final_status"]] = counts.get(r["final_status"], 0) + 1
    print(json.dumps({"finalists": len(rows), "final_status": counts, "incomplete_not_written": incomplete,
                      "sweep_verdicts_applied": sweep_used}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
