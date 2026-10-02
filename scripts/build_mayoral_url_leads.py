"""Combine the stage 2 URL discovery outputs with the independent checker verdicts.

Inputs (data/municipal_discovery/top100_coverage/url_discovery/):
  stage2_args.json          the finalists sent to discovery (one list per city)
  <slug>_url_leads.json     one object per finalist, written by a discover agent
  checker_verdicts.json     {slug: {"checked": int, "problems": [...]}} from the check agents
  checker_proposal_verdicts.json  optional; a third agent's verdicts on sites that a checker
                            proposed for finalists the search marked not_found

A "reject" verdict removes the URL (final status "rejected"). A "downgrade" verdict
sets an accepted lead to "outside_window". "minor" verdicts are kept as notes.
A verified proposal replaces a not_found entry and gets its own basis label.
Every finalist sent to discovery gets one row; a city without a leads file or
without a checker verdict is marked, never counted as accepted.

The build reads data/municipal_discovery/top100_coverage/linkage/discovered_url_leads.csv.
"""
from __future__ import annotations

import csv
import json
import re
import sys
from pathlib import Path

BASE = Path("data/municipal_discovery/top100_coverage")
DISCOVERY = BASE / "url_discovery"
OUTPUT = BASE / "linkage" / "discovered_url_leads.csv"
STATUSES = {"accepted", "outside_window", "not_found"}
COLUMNS = ["candidate_election_id", "city_slug", "candidate_name", "election_date", "discovery_status",
           "checker_severity", "final_status", "campaign_url", "registrable_domain", "first_capture_in_window",
           "checked_capture_timestamp", "checked_capture_url", "wayback_window_captures", "page_evidence",
           "lead_sources", "checker_issue", "discovery_note", "link_basis"]
BASIS = "agent_search_wayback_capture_checked"
PROPOSAL_BASIS = "checker_proposal_wayback_capture_verified"


def capture_timestamp(url: str) -> str:
    match = re.search(r"/web/(\d{14})", url or "")
    return match.group(1) if match else ""


def combine(cities: list[dict], leads_by_slug: dict[str, list[dict]], verdicts: dict[str, dict],
            proposals: list[dict] = ()) -> list[dict]:
    proposal_by_id = {}
    for proposal in proposals:
        if proposal.get("status") not in STATUSES or proposal["candidate_election_id"] in proposal_by_id:
            raise ValueError(f"Bad or duplicate proposal verdict for {proposal.get('candidate_election_id')}")
        proposal_by_id[proposal["candidate_election_id"]] = proposal
    used = set()
    rows = []
    for city in cities:
        slug = city["slug"]
        leads = leads_by_slug.get(slug)
        verdict = verdicts.get(slug)
        by_id = {}
        for lead in leads or []:
            if lead.get("status") not in STATUSES:
                raise ValueError(f"{slug}: unknown status {lead.get('status')!r}")
            if lead["candidate_election_id"] in by_id:
                raise ValueError(f"{slug}: duplicate lead for {lead['candidate_election_id']}")
            by_id[lead["candidate_election_id"]] = lead
        expected = {f["candidate_election_id"] for f in city["finalists"]}
        if leads is not None and set(by_id) - expected:
            raise ValueError(f"{slug}: leads for finalists not sent to discovery: {sorted(set(by_id) - expected)}")
        problems = {}
        for problem in (verdict or {}).get("problems", []):
            if problem["candidate_election_id"] not in expected:
                # Checkers file city-wide notes under ids such as "all"; only minor ones are allowed.
                if problem["severity"] == "minor":
                    continue
                raise ValueError(f"{slug}: checker problem for unknown finalist {problem['candidate_election_id']}")
            problems.setdefault(problem["candidate_election_id"], []).append(problem)
        for finalist in city["finalists"]:
            key = finalist["candidate_election_id"]
            lead = by_id.get(key)
            found = problems.get(key, [])
            severity = ("reject" if any(p["severity"] == "reject" for p in found)
                        else "downgrade" if any(p["severity"] == "downgrade" for p in found)
                        else "minor" if found else "")
            if lead is None:
                final = "not_run" if leads is None else "missing_from_output"
            elif lead["status"] == "accepted" and verdict is None:
                final = "accepted_unchecked"
            elif lead["status"] == "accepted" and severity == "reject":
                final = "rejected"
            elif lead["status"] == "accepted" and severity == "downgrade":
                final = "outside_window"
            else:
                final = lead["status"]
            lead = lead or {}
            basis = BASIS
            proposal = proposal_by_id.get(key)
            if proposal:
                if final != "not_found":
                    raise ValueError(f"{slug}: proposal verdict for {key}, which is {final}, not not_found")
                used.add(key)
                if proposal["status"] != "not_found":
                    lead = dict(proposal, lead_sources=["checker proposal"],
                                note=proposal["note"] + " | search note: " + lead.get("note", ""))
                    final, basis = proposal["status"], PROPOSAL_BASIS
            keep_url = final in {"accepted", "outside_window", "accepted_unchecked"}
            rows.append({
                "candidate_election_id": key, "city_slug": slug, "candidate_name": finalist["candidate_name"],
                "election_date": finalist["election_date"], "discovery_status": lead.get("status", ""),
                "checker_severity": severity, "final_status": final,
                "campaign_url": lead.get("campaign_url", "") if keep_url else "",
                "registrable_domain": lead.get("registrable_domain", "") if keep_url else "",
                "first_capture_in_window": lead.get("first_capture_in_window", "") if final == "accepted" else "",
                "checked_capture_timestamp": capture_timestamp(lead.get("checked_capture_url", "")) if final == "accepted" else "",
                "checked_capture_url": lead.get("checked_capture_url", "") if final == "accepted" else "",
                "wayback_window_captures": lead.get("wayback_window_captures", ""),
                "page_evidence": lead.get("page_evidence", "") if final == "accepted" else "",
                "lead_sources": " | ".join(lead.get("lead_sources", [])),
                "checker_issue": " | ".join(f"{p['severity']}: {p['issue']}" for p in found),
                "discovery_note": lead.get("note", ""), "link_basis": basis if keep_url and lead.get("campaign_url") else ""})
    if set(proposal_by_id) - used:
        raise ValueError(f"Proposal verdicts for finalists not in discovery: {sorted(set(proposal_by_id) - used)}")
    return rows


def main() -> int:
    cities = json.loads((DISCOVERY / "stage2_args.json").read_text())
    leads = {}
    for city in cities:
        path = DISCOVERY / f"{city['slug']}_url_leads.json"
        if path.exists():
            leads[city["slug"]] = json.loads(path.read_text())
    verdicts_path = DISCOVERY / "checker_verdicts.json"
    verdicts = json.loads(verdicts_path.read_text()) if verdicts_path.exists() else {}
    proposals_path = DISCOVERY / "checker_proposal_verdicts.json"
    proposals = json.loads(proposals_path.read_text()) if proposals_path.exists() else []
    rows = combine(cities, leads, verdicts, proposals)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    counts = {}
    for row in rows:
        counts[row["final_status"]] = counts.get(row["final_status"], 0) + 1
    print(json.dumps({"finalists": len(rows), "final_status": counts}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
