"""Combine the stage 2 URL discovery outputs with the independent checker verdicts.

Inputs (data/municipal_discovery/top100_coverage/url_discovery/):
  stage2_args.json          the finalists sent to discovery (one list per city)
  <slug>_url_leads.json     one object per finalist, written by a discover agent
  checker_verdicts.json     {slug: {"checked": int, "problems": [...]}} from the check agents
  checker_proposal_verdicts.json  optional; a third agent's verdicts on sites that a checker
                            proposed for finalists the search marked not_found
  sweep_20261002/sweep_check_verdicts.json  optional; agent verdicts on domain-pattern sweep hits
  sweep_20261002/commoncrawl_verdicts.json  optional; checked Common Crawl hits (capture_timestamp given directly)
  repair_20261004/          optional; a repeated search for stage 2 not_found entries whose search never ran or
                            whose archive check failed: <slug>_url_leads.json plus repair_checker_verdicts.json.
                            "incomplete" entries are not applied and keep their earlier status.
  sweep_after_20261007/sweep_check_verdicts.json  optional; checked hits from the 120 days after the election
                            (option 3); applied last.
  repair_20261004/followup/ and followup2/  optional; repair attempts 2 and 3 for unfinished or checker-flagged
                            entries. Each later pass replaces the earlier result for every finalist it redid.
  pass4_20261010/           optional; the fourth search pass (October 10, 2026), which includes finalists added by
                            the population repair. Written to a separate file, linkage/pass4_url_leads.csv; the
                            coverage build lets an accepted or outside-window result there replace an earlier lead.

A "reject" verdict removes the URL (final status "rejected"). A "downgrade" verdict
sets an accepted lead to "outside_window". "minor" verdicts are kept as notes.
A verified proposal or sweep hit replaces a not_found or rejected entry and gets its own basis
label; the checker proposals are applied first.
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
SWEEP_BASIS = "domain_sweep_wayback_capture_checked"
COMMONCRAWL_BASIS = "domain_sweep_commoncrawl_capture_checked"
REPAIR_BASIS = "agent_search_repair_wayback_capture_checked"
SWEEP_AFTER_BASIS = "domain_sweep_after_election_capture_checked"
PASS4_BASIS = "agent_search_pass4_wayback_capture_checked"
PASS4_OUTPUT = BASE / "linkage" / "pass4_url_leads.csv"


def capture_timestamp(url: str) -> str:
    match = re.search(r"/web/(\d{14})", url or "")
    return match.group(1) if match else ""


def combine(cities: list[dict], leads_by_slug: dict[str, list[dict]], verdicts: dict[str, dict],
            proposals: list[dict] = (), sweep: list[dict] = (), commoncrawl: list[dict] = (),
            repair: list[dict] = (), sweep_after: list[dict] = ()) -> list[dict]:
    sources = []
    for basis, label, items in ((PROPOSAL_BASIS, "checker proposal", proposals), (SWEEP_BASIS, "domain sweep", sweep),
                                 (COMMONCRAWL_BASIS, "common crawl", commoncrawl), (REPAIR_BASIS, "repair search", repair),
                                 (SWEEP_AFTER_BASIS, "after-election sweep", sweep_after)):
        source_items = {}
        for item in items:
            if item.get("status") not in STATUSES or item["candidate_election_id"] in source_items:
                raise ValueError(f"Bad or duplicate {label} verdict for {item.get('candidate_election_id')}")
            source_items[item["candidate_election_id"]] = item
        sources.append((basis, label, source_items))
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
            for source_basis, label, source_items in sources:
                proposal = source_items.get(key)
                if not proposal:
                    continue
                # An outside_window entry without a URL is an unconfirmed domain, not a lead.
                if final not in {"not_found", "rejected"} and not (final == "outside_window" and not lead.get("campaign_url")):
                    raise ValueError(f"{slug}: {label} verdict for {key}, which is already {final}")
                used.add((label, key))
                if proposal["status"] != "not_found":
                    lead = dict(proposal, lead_sources=[label],
                                note=proposal["note"] + " | search note: " + lead.get("note", ""))
                    final, basis = proposal["status"], source_basis
            keep_url = final in {"accepted", "outside_window", "accepted_unchecked"}
            rows.append({
                "candidate_election_id": key, "city_slug": slug, "candidate_name": finalist["candidate_name"],
                "election_date": finalist["election_date"], "discovery_status": lead.get("status", ""),
                "checker_severity": severity, "final_status": final,
                "campaign_url": lead.get("campaign_url", "") if keep_url else "",
                "registrable_domain": lead.get("registrable_domain", "") if keep_url else "",
                "first_capture_in_window": lead.get("first_capture_in_window", "") if final == "accepted" else "",
                "checked_capture_timestamp": (lead.get("capture_timestamp") or capture_timestamp(lead.get("checked_capture_url", "")))
                                             if final == "accepted" else "",
                "checked_capture_url": lead.get("checked_capture_url", "") if final == "accepted" else "",
                "wayback_window_captures": lead.get("wayback_window_captures", ""),
                "page_evidence": lead.get("page_evidence", "") if final == "accepted" else "",
                "lead_sources": " | ".join(lead.get("lead_sources", [])),
                "checker_issue": " | ".join(f"{p['severity']}: {p['issue']}" for p in found),
                "discovery_note": lead.get("note", ""), "link_basis": basis if keep_url and lead.get("campaign_url") else ""})
    unused = {(label, key) for _, label, source_items in sources for key in source_items} - used
    if unused:
        raise ValueError(f"Verdicts for finalists not in discovery: {sorted(unused)[:5]}")
    return rows


def repair_verdicts(leads_by_slug: dict[str, list[dict]], verdicts: dict[str, dict]) -> tuple[list[dict], int]:
    """Apply the repair checker's verdicts; return the applicable items and the count left incomplete."""
    items, incomplete = [], 0
    for slug, leads in leads_by_slug.items():
        if slug not in verdicts:
            raise ValueError(f"repair {slug}: no checker verdict")
        severity = {}
        for problem in verdicts[slug].get("problems", []):
            if problem["severity"] in {"reject", "downgrade"}:
                if severity.get(problem["candidate_election_id"]) != "reject":
                    severity[problem["candidate_election_id"]] = problem["severity"]
        for lead in leads:
            if lead.get("status") == "incomplete":
                incomplete += 1
                continue
            lead = dict(lead)
            found = severity.get(lead["candidate_election_id"])
            if lead.get("status") == "accepted" and found == "reject":
                lead.update(status="not_found", campaign_url="", note=lead.get("note", "") + " | repair checker rejected")
            elif lead.get("status") == "accepted" and found == "downgrade":
                lead.update(status="outside_window", note=lead.get("note", "") + " | repair checker downgraded")
            items.append(lead)
    return items, incomplete


def f_list(city: dict) -> list[dict]:
    return city["finalists"]


def merge_followup(first_leads: dict[str, list[dict]], first: list[dict], redone: set[str],
                   followup: list[dict], followup_incomplete: int) -> tuple[list[dict], int]:
    """The follow-up pass replaces the first repair result for every finalist it redid."""
    if {i["candidate_election_id"] for i in followup} - redone:
        raise ValueError("follow-up results for finalists that were not sent to the follow-up")
    kept = [i for i in first if i["candidate_election_id"] not in redone]
    first_incomplete = sum(1 for leads in first_leads.values() for lead in leads
                           if lead.get("status") == "incomplete" and lead["candidate_election_id"] not in redone)
    return kept + followup, first_incomplete + followup_incomplete


def pass4_rows(pass_dir: Path) -> tuple[list[dict], int]:
    """Rows for the fourth search pass after its checker verdicts; incomplete entries are not applied."""
    pass_args = json.loads((pass_dir / "pass4_args.json").read_text())
    leads = {c["slug"]: json.loads((pass_dir / f"{c['slug']}_url_leads.json").read_text()) for c in pass_args}
    items, incomplete = repair_verdicts(leads, json.loads((pass_dir / "pass4_checker_verdicts.json").read_text()))
    sent = {f["candidate_election_id"]: (c["slug"], f) for c in pass_args for f in f_list(c)}
    rows, seen = [], set()
    for item in items:
        key = item["candidate_election_id"]
        if key not in sent or key in seen or item.get("status") not in STATUSES:
            raise ValueError(f"pass4: bad, unknown or duplicate entry for {key}")
        seen.add(key)
        slug, finalist = sent[key]
        final = item["status"]
        keep_url = final in {"accepted", "outside_window"} and bool(item.get("campaign_url"))
        rows.append({
            "candidate_election_id": key, "city_slug": slug, "candidate_name": finalist["candidate_name"],
            "election_date": finalist["election_date"], "discovery_status": final, "checker_severity": "",
            "final_status": final, "campaign_url": item.get("campaign_url", "") if keep_url else "",
            "registrable_domain": item.get("registrable_domain", "") if keep_url else "",
            "first_capture_in_window": item.get("first_capture_in_window", "") if final == "accepted" else "",
            "checked_capture_timestamp": capture_timestamp(item.get("checked_capture_url", "")) if final == "accepted" else "",
            "checked_capture_url": item.get("checked_capture_url", "") if final == "accepted" else "",
            "wayback_window_captures": item.get("wayback_window_captures", ""),
            "page_evidence": item.get("page_evidence", "") if final == "accepted" else "",
            "lead_sources": " | ".join(item.get("lead_sources", [])), "checker_issue": "",
            "discovery_note": item.get("note", ""), "link_basis": PASS4_BASIS if keep_url else "",
            "replaces_legacy": "True", "search_group": finalist["group"]})
    return rows, incomplete


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
    sweep_path = DISCOVERY / "sweep_20261002" / "sweep_check_verdicts.json"
    sweep = json.loads(sweep_path.read_text()) if sweep_path.exists() else []
    cc_path = DISCOVERY / "sweep_20261002" / "commoncrawl_verdicts.json"
    commoncrawl = json.loads(cc_path.read_text()) if cc_path.exists() else []
    repair_dir = DISCOVERY / "repair_20261004"
    repair, repair_incomplete = [], 0
    if (repair_dir / "repair_checker_verdicts.json").exists():
        repair_args = json.loads((repair_dir / "repair_args.json").read_text())
        repair_leads = {c["slug"]: json.loads((repair_dir / f"{c['slug']}_url_leads.json").read_text()) for c in repair_args}
        repair, repair_incomplete = repair_verdicts(repair_leads, json.loads((repair_dir / "repair_checker_verdicts.json").read_text()))
        # Later passes (attempt 2 in followup/, attempt 3 in followup2/) replace earlier results for the finalists they redid.
        current = {lead["candidate_election_id"]: lead for leads in repair_leads.values() for lead in leads}
        for name in ("followup", "followup2"):
            followup_dir = repair_dir / name
            if not (followup_dir / "followup_checker_verdicts.json").exists():
                break
            followup_args = json.loads((followup_dir / "followup_args.json").read_text())
            followup_leads = {c["slug"]: json.loads((followup_dir / f"{c['slug']}_url_leads.json").read_text()) for c in followup_args}
            redone = {f["candidate_election_id"] for c in followup_args for f in f_list(c)}
            followup, followup_incomplete = repair_verdicts(followup_leads, json.loads((followup_dir / "followup_checker_verdicts.json").read_text()))
            repair, repair_incomplete = merge_followup({"all": list(current.values())}, repair, redone, followup, followup_incomplete)
            current.update({lead["candidate_election_id"]: lead for leads in followup_leads.values() for lead in leads})
    after_path = DISCOVERY / "sweep_after_20261007" / "sweep_check_verdicts.json"
    sweep_after = json.loads(after_path.read_text()) if after_path.exists() else []
    after_incomplete = sum(1 for item in sweep_after if item["status"] == "incomplete")
    sweep_after = [item for item in sweep_after if item["status"] != "incomplete"]
    rows = combine(cities, leads, verdicts, proposals, sweep, commoncrawl, repair, sweep_after)
    OUTPUT.parent.mkdir(parents=True, exist_ok=True)
    with OUTPUT.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)
    counts = {}
    for row in rows:
        counts[row["final_status"]] = counts.get(row["final_status"], 0) + 1
    print(json.dumps({"finalists": len(rows), "final_status": counts, "repair_incomplete_not_applied": repair_incomplete,
                      "after_sweep_incomplete_not_applied": after_incomplete}, indent=1))
    pass_dir = DISCOVERY / "pass4_20261010"
    if (pass_dir / "pass4_checker_verdicts.json").exists():
        rows4, incomplete4 = pass4_rows(pass_dir)
        with PASS4_OUTPUT.open("w", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=COLUMNS + ["replaces_legacy", "search_group"])
            writer.writeheader()
            writer.writerows(rows4)
        counts4 = {}
        for row in rows4:
            counts4[row["final_status"]] = counts4.get(row["final_status"], 0) + 1
        print(json.dumps({"pass4_finalists": len(rows4), "final_status": counts4, "incomplete_not_applied": incomplete4}, indent=1))
    return 0


if __name__ == "__main__":
    sys.exit(main())
