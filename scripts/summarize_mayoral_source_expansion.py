#!/usr/bin/env python3
"""Combine the frozen mayoral roster with unreviewed source-page URL claims."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from scripts.reconcile_mayoral_discovery import as_json, stable_id, truth, url_identity
from src.mayoral_discovery import canonical_http_url, site_root_url
from src.mayoral_source_expansion import eligible_site_claim

csv.field_size_limit(sys.maxsize)
DEFAULT_RUN = ROOT / "data/municipal_discovery/exhaustive_source_expansion_20260908"
VERSION = "mayoral-source-expansion-summary-v2"
SITE_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
CLAIM_FLAGS = {"human_accepted": False, "sampled_audit_accepted": False, "historical_url_identified": False, "strict_custom_domain": False, "strict_exact_url": False, "strict_coverage_eligible": False, "archive_capture_available": "unknown", "archive_capture_within_headline_window": "unknown", "downloaded_content_available": "unknown"}
EXTRA_FIELDS = ["summary_source_claim_id", "summary_origin", "summary_disposition", "summary_eligible_url_lead", "summary_review_required", "summary_change_type", "summary_candidate_url_id", "summary_version", "input_claim_review_status", "input_claim_evidence_grade", "input_claim_acceptance", "input_exact_normalized_url", *["input_candidate_" + key for key in CLAIM_FLAGS]]
IDENTITY_FIELDS = ["candidate_year_id", "candidate_contest_id", "contest", "contest_id", "full_name", "geo_name", "state_abb", "year", "month"]
EXCLUSION_FIELDS = ["attribution_exclusion_" + field for field in ("version", "reason", "review_file", "reviewer", "model", "capture_sha256", "capture_timestamp", "evidence_path")]


def _accepted(row: dict) -> bool:
    return any(truth(row.get(field)) for field in ("human_accepted", "ai_accepted_website", "sampled_audit_accepted")) or row.get("review_decision") in {"human_accepted", "ai_accepted"}


def _exclusions(path: Path | None, roster: dict) -> dict:
    result = {}
    if path is None:
        return result
    if not path.is_file():
        raise FileNotFoundError(path)
    for row in _rows(path):
        if any(not row.get(field, "").strip() for field in ["candidate_year_id", "candidate_url_id", "exact_normalized_url", *EXCLUSION_FIELDS]):
            raise ValueError("Attribution exclusion lacks evidence or provenance")
        cid, exact = row["candidate_year_id"], row["exact_normalized_url"]
        if cid not in roster:
            raise ValueError("Unknown attribution exclusion candidate")
        if not canonical_http_url(exact) or canonical_http_url(exact) != exact or not re.fullmatch(r"[0-9a-f]{64}", row["attribution_exclusion_capture_sha256"]) or not re.fullmatch(r"\d{14}", row["attribution_exclusion_capture_timestamp"]):
            raise ValueError("Invalid attribution exclusion evidence")
        try:
            datetime.strptime(row["attribution_exclusion_capture_timestamp"], "%Y%m%d%H%M%S")
        except ValueError as exc:
            raise ValueError("Invalid attribution exclusion evidence timestamp") from exc
        if _accepted(row):
            raise ValueError("Attribution exclusion conflicts with prior acceptance")
        key = cid, exact
        if key in result:
            raise ValueError("Duplicate attribution exclusion key")
        result[key] = row
    return result


def _mark_exclusion(row: dict, exclusion: dict) -> None:
    if _accepted(row):
        raise ValueError("Attribution exclusion conflicts with prior acceptance")
    for field in EXCLUSION_FIELDS:
        if row.get(field) not in (None, "", exclusion[field]):
            raise ValueError("Conflicting attribution exclusion provenance")
        row[field] = exclusion[field]
    row.update(summary_disposition="ai_candidate_attribution_rejected", summary_eligible_url_lead=False, summary_review_required=True)


def _rows(path: Path):
    if path.is_file():
        with path.open(newline="", encoding="utf-8") as handle:
            yield from csv.DictReader(handle)


def _header(path: Path) -> list[str]:
    if not path.is_file():
        return []
    with path.open(newline="", encoding="utf-8") as handle:
        return csv.DictReader(handle).fieldnames or []


def _hash(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _json_list(value: str) -> list:
    parsed = json.loads(value or "[]")
    if not isinstance(parsed, list):
        raise ValueError("Expected a JSON list in a frozen URL record")
    return parsed


def _active(row: dict) -> bool:
    return truth(row.get("active_2010_2021")) if "active_2010_2021" in row else 2010 <= int(float(row["year"])) <= 2021


def _site_identity(row: dict) -> str:
    target = row["exact_normalized_url"]
    if row.get("url_type") in {"custom_domain", "candidate_subdomain"}:
        target = site_root_url(target)
    return url_identity(target)


def _write_rows(path: Path, rows: list[dict], fields: list[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def summarize(run: Path = DEFAULT_RUN, attribution_exclusions: Path | None = None, output_dir: Path | None = None) -> dict:
    """Rebuild all combined outputs from explicit frozen and expanded inputs.

    Source claims stream through CSV readers. Memory holds the candidate roster,
    unique candidate URLs, and claim IDs, rather than every source-page context.
    """
    run = Path(run)
    frozen = run / "roster"
    paths = {name: frozen / name for name in ("candidate_roster.csv", "candidate_urls.csv", "combined_claims.csv", "claim_dispositions.csv")}
    for path in paths.values():
        if not path.is_file():
            raise FileNotFoundError(path)
    expanded_path = run / "expanded_url_claims.csv"
    if expanded_path.is_file():
        paths["expanded_url_claims.csv"] = expanded_path
    if attribution_exclusions is not None:
        attribution_exclusions = Path(attribution_exclusions)
        if not attribution_exclusions.is_file():
            raise FileNotFoundError(attribution_exclusions)
        paths["attribution_exclusions.csv"] = attribution_exclusions
    signatures = {name: (p.stat().st_ino, p.stat().st_size, p.stat().st_mtime_ns) for name, p in paths.items()}
    roster_rows = list(_rows(paths["candidate_roster.csv"]))
    roster = {r["candidate_year_id"]: r for r in roster_rows}
    if len(roster) != len(roster_rows) or "" in roster:
        raise ValueError("Frozen roster has duplicate or empty candidate-year keys")
    exclusions = _exclusions(attribution_exclusions, roster)
    matched_exclusions = set()
    excluded_baseline_urls = excluded_baseline_claims = excluded_expanded_claims = 0
    rejected = set()
    for row in _rows(paths["claim_dispositions.csv"]):
        if row.get("review_decision") == "human_rejected" or row.get("disposition") in {"human_rejected_url", "conflicting_human_reviews"}:
            rejected.add((row["candidate_year_id"], url_identity(row["exact_normalized_url"])))
    urls, baseline_url_keys, baseline_sites = {}, set(), set()
    seen_baseline_keys = set()
    for original in _rows(paths["candidate_urls.csv"]):
        row = dict(original)
        candidate, exact = row["candidate_year_id"], row["exact_normalized_url"]
        key = candidate, exact
        if candidate not in roster or key in seen_baseline_keys:
            raise ValueError("Frozen candidate URL keys are duplicate or outside the roster")
        seen_baseline_keys.add(key)
        exclusion_key = candidate, canonical_http_url(exact)
        if exclusion_key in exclusions:
            if row["candidate_url_id"] != exclusions[exclusion_key]["candidate_url_id"]:
                raise ValueError("Attribution exclusion candidate URL key mismatch")
            _mark_exclusion(row, exclusions[exclusion_key])
            matched_exclusions.add(exclusion_key)
            excluded_baseline_urls += 1
            continue
        baseline_url_keys.add(key)
        baseline_sites.add((candidate, _site_identity(row)))
        row["baseline_url"] = True
        row["new_to_baseline_candidate"] = False
        row["source_expansion_claim_count"] = 0
        row["source_expansion_claim_ids_json"] = "[]"
        row["source_expansion_source_ids_json"] = "[]"
        row["source_expansion_source_urls_json"] = "[]"
        row["summary_source_claim_ids_json"] = as_json(["baseline:" + str(cid) for cid in _json_list(row.get("reconciliation_claim_ids_json", "[]"))])
        row["summary_change_type"] = "baseline_url"
        row["source_attributed_candidate_lead"] = False
        row["discovery_only_pending_review"] = False
        urls[key] = row
    baseline_candidates = {candidate for candidate, _ in baseline_url_keys}
    excluded_baseline_candidates = {candidate for candidate, exact in seen_baseline_keys if (candidate, canonical_http_url(exact)) in exclusions}
    source_fields = list(dict.fromkeys([*_header(paths["combined_claims.csv"]), *_header(expanded_path), *EXTRA_FIELDS, *(EXCLUSION_FIELDS if attribution_exclusions is not None else []), *CLAIM_FLAGS, "evidence_grade", "review_status", "acceptance"]))
    output = Path(output_dir) if output_dir is not None else run / "combined"
    if output.resolve() == frozen.resolve() or output.resolve() == run.resolve():
        raise ValueError("Output directory would overwrite source inputs")
    output.mkdir(parents=True, exist_ok=True)
    counts, attribution_counts, change_counts = Counter(), Counter(), Counter()
    old_count = expanded_count = eligible_count = review_count = reset_count = 0
    unknown = set()
    seen_claim_ids = set()
    additions = defaultdict(lambda: {"claim_ids": [], "source_ids": set(), "source_urls": set()})
    temp_sources, temp_reviews = output / "source_claims.csv.tmp", output / "review_claims.csv.tmp"
    with temp_sources.open("w", newline="", encoding="utf-8") as source_handle, temp_reviews.open("w", newline="", encoding="utf-8") as review_handle:
        source_writer = csv.DictWriter(source_handle, fieldnames=source_fields, extrasaction="raise")
        review_writer = csv.DictWriter(review_handle, fieldnames=source_fields, extrasaction="raise")
        source_writer.writeheader()
        review_writer.writeheader()
        for original in _rows(paths["combined_claims.csv"]):
            row = dict(original)
            sid = "baseline:" + row["reconciliation_claim_id"]
            if sid in seen_claim_ids:
                raise ValueError("Duplicate frozen source claim ID")
            seen_claim_ids.add(sid)
            row.update(summary_source_claim_id=sid, summary_origin="frozen_baseline", summary_disposition=row.get("disposition", "baseline"), summary_eligible_url_lead=truth(row.get("eligible_url_lead")), summary_review_required=truth(row.get("review_required")), summary_version=VERSION)
            key = row.get("candidate_year_id"), canonical_http_url(row.get("exact_normalized_url", ""))
            if key in exclusions:
                _mark_exclusion(row, exclusions[key])
                matched_exclusions.add(key)
                excluded_baseline_claims += 1
            source_writer.writerow(row)
            old_count += 1
            if truth(row.get("summary_review_required")):
                review_writer.writerow(row)
                review_count += 1
        for original in _rows(expanded_path):
            expanded_count += 1
            row = dict(original)
            cid, raw_id = row.get("candidate_year_id", ""), row.get("claim_id", "")
            exclusion_key = cid, canonical_http_url(row.get("exact_normalized_url") or row.get("raw_url", ""))
            if exclusion_key in exclusions and _accepted(row):
                raise ValueError("Attribution exclusion conflicts with prior acceptance")
            if not raw_id:
                raise ValueError("Expanded source claim lacks a claim ID")
            sid = "expanded:" + raw_id
            if sid in seen_claim_ids:
                raise ValueError("Duplicate expanded source claim ID")
            seen_claim_ids.add(sid)
            for field in ("review_status", "evidence_grade", "acceptance"):
                row["input_claim_" + field] = row.get(field, "")
            row["input_exact_normalized_url"] = row.get("exact_normalized_url", "")
            for field, default in CLAIM_FLAGS.items():
                row["input_candidate_" + field] = row.get(field, "")
                if truth(row.get(field)):
                    reset_count += 1
                row[field] = default
            row.update(evidence_grade="D", review_status="pending", acceptance="pending")
            exact = canonical_http_url(row.get("exact_normalized_url") or row.get("raw_url", ""))
            row["exact_normalized_url"] = exact
            key = cid, exact
            attribution = row.get("attribution_status", "match_unresolved")
            attribution_counts[attribution] += 1
            date_conflict = row.get("temporal_status") == "date_conflict_review" or truth(row.get("date_conflict"))
            if cid not in roster:
                disposition = "unknown_candidate_key"
                unknown.add(cid)
            elif key in exclusions:
                disposition = "ai_candidate_attribution_rejected"
                matched_exclusions.add(key)
                excluded_expanded_claims += 1
            elif not exact:
                disposition = "invalid_url"
            elif (cid, url_identity(exact)) in rejected or attribution == "prior_rejection_review" or truth(row.get("prior_rejection")):
                disposition = "prior_human_rejection"
            elif truth(row.get("wrong_office_flag")):
                disposition = "wrong_office_review"
            elif date_conflict:
                disposition = "date_conflict_review"
            elif row.get("candidate_site_ownership_reason") in {
                "account_route_requires_site_root_review", "undated_personal_site_requires_campaign_review",
                "target_ownership_unresolved", "identity_requires_review_after_reattribution",
            }:
                disposition = "identity_or_ownership_unresolved"
            elif row.get("url_type") not in SITE_TYPES:
                disposition = "excluded_url_type"
            elif not eligible_site_claim(row):
                disposition = "identity_or_ownership_unresolved"
            else:
                disposition = "source_attributed_candidate_lead"
            eligible = disposition == "source_attributed_candidate_lead"
            review = disposition in {"ai_candidate_attribution_rejected", "prior_human_rejection", "wrong_office_review", "date_conflict_review", "unknown_candidate_key", "invalid_url", "identity_or_ownership_unresolved"}
            change = "not_eligible"
            if eligible:
                eligible_count += 1
                if key in baseline_url_keys:
                    change = "additional_source_for_known_exact_url"
                elif cid not in baseline_candidates:
                    change = "new_candidate_url_lead"
                elif (cid, _site_identity(row)) in baseline_sites:
                    change = "alternative_exact_url_for_known_site"
                else:
                    change = "additional_site_for_known_candidate"
                change_counts[change] += 1
                if key not in urls:
                    context = roster[cid]
                    urls[key] = {
                        **{field: context.get(field, "") for field in IDENTITY_FIELDS},
                        "raw_url": row.get("raw_url", exact), "exact_normalized_url": exact,
                        "site_root_url": site_root_url(exact), "registrable_domain": row.get("registrable_domain", ""), "url_type": row["url_type"],
                        "candidate_url_id": stable_id("candidate_url", cid, exact), "reconciliation_claim_ids_json": "[]", "source_claim_count": 0,
                        "source_ids_json": "[]", "source_urls_json": "[]", "model_cited_source_urls_json": "[]",
                        **{key: value for key, value in CLAIM_FLAGS.items() if key != "strict_coverage_eligible"},
                        "review_required": False, "baseline_url": False, "new_to_baseline_candidate": cid not in baseline_candidates,
                        "summary_change_type": change, "source_attributed_candidate_lead": True, "discovery_only_pending_review": True,
                        "source_expansion_claim_count": 0, "source_expansion_claim_ids_json": "[]", "source_expansion_source_ids_json": "[]", "source_expansion_source_urls_json": "[]", "summary_source_claim_ids_json": "[]",
                    }
                additions[key]["claim_ids"].append(raw_id)
                if row.get("source_id"):
                    additions[key]["source_ids"].add(row["source_id"])
                if row.get("source_url"):
                    additions[key]["source_urls"].add(row["source_url"])
            row.update(summary_source_claim_id=sid, summary_origin="source_expansion", summary_disposition=disposition, summary_eligible_url_lead=eligible, summary_review_required=review, summary_change_type=change, summary_candidate_url_id=stable_id("candidate_url", cid, exact) if exact else "", summary_version=VERSION)
            counts[disposition] += 1
            if key in exclusions:
                _mark_exclusion(row, exclusions[key])
            source_writer.writerow(row)
            if review:
                review_writer.writerow(row)
                review_count += 1
    if set(exclusions) - matched_exclusions:
        raise ValueError("Attribution exclusions do not match any input URL or claim")
    for key, extra in additions.items():
        row = urls[key]
        row["source_expansion_claim_count"] = len(extra["claim_ids"])
        row["source_claim_count"] = int(row.get("source_claim_count") or 0) + len(extra["claim_ids"])
        row["source_expansion_claim_ids_json"] = as_json(sorted(extra["claim_ids"]))
        row["source_expansion_source_ids_json"] = as_json(sorted(extra["source_ids"]))
        row["source_expansion_source_urls_json"] = as_json(sorted(extra["source_urls"]))
        row["summary_source_claim_ids_json"] = as_json([*_json_list(row.get("summary_source_claim_ids_json", "[]")), *["expanded:" + claim_id for claim_id in sorted(extra["claim_ids"])]] )
        row["source_ids_json"] = as_json(sorted(set(_json_list(row.get("source_ids_json", "[]"))) | extra["source_ids"]))
        row["source_urls_json"] = as_json(sorted(set(_json_list(row.get("source_urls_json", "[]"))) | extra["source_urls"]))
        row["source_attributed_candidate_lead"] = True
    combined_urls = [urls[key] for key in sorted(urls)]
    new_urls = [row for row in combined_urls if not truth(row["baseline_url"])]
    url_fields = list(dict.fromkeys([*_header(paths["candidate_urls.csv"]), *[field for row in combined_urls for field in row]]))
    by_candidate = defaultdict(list)
    for row in combined_urls:
        by_candidate[row["candidate_year_id"]].append(row)
    combined_roster = []
    for original in roster_rows:
        row, cid = dict(original), original["candidate_year_id"]
        group = by_candidate[cid]
        row["baseline_has_url_lead"] = cid in baseline_candidates
        row["baseline_candidate_url_count"] = original.get("candidate_url_count", "")
        row["has_url_lead"] = bool(group)
        row["candidate_url_count"] = len(group)
        row["candidate_site_count"] = len({_site_identity(url) for url in group})
        row["registrable_domain_count"] = len({url["registrable_domain"] for url in group})
        row["new_candidate_from_source_expansion"] = bool(group) and cid not in baseline_candidates
        row["source_expansion_eligible_claim_count"] = sum(url["source_expansion_claim_count"] for url in group)
        row["source_expansion_new_exact_url_count"] = sum(not truth(url["baseline_url"]) for url in group)
        row["source_expansion_status"] = "new_candidate_url_leads_pending_review" if row["new_candidate_from_source_expansion"] else "additional_evidence_or_urls" if row["source_expansion_eligible_claim_count"] else "no_eligible_addition"
        if row["new_candidate_from_source_expansion"]:
            row["discovery_status"] = "candidate_url_leads_only"
        if not group and cid in excluded_baseline_candidates:
            row["attribution_exclusion_input_discovery_status"] = original.get("discovery_status", "")
            row["discovery_status"] = "unresolved"
        combined_roster.append(row)
    roster_fields = list(dict.fromkeys([*_header(paths["candidate_roster.csv"]), *[field for row in combined_roster for field in row]]))
    _write_rows(output / "candidate_urls.csv.tmp", combined_urls, url_fields)
    _write_rows(output / "new_candidate_urls.csv.tmp", new_urls, url_fields)
    _write_rows(output / "candidate_roster.csv.tmp", combined_roster, roster_fields)
    active_roster = [row for row in combined_roster if _active(row)]
    active_new = [row for row in new_urls if _active(roster[row["candidate_year_id"]])]
    source_state = json.loads((run / "audit.json").read_text()) if (run / "audit.json").is_file() else {}
    audit = {
        "version": VERSION, "roster_rows": len(roster), "active_candidate_years": len(active_roster),
        "baseline_active_candidate_years_with_url_leads": sum(row["baseline_has_url_lead"] for row in active_roster),
        "active_candidate_years_with_url_leads": sum(row["has_url_lead"] for row in active_roster),
        "active_unresolved_candidate_years": sum(not row["has_url_lead"] for row in active_roster),
        "new_active_candidate_years_with_source_attributed_leads": sum(row["new_candidate_from_source_expansion"] for row in active_roster),
        "new_active_exact_url_rows": len(active_new),
        "new_active_sites_for_known_candidates": len({(row["candidate_year_id"], _site_identity(row)) for row in active_new if row["summary_change_type"] == "additional_site_for_known_candidate"}),
        "new_active_exact_url_change_counts": dict(Counter(row["summary_change_type"] for row in active_new)),
        "new_strict_or_human_approved_claims": 0,
        "baseline_historical_url_candidate_years_active": sum(truth(row.get("historical_url_identified")) for row in active_roster),
        "baseline_source_claim_rows": old_count, "expanded_source_claim_rows": expanded_count,
        "combined_source_claim_rows": old_count + expanded_count, "expanded_eligible_claim_rows": eligible_count,
        "review_claim_rows": review_count, "expanded_claim_dispositions": dict(counts),
        "expanded_attribution_statuses": dict(attribution_counts), "expanded_eligible_change_counts": dict(change_counts),
        "inherited_candidate_claim_flags_reset": reset_count, "unknown_expanded_candidate_keys": sorted(unknown),
        "exact_roster_key_agreement": {row["candidate_year_id"] for row in combined_roster} == set(roster),
        "duplicate_url_keys": len(combined_urls) - len({(row["candidate_year_id"], row["exact_normalized_url"]) for row in combined_urls}),
        "duplicate_source_claim_keys": old_count + expanded_count - len(seen_claim_ids),
        "filter_audit": {"before_expanded_claims": expanded_count, "eligible_after": eligible_count, "excluded_fraction": 1 - eligible_count / expanded_count if expanded_count else 0, "reason": "Only identity-supported claims with explicit target ownership support and no office conflict, date conflict, or prior human rejection enter the URL list. All claims remain in source_claims.csv."},
        "interpretation": "New URLs are source-attributed candidate leads pending review. Historical URL coverage and archive acquisition flags remain those of the frozen baseline.",
        "source_run_state": {key: source_state[key] for key in ("checked_at", "stage", "source_rows", "processed_sources", "never_attempted_sources", "retryable_sources", "source_status_counts", "source_dispositions", "deferred_followup_links", "unprocessed_ocr_pages", "stop_reason") if key in source_state},
        "inputs": {name: {"sha256": _hash(path), "bytes": path.stat().st_size} for name, path in paths.items()},
    }
    for name, path in paths.items():
        if (path.stat().st_ino, path.stat().st_size, path.stat().st_mtime_ns) != signatures[name]:
            raise RuntimeError(f"Summary input changed during export: {name}")
    if attribution_exclusions is not None:
        audit["attribution_exclusions"] = dict(input_rows=len(exclusions), matched_exact_candidate_url_keys=len(matched_exclusions), excluded_baseline_url_rows=excluded_baseline_urls, excluded_baseline_claim_rows=excluded_baseline_claims, excluded_expanded_claim_rows=excluded_expanded_claims, unknown_candidate_keys=[], acceptance_conflicts=0, evidence_validation="Required provenance and hash/timestamp format validated; remote capture bytes are not re-read.")
    output_names = ["source_claims.csv", "review_claims.csv", "candidate_urls.csv", "new_candidate_urls.csv", "candidate_roster.csv"]
    audit["outputs"] = {name: {"sha256": _hash(output / (name + ".tmp")), "bytes": (output / (name + ".tmp")).stat().st_size} for name in output_names}
    (output / "audit.json.tmp").write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n")
    for name in [*output_names, "audit.json"]:
        (output / (name + ".tmp")).replace(output / name)
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN)
    parser.add_argument("--attribution-exclusions", type=Path)
    parser.add_argument("--output-dir", type=Path)
    args = parser.parse_args()
    print(json.dumps(summarize(args.run_dir, args.attribution_exclusions, args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
