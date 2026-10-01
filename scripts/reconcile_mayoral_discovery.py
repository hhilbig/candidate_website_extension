#!/usr/bin/env python3
"""Reconcile explicit mayoral discovery inputs without network requests."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit, urlunsplit

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.build_mayoral_evidence_claims import classify_url, registrable_domain
from src.mayoral_discovery import canonical_http_url, site_root_url

VERSION = "mayoral-discovery-reconciliation-v1"
DEFAULT_MANIFEST = ROOT / "config/mayoral_discovery_reconciliation_20260908.json"
DEFAULT_OUTPUT = ROOT / "data/municipal_discovery/exhaustive_source_expansion_20260908/roster"
SITE_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path", "candidate_subdomain_or_shared_host_path"}
URL_FIELDS = ("exact_normalized_url", "accepted_url", "raw_url", "candidate_url", "proposed_campaign_url", "discovered_campaign_url", "validated_campaign_url", "evidence_url")
SOURCE_FIELDS = ("source_profile_url", "source_page_url", "source_url", "evidence_source_url")
ID_FIELDS = ("claim_id", "url_claim_id", "evidence_id", "discovery_claim_id")


def text(value: object) -> str:
    return "" if value is None or pd.isna(value) else str(value).strip()


def truth(value: object) -> bool:
    return text(value).casefold() in {"true", "yes", "1", "1.0", "accepted", "approved"}


def first(row: dict, fields: tuple[str, ...]) -> str:
    return next((text(row.get(k)) for k in fields if text(row.get(k))), "")


def stable_id(prefix: str, *parts: str) -> str:
    return prefix + "_" + hashlib.sha256("\x1f".join(parts).encode()).hexdigest()[:24]


def as_json(value: object) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def url_identity(url: str) -> str:
    """Match scheme and www aliases without changing saved URL case or paths."""
    parsed = urlsplit(canonical_http_url(url))
    host = (parsed.hostname or "").casefold().removeprefix("www.")
    port = parsed.port
    if port and port not in {80, 443}:
        host += f":{port}"
    return urlunsplit(("", host, parsed.path or "/", parsed.query, ""))


def review_decision(row: dict) -> str:
    status = first(row, ("coded_accept_url", "accept_url", "review_status")).casefold()
    batch = text(row.get("batch_disposition"))
    if batch == "accepted_under_sampled_audit_rule":
        return "accepted_under_sampled_audit_rule"
    if status in {"no", "false", "rejected", "declined", "exclude", "reject"} or batch == "human_rejected":
        return "human_rejected"
    if status in {"yes", "true", "accepted", "approved", "accept"} or batch == "human_accepted":
        return "human_accepted"
    return "unreviewed"


def normalize_claim(row: dict, spec: dict, row_number: int, universe: dict) -> dict:
    candidate = text(row.get("candidate_year_id"))
    raw = first(row, ("raw_url", "candidate_url", "proposed_campaign_url", "discovered_campaign_url", "evidence_url", "exact_normalized_url"))
    exact = canonical_http_url(first(row, URL_FIELDS))
    source_url = first(row, SOURCE_FIELDS)
    origin_id = first(row, ID_FIELDS)
    if not origin_id:
        origin_id = stable_id("legacy_source_claim", candidate, exact, source_url, first(row, ("discovery_method", "extraction_method")), text(row.get("source_snapshot_timestamp")))
    claim_id = stable_id("reconciled_claim", candidate, exact, origin_id)
    url_type = text(row.get("url_type")) or (classify_url(exact) if exact else "invalid_url")
    url_type = {"candidate_owned_custom_domain": "custom_domain"}.get(url_type, url_type)
    # Candidate subdomains remain distinct from shared-host roots.
    if url_type == "shared_host_root" and urlsplit(exact).hostname not in {registrable_domain(exact), "www." + registrable_domain(exact)}:
        url_type = "candidate_subdomain"
    kind = text(row.get("claim_kind"))
    if kind == "evidence_url":
        url_type = "evidence_source_page"
    decision = review_decision(row)
    metadata = universe.get(candidate, {})
    conflicts = {}
    for key in ("year", "month", "candidate_contest_id"):
        supplied, authoritative = text(row.get(key)), text(metadata.get(key))
        if supplied and authoritative:
            left, right = supplied, authoritative
            if key in {"year", "month"}:
                try:
                    left, right = str(int(float(left))), str(int(float(right)))
                except ValueError:
                    pass
            if left != right:
                conflicts[key] = {"source": supplied, "roster": authoritative}
    record = {
        "reconciliation_claim_id": claim_id, "original_claim_id": origin_id,
        "candidate_year_id": candidate, "raw_url": raw, "exact_normalized_url": exact,
        "site_root_url": site_root_url(exact), "registrable_domain": registrable_domain(exact),
        "url_type": url_type, "url_identity": url_identity(exact),
        "source_id": first(row, ("source_id", "source_channel", "url_source")) or spec["source_id"],
        "assistant_output_id": text(row.get("assistant_output_id")),
        "query_attempt_id": first(row, ("query_attempt_id", "attempt_id")),
        "source_profile_url": source_url, "source_date": text(row.get("source_date")),
        "source_snapshot_timestamp": text(row.get("source_snapshot_timestamp")),
        "evidence_text": first(row, ("evidence_text", "result_snippet", "evidence_type")),
        "parser_version": first(row, ("parser_version", "discovery_method")) or spec["source_id"],
        "evidence_grade": text(row.get("coded_evidence_grade")) or text(row.get("evidence_grade")),
        "identity_status": first(row, ("coded_identity_match", "identity_attribution_status", "candidate_attribution_status", "identity_match", "identity_status")),
        "temporal_status": first(row, ("coded_temporal_match", "temporal_match", "temporal_status")),
        "review_decision": decision, "review_status": text(row.get("review_status")),
        "acceptance": text(row.get("acceptance")), "claim_origin": text(row.get("claim_origin")),
        "strict_custom_domain": truth(row.get("strict_custom_domain")),
        "strict_exact_url": truth(row.get("strict_exact_url")),
        "strict_under_batch_audit_rule": truth(row.get("strict_under_batch_audit_rule")),
        "explicit_source_exclusion": text(row.get("candidate_site_exclusion_reason")),
        "identity_unresolved": any(word in first(row, ("identity_attribution_status", "candidate_attribution_status", "identity_match")).casefold() for word in ("unresolved", "mismatch", "conflict")),
        "date_conflict": truth(row.get("temporal_conflict")) or any(word in text(row.get("temporal_match")).casefold() for word in ("conflict", "outside")),
        "wrong_office_flag": truth(row.get("wrong_office_flag")) or truth(row.get("office_mismatch_signal")),
        "input_file": spec["path"], "input_row": row_number,
        "input_priority": int(spec.get("priority", 0)),
        "input_record_json": as_json(row), "metadata_conflicts_json": as_json(conflicts),
    }
    for key in ("candidate_contest_id", "full_name", "geo_name", "state_abb", "year", "month", "contest"):
        record[key] = text(metadata.get(key)) or text(row.get(key))
    record["contest_id"] = record["contest"] or text(row.get("contest_id"))
    return record


def archive_signals(row: dict) -> tuple[bool, bool, bool]:
    count = first(row, ("eligible_capture_count", "archive_capture_count"))
    try:
        has_count = float(count or "0") > 0
    except ValueError:
        has_count = False
    available = truth(row.get("nearest_capture_available")) or has_count
    timestamp = first(row, ("nearest_capture_timestamp", "archive_nearest_capture_timestamp"))
    if timestamp and text(row.get("archive_query_status")).casefold() in {"checked", "complete", "found"}:
        available = True
    within = truth(row.get("within_headline_window")) or truth(row.get("archive_within_headline_window"))
    if text(row.get("temporal_validation_status")) == "archive_capture_within_365_days":
        within = True
    downloaded = bool(text(row.get("capture_content_sha256"))) and text(row.get("capture_fetch_status")).startswith(("fetched", "success"))
    return available, available and within, downloaded


def reconcile(manifest: dict, root: Path) -> dict:
    roster_path = root / manifest["roster"]
    roster = pd.read_csv(roster_path, dtype=str, keep_default_na=False)
    if roster.candidate_year_id.eq("").any() or roster.candidate_year_id.duplicated().any():
        raise ValueError("Roster requires one nonempty row per candidate-year")
    universe = roster.set_index("candidate_year_id").to_dict("index")
    specs = manifest["inputs"]
    if len({s["path"] for s in specs}) != len(specs):
        raise ValueError("Repeated manifest input path")
    snapshots, observations, validation_rows, attempts = [], [], [], []
    ownership_filters, source_contexts = {}, {}
    baseline = set()
    for spec in [{"path": manifest["roster"], "role": "roster"}, *specs]:
        path = root / spec["path"]
        if not path.is_file():
            raise FileNotFoundError(path)
        content = path.read_bytes()
        snap = {**spec, "sha256": hashlib.sha256(content).hexdigest(), "bytes": len(content)}
        if path.suffix == ".csv":
            frame = pd.read_csv(path, dtype=str, keep_default_na=False)
            snap["rows"] = len(frame)
            rows = frame.to_dict("records")
        else:
            rows = []
        snapshots.append(snap)
        role = spec["role"]
        if role == "baseline":
            baseline = {text(row.get("candidate_year_id")) for row in rows}
        if role in {"claims", "review", "dispositions"}:
            for number, row in enumerate(rows, 2):
                observations.append(normalize_claim(row, spec, number, universe))
                validation_rows.append((spec["path"], number, row))
        elif role == "validation":
            validation_rows.extend((spec["path"], i, row) for i, row in enumerate(rows, 2))
        elif role == "attempts":
            attempts.extend((spec["path"], i, row) for i, row in enumerate(rows, 2))
        elif role == "claim_filters":
            for row in rows:
                ownership_filters[text(row.get("url_claim_id"))] = row
        elif role == "claim_contexts":
            for number, row in enumerate(rows, 2):
                source_contexts[text(row.get("assistant_output_id"))] = {**row, "context_file": spec["path"], "context_row": number}
    model_sources = defaultdict(set)
    for row in observations:
        if row["assistant_output_id"] and row["url_type"] == "evidence_source_page":
            model_sources[row["assistant_output_id"]].add(row["exact_normalized_url"])
    for row in observations:
        context = source_contexts.get(row["assistant_output_id"], {})
        row["model_context_ref_json"] = as_json({k: context[k] for k in ("context_file", "context_row") if k in context})
        row["model_cited_source_urls_json"] = as_json(sorted(model_sources[row["assistant_output_id"]] - {""}))
        if context and not row["evidence_text"]:
            row["evidence_text"] = text(context.get("notes"))
        if row["assistant_output_id"] and row["url_type"] in SITE_TYPES:
            row["identity_status"] = row["identity_status"] or "model_attributed_pending_source_verification"
            row["temporal_status"] = row["temporal_status"] or "not_assessed"
    unknown = sorted({r["candidate_year_id"] for r in observations if r["candidate_year_id"] not in universe})
    # Unknown claims remain in dispositions; they never enter coverage.
    by_claim = defaultdict(list)
    for row in observations:
        by_claim[row["reconciliation_claim_id"]].append(row)
    claims = []
    for claim_id, group in sorted(by_claim.items()):
        group = sorted(group, key=lambda r: (r["input_priority"], r["input_file"], r["input_row"]))
        merged = dict(group[-1])
        for key in ("source_profile_url", "source_date", "evidence_text", "source_snapshot_timestamp", "claim_origin", "identity_status", "temporal_status"):
            merged[key] = next((r[key] for r in reversed(group) if r[key]), "")
        decisions = {r["review_decision"] for r in group} - {"unreviewed"}
        merged["review_decision"] = "human_rejected" if "human_rejected" in decisions else "human_accepted" if "human_accepted" in decisions else "accepted_under_sampled_audit_rule" if decisions else "unreviewed"
        merged["review_conflict"] = "human_accepted" in decisions and "human_rejected" in decisions
        merged["input_row_refs_json"] = as_json([{ "file": r["input_file"], "row": r["input_row"]} for r in group])
        merged["input_records_json"] = as_json([json.loads(r["input_record_json"]) for r in group])
        merged["source_urls_json"] = as_json(sorted({r["source_profile_url"] for r in group if r["source_profile_url"]}))
        merged["model_cited_source_urls_json"] = as_json(sorted({u for r in group for u in json.loads(r["model_cited_source_urls_json"])}))
        merged["raw_urls_json"] = as_json(sorted({r["raw_url"] for r in group if r["raw_url"]}))
        merged["strict_under_batch_audit_rule"] = any(r["strict_under_batch_audit_rule"] for r in group)
        claims.append(merged)
    reviewed = defaultdict(set)
    for row in claims:
        if row["review_decision"] != "unreviewed":
            reviewed[(row["candidate_year_id"], row["url_identity"])].add(row["review_decision"])
    validation = defaultdict(list)
    for path, number, row in validation_rows:
        key = (text(row.get("candidate_year_id")), url_identity(first(row, URL_FIELDS)))
        if key[1]:
            validation[key].append((path, number, row))
    for row in claims:
        key = row["candidate_year_id"], row["url_identity"]
        decisions = reviewed[key]
        conflicting = {"human_accepted", "human_rejected"} <= decisions
        row["review_conflict"] = row["review_conflict"] or conflicting
        prior_filter = ownership_filters.get(row["original_claim_id"], {})
        row["prior_ownership_filter_reason"] = text(prior_filter.get("eligibility_reason"))
        if row["candidate_year_id"] not in universe:
            disposition = "unknown_candidate_key"
        elif not row["exact_normalized_url"]:
            disposition = "invalid_or_missing_url"
        elif conflicting:
            disposition = "conflicting_human_reviews"
        elif "human_rejected" in decisions:
            disposition = "human_rejected_url"
        elif row["url_type"] not in SITE_TYPES:
            disposition = "excluded_url_type"
        elif row["explicit_source_exclusion"] or row["wrong_office_flag"]:
            disposition = "source_flagged_review_lead"
        elif row["identity_unresolved"] and "human_accepted" not in decisions:
            disposition = "identity_unresolved"
        elif row["prior_ownership_filter_reason"] == "ownership_signal_too_weak_for_archive_spend" and "human_accepted" not in decisions:
            disposition = "ownership_unresolved"
        else:
            disposition = "candidate_url_lead"
        row["disposition"] = disposition
        row["eligible_url_lead"] = disposition == "candidate_url_lead"
        row["human_accepted"] = row["review_decision"] == "human_accepted"
        row["sampled_audit_accepted"] = row["review_decision"] == "accepted_under_sampled_audit_rule"
        row["review_required"] = row["review_conflict"] or row["date_conflict"] or disposition in {"source_flagged_review_lead", "identity_unresolved", "ownership_unresolved"}
        row["source_file"] = row["input_file"]
        row["source_row"] = row["input_row"]
        signals = [archive_signals(v) for _, _, v in validation[key]]
        row["archive_capture_available"] = "true" if any(s[0] for s in signals) else "unknown"
        row["archive_capture_within_headline_window"] = "true" if any(s[1] for s in signals) else "unknown"
        row["downloaded_content_available"] = "true" if any(s[2] for s in signals) else "unknown"
        row["archive_evidence_refs_json"] = as_json([{ "file": p, "row": n} for p, n, v in validation[key] if any(archive_signals(v))])
        archive_records = [
            {"file": p, "row": n, "status": first(v, ("temporal_validation_status", "archive_query_status", "availability_status")), "error": text(v.get("error"))}
            for p, n, v in validation[key]
            if any(text(v.get(k)) for k in ("archive_query_status", "availability_status", "capture_fetch_status", "temporal_validation_status"))
        ]
        row["archive_observation_refs_json"] = as_json(archive_records)
        row["archive_recorded_error_rows"] = sum(bool(r["error"]) for r in archive_records)
        accepted_historical = (row["human_accepted"] or row["sampled_audit_accepted"]) and row["evidence_grade"] in {"A", "B", "C"} and row["temporal_status"] in {"yes", "true", "year_only"}
        source_historical = row["claim_origin"] == "legacy_baseline" or (row["evidence_grade"] in {"A", "B", "C"} and row["identity_status"] in {"identity_supported", "yes", "1.0", "source_label_middle_name_tolerant"} and row["temporal_status"] in {"yes", "year_only", "source_date_in_election_period_unarchived"} and bool(row["source_profile_url"]))
        row["historical_url_identified"] = row["eligible_url_lead"] and not row["date_conflict"] and (accepted_historical or source_historical)
        if not row["eligible_url_lead"]:
            row["strict_custom_domain"] = row["strict_exact_url"] = False
    urls = []
    url_groups = defaultdict(list)
    for row in claims:
        if row["eligible_url_lead"]:
            url_groups[(row["candidate_year_id"], row["exact_normalized_url"])].append(row)
    for (candidate, exact), group in sorted(url_groups.items()):
        item = {k: group[0][k] for k in ("candidate_year_id", "candidate_contest_id", "full_name", "geo_name", "state_abb", "year", "month", "contest_id", "contest", "raw_url", "exact_normalized_url", "site_root_url", "registrable_domain", "url_type")}
        item["candidate_url_id"] = stable_id("candidate_url", candidate, exact)
        item["reconciliation_claim_ids_json"] = as_json(sorted(r["reconciliation_claim_id"] for r in group))
        item["source_ids_json"] = as_json(sorted({r["source_id"] for r in group}))
        item["source_urls_json"] = as_json(sorted({u for r in group for u in json.loads(r["source_urls_json"])}))
        item["model_cited_source_urls_json"] = as_json(sorted({u for r in group for u in json.loads(r["model_cited_source_urls_json"])}))
        item["source_claim_count"] = len(group)
        for key in ("historical_url_identified", "human_accepted", "sampled_audit_accepted", "review_required", "strict_custom_domain", "strict_exact_url"):
            item[key] = any(r[key] for r in group)
        for key in ("archive_capture_available", "archive_capture_within_headline_window", "downloaded_content_available"):
            item[key] = "true" if any(r[key] == "true" for r in group) else "unknown"
        urls.append(item)
    by_candidate, by_contest = defaultdict(list), defaultdict(list)
    for path, number, row in attempts:
        candidate = first(row, ("candidate_year_id", "matched_candidate_year_id"))
        contest = first(row, ("contest_id", "contest"))
        entry = {"file": path, "row": number, "status": first(row, ("request_status", "source_status", "terminal_status", "fetch_status")), "error": text(row.get("error"))}
        if candidate:
            by_candidate[candidate].append(entry)
        elif contest:
            by_contest[contest].append(entry)
    url_lookup, claim_lookup = defaultdict(list), defaultdict(list)
    for row in urls:
        url_lookup[row["candidate_year_id"]].append(row)
    for row in claims:
        claim_lookup[row["candidate_year_id"]].append(row)
    additions = []
    for original in roster.to_dict("records"):
        candidate = original["candidate_year_id"]
        group = url_lookup[candidate]
        history = by_candidate[candidate] + by_contest[original["contest"]]
        row = dict(original)
        row["active_2010_2021"] = 2010 <= int(float(row["year"])) <= 2021
        row["in_previous_combined_list"] = candidate in baseline
        row["has_url_lead"] = bool(group)
        row["candidate_url_count"] = len(group)
        row["candidate_site_count"] = len({url_identity(r["site_root_url"]) if r["url_type"] == "custom_domain" else url_identity(r["exact_normalized_url"]) for r in group})
        row["registrable_domain_count"] = len({r["registrable_domain"] for r in group})
        for key in ("historical_url_identified", "human_accepted", "sampled_audit_accepted", "review_required"):
            row[key] = any(r[key] for r in group)
        row["pending_review_claim_count"] = sum(r["review_required"] for r in claim_lookup[candidate])
        row["archive_recorded_error_rows"] = sum(r["archive_recorded_error_rows"] for r in claim_lookup[candidate])
        for key in ("archive_capture_available", "archive_capture_within_headline_window", "downloaded_content_available"):
            row[key] = "true" if any(r[key] == "true" for r in group) else "unknown"
        row["prior_attempt_rows"] = len(history)
        row["prior_attempt_error_rows"] = sum(bool(r["error"]) for r in history)
        row["prior_attempt_status_counts_json"] = as_json(dict(Counter(r["status"] for r in history)))
        row["prior_attempt_refs_json"] = as_json(history)
        row["discovery_status"] = "historical_url_identified" if row["historical_url_identified"] else "candidate_url_leads_only" if group else "unresolved"
        additions.append(row)
    result = pd.DataFrame(additions)
    active = result[result.active_2010_2021]
    unresolved = result[result.active_2010_2021 & ~result.has_url_lead]
    combined = pd.DataFrame(claims)
    candidate_urls = pd.DataFrame(urls)
    audit = {
        "version": VERSION, "builder_sha256": hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),
        "roster_rows": len(result), "active_candidate_years": len(active),
        "prior_combined_active_candidate_years": int(active.in_previous_combined_list.sum()),
        "active_candidate_years_with_url_leads": int(active.has_url_lead.sum()),
        "active_candidate_years_with_historical_urls": int(active.historical_url_identified.sum()),
        "active_unresolved_candidate_years": len(unresolved),
        "newly_recovered_prior_lead_candidate_years": int((active.has_url_lead & ~active.in_previous_combined_list).sum()),
        "prior_candidate_years_now_flagged": active.loc[active.in_previous_combined_list & ~active.has_url_lead, "candidate_year_id"].tolist(),
        "input_observation_rows": len(observations), "unique_source_claim_rows": len(combined),
        "repeated_claim_observations_collapsed": len(observations) - len(combined),
        "candidate_exact_url_rows_all_years": len(candidate_urls),
        "candidate_exact_url_rows_active": sum(2010 <= int(float(r["year"])) <= 2021 for r in urls),
        "distinct_exact_urls_active": len({r["exact_normalized_url"] for r in urls if 2010 <= int(float(r["year"])) <= 2021}),
        "distinct_registrable_domains_active": len({r["registrable_domain"] for r in urls if 2010 <= int(float(r["year"])) <= 2021}),
        "new_prior_leads_with_historical_source_support": int((active.has_url_lead & ~active.in_previous_combined_list & active.historical_url_identified).sum()),
        "unknown_claim_candidate_keys": unknown,
        "duplicate_roster_keys": int(result.candidate_year_id.duplicated().sum()),
        "duplicate_claim_keys": int(combined.reconciliation_claim_id.duplicated().sum()),
        "duplicate_candidate_url_keys": int(candidate_urls.duplicated(["candidate_year_id", "exact_normalized_url"]).sum()) if len(candidate_urls) else 0,
        "claim_dispositions": dict(Counter(r["disposition"] for r in claims)),
        "excluded_url_types": dict(Counter(r["url_type"] for r in claims if r["disposition"] == "excluded_url_type")),
        "historical_review_conflicts": int(combined.review_conflict.sum()),
        "metadata_conflict_claims": int(combined.metadata_conflicts_json.ne("{}").sum()),
        "missing_roster_fields": {k: int(result[k].eq("").sum()) for k in ("candidate_year_id", "candidate_contest_id", "contest", "year", "month", "full_name")},
        "missing_eligible_claim_fields": {k: int(combined.loc[combined.eligible_url_lead, k].eq("").sum()) for k in ("source_date", "source_profile_url", "evidence_text", "identity_status", "temporal_status")},
        "candidate_key_agreement": set(result.candidate_year_id) == set(universe),
        "input_observations_retained_in_lineage": sum(len(json.loads(r["input_row_refs_json"])) for r in claims) == len(observations),
        "archive_capture_candidate_years_active": int(active.archive_capture_available.eq("true").sum()),
        "archive_capture_headline_candidate_years_active": int(active.archive_capture_within_headline_window.eq("true").sum()),
        "recorded_download_candidate_years_active": int(active.downloaded_content_available.eq("true").sum()),
        "attempt_input_rows": len(attempts), "attempt_errors_mean_absence": False,
        "archive_observations_scope": "Local recorded validation only; remote production scrape state is unknown until separately imported.",
        "filter_audit": [
            {"filter": "active_2010_2021", "before": len(result), "after": len(active), "drop_fraction": 1-len(active)/len(result), "reason": "Approved active period; all historical roster rows remain in candidate_roster.csv."},
            {"filter": "eligible_candidate_url_lead", "before": len(combined), "after": int(combined.eligible_url_lead.sum()), "drop_fraction": 1-float(combined.eligible_url_lead.mean()), "reason": "Types, human rejections, identity uncertainty and explicit source flags; every source claim remains in combined_claims.csv and claim_dispositions.csv."},
        ],
    }
    return {"combined_claims": combined, "candidate_urls": candidate_urls, "candidate_roster": result, "unresolved_candidates": unresolved, "claim_dispositions": combined[["reconciliation_claim_id", "original_claim_id", "candidate_year_id", "exact_normalized_url", "url_type", "disposition", "review_decision", "review_conflict", "review_required", "input_row_refs_json"]], "audit": audit, "input_manifest": {"version": VERSION, "inputs": snapshots, "configuration": manifest}}


def run(manifest_path: Path = DEFAULT_MANIFEST, output_dir: Path = DEFAULT_OUTPUT, root: Path = ROOT) -> dict:
    manifest = json.loads(manifest_path.read_text())
    results = reconcile(manifest, root)
    output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in results.items():
        path = output_dir / (name + (".csv" if isinstance(value, pd.DataFrame) else ".json"))
        temporary = path.with_suffix(path.suffix + ".tmp")
        if isinstance(value, pd.DataFrame):
            value.to_csv(temporary, index=False)
        else:
            temporary.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n")
        temporary.replace(path)
    results["audit"]["output_files"] = {
        name + (".csv" if isinstance(value, pd.DataFrame) else ".json"): {
            "sha256": hashlib.sha256((output_dir / (name + (".csv" if isinstance(value, pd.DataFrame) else ".json"))).read_bytes()).hexdigest(),
            **({"rows": len(value)} if isinstance(value, pd.DataFrame) else {}),
        }
        for name, value in results.items() if name != "audit"
    }
    temporary = output_dir / "audit.json.tmp"
    temporary.write_text(json.dumps(results["audit"], indent=2, sort_keys=True) + "\n")
    temporary.replace(output_dir / "audit.json")
    return results["audit"]


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    print(json.dumps(run(args.manifest, args.output_dir), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
