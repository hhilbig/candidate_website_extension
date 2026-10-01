#!/usr/bin/env python3
"""Recheck the 36 flagged proposals from saved evidence. No network requests."""
import argparse
import csv
import gzip
import hashlib
import io
import json
from collections import Counter
from pathlib import Path

from bs4 import BeautifulSoup
import validate_search_lead_wayback_available_content as content_validator
from finalize_repaired_mayoral_validation import atomic_write
from validate_search_lead_wayback_available_content import (
    CONTENT_SIGNAL_VERSION, archive_response_error, content_signals,
)

VERSION = "mayoral_flag_resolution_v1"


def sha(data):
    return hashlib.sha256(data).hexdigest()


def yes(value):
    return str(value).lower() == "true"


def read_html(path, expected):
    with gzip.open(path, "rb") as handle:
        raw = handle.read(8 * 1024 * 1024 + 1)
    if len(raw) > 8 * 1024 * 1024 or sha(raw) != expected:
        raise ValueError("Saved content hash or size mismatch")
    return raw.decode("utf-8", errors="replace")


def side_assessment(html, name, city, digest, reviews):
    if archive_response_error(html):
        return "archive_service_error"
    if digest in reviews:
        return "unrelated_content_confirmed"
    if content_signals(html, name, city)["candidate_identity_match"]:
        return "same_candidate"
    return "insufficient_surrounding_content"


def resolution(main_ok, side_states, saved_only=False):
    if saved_only:
        return "saved_sides_consistent_inventory_incomplete" if side_states and all(
            state == "same_candidate" for state in side_states
        ) else "saved_side_evidence_missing_or_insufficient"
    if "archive_service_error" in side_states:
        return "archive_service_error_requires_replay"
    if "unrelated_content_confirmed" in side_states:
        return "later_domain_reuse_confirmed"
    if main_ok and side_states and all(s == "same_candidate" for s in side_states):
        return "text_rule_flags_cleared"
    return "surrounding_content_insufficient"


def run(base, corpus, reviews_path, saved_review_path, output):
    input_path = base / "final/validation.csv"
    if output.resolve() == input_path.parent.resolve():
        raise ValueError("Preserve the prior final directory")
    rows = list(csv.DictReader(input_path.open()))
    if len(rows) != 120 or len({r["candidate_url_id"] for r in rows}) != 120:
        raise ValueError("Expected 120 unique keys")
    targets = [r for r in rows if yes(r["historical_campaign_content_supported"])
               and not yes(r["strict_coverage_eligible_result"])]
    if len(targets) != 36:
        raise ValueError("Expected the 36 approved targets")
    reviews = json.loads(reviews_path.read_text())
    saved_review = json.loads(saved_review_path.read_text())
    if saved_review["validation_sha256"] != sha(input_path.read_bytes()):
        raise ValueError("Saved review input differs")
    saved = {r["candidate_url_id"]: r for r in saved_review["records"]}
    requests = [json.loads(line) for line in (base / "results/evidence/requests.jsonl").read_text().splitlines()]
    request_index = {}
    for request in requests:
        if request.get("status") == 200 and request.get("sha256"):
            request_index.setdefault(request["requested_url"], []).append(request)
    audit, regression, used_reviews = [], [], set()
    for row in rows:
        target = row in targets
        main_hash = row["capture_content_sha256"] or row.get("supplement_raw_html_sha256", "")
        main_signals = None
        if main_hash:
            path = (base / "results/evidence" / (main_hash + ".gz")) if row["capture_content_sha256"] else (
                corpus / row["supplement_raw_html_path"])
            html = read_html(path, main_hash)
            main_signals = content_signals(html, row["full_name"], row["geo_name"])
            main_ok = not archive_response_error(html) and all(main_signals[f] for f in (
                "candidate_identity_match", "mayoral_campaign_match", "city_match"))
            regression.append({"candidate_url_id": row["candidate_url_id"],
                               "main_sha256": main_hash, "all_signals": main_ok,
                               "previous_strict": yes(row["strict_coverage_eligible_result"])})
            if yes(row["strict_coverage_eligible_result"]) and not main_ok:
                raise ValueError("Parser regression on previously cleared row: " + row["full_name"])
        if not target:
            continue
        sides = []
        saved_only = not bool(row["capture_content_sha256"])
        if saved_only:
            record = saved[row["candidate_url_id"]]
            for side in record["captures"]:
                html = read_html(corpus / side["raw_html_path"], side["raw_html_sha256"])
                state = side_assessment(html, row["full_name"], row["geo_name"], side["raw_html_sha256"], reviews)
                # The saved review may establish identity despite a text alias.
                if state == "insufficient_surrounding_content" and side.get("same_candidate") is True:
                    state = "same_candidate"
                sides.append({"sha256": side["raw_html_sha256"], "state": state,
                              "timestamp": side["effective_timestamp"], "url": side["effective_replay_url"],
                              "raw_html_path": side["raw_html_path"], "side": side["side"]})
        else:
            for side in json.loads(row["reuse_fetch_summary_json"]):
                matches = request_index.get(side["requested_url"], [])
                if len({r["sha256"] for r in matches}) != 1:
                    raise ValueError("Missing or ambiguous request evidence")
                digest = matches[0]["sha256"]
                html = read_html(base / "results/evidence" / (digest + ".gz"), digest)
                state = side_assessment(html, row["full_name"], row["geo_name"], digest, reviews)
                if digest in reviews:
                    used_reviews.add(digest)
                sides.append({"sha256": digest, "state": state, "timestamp": side["effective_timestamp"],
                              "url": side["effective_url"], "request_url": side["requested_url"],
                              "review_reason": reviews.get(digest, "")})
        status = resolution(main_ok, [s["state"] for s in sides], saved_only)
        row["previous_final_validation_status"] = row["final_validation_status"]
        row["previous_validation_reason"] = row["validation_reason"]
        row["previous_domain_reuse_status"] = row["domain_reuse_status"]
        row["previous_reuse_conflict_count"] = row["reuse_conflict_count"]
        row["flag_resolution_status"] = status
        row["content_signal_version"] = CONTENT_SIGNAL_VERSION
        row["resolution_main_signal_summary"] = main_signals["content_signal_summary"]
        row["resolution_side_checks_json"] = json.dumps(sides, sort_keys=True)
        row["resolution_confirmed_reuse_count"] = sum(s["state"] == "unrelated_content_confirmed" for s in sides)
        row["resolution_archive_error_count"] = sum(s["state"] == "archive_service_error" for s in sides)
        if status == "text_rule_flags_cleared":
            if row["availability_status"] != "checked" or not yes(row["within_headline_window"]):
                raise ValueError("Cannot clear an incomplete temporal check")
            row.update(strict_coverage_eligible_result=True, proposed_evidence_grade="A",
                       final_validation_status="grade_a_content_confirmed_pending_review",
                       validation_reason="saved_evidence_text_rules_corrected", domain_reuse_status="same_candidate_only",
                       reuse_conflict_count=0, content_signal_summary=main_signals["content_signal_summary"])
            for field in ("candidate_identity_match", "mayoral_campaign_match", "city_match"):
                row[field] = main_signals[field]
        elif status == "archive_service_error_requires_replay":
            row.update(retryable=True, domain_reuse_status="archive_service_error",
                       final_validation_status="request_error_retryable",
                       validation_reason="saved_archive_service_error_requires_replay")
        elif status == "surrounding_content_insufficient":
            row["domain_reuse_status"] = "insufficient_surrounding_content"
        elif status == "later_domain_reuse_confirmed":
            row["domain_reuse_status"] = "unrelated_content_confirmed"
        audit.append({"candidate_url_id": row["candidate_url_id"], "full_name": row["full_name"],
                      "year": row["year"], "resolution": status, "main_sha256": main_hash,
                      "main_signals": main_signals, "side_checks": sides})
    if used_reviews != set(reviews):
        raise ValueError("Unused content review hashes")
    if any(yes(r["human_accepted"]) for r in rows):
        raise ValueError("Human acceptance changed")
    buffer = io.StringIO()
    fields = sorted(set().union(*(r.keys() for r in rows)))
    writer = csv.DictWriter(buffer, fieldnames=fields)
    writer.writeheader()
    writer.writerows(rows)
    atomic_write(output / "validation.csv", buffer.getvalue())
    atomic_write(output / "audit.json", json.dumps(audit, indent=2, sort_keys=True) + "\n")
    atomic_write(output / "parser_regression.json", json.dumps(regression, indent=2) + "\n")
    summary = {"version": VERSION, "input_sha256": sha(input_path.read_bytes()), "input_rows": len(rows),
               "target_rows": len(targets), "observed_target_keys": len(audit), "new_archive_requests": 0,
               "source_corpus_ledger_sha256": saved_review["ledger_sha256"],
               "content_signal_version": CONTENT_SIGNAL_VERSION, "script_sha256": sha(Path(__file__).read_bytes()),
               "parser_sha256": sha(Path(content_validator.__file__).read_bytes()),
               "reviews_sha256": sha(reviews_path.read_bytes()), "saved_review_sha256": sha(saved_review_path.read_bytes()),
               "resolution_counts": dict(Counter(a["resolution"] for a in audit)),
               "strict_before": 37, "strict_after": sum(yes(r["strict_coverage_eligible_result"]) for r in rows),
               "historical_content_supported": sum(yes(r["historical_campaign_content_supported"]) for r in rows),
               "human_accepted": 0, "parser_regression_rows": len(regression),
               "verified_side_references": sum(len(a["side_checks"]) for a in audit),
               "validation_sha256": sha((output / "validation.csv").read_bytes())}
    atomic_write(output / "summary.json", json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", type=Path, required=True)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--reviews", type=Path, required=True)
    parser.add_argument("--saved-review", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(run(args.base, args.corpus, args.reviews, args.saved_review, args.output), sort_keys=True))
