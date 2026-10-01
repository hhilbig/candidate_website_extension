#!/usr/bin/env python3
"""Classify deferred mayoral capture keys using URL-type evidence only.

This is an acquisition-scope proposal. It does not establish page content,
candidate ownership, candidate acceptance, or equivalence to the approved URL.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import re
from collections import Counter
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

MODEL = "gpt-5.6-sol"
REVIEW_METHOD = "GPT-5.6-authored deterministic URL-rule screening"
DECISIONS = {
    "eligible_download_lead",
    "needs_scope_review",
    "not_campaign_page_target",
}

TECHNICAL_PATTERNS = (
    (re.compile(r"/(?:wp-admin/admin-ajax|wp-login)\.php/?$", re.I), "explicit WordPress administration endpoint"),
    (re.compile(r"/crmapi/(?:subscribe|volunteer)/?$", re.I), "explicit CRM API endpoint"),
)

PAGE_ROLE_LEAD_PATTERNS = (
    (re.compile(r"/(?:author|category|tag|type)/", re.I), "same-host taxonomy or author listing page is eligible for acquisition"),
    (re.compile(r"/\d{4}/(?:\d{2}/?){1,2}$", re.I), "same-host date-index page is eligible for acquisition"),
    (re.compile(r"/embed/?$", re.I), "same-host embedded article or event view is eligible for acquisition"),
)

REVIEW_PATTERNS = (
    (re.compile(r"/page/\d+/?$", re.I), "pagination path; may duplicate an archive listing"),
    (re.compile(r"/(?:template-item)/", re.I), "template-system path; campaign-page status is uncertain"),
)


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def normalized_host(url: str) -> str:
    host = (urlsplit(url).hostname or "").lower().rstrip(".")
    return host[4:] if host.startswith("www.") else host


def classify_url(url: str, approved_host: str) -> tuple[str, str, str]:
    parsed = urlsplit(url)
    host = normalized_host(url)
    path = parsed.path or "/"
    if host != approved_host:
        return "needs_scope_review", "host differs from approved campaign host", "host_comparison"
    for pattern, reason in TECHNICAL_PATTERNS:
        if pattern.search(path):
            return "not_campaign_page_target", reason, "explicit_url_type"
    for pattern, reason in PAGE_ROLE_LEAD_PATTERNS:
        if pattern.search(path):
            return "eligible_download_lead", reason, "same_host_page_role"
    if parsed.query:
        if host == "bevandufty.com" and path.rstrip("/").lower() == "/support-bevan" and parsed.query.lower() == "v=endorse":
            return "eligible_download_lead", "same-host endorsement view is eligible for acquisition", "query_page_role"
        if host == "indiawalton.com" and path.rstrip("/").lower() == "/news" and re.fullmatch(r"offset=\d+", parsed.query, re.I):
            return "eligible_download_lead", "same-host news pagination view is eligible for acquisition", "query_page_role"
        if host == "lelandyee.com" and path.rstrip("/").lower() == "/take-action/support-our-bills" and parsed.query.startswith("/take-action/"):
            return "needs_scope_review", "malformed route-as-query syntax leaves the intended page uncertain", "malformed_query_route"
        return "needs_scope_review", "query-bearing URL may be a duplicate, filtered view, or technical request", "query_string"
    for pattern, reason in REVIEW_PATTERNS:
        if pattern.search(path):
            return "needs_scope_review", reason, "path_pattern"
    return (
        "eligible_download_lead",
        "same approved campaign host and no explicit technical, archive, taxonomy, pagination, or query marker",
        "host_and_path_pattern",
    )


def stable_key(row: dict[str, str]) -> str:
    payload = "\x1f".join((row["candidate_url_id"], row["timestamp"], row["original_url"]))
    return "deferred_capture_" + hashlib.sha256(payload.encode()).hexdigest()[:24]


def run(
    retry_path: Path,
    accepted_path: Path,
    out_dir: Path,
    review_date: str,
    prior_path: Path | None = None,
    review_overlay_path: Path | None = None,
) -> dict:
    retry_hash, accepted_hash = sha256(retry_path), sha256(accepted_path)
    with retry_path.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))
    with accepted_path.open(newline="", encoding="utf-8") as f:
        accepted = [r for r in csv.DictReader(f) if r.get("ai_accepted_website", "").lower() == "true"]

    accepted_by_id = {r["candidate_url_id"]: r for r in accepted}
    if len(accepted_by_id) != len(accepted):
        raise ValueError("accepted candidate_url_id is not unique")
    if len({stable_key(r) for r in rows}) != len(rows):
        raise ValueError("deferred capture keys are not unique")
    missing = sorted({r["candidate_url_id"] for r in rows} - set(accepted_by_id))
    if missing:
        raise ValueError(f"deferred rows absent from accepted roster: {missing[:3]}")

    overlay = None
    overlay_keys = set()
    overlay_hash = None
    if review_overlay_path is not None:
        overlay_hash = sha256(review_overlay_path)
        overlay = json.loads(review_overlay_path.read_text(encoding="utf-8"))
        overlay_keys = {
            (str(r["timestamp"]), r["original_url"])
            for r in overlay["pending_capture_keys"]
        }
        if len(overlay_keys) != len(overlay["pending_capture_keys"]):
            raise ValueError("review overlay contains duplicate exact timestamp/URL keys")
        input_keys = {(r["timestamp"], r["original_url"]) for r in rows}
        if not overlay_keys <= input_keys:
            raise ValueError("review overlay contains keys absent from frozen input")

    output = []
    for row in rows:
        accepted_row = accepted_by_id[row["candidate_url_id"]]
        approved_host = normalized_host(accepted_row["exact_normalized_url"])
        decision, reason, evidence = classify_url(row["original_url"], approved_host)
        exact_overlay_key = (row["timestamp"], row["original_url"])
        decision_provenance = REVIEW_METHOD
        saved_evidence_path = ""
        saved_evidence_sha256 = ""
        saved_evidence_review_type = ""
        if exact_overlay_key in overlay_keys:
            if row["candidate_year_id"] != str(overlay["candidate_year_id"]):
                raise ValueError("review overlay candidate-year does not match exact input key")
            decision = overlay["decision"]
            reason = overlay["reason"]
            evidence = "coordinator_hash_verified_saved_content_review"
            decision_provenance = overlay["review_type"]
            saved_evidence_path = str(review_overlay_path)
            saved_evidence_sha256 = overlay_hash
            saved_evidence_review_type = overlay["review_type"]
        out = dict(row)
        out.update(
            deferred_capture_key=stable_key(row),
            scope_decision=decision,
            scope_reason=reason,
            scope_evidence_type=evidence,
            approved_campaign_url=accepted_row["exact_normalized_url"],
            approved_campaign_host=approved_host,
            observed_original_host=normalized_host(row["original_url"]),
            review_model=MODEL,
            review_method=REVIEW_METHOD,
            decision_provenance=decision_provenance,
            saved_evidence_path=saved_evidence_path,
            saved_evidence_sha256=saved_evidence_sha256,
            saved_evidence_review_type=saved_evidence_review_type,
            review_date=review_date,
            retry_input_sha256=retry_hash,
            accepted_roster_sha256=accepted_hash,
            review_limitation="URL-type review only; no content, ownership, acceptance, or exact-URL-equivalence inference",
            eligibility_scope="acquisition eligibility only; not content review, ownership approval, candidate acceptance, or exact-URL equivalence",
        )
        output.append(out)

    if len(output) != len(rows) or {r["scope_decision"] for r in output} - DECISIONS:
        raise AssertionError("output preservation or decision contract failed")

    out_dir.mkdir(parents=True, exist_ok=True)
    output_path = out_dir / "deferred_capture_scope_reviews.csv"
    with output_path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(output[0]))
        writer.writeheader()
        writer.writerows(output)

    changes = []
    prior_hash = None
    change_path = None
    if prior_path is not None:
        prior_hash = sha256(prior_path)
        with prior_path.open(newline="", encoding="utf-8") as f:
            prior = {r["deferred_capture_key"]: r for r in csv.DictReader(f)}
        current = {r["deferred_capture_key"]: r for r in output}
        if prior.keys() != current.keys():
            raise ValueError("prior and current capture-key universes differ")
        for key in sorted(current):
            old, new = prior[key], current[key]
            if old["scope_decision"] != new["scope_decision"]:
                changes.append({
                    "deferred_capture_key": key,
                    "candidate_url_id": new["candidate_url_id"],
                    "candidate_year_id": new["candidate_year_id"],
                    "timestamp": new["timestamp"],
                    "original_url": new["original_url"],
                    "prior_scope_decision": old["scope_decision"],
                    "prior_scope_reason": old["scope_reason"],
                    "current_scope_decision": new["scope_decision"],
                    "current_scope_reason": new["scope_reason"],
                })
        prior_label = re.sub(r"[^A-Za-z0-9_-]+", "_", prior_path.parent.name)
        change_path = out_dir / f"decision_changes_from_{prior_label}.csv"
        fields = list(changes[0]) if changes else ["deferred_capture_key"]
        with change_path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fields)
            writer.writeheader()
            writer.writerows(changes)

    counts = Counter(r["scope_decision"] for r in output)
    unique_url_counts = Counter()
    seen = set()
    for r in output:
        key = (r["original_url"], r["scope_decision"])
        if key not in seen:
            unique_url_counts[r["scope_decision"]] += 1
            seen.add(key)
    manifest = {
        "review_date": review_date,
        "review_model": MODEL,
        "review_method": REVIEW_METHOD,
        "review_provenance": "Rules authored with GPT-5.6; decisions applied deterministically from URL strings and approved-host metadata. This is not individual semantic or content review.",
        "eligibility_scope": "acquisition eligibility only; all exact URL and timestamp keys retained without deduplication",
        "objective": "acquisition scope proposal for saved deferred capture keys",
        "network_requests": 0,
        "input_rows": len(rows),
        "output_rows": len(output),
        "distinct_capture_keys": len({r["deferred_capture_key"] for r in output}),
        "distinct_original_urls": len({r["original_url"] for r in output}),
        "distinct_candidate_years": len({r["candidate_year_id"] for r in output}),
        "row_decision_counts": dict(sorted(counts.items())),
        "distinct_url_decision_counts": dict(sorted(unique_url_counts.items())),
        "inputs": {
            str(retry_path): retry_hash,
            str(accepted_path): accepted_hash,
        },
        "output": {str(output_path): sha256(output_path)},
        "limitations": [
            "Decisions use URL host, path, and query evidence only.",
            "eligible_download_lead is a proposal, not acquisition authorization or candidate acceptance.",
            "No decision establishes content, ownership, or equivalence to the approved exact URL.",
            (
                "The template-system path remains a judgment call; three Leland keys use the separate saved-content evidence overlay."
                if review_overlay_path is not None
                else "The malformed Leland route-as-query URLs and template-system path remain judgment calls."
            ),
        ],
    }
    if prior_path is not None:
        manifest["inputs"][str(prior_path)] = prior_hash
        manifest["decision_changes_from_prior"] = len(changes)
        manifest["output"][str(change_path)] = sha256(change_path)
    if review_overlay_path is not None:
        manifest["inputs"][str(review_overlay_path)] = overlay_hash
        manifest["saved_review_overlay"] = {
            "path": str(review_overlay_path),
            "sha256": overlay_hash,
            "exact_keys_applied": len(overlay_keys),
            "review_type": overlay["review_type"],
            "decision_scope": overlay["decision_scope"],
            "archive_requests": overlay["archive_requests"],
        }
    manifest_path = out_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--retry", type=Path, required=True)
    parser.add_argument("--accepted", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--prior", type=Path)
    parser.add_argument("--review-overlay", type=Path)
    parser.add_argument("--review-date", default=date.today().isoformat())
    args = parser.parse_args()
    print(json.dumps(run(args.retry, args.accepted, args.out, args.review_date, args.prior, args.review_overlay), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
