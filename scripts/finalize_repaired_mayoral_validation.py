#!/usr/bin/env python3
"""Verify and finalize the 120 repaired mayoral archive assessments."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import os
import re
import tempfile
from collections import Counter
from pathlib import Path
from urllib.parse import urlsplit

VERSION = "repaired_mayoral_validation_final_v1"


def sha256(data):
    return hashlib.sha256(data).hexdigest()


def atomic_write(path, text):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, name = tempfile.mkstemp(prefix=".finalize-", dir=path.parent)
    try:
        with os.fdopen(fd, "w") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(name, path)
    finally:
        if os.path.exists(name):
            os.unlink(name)


def replay_target(url):
    parts = urlsplit(url)
    if parts.hostname not in {"web.archive.org", "wayback.archive-it.org"}:
        return ""
    match = re.fullmatch(r"/(?:web|all)/\d{14}(?:[a-z]+_)?/(https?://.*)", parts.path)
    return match.group(1) + ("?" + parts.query if parts.query else "") if match else ""


def url_equivalence(original, effective):
    if original == effective:
        return "literal"
    def canonical(url):
        parts = urlsplit(url)
        if parts.scheme not in {"http", "https"} or not parts.hostname or parts.username or parts.password:
            raise ValueError("Invalid target URL")
        host = parts.hostname.removeprefix("www.")
        port = parts.port
        if port == {"http": 80, "https": 443}[parts.scheme]:
            port = None
        return host, port, parts.path or "/", parts.query, parts.fragment
    try:
        return "scheme_www_defaultport_equivalent" if canonical(original) == canonical(effective) else "changed_target"
    except ValueError:
        return "changed_target"


def read_jsonl(path):
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def normalized_month(value):
    if value in (None, ""):
        return None
    number = float(value)
    if not number.is_integer() or not 1 <= number <= 12:
        raise ValueError("Invalid election month")
    return int(number)


def finalize(input_path, results_dir, output, reviews=None, saved_supplement=None):
    if output.resolve() == results_dir.resolve() or results_dir.resolve() in output.resolve().parents:
        raise ValueError("Final output must be separate from raw results")
    rows = list(csv.DictReader(io.StringIO(input_path.read_text())))
    keys = [r["candidate_url_id"] for r in rows]
    if len(rows) != 120 or len(set(keys)) != 120 or not all(keys):
        raise ValueError("Expected exactly 120 unique input keys")
    input_hash = sha256(input_path.read_bytes())
    manifest = json.loads((results_dir / "manifest.json").read_text())
    if manifest["input_sha256"] != input_hash or manifest["input_rows"] != 120:
        raise ValueError("Input manifest mismatch")
    attempts = read_jsonl(results_dir / "results.jsonl")
    current = {}
    for result in attempts:
        key = result["validation_key"]
        if key not in keys or result["candidate_url_id"] != key or result["input_sha256"] != input_hash:
            raise ValueError("Unexpected result key or input hash")
        current[key] = result
    if set(current) != set(keys):
        raise ValueError("Missing result keys")
    evidence = {}
    for path in sorted((results_dir / "evidence").glob("*.gz")):
        with gzip.open(path, "rb") as handle:
            body = handle.read(2 * 1024 * 1024 + 1)
        if len(body) > 2 * 1024 * 1024 or path.stem != sha256(body):
            raise ValueError("Evidence size or hash mismatch: " + path.name)
        evidence[path.stem] = len(body)
    requests_path = results_dir / "evidence" / "requests.jsonl"
    requests = read_jsonl(requests_path) if requests_path.exists() else []
    for request in requests:
        if request.get("sha256"):
            sha = request["sha256"]
            if sha not in evidence or request.get("evidence_file") != sha + ".gz" or request.get("bytes") != evidence[sha]:
                raise ValueError("Request evidence link mismatch")
        elif not request.get("error"):
            raise ValueError("Request lacks evidence or error")
    reviews = reviews or {}
    if set(reviews) - set(keys):
        raise ValueError("Unknown review keys")
    supplements = {}
    if saved_supplement:
        saved_rows = list(csv.DictReader(saved_supplement.open()))
        saved_keys = [r["candidate_url_id"] for r in saved_rows]
        if len(saved_keys) != 120 or set(saved_keys) != set(keys):
            raise ValueError("Supplement keys mismatch")
        for saved in saved_rows:
            if saved["input_sha256"] != input_hash:
                raise ValueError("Supplement input hash mismatch")
            if saved.get("hash_verified", "").lower() == "true":
                corpus = Path(saved["corpus_dir"]).resolve()
                path = (corpus / saved["raw_html_path"]).resolve()
                if corpus not in path.parents:
                    raise ValueError("Supplement evidence escapes corpus")
                with gzip.open(path, "rb") as handle:
                    digest = hashlib.sha256()
                    for chunk in iter(lambda: handle.read(65536), b""):
                        digest.update(chunk)
                if digest.hexdigest() != saved["raw_html_sha256"]:
                    raise ValueError("Supplement evidence hash mismatch")
            supplements[saved["candidate_url_id"]] = saved
    final = []
    for source in rows:
        result = dict(current[source["candidate_url_id"]])
        for field in ("candidate_year_id", "candidate_contest_id", "full_name", "geo_name", "state_abb", "year", "exact_normalized_url"):
            if str(result.get(field, "")) != source[field]:
                raise ValueError("Candidate context mismatch: " + field)
        if normalized_month(result.get("month")) != normalized_month(source.get("month")):
            raise ValueError("Candidate context mismatch: month")
        if result.get("source_claim_ids_json") != source["source_expansion_claim_ids_json"]:
            raise ValueError("Source claim provenance mismatch")
        # Reattach input columns which the older validator does not export.
        for field, value in source.items():
            if field not in result or result[field] is None:
                result[field] = value
        capture_hash = result.get("capture_content_sha256")
        if capture_hash and capture_hash not in evidence:
            raise ValueError("Main capture evidence missing")
        if capture_hash and not any(r.get("sha256") == capture_hash for r in requests):
            raise ValueError("Main capture request provenance missing")
        result["automated_validation_status"] = result["temporal_validation_status"]
        result["automated_validation_reason"] = result["validation_reason"]
        result["automated_proposed_evidence_grade"] = result["proposed_evidence_grade"]
        result["automated_strict_coverage_eligible_result"] = result["strict_coverage_eligible_result"]
        result["final_validation_status"] = result["temporal_validation_status"]
        result["effective_original_url"] = replay_target(result.get("capture_effective_url") or "")
        result["replay_target_equivalence"] = url_equivalence(source["exact_normalized_url"], result["effective_original_url"]) if capture_hash else "not_fetched"
        if capture_hash and result["replay_target_equivalence"] == "changed_target":
            result.update(strict_coverage_eligible_result=False, proposed_evidence_grade="C",
                          final_validation_status="content_validation_unresolved",
                          validation_reason="effective_replay_target_changed")
        saved = supplements.get(source["candidate_url_id"])
        if saved:
            result.update({"supplement_" + field: value for field, value in saved.items()})
            saved_status = saved["saved_capture_status"]
            if (result["automated_validation_status"] == "no_archive_capture" and not capture_hash
                    and saved.get("hash_verified", "").lower() == "true"
                    and saved_status in {"saved_capture_content_confirmed_reuse_pending", "saved_capture_content_unresolved_reuse_pending"}):
                equivalence = url_equivalence(source["exact_normalized_url"], replay_target(saved.get("effective_replay_url", "")))
                result.update(strict_coverage_eligible_result=False, proposed_evidence_grade="C",
                              final_validation_status=saved_status if equivalence != "changed_target" else "saved_capture_content_unresolved_reuse_pending",
                              validation_reason="saved_corpus_capture_requires_reuse_review",
                              supplement_replay_target_equivalence=equivalence)
        review = reviews.get(source["candidate_url_id"])
        result["historical_campaign_content_supported"] = (
            True if (result["automated_proposed_evidence_grade"] == "A"
                     and result["replay_target_equivalence"] in {"literal", "scheme_www_defaultport_equivalent"})
            or result["final_validation_status"] == "saved_capture_content_confirmed_reuse_pending"
            else None)
        if review:
            if set(review) != {"expected_capture_sha256", "historical_campaign_content_supported", "review_reason", "reviewer"}:
                raise ValueError("Unexpected review fields")
            review_hash = capture_hash
            if not review_hash and saved and saved.get("hash_verified", "").lower() == "true":
                review_hash = saved.get("raw_html_sha256")
            if not review_hash or review["expected_capture_sha256"] != review_hash:
                raise ValueError("Review capture hash mismatch")
            if type(review["historical_campaign_content_supported"]) is not bool or review["reviewer"] != "codex_archived_text" or not review["review_reason"]:
                raise ValueError("Invalid review")
            if review["historical_campaign_content_supported"]:
                in_window = result.get("within_headline_window") is True if capture_hash else (
                    saved is not None and saved.get("days_absolute") not in (None, "")
                    and 0 <= float(saved["days_absolute"]) <= 365)
                if not in_window:
                    raise ValueError("Supported review requires election-period capture")
            result.update(review)
            if (not capture_hash and review["historical_campaign_content_supported"]
                    and result.get("supplement_replay_target_equivalence") in {"literal", "scheme_www_defaultport_equivalent"}):
                result.update(strict_coverage_eligible_result=False, proposed_evidence_grade="C",
                              final_validation_status="saved_capture_content_confirmed_reuse_pending",
                              validation_reason="saved_corpus_content_review_supported_reuse_pending")
            if not review["historical_campaign_content_supported"]:
                result.update(strict_coverage_eligible_result=False, proposed_evidence_grade="C",
                              final_validation_status="content_validation_unresolved",
                              validation_reason="archived_text_review_not_supported")
        result["finalizer_version"] = VERSION
        final.append(result)
    summary = {"version": VERSION, "input_sha256": input_hash, "input_rows": 120,
               "observed_keys": len(current), "result_attempts": len(attempts),
               "superseded_results": len(attempts) - len(current),
               "missing_keys": [], "unexpected_keys": [], "key_and_provenance_checks_passed": True,
               "evidence_hash_checks_passed": True, "request_records": len(requests),
               "request_errors": sum(bool(r.get("error")) for r in requests),
               "statuses": dict(Counter(r["final_validation_status"] for r in final)),
               "raw_statuses": dict(Counter(r["automated_validation_status"] for r in final)),
               "reviewed": len(reviews),
               "review_supported": sum(r["historical_campaign_content_supported"] is True for r in reviews.values()),
               "review_not_supported": sum(r["historical_campaign_content_supported"] is False for r in reviews.values()),
               "historical_campaign_content_supported": sum(r["historical_campaign_content_supported"] is True for r in final),
               "historical_campaign_content_not_supported": sum(r["historical_campaign_content_supported"] is False for r in final),
               "historical_campaign_content_unresolved": sum(r["historical_campaign_content_supported"] is None for r in final),
               "strict_eligible": sum(r["strict_coverage_eligible_result"] is True for r in final),
               "target_equivalence": dict(Counter(r["replay_target_equivalence"] for r in final)),
               "evidence_files": len(evidence), "evidence_uncompressed_bytes": sum(evidence.values()),
               "evidence_compressed_bytes": sum(p.stat().st_size for p in (results_dir / "evidence").glob("*.gz")),
               "raw_results_sha256": sha256((results_dir / "results.jsonl").read_bytes()),
               "requests_sha256": sha256(requests_path.read_bytes()) if requests_path.exists() else None,
               "reviews_sha256": sha256(json.dumps(reviews, sort_keys=True).encode())}
    summary["saved_supplement_sha256"] = sha256(saved_supplement.read_bytes()) if saved_supplement else None
    stream = io.StringIO()
    writer = csv.DictWriter(stream, fieldnames=sorted(set().union(*(r.keys() for r in final))))
    writer.writeheader()
    writer.writerows(final)
    atomic_write(output / "validation.csv", stream.getvalue())
    summary["validation_csv_sha256"] = sha256((output / "validation.csv").read_bytes())
    atomic_write(output / "summary.json", json.dumps(summary, indent=2, sort_keys=True) + "\n")
    return summary


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--results-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--reviews", type=Path)
    parser.add_argument("--saved-supplement", type=Path)
    args = parser.parse_args()
    reviews = json.loads(args.reviews.read_text()) if args.reviews else None
    print(json.dumps(finalize(args.input, args.results_dir, args.output, reviews, args.saved_supplement), sort_keys=True))


if __name__ == "__main__":
    main()
