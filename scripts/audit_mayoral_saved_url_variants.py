#!/usr/bin/env python3
"""Audit saved URL variants. This script makes no archive or search requests."""
import argparse
import collections
import csv
import datetime as dt
import gzip
import hashlib
import json
from pathlib import Path
import re
import subprocess
from urllib.parse import urlsplit, urlunsplit

BASE = Path("quality_reports/mayoral_review_reconciliation_2026-09-13/reconciliation")
VALIDATION = Path("quality_reports/mayoral_archive_validation_2026-09-10/reuse_resolution/validation.csv")
OUT = Path("quality_reports/mayoral_saved_evidence_followup_2026-09-13/variants")
ACTIONS = {"inspect_saved_validation_capture_and_exact_url_equivalence",
           "inspect_earlier_saved_domain_pages_and_exact_path_gap",
           "inspect_saved_domain_pages_and_exact_path_gap"}


def rows(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as stream:
        return list(csv.DictReader(stream))


def sha(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as stream:
        for block in iter(lambda: stream.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


def original(url):
    p = urlsplit(url)
    if p.hostname in {"web.archive.org", "wayback.archive-it.org"}:
        match = re.search(r"/(?:\d{1,14})(?:[a-z]+_)?/(https?://.*)$", url)
        return match.group(1) if match else ""
    return url


def exact(url):
    p = urlsplit(original(url))
    if p.scheme.lower() not in {"http", "https"} or not p.hostname:
        return ""
    return urlunsplit((p.scheme.lower(), p.netloc.lower(), p.path or "/", p.query, ""))


def variant_key(url):
    p = urlsplit(exact(url))
    if not p.hostname:
        return None
    host = p.hostname.removeprefix("www.")
    try:
        port = p.port
    except ValueError:
        return None
    if port == (80 if p.scheme == "http" else 443):
        port = None
    return host, port, p.path, p.query


def relation(target, requested, effective):
    if not exact(effective):
        return "effective_url_unknown"
    if exact(target) == exact(effective):
        return "saved_effective_exact_url"
    if variant_key(target) == variant_key(effective):
        if exact(target) == exact(requested):
            return "saved_request_to_variant_redirect"
        return "saved_syntactic_variant_only"
    return "different_path_query_or_host"


def distance(stamp, date):
    try:
        return abs((dt.datetime.strptime(stamp[:8], "%Y%m%d").date() - dt.date.fromisoformat(date)).days)
    except (ValueError, TypeError):
        return None


def verify_raw(path, expected):
    if not Path(path).is_file():
        return "missing"
    try:
        opener = gzip.open if str(path).endswith(".gz") else open
        with opener(path, "rb") as stream:
            found = hashlib.sha256(stream.read()).hexdigest()
        return "verified" if found == expected else "hash_mismatch"
    except (OSError, EOFError):
        return "read_error"


def context_matches(request, capture, target):
    return (request["candidate_year_id"] == capture["candidate_year_id"]
            == target.get("candidate_year_id")
            and request["registrable_domain"] == target.get("registrable_domain"))


def validate_keys(requests, results, expected_count=None):
    ids = {r["candidate_url_id"] for r in requests}
    if len(ids) != len(requests):
        raise ValueError("Duplicate requested URL keys")
    if expected_count is not None and len(requests) != expected_count:
        raise ValueError("Unexpected requested row count")
    if set(results) != ids:
        raise ValueError("Missing or unexpected saved-evidence keys")


def validate_cache_provenance(remote, expected):
    if remote.get("local_input_hashes") != expected:
        raise ValueError("Saved cache input hashes differ or are absent. Repeat the read-only scan.")


def scan(requests, validations):
    """Run on the corpus host. Only read existing files."""
    full = Path("/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory")
    acquisition = Path("/root/candidate_website_extension/data/municipal_archive_acquisition")
    batches = [("full_corpus", full, acquisition / "full_discovery_740_20260826/frozen/targets.csv")]
    for name in ["covered_151_20260826", "openai_residual_100_20260826", "openai_residual_expansion_357_20260826", "full_discovery_740_20260826"]:
        batches.append((name, acquisition / name / "runs/production", acquisition / name / "frozen/targets.csv"))
    by_candidate = collections.defaultdict(list)
    results = {}
    for r in requests:
        by_candidate[r["candidate_year_id"]].append(r)
        results[r["candidate_url_id"]] = {"counts": collections.Counter(), "best": {}}
    hashes = {}
    ledger_audit = []

    def add(r, proof):
        record = results[r["candidate_url_id"]]
        rel = relation(r["exact_normalized_url"], proof["requested_original_url"], proof["effective_original_url"])
        proof["relation"] = rel
        gap = distance(proof["effective_timestamp"], proof["election_reference_date"])
        proof["days_absolute"] = gap
        proof["within_365_days"] = gap is not None and gap <= 365
        record["counts"][rel] += 1
        record["counts"][rel + "_within_365"] += proof["within_365_days"]
        old = record["best"].get(rel)
        if old is None or (gap if gap is not None else 999999) < (old["days_absolute"] if old["days_absolute"] is not None else 999999):
            record["best"][rel] = proof

    for name, root, frozen in batches:
        checkpoint = root / "fetch_attempts.csv"
        hashes[str(checkpoint)] = sha(checkpoint)
        hashes[str(frozen)] = sha(frozen)
        targets = {r["target_id"]: r for r in rows(frozen)}
        audit = collections.Counter()
        with checkpoint.open(newline="") as stream:
            for c in csv.DictReader(stream):
                audit["rows"] += 1
                if c["fetch_status"] not in {"fetched", "fetched_timestamp_mismatch"}:
                    continue
                target = targets.get(c["target_id"], {})
                if target.get("candidate_year_id") != c["candidate_year_id"]:
                    audit["candidate_context_mismatch"] += 1
                    continue
                for r in by_candidate.get(c["candidate_year_id"], []):
                    if not context_matches(r, c, target):
                        continue
                    raw = root / c["raw_html_path"]
                    if not raw.is_file():
                        audit["missing_matched_raw_files"] += 1
                        continue
                    add(r, {"source": name, "capture_id": c["capture_id"],
                            "requested_original_url": c["original_url"],
                            "effective_original_url": original(c.get("effective_replay_url", "")),
                            "requested_replay_url": c.get("requested_replay_url", ""),
                            "effective_replay_url": c.get("effective_replay_url", ""),
                            "effective_timestamp": c.get("effective_timestamp", ""),
                            "election_reference_date": target["election_reference_date"],
                            "raw_path": str(raw), "raw_sha256": c["raw_html_sha256"],
                            "http_status": c["http_status"], "fetch_status": c["fetch_status"]})
        ledger_audit.append({"batch": name, **audit})

    for v in validations:
        r = next((r for r in by_candidate[v["candidate_year_id"]] if r["candidate_url_id"] == v["candidate_url_id"]), None)
        if r is None:
            continue
        for prefix in ["capture", "supplement"]:
            if prefix == "capture":
                digest = v.get("capture_content_sha256", "")
                path = Path("/mnt/mayoral_wayback_full_snapshots/ownership_validation_20260910/results/evidence") / (digest + ".gz")
                requested, effective = v.get("capture_requested_url", ""), v.get("capture_effective_url", "")
                stamp = v.get("capture_effective_timestamp", "")
            else:
                digest = v.get("supplement_raw_html_sha256", "")
                path = full / v.get("supplement_raw_html_path", "")
                requested, effective = v.get("supplement_requested_replay_url", ""), v.get("supplement_effective_replay_url", "")
                stamp = v.get("supplement_effective_timestamp", "")
            if not digest or not path.is_file():
                continue
            add(r, {"source": "validation_" + prefix, "capture_id": v.get("supplement_capture_id", "") if prefix == "supplement" else digest,
                    "requested_original_url": original(requested), "effective_original_url": original(effective),
                    "requested_replay_url": requested, "effective_replay_url": effective,
                    "effective_timestamp": stamp, "election_reference_date": v["election_reference_date"],
                    "raw_path": str(path), "raw_sha256": digest,
                    "fetch_status": v.get("capture_fetch_status", ""), "http_status": ""})
    # Verify one nearest response per relationship. Counts are ledger counts, not distinct pages.
    cache = {}
    for record in results.values():
        for proof in record["best"].values():
            key = proof["raw_path"], proof["raw_sha256"]
            if key not in cache:
                cache[key] = verify_raw(*key)
            proof["raw_hash_check"] = cache[key]
    return {"results": results, "input_hashes": hashes, "ledger_audit": ledger_audit,
            "raw_checks": dict(collections.Counter(cache.values()))}


def summarize(requests, remote):
    output = []
    for r in requests:
        evidence = remote["results"][r["candidate_url_id"]]
        counts = evidence["counts"]
        classes = ["saved_effective_exact_url", "saved_request_to_variant_redirect", "saved_syntactic_variant_only"]
        # A verified saved response establishes acquisition, not campaign ownership.
        verified = {k: p for k, p in evidence["best"].items() if p["raw_hash_check"] == "verified"}
        selected = next((k for k in classes if k in verified and verified[k]["within_365_days"]), None)
        if selected is None:
            selected = next((k for k in classes if k in verified), None)
        classification = selected or ("different_path_query_or_host_only" if "different_path_query_or_host" in verified else "no_verified_saved_response")
        output.append({"candidate_url_id": r["candidate_url_id"], "candidate_year_id": r["candidate_year_id"],
                       "exact_normalized_url": r["exact_normalized_url"], "review_action": r["review_action"],
                       "classification": classification,
                       "same_path_saved_within_365_days": any(k in verified and verified[k]["within_365_days"] for k in classes),
                       "strict_coverage_newly_accepted": False,
                       "counts_json": json.dumps(counts, sort_keys=True),
                       "evidence_json": json.dumps(evidence["best"], sort_keys=True)})
    return output


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--reuse-remote", action="store_true")
    args = parser.parse_args()
    requests = [r for r in rows(BASE / "remaining_acquisition_review_queue.csv") if r["review_action"] in ACTIONS]
    validate_keys(requests, {r["candidate_url_id"]: None for r in requests}, 521)
    ids = {r["candidate_url_id"] for r in requests}
    validations = [r for r in rows(VALIDATION) if r["candidate_url_id"] in ids]
    local_hashes = {str(p): sha(p) for p in [BASE / "remaining_acquisition_review_queue.csv", VALIDATION]}
    OUT.mkdir(parents=True, exist_ok=True)
    cache = OUT / "remote_saved_variant_evidence.json"
    if args.reuse_remote:
        remote = json.loads(cache.read_text())
        validate_cache_provenance(remote, local_hashes)
    else:
        source = Path(__file__).read_text().rsplit('if __name__ == "__main__":', 1)[0]
        source += "\nprint(json.dumps(scan(" + repr(requests) + ", " + repr(validations) + ")))\n"
        result = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "root@droplet", "python3", "-"], input=source, text=True, capture_output=True, timeout=180)
        if result.returncode:
            raise RuntimeError(result.stderr)
        remote = json.loads(result.stdout)
        remote["local_input_hashes"] = local_hashes
        cache.write_text(json.dumps(remote, indent=2) + "\n")
    validate_keys(requests, remote["results"], 521)
    output = summarize(requests, remote)
    with (OUT / "saved_url_variant_audit.csv").open("w", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=output[0].keys())
        writer.writeheader()
        writer.writerows(output)
    summary = {"rows": len(output), "candidate_years": len({r["candidate_year_id"] for r in output}),
               "classifications": dict(collections.Counter(r["classification"] for r in output)),
               "same_path_saved_within_365_days": sum(r["same_path_saved_within_365_days"] for r in output),
               "by_previous_action": {a: dict(collections.Counter(r["classification"] for r in output if r["review_action"] == a)) for a in sorted(ACTIONS)},
               "raw_checks": remote["raw_checks"], "ledger_audit": remote["ledger_audit"],
               "new_archive_requests": 0, "new_human_approvals": 0,
               "limitations": ["Syntactic variants do not prove host equivalence.", "Matching saved URLs does not establish campaign ownership.", "Only each relationship's nearest saved response received a byte-hash check.", "Election dates use the recorded election month and day 15.", "Counts combine prior ledgers and can repeat the same capture."],
               "input_hashes": {str(p): sha(p) for p in [BASE / "remaining_acquisition_review_queue.csv", VALIDATION, Path(__file__)]}}
    (OUT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
