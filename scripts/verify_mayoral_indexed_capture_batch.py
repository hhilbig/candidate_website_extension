#!/usr/bin/env python3
"""Verify a frozen indexed Wayback capture batch without network access."""
from __future__ import annotations

import argparse
import collections
import csv
import gzip
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
from urllib.parse import urljoin, urlsplit


EXPECTED_LIMITS = {
    "workers": 1,
    "attempts_per_capture": 3,
    "max_response_decoded_bytes": 2097152,
    "max_tranche_decoded_bytes": 134217728,
    "min_volume_free_bytes": 8000000000,
}
MAX_REDIRECTS = 5
TERMINAL_STATUSES = {
    "fetched", "response_too_large", "redirect_missing_location",
    "redirect_blocked", "redirect_limit", "terminal_http", "capture_mismatch",
    "non_html", "no_content",
}


def sha256_bytes(data):
    return hashlib.sha256(data).hexdigest()


def sha256_file(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def capture_key(row):
    identity = {key: row[key] for key in ("candidate_year_id", "timestamp", "original_url")}
    identity["provider"] = "web.archive.org"
    return sha256_bytes(json.dumps(identity, sort_keys=True).encode())


def replay_identity(url):
    parsed = urlsplit(url)
    if (parsed.scheme != "https" or parsed.hostname != "web.archive.org"
            or parsed.username or parsed.password or parsed.port not in (None, 443)):
        raise ValueError("the replay URL is outside web.archive.org")
    if parsed.fragment:
        raise ValueError("the archive replay URL has a fragment")
    match = re.fullmatch(r"https://web\.archive\.org/web/(\d{14})(?:id_)?/(https?://.+)", url)
    if not match:
        raise ValueError("the archive replay URL is invalid")
    return match[1], match[2]


def _manifest_hash(manifest, queue_name):
    for key in ("approved_queue_sha256", "output_sha256", "queue_sha256", "exact_capture_queue_sha256"):
        if isinstance(manifest.get(key), str):
            return manifest[key]
    outputs = manifest.get("outputs", {})
    for name, digest in outputs.items():
        if Path(name).name == queue_name:
            return digest
    return None


def _manifest_count(manifest):
    for key in ("selected_capture_keys", "capture_count", "queue_rows"):
        if key in manifest:
            return int(manifest[key])
    return None


def _normalize_limits(limits):
    aliases = {"max_attempts_per_capture": "attempts_per_capture",
               "max_batch_decoded_bytes": "max_tranche_decoded_bytes"}
    return {aliases.get(key, key): value for key, value in limits.items()}


def verify_batch(batch_root, terminal=False):
    root = Path(batch_root)
    queue_path = root / "inputs" / "exact_capture_queue.csv"
    manifest_path = root / "inputs" / "manifest.json"
    ledger_path = root / "run" / "ledger.sqlite3"
    errors = []

    def check(condition, message):
        if not condition:
            errors.append(message)

    queue_bytes = queue_path.read_bytes()
    queue_sha = sha256_bytes(queue_bytes)
    manifest_bytes = manifest_path.read_bytes()
    manifest = json.loads(manifest_bytes)
    rows = list(csv.DictReader(queue_bytes.decode("utf-8").splitlines()))
    expected_hash = _manifest_hash(manifest, queue_path.name)
    expected_count = _manifest_count(manifest)
    check(expected_hash == queue_sha, "the queue hash differs from the manifest")
    check(expected_count == len(rows), "the queue row count differs from the manifest")
    check(manifest.get("contract_version") == "mayoral_indexed_capture_batch_v1", "the indexed manifest contract version differs")
    check(manifest.get("approval", {}).get("status") == "approved", "the indexed manifest is not approved")
    check(manifest.get("preserve_original_proposal_bytes") is True, "the manifest does not preserve the proposal bytes")
    check(_normalize_limits(manifest.get("approved_limits", {})) == EXPECTED_LIMITS, "the approved manifest limits differ")

    queue_by_key = {}
    for number, row in enumerate(rows, 2):
        try:
            key = capture_key(row)
            expected_url = f"https://web.archive.org/web/{row['timestamp']}id_/{row['original_url']}"
            check(row.get("provider", "web.archive.org") == "web.archive.org", f"queue row {number} has the wrong provider")
            check(row.get("proposed_capture_key") == key, f"queue row {number} has an invalid key")
            check(key not in queue_by_key, f"queue row {number} duplicates a key")
            check(row.get("requested_replay_url") == expected_url, f"queue row {number} has an invalid replay URL")
            check(replay_identity(expected_url) == (row["timestamp"], row["original_url"]), f"queue row {number} has an invalid identity")
            queue_by_key[key] = row
        except (KeyError, ValueError) as exc:
            errors.append(f"queue row {number} is invalid: {exc}")
    relation_counts = dict(sorted(collections.Counter(row.get("url_relation") for row in rows).items()))
    approved_batches = manifest.get("approval", {}).get("approved_batches", {})
    check(approved_batches == relation_counts, "the approved URL relation counts differ from the queue")
    check(sorted(manifest.get("approved_url_relations", [])) == sorted(relation_counts), "the approved URL relations differ from the queue")

    uri = "file:" + str(ledger_path.resolve()) + "?mode=ro&immutable=1"
    with sqlite3.connect(uri, uri=True) as db:
        db.row_factory = sqlite3.Row
        contract_rows = list(db.execute("SELECT value FROM contract"))
        current_rows = list(db.execute("SELECT key,status,attempts,next_due,result FROM current"))
        attempt_rows = list(db.execute("SELECT id,key,started,finished,status,result FROM attempts"))
        hop_rows = list(db.execute("SELECT attempt_id,hop,url,charged,decoded,result FROM hops ORDER BY attempt_id,hop"))

    check(len(contract_rows) == 1, "the ledger must contain one contract")
    contract = json.loads(contract_rows[0]["value"]) if len(contract_rows) == 1 else {}
    check(contract.get("queue_sha256") == queue_sha, "the ledger queue hash differs")
    check(_normalize_limits(contract.get("limits", {})) == EXPECTED_LIMITS, "the ledger limits differ")
    check(contract.get("max_redirects") == MAX_REDIRECTS, "the ledger redirect limit differs")
    manifest_sha = sha256_bytes(manifest_bytes)
    binding = (contract.get("approval_manifest_sha256") or contract.get("manifest_sha256")
               or contract.get("indexed_manifest_sha256"))
    check(binding == manifest_sha, "the ledger manifest binding differs")
    check(contract.get("contract_version") == "mayoral_indexed_capture_batch_v1", "the ledger contract version differs")
    check(contract.get("proposal_manifest_sha256") == manifest.get("proposal_manifest_sha256"), "the proposal manifest binding differs")
    proposal_copy = root / "inputs" / "proposal_manifest.json"
    if proposal_copy.exists():
        check(sha256_file(proposal_copy) == manifest.get("proposal_manifest_sha256"), "the proposal manifest copy hash differs")

    current = {row["key"]: row for row in current_rows}
    check(len(current) == len(current_rows), "the current table has duplicate keys")
    check(set(current) == set(queue_by_key), "the current keys differ from the frozen queue")
    attempts_by_id = {row["id"]: row for row in attempt_rows}
    check(len(attempts_by_id) == len(attempt_rows), "the attempt table has duplicate IDs")
    attempt_counts = collections.Counter(row["key"] for row in attempt_rows)
    for row in attempt_rows:
        check(row["key"] in queue_by_key, f"attempt {row['id']} has an unknown key")
        check(row["status"] == "started" or row["finished"] is not None, f"attempt {row['id']} lacks a finish time")
    for key, row in current.items():
        check(row["attempts"] == attempt_counts[key], f"current attempt count differs for {key}")
        check(0 <= row["attempts"] <= EXPECTED_LIMITS["attempts_per_capture"], f"attempt limit exceeded for {key}")

    hops_by_attempt = collections.defaultdict(list)
    raw_decoded = raw_charged = raw_stored = raw_files = 0
    for hop in hop_rows:
        attempt_id = hop["attempt_id"]
        check(attempt_id in attempts_by_id, f"hop refers to unknown attempt {attempt_id}")
        hops_by_attempt[attempt_id].append(hop)
        check(0 <= hop["charged"] <= EXPECTED_LIMITS["max_response_decoded_bytes"], f"hop charge is invalid for {attempt_id}")
        if hop["decoded"] is None or hop["result"] is None:
            allowed = attempt_id in attempts_by_id and attempts_by_id[attempt_id]["status"] in {"started", "retryable", "interrupted"}
            check(allowed and hop["decoded"] is None and hop["result"] is None,
                  f"terminal attempt {attempt_id} has an incomplete hop")
            check(hop["charged"] == EXPECTED_LIMITS["max_response_decoded_bytes"], f"incomplete hop charge differs for {attempt_id}")
            raw_charged += hop["charged"]
            continue
        raw_charged += hop["charged"]
        raw_decoded += hop["decoded"]
        try:
            evidence = json.loads(hop["result"])
            check(evidence["url"] == hop["url"], f"hop URL differs for {attempt_id}")
            check(evidence["decoded_bytes"] == hop["decoded"], f"hop byte count differs for {attempt_id}")
            check(hop["charged"] == (EXPECTED_LIMITS["max_response_decoded_bytes"] if evidence.get("oversized") else hop["decoded"]), f"hop charge differs for {attempt_id}")
            replay_identity(hop["url"])
            check(evidence.get("raw_path") == f"raw/{attempt_id}-{hop['hop']}.gz",
                  f"raw path differs from the runner contract for {attempt_id}")
            raw_path = root / "run" / evidence["raw_path"]
            compressed = raw_path.read_bytes()
            raw = gzip.decompress(compressed)
            check(len(raw) == hop["decoded"], f"raw byte count differs for {attempt_id}")
            check(sha256_bytes(raw) == evidence["raw_sha256"], f"raw hash differs for {attempt_id}")
            raw_stored += len(compressed)
            raw_files += 1
        except (KeyError, ValueError, OSError, EOFError, json.JSONDecodeError) as exc:
            errors.append(f"raw evidence is invalid for {attempt_id}: {exc}")
    check(raw_charged <= EXPECTED_LIMITS["max_tranche_decoded_bytes"], "the batch byte budget is exceeded")

    text_stored = text_files = 0
    fetched_keys = []
    for attempt_id, attempt in attempts_by_id.items():
        hops = hops_by_attempt[attempt_id]
        check(len(hops) <= MAX_REDIRECTS + 1, f"attempt {attempt_id} exceeds the redirect limit")
        check([hop["hop"] for hop in hops] == list(range(len(hops))), f"attempt {attempt_id} has a non-contiguous hop chain")
        if attempt["status"] in {"started", "interrupted"} and attempt["result"] is None:
            continue
        try:
            result = json.loads(attempt["result"])
        except (TypeError, json.JSONDecodeError):
            errors.append(f"attempt {attempt_id} has an invalid result")
            continue
        redirects = result.get("redirects", [])
        completed_hops = [hop for hop in hops if hop["result"] is not None]
        # Exceptions escaping fetch() produce only {"error": ...}; durable hop
        # evidence remains authoritative for those retryable attempts.
        if "redirects" in result:
            check(len(redirects) == len(completed_hops), f"attempt {attempt_id} has a different result hop count")
            for evidence, hop in zip(redirects, completed_hops):
                check(evidence.get("url") == hop["url"], f"attempt {attempt_id} has a different result hop URL")
                if hop["result"] is not None:
                    try:
                        check(evidence == json.loads(hop["result"]), f"attempt {attempt_id} has different hop evidence")
                    except json.JSONDecodeError:
                        pass
        for number, (evidence, following) in enumerate(zip(redirects, redirects[1:])):
            location = evidence.get("headers", {}).get("Location", evidence.get("headers", {}).get("location"))
            target = urljoin(evidence.get("url", ""), location) if location else None
            check(target == following.get("url"), f"attempt {attempt_id} has a different redirect target at hop {number}")
        row = queue_by_key.get(attempt["key"])
        if row and hops:
            check(hops[0]["url"] == row["requested_replay_url"],
                  f"attempt {attempt_id} does not start at the requested replay URL")
        for number, (hop, following) in enumerate(zip(hops, hops[1:])):
            if hop["result"] is None:
                check(False, f"attempt {attempt_id} continues after an incomplete hop {number}")
                continue
            try:
                hop_evidence = json.loads(hop["result"])
                location = hop_evidence.get("headers", {}).get(
                    "Location", hop_evidence.get("headers", {}).get("location"))
                target = urljoin(hop["url"], location) if location else None
                check(hop_evidence.get("status_code") in {301, 302, 303, 307, 308}
                      and target == following["url"],
                      f"attempt {attempt_id} has an invalid durable hop transition at {number}")
            except (TypeError, json.JSONDecodeError):
                pass
        if row and redirects:
            check(result.get("requested_replay_url") == row["requested_replay_url"], f"attempt {attempt_id} has a different requested replay URL")
            check(result.get("requested_timestamp") == row["timestamp"], f"attempt {attempt_id} has a different requested timestamp")
            check(result.get("requested_original_url") == row["original_url"], f"attempt {attempt_id} has a different requested original URL")
        if result.get("text_path"):
            try:
                check(result["text_path"] == f"text/{attempt_id}.txt",
                      f"text path differs from the runner contract for {attempt_id}")
                text_path = root / "run" / result["text_path"]
                text = text_path.read_bytes()
                check(len(text) == result["text_bytes"], f"text byte count differs for {attempt_id}")
                check(sha256_bytes(text) == result["text_sha256"], f"text hash differs for {attempt_id}")
                text_stored += len(text)
                text_files += 1
            except (KeyError, OSError) as exc:
                errors.append(f"text evidence is invalid for {attempt_id}: {exc}")
        if attempt["status"] == "fetched" and row:
            fetched_keys.append(attempt["key"])
            final = redirects[-1] if redirects else {}
            try:
                effective = replay_identity(result.get("effective_replay_url", ""))
            except ValueError as exc:
                errors.append(f"fetched attempt {attempt_id} has an invalid effective URL: {exc}")
                effective = None
            content_type = final.get("headers", {}).get("Content-Type", final.get("headers", {}).get("content-type", "")).lower()
            check(final.get("status_code") == 200, f"fetched attempt {attempt_id} is not HTTP 200")
            check(not final.get("oversized"), f"fetched attempt {attempt_id} is oversized")
            check("html" in content_type, f"fetched attempt {attempt_id} is not HTML")
            check(result.get("text_bytes", 0) > 0, f"fetched attempt {attempt_id} has no text")
            check(effective == (row["timestamp"], row["original_url"]), f"fetched attempt {attempt_id} has an effective identity mismatch")
            check(result.get("effective_timestamp") == row["timestamp"] and result.get("effective_original_url") == row["original_url"], f"fetched attempt {attempt_id} has different effective fields")
            check(bool(hops) and result.get("effective_replay_url") == hops[-1]["url"],
                  f"fetched attempt {attempt_id} effective URL differs from its final hop")

    for key, row in current.items():
        key_attempts = sorted((attempt for attempt in attempt_rows if attempt["key"] == key),
                              key=lambda attempt: (attempt["started"], attempt["id"]))
        if key_attempts:
            latest = key_attempts[-1]
            crash_recovery = row["status"] == "retryable" and latest["status"] == "interrupted" and latest["result"] is None
            check(crash_recovery or row["status"] == latest["status"], f"current status differs from the latest attempt for {key}")
            check(crash_recovery or latest["status"] == "started"
                  or row["result"] == latest["result"],
                  f"current result differs from the latest attempt for {key}")
        if row["status"] == "fetched":
            candidates = [attempt for attempt in attempt_rows if attempt["key"] == key and attempt["status"] == "fetched"]
            check(bool(candidates), f"fetched current row {key} lacks a fetched attempt")
        if row["status"] not in {"pending", "retryable", "started"} | TERMINAL_STATUSES:
            errors.append(f"current row {key} has an unknown status")

    started = sum(row["status"] == "started" for row in attempt_rows)
    if terminal:
        check(started == 0, "terminal verification found a started attempt")
        check(all(row["status"] != "started" for row in current_rows), "terminal verification found started current state")
    statuses = dict(sorted(collections.Counter(row["status"] for row in current_rows).items()))
    candidate_years = sorted({row.get("candidate_year_id", "") for row in rows})
    complete = bool(rows) and statuses == {"fetched": len(rows)} and started == 0
    return {
        "valid": not errors,
        "terminal_verification": terminal,
        "acquisition_complete": complete and not errors,
        "queue_rows": len(rows),
        "queue_sha256": queue_sha,
        "manifest_sha256": manifest_sha,
        "ledger_sha256": sha256_file(ledger_path),
        "candidate_year_count": len(candidate_years),
        "candidate_years": candidate_years,
        "url_relations": relation_counts,
        "statuses": statuses,
        "attempts": len(attempt_rows),
        "started_attempts": started,
        "http_hops": len(hop_rows),
        "fetched_keys": len(set(fetched_keys)),
        "raw_files": raw_files,
        "text_files": text_files,
        "decoded_bytes": raw_decoded,
        "charged_decoded_bytes": raw_charged,
        "raw_stored_bytes": raw_stored,
        "text_stored_bytes": text_stored,
        "total_stored_bytes": raw_stored + text_stored,
        "errors": errors,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--batch-root", type=Path, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--terminal", action="store_true")
    args = parser.parse_args()
    try:
        result = verify_batch(args.batch_root, terminal=args.terminal)
    except Exception as exc:
        result = {"valid": False, "terminal_verification": args.terminal,
                  "acquisition_complete": False,
                  "errors": [f"{type(exc).__name__}: {exc}"]}
    payload = json.dumps(result, indent=2, sort_keys=True) + "\n"
    if args.output:
        args.output.write_text(payload)
    sys.stdout.write(payload)
    raise SystemExit(0 if result["valid"] else 1)


if __name__ == "__main__":
    main()
