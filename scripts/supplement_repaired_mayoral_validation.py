#!/usr/bin/env python3
"""Check repaired leads against saved HTML; perform no network requests."""
import argparse
import csv
import gzip
import hashlib
import json
from collections import Counter, defaultdict
from datetime import datetime
from pathlib import Path
from urllib.parse import urlsplit

from validate_search_lead_wayback_available_content import content_signals

VERSION = "saved_mayoral_capture_supplement_v1"
MAX_BYTES = 8 * 1024 * 1024


def variant_key(url):
    parsed = urlsplit(url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise ValueError("Expected HTTP(S) URL")
    port = parsed.port
    host = parsed.hostname.lower().removeprefix("www.")
    if port and (parsed.scheme, port) not in {("http", 80), ("https", 443)}:
        host += ":" + str(port)
    return host, parsed.path or "/", parsed.query


def sha(path):
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def read_capture(corpus, capture, max_bytes=MAX_BYTES):
    path = (corpus / capture["raw_html_path"]).resolve()
    if not path.is_relative_to(corpus.resolve()):
        raise ValueError("Capture path escapes corpus")
    with gzip.open(path, "rb") as handle:
        raw = handle.read(max_bytes + 1)
    if len(raw) > max_bytes:
        raise ValueError("Capture exceeds decompressed byte limit")
    if hashlib.sha256(raw).hexdigest() != capture["raw_html_sha256"]:
        raise ValueError("Capture hash mismatch")
    return raw


def supplement(input_path, corpus, output):
    rows = list(csv.DictReader(input_path.open()))
    keys = [r["candidate_url_id"] for r in rows]
    if len(set(keys)) != len(keys) or not all(keys):
        raise ValueError("Candidate URL keys must be unique and nonempty")
    index = defaultdict(list)
    for row in rows:
        row["_reference"] = datetime(int(row["year"]), int(float(row["month"])), 15)
        index[variant_key(row["exact_normalized_url"])].append(row)
    selected = {}
    ledger = corpus / "fetch_attempts.csv"
    ledger_hash = sha(ledger)
    scanned = 0
    with ledger.open() as handle:
        for capture in csv.DictReader(handle):
            scanned += 1
            if capture["fetch_status"] != "fetched":
                continue
            try:
                variant = variant_key(capture["original_url"])
                when = datetime.strptime(capture["effective_timestamp"], "%Y%m%d%H%M%S")
            except (ValueError, KeyError):
                continue
            for row in index.get(variant, []):
                days = (when - row["_reference"]).total_seconds() / 86400
                if abs(days) > 365:
                    continue
                exact = capture["original_url"] == row["exact_normalized_url"]
                rank = (not exact, abs(days), capture["effective_timestamp"], capture["capture_id"])
                key = row["candidate_url_id"]
                if key not in selected or rank < selected[key][0]:
                    selected[key] = rank, capture, days
    input_hash = sha(input_path)
    results = []
    for row in rows:
        key = row["candidate_url_id"]
        result = {field: row.get(field, "") for field in (
            "candidate_url_id", "candidate_year_id", "full_name", "geo_name", "year", "month", "exact_normalized_url")}
        result.update(version=VERSION, input_sha256=input_hash, ledger_sha256=ledger_hash,
                      corpus_dir=str(corpus), election_reference_date=row["_reference"].date().isoformat(),
                      saved_capture_status="not_found_in_saved_corpus", hash_verified=False,
                      domain_reuse_status="not_assessed_by_supplement", strict_coverage_eligible_result=False,
                      human_accepted=False, error="")
        if key in selected:
            rank, capture, days = selected[key]
            result.update({field: capture.get(field, "") for field in (
                "capture_id", "original_url", "timestamp", "effective_timestamp", "effective_replay_url",
                "requested_replay_url", "raw_html_path", "raw_html_sha256", "attempted_at_utc")})
            result.update(url_match="variant" if rank[0] else "exact", days_signed=days,
                          days_absolute=abs(days), saved_capture_status="saved_capture_verification_error")
            try:
                raw = read_capture(corpus, capture)
                signals = content_signals(raw.decode("utf-8", errors="replace"), row["full_name"], row["geo_name"])
                signals.pop("content_sha256")
                confirmed = all(signals[k] for k in ("candidate_identity_match", "mayoral_campaign_match", "city_match"))
                result.update(signals, hash_verified=True, raw_html_bytes=len(raw),
                              saved_capture_status="saved_capture_content_" + ("confirmed" if confirmed else "unresolved") + "_reuse_pending")
            except (OSError, ValueError, EOFError) as error:
                result["error"] = str(error)
        results.append(result)
    output.mkdir(parents=True, exist_ok=True)
    fields = sorted(set().union(*(r.keys() for r in results)))
    with (output / "saved_capture_supplement.csv").open("w") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(results)
    summary = dict(version=VERSION, input_sha256=input_hash, ledger_sha256=ledger_hash,
                   script_sha256=sha(Path(__file__)), input_rows=len(rows), output_rows=len(results),
                   ledger_rows_scanned=scanned, selected_captures=len(selected),
                   url_matches=dict(Counter(r.get("url_match") for r in results if "url_match" in r)),
                   statuses=dict(Counter(r["saved_capture_status"] for r in results)),
                   limitations="Saved corpus absence is not archive absence; content signals do not complete domain-reuse checks or confer human approval.")
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    return results, summary


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--corpus-dir", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    _, summary = supplement(args.input, args.corpus_dir, args.output)
    print(json.dumps(summary, indent=2))
