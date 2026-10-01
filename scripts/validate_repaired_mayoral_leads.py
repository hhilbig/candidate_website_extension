#!/usr/bin/env python3
"""Checkpoint the repaired ownership leads through the existing archive validator."""
from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import time
from collections import Counter
from pathlib import Path
from urllib.parse import urljoin, urlsplit

import requests
import pandas as pd

import validate_search_lead_wayback_available_content as validator

VERSION = "repaired_mayoral_archive_validation_v1"
ALLOWED_HOSTS = {"archive.org", "web.archive.org", "wayback.archive-it.org"}
MAX_BODY = 2 * 1024 * 1024
MAX_STORED = 64 * 1024 * 1024


def digest(data):
    return hashlib.sha256(data).hexdigest()


def serializable_result(result):
    return {key: None if value is pd.NA else value for key, value in result.items()}


def prepare_rows(path):
    rows = list(csv.DictReader(path.open()))
    keys = [row["candidate_url_id"] for row in rows]
    if len(keys) != len(set(keys)) or not all(keys):
        raise ValueError("Candidate URL keys must be nonempty and unique")
    for row in rows:
        row["validation_key"] = row["candidate_url_id"]
        row["month"] = int(float(row["month"])) if row["month"] else ""
        row["validation_method_version"] = VERSION
    return rows


class EvidenceSession:
    """Serial, archive-only requests with bounded retained response evidence."""

    def __init__(self, root):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.used = sum(p.stat().st_size for p in root.glob("*.gz"))
        self.session = requests.Session()
        self.last_request = 0.0

    def get(self, url, headers, timeout):
        original = url
        started = time.time()
        response = None
        try:
            for _ in range(6):
                parsed = urlsplit(url)
                if parsed.scheme not in {"http", "https"} or parsed.hostname not in ALLOWED_HOSTS:
                    raise requests.RequestException("Replay escaped permitted archive hosts")
                time.sleep(max(0, 1 - (time.monotonic() - self.last_request)))
                self.last_request = time.monotonic()
                response = self.session.get(url, headers=headers, timeout=timeout,
                                            stream=True, allow_redirects=False)
                if response.is_redirect:
                    url = urljoin(url, response.headers["Location"])
                    response.close()
                    continue
                chunks, size = [], 0
                for chunk in response.iter_content(65536):
                    size += len(chunk)
                    if size > MAX_BODY or time.time() - started > 60:
                        raise requests.RequestException("Response exceeded byte/time limit")
                    chunks.append(chunk)
                response._content = b"".join(chunks)
                response._content_consumed = True
                packed = gzip.compress(response.content, mtime=0)
                sha = digest(response.content)
                path = self.root / (sha + ".gz")
                if not path.exists():
                    if self.used + len(packed) > MAX_STORED:
                        raise requests.RequestException("Evidence storage limit reached")
                    path.write_bytes(packed)
                    self.used += len(packed)
                self.record({"requested_url": original, "effective_url": response.url,
                             "status": response.status_code, "sha256": sha,
                             "bytes": size, "content_type": response.headers.get("content-type", ""),
                             "evidence_file": path.name})
                return response
            raise requests.RequestException("Too many archive redirects")
        except requests.RequestException as error:
            self.record({"requested_url": original, "error": str(error)})
            raise
        finally:
            if response is not None:
                response.close()

    def record(self, record):
        record["recorded_at_unix"] = time.time()
        with (self.root / "requests.jsonl").open("a") as handle:
            handle.write(json.dumps(record, sort_keys=True) + "\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--retry-errors", action="store_true")
    args = parser.parse_args()
    rows = prepare_rows(args.input)
    args.output.mkdir(parents=True, exist_ok=True)
    manifest = {"version": VERSION, "input_sha256": digest(args.input.read_bytes()),
                "input_rows": len(rows), "validator_sha256": digest(Path(validator.__file__).read_bytes()),
                "driver_sha256": digest(Path(__file__).read_bytes()),
                "max_response_bytes": MAX_BODY, "max_stored_evidence_bytes": MAX_STORED,
                "missing_month_rows": sum(r["month"] == "" for r in rows)}
    manifest_path = args.output / "manifest.json"
    if manifest_path.exists():
        previous = json.loads(manifest_path.read_text())
        if any(previous.get(k) != v for k, v in manifest.items() if k != "driver_sha256"):
            raise ValueError("Input or validator changed since checkpoint")
        (args.output / ("manifest_" + previous["driver_sha256"] + ".json")).write_text(
            json.dumps(previous, indent=2) + "\n")
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    journal = args.output / "results.jsonl"
    completed = {}
    if journal.exists():
        for line in journal.read_text().splitlines():
            result = json.loads(line)
            completed[result["validation_key"]] = result
    session = EvidenceSession(args.output / "evidence")
    original_request = validator._request

    def request(url, **kwargs):
        kwargs["session"] = session
        return original_request(url, **kwargs)

    validator._request = request
    availability_cache, capture_cache = {}, {}
    processed = 0
    for row in rows:
        prior = completed.get(row["validation_key"])
        if prior and not (args.retry_errors and prior["retryable"]):
            continue
        if args.limit is not None and processed >= args.limit:
            break
        result = serializable_result(validator.validate_one_lead(row, timeout=15, attempts=1, delay_seconds=0,
                                            availability_cache=availability_cache,
                                            capture_cache=capture_cache))
        result["driver_sha256"] = manifest["driver_sha256"]
        result["candidate_url_id"] = row["candidate_url_id"]
        result["summary_change_type"] = row["summary_change_type"]
        result["input_sha256"] = manifest["input_sha256"]
        result["source_claim_ids_json"] = row["source_expansion_claim_ids_json"]
        with journal.open("a") as handle:
            handle.write(json.dumps(result, sort_keys=True) + "\n")
            handle.flush()
        completed[row["validation_key"]] = result
        processed += 1
        print(json.dumps({"completed": len(completed), "candidate": row["full_name"],
                          "status": result["temporal_validation_status"]}), flush=True)
        export(args.output, completed, manifest)
    export(args.output, completed, manifest)


def export(output, completed, manifest):
    results = sorted(completed.values(), key=lambda r: r["validation_key"])
    if results:
        with (output / "validation.csv").open("w") as handle:
            writer = csv.DictWriter(handle, fieldnames=sorted(set().union(*(r.keys() for r in results))))
            writer.writeheader()
            writer.writerows(results)
    summary = {**manifest, "completed": len(results),
               "statuses": dict(Counter(r["temporal_validation_status"] for r in results)),
               "reasons": dict(Counter(r["validation_reason"] for r in results)),
               "retryable": sum(bool(r["retryable"]) for r in results),
               "evidence_compressed_bytes": sum(p.stat().st_size for p in (output / "evidence").glob("*.gz"))}
    (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")


if __name__ == "__main__":
    main()
