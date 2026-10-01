#!/usr/bin/env python3
"""Audit saved query and fetch evidence without archive requests."""
import collections
import csv
import datetime
import gzip
import hashlib
import json
from pathlib import Path
import subprocess
from urllib.parse import urlsplit


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def equivalent(url):
    parsed = urlsplit(url)
    host = (parsed.hostname or "").lower().removeprefix("www.")
    return host, parsed.path.rstrip("/") or "/", parsed.query


def classify(queries, expected, fetches, saved):
    if saved:
        return "saved_response_variant_requires_review", "Review the saved response and candidate ownership."
    if len(queries) != expected or any(q["inventory_status"] not in {"found", "no_capture"} or q.get("raw_verified") is not True for q in queries):
        return "inventory_error_or_incomplete", "Resolve missing or failed inventory evidence before acquisition."
    if queries and all(q["inventory_status"] == "no_capture" and q["raw_rows"] == 0 for q in queries):
        return "completed_empty_inventory", "Retain the empty result for the recorded query scope and dates."
    if any(r["retryable"].lower() == "true" for r in fetches):
        return "retryable_fetch", "Retain capture metadata for an authorized targeted recovery."
    if fetches and all(r.get("fetch_status") == "no_content" for r in fetches):
        return "saved_html_without_extracted_text", "Retain raw HTML. Review its structural evidence before any new acquisition."
    if fetches:
        return "terminal_fetch_without_saved_response", "Retain the terminal fetch evidence; review alternative saved captures if needed."
    return "metadata_without_matching_fetch", "Inspect the inventory and queue for the exact path."


def remote_audit(selected):
    run = Path("/mnt/mayoral_wayback_full_snapshots/runs/production_all_inventory")
    batch = Path("/root/candidate_website_extension/data/municipal_archive_acquisition/full_discovery_740_20260826")
    source = batch / "runs/production"
    tids = {tid for r in selected for tid in json.loads(r["frozen_target_ids_json"])}
    statuses = {r["target_id"]: r for r in read_csv(source / "inventory_target_status.csv")}
    queries = collections.defaultdict(list)
    for q in read_csv(source / "inventory_variant_attempts.csv"):
        if q["target_id"] not in tids:
            continue
        q = dict(q)
        q["raw_verified"] = False
        q["raw_rows"] = None
        if q["raw_response_path"]:
            raw_path = source / q["raw_response_path"]
            try:
                raw = gzip.decompress(raw_path.read_bytes())
                payload = json.loads(raw)
                q["raw_rows"] = max(0, len(payload) - 1)
                q["raw_verified"] = hashlib.sha256(raw).hexdigest() == q["raw_response_sha256"] and len(raw) == int(q["raw_response_bytes"])
                q["raw_payload_valid"] = payload == [] or payload[0] == ["timestamp", "original", "statuscode", "mimetype", "digest", "length"]
                q["raw_verified"] = q["raw_verified"] and q["raw_payload_valid"] and q["raw_rows"] == int(q["capture_rows"])
            except (OSError, ValueError) as error:
                q["raw_validation_error"] = str(error)
        queries[q["target_id"]].append(q)
    targets = {r["target_id"]: r for r in read_csv(batch / "frozen/targets.csv")}
    for tid in tids:
        expected_variants = set(json.loads(targets[tid]["url_variants_json"]))
        actual_variants = [q["variant_url"] for q in queries[tid]]
        assert len(actual_variants) == len(set(actual_variants)), "Duplicate query variants"
        assert expected_variants == set(actual_variants), "Missing or unexpected query variants"
        assert len(expected_variants) == int(statuses[tid]["variant_attempts_expected"])
        assert all(q["window_start"] == targets[tid]["scrape_window_start"].replace("-", "") and q["window_end"] == targets[tid]["scrape_window_end"].replace("-", "") for q in queries[tid])
    inventories = collections.defaultdict(list)
    for r in read_csv(run / "capture_inventory.csv"):
        if r["target_id"] in tids:
            inventories[r["target_id"]].append(r)
    fetched = collections.defaultdict(list)
    wanted = {(r["candidate_year_id"], equivalent(r["exact_normalized_url"])) for r in selected}
    saved_variants = collections.defaultdict(list)
    for r in read_csv(run / "fetch_attempts.csv"):
        if r["target_id"] in tids:
            r = dict(r)
            if r["raw_html_path"]:
                raw_file = run / r["raw_html_path"]
                raw = gzip.decompress(raw_file.read_bytes())
                r["raw_hash_verified"] = hashlib.sha256(raw).hexdigest() == r["raw_html_sha256"]
                r["raw_inspection_excerpt"] = raw.decode("utf-8", errors="replace")[:3000]
                r["raw_actual_bytes"] = len(raw)
                html = raw.decode("utf-8", errors="replace").lower()
                r["raw_markers"] = [marker for marker in ["mcc.godaddy.com/park/", "parking-lander", "adsense/domains/caf.js", "location.reload", "http-equiv=\"refresh\"", "<img", "<iframe", "<noscript"] if marker in html]
            if r["text_path"]:
                r["text_hash_verified"] = sha(run / r["text_path"]) == r["text_sha256"]
            fetched[r["target_id"]].append(r)
        key = r["candidate_year_id"], equivalent(r["original_url"])
        if key in wanted and r["fetch_status"] in {"fetched", "fetched_timestamp_mismatch"}:
            raw_path, text_path = run / r["raw_html_path"], run / r["text_path"]
            if raw_path.is_file() and text_path.is_file():
                r = dict(r)
                r["raw_hash_verified"] = hashlib.sha256(gzip.decompress(raw_path.read_bytes())).hexdigest() == r["raw_html_sha256"]
                r["text_hash_verified"] = sha(text_path) == r["text_sha256"]
                saved_variants[key].append(r)
    journal = run / "fetch_attempts.journal.csv"
    assert not journal.exists() or not read_csv(journal), "Active journal needs state reduction"
    result = []
    for selected_row in selected:
        target_ids = json.loads(selected_row["frozen_target_ids_json"])
        qs = [q for tid in target_ids for q in queries[tid]]
        fs = [f for tid in target_ids for f in fetched[tid]]
        inv = [i for tid in target_ids for i in inventories[tid]]
        expected = sum(int(statuses[tid]["variant_attempts_expected"]) for tid in target_ids)
        saved = saved_variants[(selected_row["candidate_year_id"], equivalent(selected_row["exact_normalized_url"]))]
        category, action = classify(qs, expected, fs, saved)
        result.append({
            **{k:selected_row[k] for k in ["candidate_url_id", "candidate_year_id", "full_name", "year", "exact_normalized_url", "frozen_target_ids_json"]},
            "audit_category": category, "recommended_next_action": action,
            "reason": ("Every frozen query variant returned a verified empty payload." if category == "completed_empty_inventory" else "Saved HTTP 200 HTML produced no extracted text; inspect recorded raw markers." if category == "saved_html_without_extracted_text" else category),
            "query_expected": expected, "query_recorded": len(qs),
            "query_status_counts_json": json.dumps(dict(collections.Counter(q["inventory_status"] for q in qs)), sort_keys=True),
            "query_raw_verified": sum(q["raw_verified"] for q in qs),
            "query_evidence_json": json.dumps(qs, sort_keys=True),
            "target_status_evidence_json": json.dumps([statuses[t] for t in target_ids], sort_keys=True),
            "inventory_rows": len(inv),
            "equivalent_path_inventory_rows": sum(equivalent(i["original_url"]) == equivalent(selected_row["exact_normalized_url"]) for i in inv),
            "fetch_status_counts_json": json.dumps(dict(collections.Counter(f["fetch_status"] for f in fs)), sort_keys=True),
            "fetch_evidence_json": json.dumps(fs, sort_keys=True),
            "saved_variant_evidence_json": json.dumps(saved, sort_keys=True),
            "query_scope_json": json.dumps([{k:targets[t].get(k) for k in ["target_id", "scrape_window_start", "scrape_window_end", "url_variants_json"]} for t in target_ids]),
            "query_parameters": "matchType=prefix; filter=statuscode:200,mimetype:text/html; collapse=digest; limit=50000",
        })
    files = [source/"inventory_target_status.csv", source/"inventory_variant_attempts.csv", batch/"frozen/targets.csv", run/"capture_inventory.csv", run/"fetch_attempts.csv"]
    return {"rows": result, "remote_file_sha256": {str(p):sha(p) for p in files}}


def main():
    queue = Path("quality_reports/mayoral_review_reconciliation_2026-09-13/reconciliation/remaining_acquisition_review_queue.csv")
    selected = [r for r in read_csv(queue) if r["review_action"] == "assess_exact_path_metadata_or_terminal_fetch_result"]
    assert len(selected) == len({r["candidate_url_id"] for r in selected}) == 215
    source = Path(__file__).read_text().split("\ndef main():")[0]
    command = source + "\nprint(json.dumps(remote_audit(" + repr(selected) + ")))\n"
    completed = subprocess.run(["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=15", "root@droplet", "python3", "-"], input=command, text=True, capture_output=True, check=True)
    result = json.loads(completed.stdout)
    rows = result.pop("rows")
    assert {r["candidate_url_id"] for r in rows} == {r["candidate_url_id"] for r in selected}
    out = Path("quality_reports/mayoral_terminal_audit_2026-09-14")
    out.mkdir(parents=True, exist_ok=True)
    with (out/"audit.csv").open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)
    summary = {**result, "created_at_utc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "input_path": str(queue), "input_sha256": sha(queue), "script_sha256": sha(__file__),
        "selected_rows": len(selected), "audit_rows": len(rows), "missing_keys": 0, "unexpected_keys": 0,
        "category_counts": dict(collections.Counter(r["audit_category"] for r in rows)),
        "unique_query_attempts": len({q["variant_attempt_id"] for r in rows for q in json.loads(r["query_evidence_json"])}),
        "query_variant_keys_and_windows_match_frozen_targets": True,
        "saved_no_content_responses": sum(len(json.loads(r["fetch_evidence_json"])) for r in rows if r["audit_category"] == "saved_html_without_extracted_text"),
        "saved_no_content_raw_hashes_verified": all(f.get("raw_hash_verified") for r in rows for f in json.loads(r["fetch_evidence_json"]) if f["fetch_status"] == "no_content"),
        "limitations": ["Empty means no HTTP 200 HTML captures in the recorded prefix queries and dates, not no archive history.",
          "Queries used a 50000-row cap without pagination. Empty payloads do not reach this cap.",
          "Saved URL variants ignore scheme, www, and a trailing slash. They need ownership and content review.",
          "Terminal fetch states describe attempted captures, not absence of a historical campaign website.",
          "No archive requests or model labels were generated."]}
    (out/"summary.json").write_text(json.dumps(summary, indent=2)+"\n")
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
