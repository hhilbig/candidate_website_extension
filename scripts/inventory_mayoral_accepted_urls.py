"""Resumable, bounded CDX inventory for a frozen mayoral exact-URL queue.

This is a metadata-only collector. It makes requests only to the fixed CDX
endpoint and retains every returned CDX row, including duplicate captures.
"""
from __future__ import annotations

import argparse
import csv
from datetime import datetime
import fcntl
import gzip
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import time
from urllib.parse import unquote_plus, urlsplit
import uuid

import requests

ENDPOINT = "https://web.archive.org/cdx/search/cdx"
FIELDS = ("urlkey", "timestamp", "original", "mimetype", "statuscode", "digest", "length")
LIMITS = {
    "workers": 1,
    "attempts_per_page": 3,
    "page_rows": 1000,
    "max_response_decoded_bytes": 2 * 1024 * 1024,
    "max_total_decoded_bytes": 32 * 1024 * 1024,
    "min_volume_free_bytes": 8_000_000_000,
    "max_http_requests": 300,
    "request_delay_seconds": 1,
    "connect_timeout_seconds": 15,
    "read_timeout_seconds": 30,
    "response_deadline_seconds": 60,
    "circuit_failures": 5,
}
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
PARSER_VERSION = "cdx-plaintext-resume-v1"
USER_AGENT = "candidate-website-research/1.0 academic CDX metadata inventory"
BYTE_CONTRACT = "requests.iter_content decoded HTTP response bytes; not WARC bytes"


class InventoryError(Exception):
    pass


class BudgetStop(InventoryError):
    pass


def atomic_write(path: Path, data: bytes) -> None:
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)
    directory = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)


def _valid_url(value: str) -> bool:
    parsed = urlsplit(value)
    return bool(parsed.scheme in {"http", "https"} and parsed.hostname and
                not parsed.username and not parsed.password)


def _date_bound(value: str, end: bool) -> str:
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        raise ValueError(f"invalid inventory window date: {value!r}")
    compact = value.replace("-", "")
    # Validate calendar semantics, not only shape.
    datetime.strptime(compact, "%Y%m%d")
    return compact + ("235959" if end else "000000")


def query_key(row: dict[str, str]) -> str:
    identity = {
        "candidate_url_id": row["candidate_url_id"],
        "url": row["exact_normalized_url"],
        "from": _date_bound(row["proposed_window_start"], False),
        "to": _date_bound(row["proposed_window_end"], True),
        "matchType": "exact",
        "fl": ",".join(FIELDS),
    }
    return hashlib.sha256(json.dumps(identity, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def load_queue(path: Path) -> tuple[list[dict[str, str]], str]:
    payload = path.read_bytes()
    digest = hashlib.sha256(payload).hexdigest()
    rows = list(csv.DictReader(payload.decode("utf-8-sig").splitlines()))
    required = {"candidate_url_id", "candidate_year_id", "exact_normalized_url",
                "proposed_window_start", "proposed_window_end"}
    if not rows or not required.issubset(rows[0]):
        raise ValueError("queue is empty or lacks required columns")
    seen = set()
    for row in rows:
        if not _valid_url(row["exact_normalized_url"]):
            raise ValueError("queue contains invalid exact_normalized_url")
        start = _date_bound(row["proposed_window_start"], False)
        end = _date_bound(row["proposed_window_end"], True)
        key = query_key(row)
        if start > end or key in seen:
            raise ValueError("queue contains an invalid window or duplicate query")
        seen.add(key)
        row["query_key"] = key
        row["from_timestamp"] = start
        row["to_timestamp"] = end
    return rows, digest


def request_params(row: dict[str, str], resume_key: str | None) -> dict[str, str | int]:
    params: dict[str, str | int] = {
        "url": row["exact_normalized_url"],
        "matchType": "exact",
        "from": row["from_timestamp"],
        "to": row["to_timestamp"],
        "fl": ",".join(FIELDS),
        "showResumeKey": "true",
        "limit": LIMITS["page_rows"],
    }
    if resume_key is not None:
        # Store decoded keys. requests performs exactly one URL encoding pass.
        params["resumeKey"] = resume_key
    return params


def parse_cdx_plaintext(body: bytes, row: dict[str, str]) -> tuple[list[tuple[str, ...]], str | None, str | None]:
    try:
        text = body.decode("utf-8")
    except UnicodeDecodeError as exc:
        raise InventoryError(f"non-UTF-8 CDX response: {exc}") from exc
    stripped = text.lstrip()
    if stripped.startswith(("<", "{", "[")):
        raise InventoryError("CDX response is HTML or JSON, not plaintext rows")
    if text and not text.endswith(("\n", "\r")):
        raise InventoryError("nonempty CDX response lacks a final newline")
    lines = text.splitlines()
    separator = None
    for index, line in enumerate(lines):
        if not line.strip():
            separator = index
            break
    resume_raw = None
    data_lines = lines
    if separator is not None:
        data_lines = lines[:separator]
        tail = [line.strip() for line in lines[separator + 1:] if line.strip()]
        if len(tail) > 1:
            raise InventoryError("ambiguous plaintext continuation trailer")
        resume_raw = tail[0] if tail else None
    parsed_rows: list[tuple[str, ...]] = []
    for line in data_lines:
        if not line.strip():
            continue
        values = tuple(line.split())
        if len(values) != len(FIELDS):
            raise InventoryError("CDX row does not contain exactly seven fields")
        record = dict(zip(FIELDS, values))
        if (not re.fullmatch(r"\d{14}", record["timestamp"])
                or not row["from_timestamp"] <= record["timestamp"] <= row["to_timestamp"]):
            raise InventoryError("CDX timestamp is invalid or outside the requested window")
        try:
            datetime.strptime(record["timestamp"], "%Y%m%d%H%M%S")
        except ValueError as exc:
            raise InventoryError("CDX timestamp is not a calendar timestamp") from exc
        if not _valid_url(record["original"]):
            raise InventoryError("CDX original URL is invalid")
        if record["length"] != "-" and not record["length"].isdigit():
            raise InventoryError("CDX length is invalid")
        parsed_rows.append(values)
    if resume_raw is not None and not resume_raw:
        raise InventoryError("blank continuation key")
    resume_decoded = unquote_plus(resume_raw) if resume_raw is not None else None
    if resume_decoded is not None and (not resume_decoded or "\n" in resume_decoded or "\r" in resume_decoded):
        raise InventoryError("invalid continuation key")
    if len(parsed_rows) > LIMITS["page_rows"]:
        raise InventoryError("CDX response exceeds the requested row limit")
    return parsed_rows, resume_decoded, resume_raw


SCHEMA = """
CREATE TABLE IF NOT EXISTS contract (id INTEGER PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS queries (
  query_key TEXT PRIMARY KEY, queue_index INTEGER NOT NULL, candidate_url_id TEXT NOT NULL,
  candidate_year_id TEXT NOT NULL, requested_url TEXT NOT NULL, from_timestamp TEXT NOT NULL,
  to_timestamp TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'PENDING', page_index INTEGER NOT NULL DEFAULT 0,
  resume_key TEXT, page_attempts INTEGER NOT NULL DEFAULT 0, total_attempts INTEGER NOT NULL DEFAULT 0,
  next_due REAL NOT NULL DEFAULT 0, accepted_rows INTEGER NOT NULL DEFAULT 0, result TEXT);
CREATE TABLE IF NOT EXISTS attempts (
  attempt_id TEXT PRIMARY KEY, query_key TEXT NOT NULL, page_index INTEGER NOT NULL,
  resume_key TEXT, started REAL NOT NULL, finished REAL, status TEXT NOT NULL,
  http_status INTEGER, headers TEXT, error TEXT, raw_path TEXT, raw_sha256 TEXT,
  decoded_bytes INTEGER, charged_bytes INTEGER NOT NULL, params TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS pages (
  page_id INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL UNIQUE, query_key TEXT NOT NULL,
  page_index INTEGER NOT NULL, input_resume_key TEXT, output_resume_key TEXT, output_resume_key_raw TEXT,
  row_count INTEGER NOT NULL, accepted INTEGER NOT NULL, status TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS capture_rows (
  row_id INTEGER PRIMARY KEY AUTOINCREMENT, attempt_id TEXT NOT NULL, query_key TEXT NOT NULL,
  page_index INTEGER NOT NULL, row_index INTEGER NOT NULL, accepted INTEGER NOT NULL,
  urlkey TEXT NOT NULL, timestamp TEXT NOT NULL, original TEXT NOT NULL, mimetype TEXT NOT NULL,
  statuscode TEXT NOT NULL, digest TEXT NOT NULL, length TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS resume_keys (
  query_key TEXT NOT NULL, resume_key TEXT NOT NULL, first_attempt_id TEXT NOT NULL,
  PRIMARY KEY(query_key, resume_key));
CREATE TABLE IF NOT EXISTS control (
  id INTEGER PRIMARY KEY, consecutive_failures INTEGER NOT NULL, next_request REAL NOT NULL,
  circuit_until REAL NOT NULL);
"""


def open_ledger(path: Path, rows: list[dict[str, str]], queue_hash: str) -> sqlite3.Connection:
    db = sqlite3.connect(path)
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA synchronous=FULL")
    db.executescript(SCHEMA)
    contract = json.dumps({"queue_sha256": queue_hash, "endpoint": ENDPOINT, "fields": FIELDS,
                           "limits": LIMITS, "parser_version": PARSER_VERSION,
                           "byte_contract": BYTE_CONTRACT, "redirects": False}, sort_keys=True)
    old = db.execute("SELECT value FROM contract WHERE id=1").fetchone()
    if old and old[0] != contract:
        db.close()
        raise ValueError("frozen input hash or inventory contract differs")
    with db:
        db.execute("INSERT OR IGNORE INTO contract VALUES (1, ?)", (contract,))
        db.execute("INSERT OR IGNORE INTO control VALUES (1, 0, 0, 0)")
        db.executemany("""INSERT OR IGNORE INTO queries
          (query_key,queue_index,candidate_url_id,candidate_year_id,requested_url,from_timestamp,to_timestamp)
          VALUES (?,?,?,?,?,?,?)""", [(r["query_key"], i, r["candidate_url_id"], r["candidate_year_id"],
          r["exact_normalized_url"], r["from_timestamp"], r["to_timestamp"]) for i, r in enumerate(rows)])
        expected = {r["query_key"] for r in rows}
        observed = {r[0] for r in db.execute("SELECT query_key FROM queries")}
        if expected != observed:
            raise ValueError("unexpected or missing query keys in resume ledger")
        interrupted = list(db.execute("SELECT attempt_id,query_key FROM attempts WHERE status='STARTED'"))
        now = time.time()
        for item in interrupted:
            db.execute("UPDATE attempts SET status='INTERRUPTED',finished=?,error=? WHERE attempt_id=?",
                       (now, "process ended before attempt commit", item["attempt_id"]))
            db.execute("UPDATE queries SET status='RETRYABLE',next_due=? WHERE query_key=?",
                       (now + 30, item["query_key"]))
        if interrupted:
            previous = db.execute(
                "SELECT consecutive_failures FROM control WHERE id=1").fetchone()[0]
            failures = previous + len(interrupted)
            db.execute("UPDATE control SET consecutive_failures=?,circuit_until=? WHERE id=1",
                       (failures, now + 300 if failures >= LIMITS["circuit_failures"] else 0))
    return db


def _stream_response(response, ceiling: int) -> tuple[bytes, bool]:
    body = bytearray()
    deadline = time.monotonic() + LIMITS["response_deadline_seconds"]
    chunk_size = min(8192, ceiling)
    chunks = iter(response.iter_content(chunk_size=chunk_size))
    while True:
        if time.monotonic() > deadline:
            raise requests.Timeout("response elapsed deadline exceeded")
        # Do not request a chunk which cannot fit in the durable byte budget.
        if ceiling - len(body) < chunk_size:
            return bytes(body), True
        try:
            chunk = next(chunks)
        except StopIteration:
            return bytes(body), False
        if not chunk:
            continue
        if len(chunk) > ceiling - len(body):
            return bytes(body), True
        body.extend(chunk)
        if len(body) == ceiling:
            return bytes(body), True


def _wait_for_slot(db: sqlite3.Connection, sleep) -> None:
    while True:
        wait = db.execute("SELECT next_request FROM control WHERE id=1").fetchone()[0] - time.time()
        if wait <= 0:
            break
        sleep(min(wait, 1))
    with db:
        db.execute("UPDATE control SET next_request=? WHERE id=1",
                   (time.time() + LIMITS["request_delay_seconds"],))


def execute_attempt(db, session, query, source_row, attempt_id, output: Path, sleep=time.sleep):
    params = request_params(source_row, query["resume_key"])
    _wait_for_slot(db, sleep)
    response = session.get(ENDPOINT, params=params, stream=True, allow_redirects=False,
                           timeout=(LIMITS["connect_timeout_seconds"], LIMITS["read_timeout_seconds"]))
    try:
        body, oversized = _stream_response(response, LIMITS["max_response_decoded_bytes"])
        status_code = response.status_code
        headers = dict(response.headers)
    finally:
        response.close()
    raw_relative = f"raw/{attempt_id}.txt.gz"
    atomic_write(output / raw_relative, gzip.compress(body, mtime=0))
    raw_hash = hashlib.sha256(body).hexdigest()
    common = {"http_status": status_code, "headers": json.dumps(headers, sort_keys=True),
              "raw_path": raw_relative, "raw_sha256": raw_hash, "decoded_bytes": len(body)}
    if oversized:
        common["charged_bytes"] = LIMITS["max_response_decoded_bytes"]
        return "RETRYABLE", "response reached decoded-byte ceiling", [], None, None, common
    content_type = headers.get("Content-Type", headers.get("content-type", "")).lower()
    if status_code in RETRYABLE_HTTP or status_code >= 500:
        return "RETRYABLE", f"retryable HTTP {status_code}", [], None, None, common
    if status_code != 200:
        return "RETRYABLE", f"unexpected HTTP {status_code}", [], None, None, common
    if content_type and not any(x in content_type for x in ("text/plain", "application/octet-stream")):
        return "RETRYABLE", f"unexpected content type {content_type}", [], None, None, common
    try:
        rows, resume_key, resume_raw = parse_cdx_plaintext(body, source_row)
    except InventoryError as exc:
        return "RETRYABLE", str(exc), [], None, None, common
    if resume_key is not None:
        prior = db.execute("SELECT 1 FROM resume_keys WHERE query_key=? AND resume_key=?",
                           (query["query_key"], resume_key)).fetchone()
        if resume_key == query["resume_key"] or prior:
            return "RETRYABLE", "continuation-key loop detected", rows, resume_key, resume_raw, common
        return "CONTINUE", None, rows, resume_key, resume_raw, common
    if len(rows) >= LIMITS["page_rows"]:
        return "RETRYABLE", "full-limit response omitted continuation key", rows, None, None, common
    terminal = "COMPLETE" if query["accepted_rows"] + len(rows) else "EMPTY"
    return terminal, None, rows, None, None, common


def _record_result(db, query, attempt_id, outcome, error, rows, resume_key, resume_raw, common):
    accepted = outcome in {"CONTINUE", "COMPLETE", "EMPTY"}
    with db:
        db.execute("""UPDATE attempts SET finished=?,status=?,http_status=?,headers=?,error=?,raw_path=?,
          raw_sha256=?,decoded_bytes=?,charged_bytes=? WHERE attempt_id=?""",
          (time.time(), outcome, common.get("http_status"), common.get("headers"), error,
           common.get("raw_path"), common.get("raw_sha256"), common.get("decoded_bytes"),
           common.get("charged_bytes", common.get("decoded_bytes", LIMITS["max_response_decoded_bytes"])),
           attempt_id))
        if common.get("raw_path") is not None:
            db.execute("""INSERT INTO pages(attempt_id,query_key,page_index,input_resume_key,
              output_resume_key,output_resume_key_raw,row_count,accepted,status) VALUES (?,?,?,?,?,?,?,?,?)""",
              (attempt_id, query["query_key"], query["page_index"], query["resume_key"], resume_key,
               resume_raw, len(rows), int(accepted), outcome))
            db.executemany("""INSERT INTO capture_rows(attempt_id,query_key,page_index,row_index,accepted,
              urlkey,timestamp,original,mimetype,statuscode,digest,length) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
              [(attempt_id, query["query_key"], query["page_index"], index, int(accepted), *values)
               for index, values in enumerate(rows)])
        if outcome == "CONTINUE":
            db.execute("INSERT INTO resume_keys VALUES (?,?,?)", (query["query_key"], resume_key, attempt_id))
            db.execute("""UPDATE queries SET status='PENDING',page_index=page_index+1,resume_key=?,
              page_attempts=0,next_due=0,accepted_rows=accepted_rows+?,result=NULL WHERE query_key=?""",
              (resume_key, len(rows), query["query_key"]))
        elif outcome in {"COMPLETE", "EMPTY"}:
            db.execute("""UPDATE queries SET status=?,next_due=0,accepted_rows=accepted_rows+?,result=?
              WHERE query_key=?""", (outcome, len(rows), json.dumps({"last_attempt_id": attempt_id,
              "last_page_rows": len(rows)}), query["query_key"]))
        else:
            delay = min(60, 30 * (2 ** max(0, query["page_attempts"] - 1)))
            db.execute("UPDATE queries SET status='RETRYABLE',next_due=?,result=? WHERE query_key=?",
                       (time.time() + delay, json.dumps({"error": error, "attempt_id": attempt_id}),
                        query["query_key"]))
        control = db.execute("SELECT consecutive_failures FROM control WHERE id=1").fetchone()[0]
        failures = control + 1 if outcome == "RETRYABLE" else 0
        circuit_until = time.time() + 300 if failures >= LIMITS["circuit_failures"] else 0
        db.execute("UPDATE control SET consecutive_failures=?,circuit_until=? WHERE id=1",
                   (failures, circuit_until))


def audit(db, rows, queue_hash, output: Path, stop_reason: str):
    queries = [dict(r) for r in db.execute("SELECT * FROM queries ORDER BY queue_index")]
    expected = {r["query_key"] for r in rows}
    observed = {r["query_key"] for r in queries}
    statuses: dict[str, int] = {}
    for row in queries:
        statuses[row["status"]] = statuses.get(row["status"], 0) + 1
    attempts = db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
    pages = db.execute("SELECT COUNT(*) FROM pages").fetchone()[0]
    capture_rows = db.execute("SELECT COUNT(*) FROM capture_rows").fetchone()[0]
    accepted_rows = db.execute("SELECT COUNT(*) FROM capture_rows WHERE accepted=1").fetchone()[0]
    duplicate_rows = db.execute("""SELECT COALESCE(SUM(n-1),0) FROM
      (SELECT COUNT(*) n FROM capture_rows WHERE accepted=1 GROUP BY query_key,urlkey,timestamp,original,
       mimetype,statuscode,digest,length HAVING COUNT(*)>1)""").fetchone()[0]
    page_statuses = {r[0]: r[1] for r in db.execute(
        "SELECT status,COUNT(*) FROM pages GROUP BY status ORDER BY status")}
    page_chain_issues = []
    for query in queries:
        accepted_pages = [r[0] for r in db.execute(
            "SELECT page_index FROM pages WHERE query_key=? AND accepted=1 ORDER BY page_index",
            (query["query_key"],))]
        terminal = query["status"] in {"COMPLETE", "EMPTY"}
        expected_pages = list(range(query["page_index"] + (1 if terminal else 0)))
        if accepted_pages != expected_pages:
            page_chain_issues.append({"query_key": query["query_key"],
                                      "expected_page_indexes": expected_pages,
                                      "accepted_page_indexes": accepted_pages})
    decoded = db.execute("SELECT COALESCE(SUM(decoded_bytes),0) FROM attempts").fetchone()[0]
    charged = db.execute("SELECT COALESCE(SUM(charged_bytes),0) FROM attempts").fetchone()[0]
    result = {"queue_sha256": queue_hash, "expected_query_keys": len(expected),
              "requested_endpoint": ENDPOINT, "redirects_allowed": False,
              "client_decoding_contract": BYTE_CONTRACT,
              "observed_query_keys": len(observed), "missing_keys": sorted(expected-observed),
              "unexpected_keys": sorted(observed-expected), "statuses": statuses,
              "attempts": attempts, "duplicate_attempt_ids": 0, "pages": pages,
              "page_statuses": page_statuses, "page_chain_issues": page_chain_issues,
              "capture_rows_all_attempts": capture_rows, "capture_rows_accepted": accepted_rows,
              "accepted_duplicate_rows_retained": duplicate_rows, "decoded_bytes": decoded,
              "charged_decoded_bytes": charged, "stored_gzip_bytes": sum(p.stat().st_size for p in
              (output/"raw").glob("*.gz")), "free_bytes": shutil.disk_usage(output).free,
              "stop_reason": stop_reason, "inventory_complete": bool(queries) and
              all(q["status"] in {"COMPLETE", "EMPTY"} for q in queries),
              "last_update": time.time(), "queries": queries}
    atomic_write(output/"audit.json", json.dumps(result, indent=2, sort_keys=True).encode())
    compact = b"".join((json.dumps({k: q[k] for k in ("query_key", "candidate_url_id", "requested_url",
              "from_timestamp", "to_timestamp", "status", "page_index", "page_attempts", "total_attempts",
              "accepted_rows", "result")}, sort_keys=True) + "\n").encode() for q in queries)
    atomic_write(output/"query_status.jsonl", compact)
    return result


def run_inventory(queue, output_dir, max_new_attempts=None, session=None, sleep=time.sleep):
    rows, queue_hash = load_queue(Path(queue))
    if max_new_attempts is not None and max_new_attempts < 0:
        raise ValueError("max-new-attempts must be nonnegative")
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    (output/"raw").mkdir(exist_ok=True)
    with (output/"writer.lock").open("a") as lock:
        fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        db = open_ledger(output/"ledger.sqlite3", rows, queue_hash)
        own_session = session is None
        session = session or requests.Session()
        if own_session:
            session.trust_env = False
            session.headers["User-Agent"] = USER_AGENT
        lookup = {r["query_key"]: r for r in rows}
        started = 0
        stop = "attempt_limit"
        try:
            while max_new_attempts is None or started < max_new_attempts:
                control = db.execute("SELECT * FROM control WHERE id=1").fetchone()
                if control["circuit_until"] > time.time():
                    stop = "circuit_open"
                    break
                if control["consecutive_failures"] >= LIMITS["circuit_failures"]:
                    with db:
                        db.execute("UPDATE control SET consecutive_failures=0,circuit_until=0 WHERE id=1")
                request_count = db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]
                charged = db.execute("SELECT COALESCE(SUM(charged_bytes),0) FROM attempts").fetchone()[0]
                if request_count >= LIMITS["max_http_requests"]:
                    stop = "http_request_budget"
                    break
                if charged + LIMITS["max_response_decoded_bytes"] > LIMITS["max_total_decoded_bytes"]:
                    stop = "decoded_budget"
                    break
                if shutil.disk_usage(output).free < LIMITS["min_volume_free_bytes"]:
                    stop = "storage_headroom"
                    break
                now = time.time()
                query = db.execute("""SELECT * FROM queries WHERE status IN ('PENDING','RETRYABLE')
                  AND page_attempts < ? AND next_due <= ? ORDER BY total_attempts,queue_index LIMIT 1""",
                  (LIMITS["attempts_per_page"], now)).fetchone()
                if query is None:
                    due = db.execute("""SELECT MIN(next_due) FROM queries WHERE status IN
                      ('PENDING','RETRYABLE') AND page_attempts < ?""",
                      (LIMITS["attempts_per_page"],)).fetchone()[0]
                    if due is not None:
                        audit(db, rows, queue_hash, output, "retry_backoff")
                        sleep(min(30, max(0, due-time.time())))
                        continue
                    stop = "queue_exhausted"
                    break
                attempt_id = uuid.uuid4().hex
                params = request_params(lookup[query["query_key"]], query["resume_key"])
                with db:
                    db.execute("""INSERT INTO attempts(attempt_id,query_key,page_index,resume_key,started,
                      status,charged_bytes,params) VALUES (?,?,?,?,?,'STARTED',?,?)""",
                      (attempt_id, query["query_key"], query["page_index"], query["resume_key"], now,
                       LIMITS["max_response_decoded_bytes"], json.dumps(params, sort_keys=True)))
                    db.execute("""UPDATE queries SET status='STARTED',page_attempts=page_attempts+1,
                      total_attempts=total_attempts+1 WHERE query_key=?""", (query["query_key"],))
                query = db.execute("SELECT * FROM queries WHERE query_key=?", (query["query_key"],)).fetchone()
                started += 1
                audit(db, rows, queue_hash, output, "running")
                try:
                    outcome, error, found, resume, resume_raw, common = execute_attempt(
                        db, session, query, lookup[query["query_key"]], attempt_id, output, sleep)
                except KeyboardInterrupt:
                    raise
                except Exception as exc:
                    outcome, error, found, resume, resume_raw, common = (
                        "RETRYABLE", f"{type(exc).__name__}: {exc}", [], None, None, {})
                _record_result(db, query, attempt_id, outcome, error, found, resume, resume_raw, common)
                audit(db, rows, queue_hash, output, "running")
            return audit(db, rows, queue_hash, output, stop)
        finally:
            db.close()
            if own_session:
                session.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--queue", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--max-new-attempts", type=int)
    args = parser.parse_args()
    result = run_inventory(args.queue, args.output_dir, args.max_new_attempts)
    print(json.dumps({k: v for k, v in result.items() if k != "queries"}, indent=2))


if __name__ == "__main__":
    main()
