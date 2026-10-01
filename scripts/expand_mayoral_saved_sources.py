#!/usr/bin/env python3
"""Expand saved mayoral search sources without issuing paid search requests."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import ipaddress
import json
import logging
import os
import re
import shutil
import socket
import sqlite3
import sys
import tempfile
import threading
import time
import uuid
from collections import Counter, defaultdict, deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import urljoin, urlsplit, urlunsplit

import requests

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

DEFAULT_RUN = Path("data/municipal_discovery/exhaustive_source_expansion_20260908")
DEFAULT_MANIFEST = Path("config/mayoral_saved_source_inputs_20260908.json")
VERSION = "mayoral-saved-sources-v2"
LOG = logging.getLogger(__name__)
ASSETS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".svg", ".mp3", ".mp4", ".zip", ".css", ".woff", ".woff2"}
RETRY_HTTP = {408, 425, 429, 500, 502, 503, 504}


def now():
    return datetime.now(timezone.utc).isoformat()


def sid(prefix, *parts):
    return prefix + "_" + hashlib.sha256("\x1f".join(map(str, parts)).encode()).hexdigest()[:24]


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8-sig") as handle:
        return list(csv.DictReader(handle))


def truth(value):
    return str(value).lower() in {"true", "1", "yes"}


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def atomic_bytes(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    temporary.replace(path)


def atomic_json(path, value):
    atomic_bytes(path, (json.dumps(value, indent=2, sort_keys=True, default=str) + "\n").encode())


def atomic_csv(path, rows, columns=None):
    if columns is None:
        rows = list(rows)
        columns = list(dict.fromkeys(k for r in rows for k in r))
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", newline="", encoding="utf-8", dir=path.parent, delete=False) as h:
        temporary = Path(h.name)
        writer = csv.DictWriter(h, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
        h.flush()
        os.fsync(h.fileno())
    temporary.replace(path)


def source_url(value):
    """Normalize transport URLs while retaining archive wrappers and exact paths."""
    value = str(value or "").strip()
    if value.startswith("//"):
        value = "https:" + value
    if value.startswith("www."):
        value = "https://" + value
    try:
        p = urlsplit(value)
        if p.scheme.lower() not in {"http", "https"} or not p.hostname or p.username or p.password:
            return ""
        port = p.port
        hostname = p.hostname.lower()
        netloc = f"[{hostname}]" if ":" in hostname else hostname
        if port and (p.scheme.lower(), port) not in {("http", 80), ("https", 443)}:
            netloc += ":" + str(port)
        return urlunsplit((p.scheme.lower(), netloc, p.path or "/", p.query, ""))
    except ValueError:
        return ""


def disposition(url):
    if not url:
        return "invalid_url"
    p = urlsplit(url)
    if p.hostname == "vote411.org" or p.hostname.endswith(".vote411.org"):
        return "permission_required"
    if (p.hostname in {"google.com", "www.google.com", "bing.com", "www.bing.com"} and p.path.startswith("/search")) or (p.hostname.endswith("duckduckgo.com") and "q=" in p.query):
        return "search_endpoint_outside_source_expansion"
    if Path(p.path).suffix.lower() in ASSETS:
        return "non_document_asset"
    if p.hostname in {"localhost", "localhost.localdomain"} or p.hostname.endswith((".local", ".internal")):
        return "non_public_url"
    try:
        if not ipaddress.ip_address(p.hostname).is_global:
            return "non_public_url"
    except ValueError:
        pass
    if p.port not in {None, 80, 443}:
        return "unsupported_port"
    return "pending"


def assert_public(url, resolver=socket.getaddrinfo):
    if disposition(url) != "pending":
        raise ValueError("Non-public or unsupported source URL")
    p = urlsplit(url)
    addresses = resolver(p.hostname, p.port or (443 if p.scheme == "https" else 80), type=socket.SOCK_STREAM)
    if not addresses or any(not ipaddress.ip_address(a[4][0]).is_global for a in addresses):
        raise ValueError("Source resolves to a non-public address")


def connect(run):
    run = Path(run)
    run.mkdir(parents=True, exist_ok=True)
    db = sqlite3.connect(run / "sources.sqlite")
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.executescript("""
        CREATE TABLE IF NOT EXISTS sources (
          source_id TEXT PRIMARY KEY, url TEXT NOT NULL UNIQUE, host TEXT NOT NULL,
          title TEXT NOT NULL DEFAULT '', priority INTEGER NOT NULL, depth INTEGER NOT NULL,
          disposition TEXT NOT NULL, cache_path TEXT NOT NULL DEFAULT '', cache_content_type TEXT NOT NULL DEFAULT '',
          cache_sha256 TEXT NOT NULL DEFAULT '', created_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS contexts (
          relation_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, candidate_year_id TEXT NOT NULL,
          origin TEXT NOT NULL, input_path TEXT NOT NULL, input_row INTEGER NOT NULL, title TEXT NOT NULL,
          snippet TEXT NOT NULL, raw_url TEXT NOT NULL, parent_source_id TEXT NOT NULL DEFAULT '');
        CREATE INDEX IF NOT EXISTS context_source ON contexts(source_id);
        CREATE INDEX IF NOT EXISTS context_parent ON contexts(parent_source_id,origin);
        CREATE INDEX IF NOT EXISTS source_host ON sources(host);
        CREATE TABLE IF NOT EXISTS candidates (candidate_year_id TEXT PRIMARY KEY, payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS attempts (
          attempt_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, started_at TEXT NOT NULL,
          ended_at TEXT, status TEXT NOT NULL, retryable INTEGER NOT NULL, result_json TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS attempts_source ON attempts(source_id);
        CREATE TABLE IF NOT EXISTS current (
          source_id TEXT PRIMARY KEY, attempt_id TEXT NOT NULL, status TEXT NOT NULL,
          retryable INTEGER NOT NULL, attempts INTEGER NOT NULL, result_json TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS claims (
          claim_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, candidate_year_id TEXT NOT NULL,
          exact_url TEXT NOT NULL, attribution_status TEXT NOT NULL, payload TEXT NOT NULL);
        CREATE INDEX IF NOT EXISTS claim_source ON claims(source_id);
        CREATE INDEX IF NOT EXISTS claim_candidate ON claims(candidate_year_id);
        CREATE TABLE IF NOT EXISTS deferred_links (
          link_id TEXT PRIMARY KEY, source_id TEXT NOT NULL, candidate_year_id TEXT NOT NULL,
          url TEXT NOT NULL, reason TEXT NOT NULL, depth INTEGER NOT NULL);
        CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS review_decisions (
          candidate_year_id TEXT NOT NULL, url_identity TEXT NOT NULL, decision TEXT NOT NULL,
          PRIMARY KEY(candidate_year_id,url_identity));
        CREATE TABLE IF NOT EXISTS retired_contexts (
          relation_id TEXT NOT NULL, retired_at TEXT NOT NULL, reason TEXT NOT NULL,
          payload TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS host_restrictions (
          host TEXT PRIMARY KEY, reason TEXT NOT NULL, evidence_sources TEXT NOT NULL, observed_at TEXT NOT NULL);
    """)
    if "cache_effective_url" not in {r[1] for r in db.execute("PRAGMA table_info(sources)")}:
        db.execute("ALTER TABLE sources ADD COLUMN cache_effective_url TEXT NOT NULL DEFAULT ''")
    return db


def add_source(db, *, url, candidate_id, origin, input_path, input_row, title="", snippet="", priority=50,
               depth=0, parent_id="", cache=None):
    exact = source_url(url)
    source_id = sid("source", exact or "invalid:" + str(url))
    state = disposition(exact)
    host = urlsplit(exact).hostname or "" if exact else ""
    if state == "pending" and db.execute("SELECT 1 FROM host_restrictions WHERE host=?",(host,)).fetchone():
        state = "host_deferred_access"
    db.execute("""INSERT INTO sources(source_id,url,host,title,priority,depth,disposition,created_at)
        VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(source_id) DO UPDATE SET
        priority=MIN(priority,excluded.priority), depth=MIN(depth,excluded.depth)""",
        (source_id, exact or "invalid:" + str(url), host, title, priority, depth, state, now()))
    db.execute("UPDATE sources SET disposition=? WHERE source_id=? AND disposition='stale_followup'", (state, source_id))
    relation = sid("relation", source_id, candidate_id, origin, input_path, input_row, parent_id)
    db.execute("INSERT OR IGNORE INTO contexts VALUES(?,?,?,?,?,?,?,?,?,?)",
               (relation, source_id, candidate_id, origin, input_path, input_row, title, snippet, str(url), parent_id))
    if cache:
        db.execute("UPDATE sources SET cache_path=?,cache_content_type=?,cache_sha256=? WHERE source_id=?",
                   (cache["path"], cache.get("content_type", ""), cache.get("sha256", ""), source_id))
    return source_id


def prepare(root, run, manifest_path):
    root, run = Path(root), Path(run)
    spec = json.loads((root / manifest_path).read_text())
    roster_path = root / spec["roster"]
    roster = read_csv(roster_path)
    if len({r["candidate_year_id"] for r in roster}) != len(roster):
        raise ValueError("Duplicate candidate-year keys")
    rows_by_id = {r["candidate_year_id"]: r for r in roster}
    active = {k for k, r in rows_by_id.items() if 2010 <= int(float(r["year"])) <= 2021}
    input_audit = [{"path": spec["roster"], "sha256": sha(roster_path), "rows": len(roster)}]
    db = connect(run)
    if db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0]:
        raise ValueError("Preparation cannot replace an attempted queue. Resume the existing run.")
    db.execute("DELETE FROM sources")
    db.execute("DELETE FROM contexts")
    db.execute("DELETE FROM candidates")
    for key, row in rows_by_id.items():
        db.execute("INSERT INTO candidates VALUES(?,?)", (key, json.dumps(row)))
    exclusions = []
    for item in spec["sources"]:
        path = root / item["path"]
        if not path.is_file():
            raise FileNotFoundError(path)
        records = read_csv(path)
        input_audit.append({"path": item["path"], "sha256": sha(path), "rows": len(records), "kind": item["kind"]})
        used = 0
        for number, row in enumerate(records, 2):
            cid = row.get(item.get("candidate_column", "candidate_year_id"), "")
            if cid not in rows_by_id:
                if cid:
                    exclusions.append({"path": item["path"], "row": number, "candidate_year_id": cid, "reason": "unknown_candidate_key"})
                continue
            if cid not in active:
                exclusions.append({"path": item["path"], "row": number, "candidate_year_id": cid, "reason": "outside_2010_2021"})
                continue
            columns = item.get("url_columns", ["raw_url"])
            urls = []
            for column in columns:
                value = row.get(column, "")
                if column.endswith("_json") and value:
                    try:
                        entries = json.loads(value)
                        urls.extend(x for x in entries if isinstance(x, str))
                    except (ValueError, TypeError):
                        exclusions.append({"path": item["path"], "row": number, "candidate_year_id": cid, "reason": "malformed_url_json"})
                elif value:
                    urls.append(value)
            for url in dict.fromkeys(urls):
                title = row.get("result_title", row.get("title", row.get("source_name", "")))
                priority = 25 if "credible_source" in row.get("result_classification", "") else 50
                if rows_by_id[cid].get("has_url_lead") == "False":
                    priority -= 10
                if re.search(r"ballotpedia|\.gov/|candidate|voter|election|questionnaire", url, re.I):
                    priority -= 5
                add_source(db, url=url, candidate_id=cid, origin=item["kind"], input_path=item["path"],
                           input_row=number, title=title, snippet=row.get("result_snippet", row.get("evidence_text", "")), priority=priority)
                used += 1
        input_audit[-1]["active_relations_added"] = used
    for item in spec.get("caches", []):
        path = root / item["path"]
        records = read_csv(path)
        input_audit.append({"path": item["path"], "sha256": sha(path), "rows": len(records), "kind": "saved_document_cache"})
        for row in records:
            rel = row.get(item.get("file_column", "local_file_path"), "")
            if not rel:
                continue
            cached = root / item.get("base_dir", str(Path(item["path"]).parent)) / rel
            if not cached.is_file():
                exclusions.append({"path": item["path"], "row": "", "candidate_year_id": "", "reason": "cache_file_missing", "file": str(cached)})
                continue
            wanted_hash = row.get(item.get("hash_column", "content_sha256"), "")
            observed = sha(cached)
            if wanted_hash and wanted_hash != observed:
                raise ValueError(f"Saved document hash mismatch: {cached}")
            urls = [source_url(row.get(c, "")) for c in item.get("url_columns", ["page_url", "final_url"])]
            content_type = row.get("content_type", "")
            for url in filter(None, urls):
                db.execute("UPDATE sources SET cache_path=?,cache_content_type=?,cache_sha256=?,cache_effective_url=?,priority=MIN(priority,0) WHERE url=?",
                           (str(cached.relative_to(root)), content_type, observed, source_url(row.get("final_url", "")) or url, url))
    db.execute("INSERT OR REPLACE INTO meta VALUES('version',?)", (VERSION,))
    db.execute("INSERT OR REPLACE INTO meta VALUES('prepared_at',?)", (now(),))
    db.commit()
    states = dict(db.execute("SELECT disposition,COUNT(*) FROM sources GROUP BY disposition").fetchall())
    counts = {"sources": db.execute("SELECT COUNT(*) FROM sources").fetchone()[0],
              "source_candidate_relations": db.execute("SELECT COUNT(*) FROM contexts").fetchone()[0],
              "cached_sources": db.execute("SELECT COUNT(*) FROM sources WHERE cache_path<>''").fetchone()[0],
              "candidate_years_with_source": db.execute("SELECT COUNT(DISTINCT candidate_year_id) FROM contexts").fetchone()[0]}
    audit = {"version": VERSION, "stage": "prepared", "universe_rows": len(roster), "active_candidate_years": len(active),
             **counts, "source_dispositions": states, "inputs": input_audit,
             "exclusion_counts": dict(Counter(r["reason"] for r in exclusions)),
             "large_drop_rule": "Full universe retained. Active years and source dispositions are separately recorded.",
             "paid_requests": 0}
    audit["scope_filters"] = [{"before": len(roster), "after": len(active), "reason": "Approved active years; full roster retained", "drop_fraction": 1-len(active)/len(roster)},
                              {"before": counts["source_candidate_relations"], "after": counts["sources"], "reason": "Deduplicate fetch URLs; retain all source-candidate relations", "drop_fraction": 1-counts["sources"]/counts["source_candidate_relations"]}]
    atomic_json(run / "input_manifest.json", audit)
    atomic_csv(run / "input_dispositions.csv", exclusions, ["path", "row", "candidate_year_id", "reason", "file"])
    atomic_csv(run / "source_queue.csv", [dict(r) for r in db.execute("SELECT * FROM sources ORDER BY priority,source_id")])
    atomic_csv(run / "source_contexts.csv", [dict(r) for r in db.execute("SELECT * FROM contexts ORDER BY relation_id")])
    db.close()
    return audit


def document_candidates(db, source_id, roster, contests):
    ids = {r[0] for r in db.execute("SELECT DISTINCT candidate_year_id FROM contexts WHERE source_id=?", (source_id,))}
    relevant = set(ids)
    for key in ids:
        c = roster[key]
        contest = c.get("contest") or c.get("contest_id")
        if contest:
            relevant.update(contests.get(contest, []))
    return [roster[key] for key in sorted(relevant)]


def retry_seconds(value):
    try:
        return max(0., float(value))
    except (TypeError, ValueError):
        try:
            return max(0., parsedate_to_datetime(value).timestamp() - time.time())
        except (TypeError, ValueError, OverflowError):
            return 0.


class HostLimiter:
    """Limit actual request hosts, including redirects to shared destinations."""
    def __init__(self, delay):
        self.delay, self.lock = delay, threading.Lock()
        self.active, self.ready = set(), defaultdict(float)

    def acquire(self, host):
        while True:
            with self.lock:
                if host not in self.active and time.monotonic() >= self.ready[host]:
                    self.active.add(host)
                    return
            time.sleep(.1)

    def release(self, host, retry_after=0):
        with self.lock:
            self.active.discard(host)
            self.ready[host] = time.monotonic() + max(self.delay, retry_after)


def fetch_document(source, root, run, *, timeout=20, max_bytes=25_000_000, ocr=True, session=None, limiter=None):
    from src.mayoral_source_expansion import parse_document
    start = time.monotonic()
    result = {"source_id": source["source_id"], "requested_url": source["url"], "effective_url": source["url"],
              "status": "request_error", "retryable": True, "http_status": "", "content_type": "",
              "content_bytes": 0, "raw_path": "", "raw_sha256": "", "text_path": "", "parsed_path": "",
              "request_count": 0, "downloaded_bytes": 0, "used_cache": False, "error": "", "started_at": now()}
    content = None
    response = None
    locked_host = ""
    own_session = session is None
    if session is None:
        session = requests.Session()
        session.trust_env = False
    try:
        if source.get("cache_path"):
            path = Path(root) / source["cache_path"]
            if path.is_file():
                content = path.read_bytes()
                if source.get("cache_sha256") and hashlib.sha256(content).hexdigest() != source["cache_sha256"]:
                    raise ValueError("Cached document hash mismatch")
                result.update(used_cache=True, content_type=source.get("cache_content_type", ""), http_status=200,
                              effective_url=source.get("cache_effective_url") or source["url"])
        if content is None:
            url = source["url"]
            redirects = []
            for hop in range(6):
                assert_public(url)
                if limiter:
                    locked_host = urlsplit(url).hostname
                    limiter.acquire(locked_host)
                response = session.get(url, allow_redirects=False, stream=True, timeout=(min(timeout, 10), timeout),
                                       headers={"User-Agent": "CandidateWebsiteExtension/1.0 (academic research; public source discovery)"})
                result["request_count"] += 1
                result.update(http_status=response.status_code, effective_url=url,
                              content_type=response.headers.get("Content-Type", ""))
                if response.status_code in {301, 302, 303, 307, 308} and response.headers.get("Location"):
                    target = source_url(urljoin(url, response.headers["Location"]))
                    redirects.append({"from": url, "to": target, "status": response.status_code})
                    response.close()
                    if limiter:
                        limiter.release(locked_host)
                        locked_host = ""
                    url = target
                    continue
                break
            else:
                raise ValueError("Redirect limit reached")
            result["redirect_history"] = redirects
            if response.status_code >= 400:
                result.update(status="http_error", retryable=response.status_code in RETRY_HTTP,
                              error=f"HTTP {response.status_code}", retry_after=response.headers.get("Retry-After", ""),
                              next_retry_at=time.time()+max(15, retry_seconds(response.headers.get("Retry-After", ""))))
                return result, None
            parts, size = [], 0
            for chunk in response.iter_content(65536):
                size += len(chunk)
                result["downloaded_bytes"] = size
                if size > max_bytes:
                    result.update(status="size_limit", retryable=False, error=f"Document exceeds {max_bytes} bytes")
                    return result, None
                if time.monotonic() - start > max(45, timeout * 3):
                    raise requests.Timeout("Total document download deadline exceeded")
                parts.append(chunk)
            content = b"".join(parts)
        if len(content) > max_bytes:
            result.update(status="size_limit", retryable=False, error=f"Cached document exceeds {max_bytes} bytes")
            return result, None
        digest = hashlib.sha256(content).hexdigest()
        raw_rel = f"raw/{digest}.gz"
        if not (Path(run) / raw_rel).exists():
            atomic_bytes(Path(run) / raw_rel, gzip.compress(content, mtime=0))
        result.update(raw_path=raw_rel, raw_sha256=digest, content_bytes=len(content))
        if not content:
            result.update(status="source_unavailable",retryable=False,error=f"Empty HTTP {result['http_status']} response; no source document received")
            return result,None
        parsed = parse_document(content, result["content_type"], result["effective_url"], ocr=ocr)
        parsed_rel, text_rel = save_parsed(run, parsed)
        parse_status = parsed.get("parse_status", "parsed")
        status = parsed_status(parse_status)
        result.update(status=status, retryable=False, parse_status=parse_status, parsed_path=parsed_rel, text_path=text_rel,
                      text_characters=len(parsed.get("text", "")), link_count=len(parsed.get("links", [])),
                      ocr_used=parsed.get("ocr_used", False), error=parsed.get("error", ""),
                      parser_version=parsed.get("parser_version", ""), unprocessed_pages=parsed.get("unprocessed_pages", []))
        return result, parsed
    except (requests.RequestException, socket.gaierror, TimeoutError, ConnectionError) as error:
        result.update(status="request_error", retryable=True, error=f"{type(error).__name__}: {error}"[:1000])
        return result, None
    except Exception as error:
        result.update(status="parse_error" if content is not None else "source_error", retryable=False,
                      error=f"{type(error).__name__}: {error}"[:1000])
        return result, None
    finally:
        result.update(ended_at=now(), elapsed_seconds=round(time.monotonic() - start, 3))
        if response is not None:
            response.close()
        if limiter and locked_host:
            limiter.release(locked_host, retry_seconds(result.get("retry_after", "")))
        if own_session:
            session.close()


def begin_attempt(db, source_id):
    attempt = str(uuid.uuid4())
    db.execute("INSERT INTO attempts VALUES(?,?,?,?,?,?,?)", (attempt, source_id, now(), None, "started", 1, "{}"))
    db.commit()
    return attempt


def parsed_status(value):
    return {"parsed": "fetched", "ok": "fetched", "success": "fetched", "partial": "parse_partial",
            "parse_error": "parse_error", "unsupported": "unsupported_document"}.get(value, "parse_error")


def save_parsed(run, parsed):
    encoded = json.dumps(parsed, ensure_ascii=False, sort_keys=True).encode()
    text = parsed.get("text", "").encode()
    parsed_rel = f"parsed/{hashlib.sha256(encoded).hexdigest()}.json.gz"
    text_rel = f"text/{hashlib.sha256(text).hexdigest()}.txt.gz"
    for path, content in [(parsed_rel, encoded), (text_rel, text)]:
        if not (Path(run) / path).exists():
            atomic_bytes(Path(run) / path, gzip.compress(content, mtime=0))
    return parsed_rel, text_rel


def review_url_identity(url):
    from scripts.extract_municipal_mayoral_urls import normalize_exact_url
    p = urlsplit(normalize_exact_url(url))
    return (p.hostname or "").lower().removeprefix("www.") + (p.path or "/") + ("?" + p.query if p.query else "")


def import_review_decisions(db, run):
    path = Path(run) / "roster/combined_claims.csv"
    if not path.exists():
        return
    for row in read_csv(path):
        decision = row.get("review_decision", "")
        if decision == "human_rejected":
            db.execute("INSERT OR REPLACE INTO review_decisions VALUES(?,?,?)",
                       (row["candidate_year_id"], review_url_identity(row["exact_normalized_url"]), decision))
    db.commit()


def candidate_context_hash(candidates):
    return sid("context", *sorted(r["candidate_year_id"] for r in candidates))


def complete_attempt(db, attempt, source, result, parsed, roster, contests, max_depth):
    from src.mayoral_source_expansion import attribute_links
    candidates = document_candidates(db, source["source_id"], roster, contests)
    claims = attribute_links(parsed, candidates, result["effective_url"], source["title"]) if parsed else []
    result["claim_rows"] = len(claims)
    result["attributed_context_hash"] = candidate_context_hash(candidates)
    encoded = json.dumps(result, ensure_ascii=False)
    db.execute("UPDATE attempts SET ended_at=?,status=?,retryable=?,result_json=? WHERE attempt_id=?",
               (result["ended_at"], result["status"], int(result["retryable"]), encoded, attempt))
    previous = db.execute("SELECT * FROM current WHERE source_id=?", (source["source_id"],)).fetchone()
    n_attempts = db.execute("SELECT COUNT(*) FROM attempts WHERE source_id=?", (source["source_id"],)).fetchone()[0]
    # A later error must never downgrade saved and parsed output.
    replace = previous is None or previous["status"] != "fetched" or result["status"] == "fetched" or result.get("operation") in {"reparse","response_reclassification"}
    if replace:
        db.execute("INSERT OR REPLACE INTO current VALUES(?,?,?,?,?,?)",
                   (source["source_id"], attempt, result["status"], int(result["retryable"]), n_attempts, encoded))
        if parsed:
            db.execute("DELETE FROM claims WHERE source_id=?", (source["source_id"],))
            old_edges = [dict(r) for r in db.execute("SELECT * FROM contexts WHERE parent_source_id=? AND origin='candidate_link_followup'", (source["source_id"],))]
            for edge in old_edges:
                db.execute("INSERT INTO retired_contexts VALUES(?,?,?,?)", (edge["relation_id"], now(), "source_reparsed_or_reattributed", json.dumps(edge)))
            db.execute("DELETE FROM contexts WHERE parent_source_id=? AND origin='candidate_link_followup'", (source["source_id"],))
            db.execute("DELETE FROM deferred_links WHERE source_id=?", (source["source_id"],))
            for claim in claims:
                exact = claim.get("exact_normalized_url", "")
                cid = claim["candidate_year_id"]
                claim_id = sid("expanded_claim", source["source_id"], cid, exact, claim.get("source_locator", ""), claim.get("extraction_method", ""))
                claim.update(claim_id=claim_id, source_id=source["source_id"], source_content_sha256=result["raw_sha256"],
                             source_content_path=result["raw_path"], source_retrieved_at=result.get("content_retrieved_at",result["ended_at"]), runner_version=VERSION,
                             source_date=claim.get("source_date") or claim.get("source_publication_date", ""), evidence_grade="D", review_status="pending")
                claim["source_kind"] = result.get("source_kind", "source_document")
                if claim["source_kind"] == "saved_search_snippet":
                    claim["snippet_attribution_status"] = claim.get("attribution_status", "")
                    claim["attribution_status"] = "snippet_reference"
                    claim["review_status"] = "snippet_requires_source_corroboration"
                # Candidate-level approvals and archive flags cannot approve a newly extracted URL.
                for field in ("human_accepted", "historical_url_identified", "archive_capture_available", "downloaded_content_available"):
                    if field in claim:
                        claim["input_candidate_" + field] = claim[field]
                        claim[field] = False
                if db.execute("SELECT 1 FROM review_decisions WHERE candidate_year_id=? AND url_identity=? AND decision='human_rejected'",
                              (cid, review_url_identity(exact))).fetchone():
                    claim.update(prior_rejection=True, review_status="prior_human_rejection", attribution_status="prior_rejection_review")
                db.execute("INSERT OR REPLACE INTO claims VALUES(?,?,?,?,?,?)",
                           (claim_id, source["source_id"], cid, exact, claim.get("attribution_status", "match_unresolved"), json.dumps(claim, ensure_ascii=False)))
                # Only candidate-specific references are followed; ordinary navigation is retained in parsed documents.
                url = source_url(claim.get("transport_url") or exact)
                label = claim.get("anchor_text", "") + " " + claim.get("attribution_reason", "")
                candidate_specific = claim.get("attribution_status") in {"identity_supported", "reference_lead", "source_attributed"} or (
                    claim.get("attribution_status") == "snippet_reference" and claim.get("snippet_attribution_status") in {"identity_supported", "reference_lead", "domain_shape_only"})
                if claim.get("url_type") == "source_internal_page":
                    candidate_specific = candidate_specific and bool(re.search(r"campaign|website|candidate|profile", claim.get("anchor_text", ""), re.I) or
                        any(len(t)>3 and t.lower() in url.lower() for t in roster[cid].get("full_name", "").split()))
                if not url or url == source_url(result["effective_url"]) or not candidate_specific:
                    continue
                depth = source["depth"] + 1
                if depth > max_depth:
                    db.execute("INSERT OR IGNORE INTO deferred_links VALUES(?,?,?,?,?,?)",
                               (sid("deferred", source["source_id"], cid, url), source["source_id"], cid, url, "followup_depth_limit", depth))
                else:
                    add_source(db, url=url, candidate_id=cid, origin="candidate_link_followup", input_path=result["parsed_path"],
                               input_row=0, title=label[:1000], priority=20 + 5 * depth, depth=depth, parent_id=source["source_id"])
            for link in parsed.get("links", []):
                if link.get("relation_role") != "document_download":
                    continue
                url = source_url(link.get("transport_url") or link.get("exact_normalized_url"))
                if not url or url == result["effective_url"]:
                    continue
                for candidate in candidates:
                    cid,depth = candidate["candidate_year_id"],source["depth"]+1
                    if depth <= max_depth:
                        add_source(db,url=url,candidate_id=cid,origin="candidate_link_followup",input_path=result["parsed_path"],input_row=0,
                                   title="Source document download",priority=10,depth=depth,parent_id=source["source_id"])
                    else:
                        db.execute("INSERT OR IGNORE INTO deferred_links VALUES(?,?,?,?,?,?)",
                                   (sid("deferred",source["source_id"],cid,url),source["source_id"],cid,url,"followup_depth_limit",depth))
            for edge in old_edges:
                db.execute("UPDATE sources SET disposition='stale_followup' WHERE source_id=? AND NOT EXISTS (SELECT 1 FROM contexts WHERE contexts.source_id=sources.source_id)", (edge["source_id"],))
    db.commit()
    return len(claims)


def repair_saved_ownership(db, run, source_ids=None):
    """Reassess the fixed claim universe without changing acquisition state."""
    from src.mayoral_source_expansion import attribute_links, ATTRIBUTION_VERSION
    roster = {r[0]: json.loads(r[1]) for r in db.execute("SELECT * FROM candidates")}
    sources = list(db.execute("SELECT s.source_id,s.title,c.result_json FROM sources s JOIN current c USING(source_id) ORDER BY s.source_id"))
    checked = updated = unmatched = 0
    for source in sources:
        if source_ids is not None and source["source_id"] not in source_ids:
            continue
        old = [json.loads(r[0]) for r in db.execute("SELECT payload FROM claims WHERE source_id=?", (source["source_id"],))]
        if not old:
            continue
        result = json.loads(source["result_json"])
        parsed = json.loads(gzip.decompress((Path(run) / result["parsed_path"]).read_bytes()))
        candidates = [roster[cid] for cid in sorted({r["candidate_year_id"] for r in old})]
        revised = {}
        for claim in attribute_links(parsed, candidates, result["effective_url"], source["title"]):
            key = sid("expanded_claim", source["source_id"], claim["candidate_year_id"], claim["exact_normalized_url"], claim.get("source_locator", ""), claim.get("extraction_method", ""))
            revised[key] = claim
        for claim in old:
            key = claim["claim_id"]
            previous_status = claim.get("input_attribution_status", claim.get("attribution_status"))
            claim.setdefault("input_attribution_status", previous_status)
            previous_flags = {k: claim.get(k) for k in ("source_date", "source_kind", "prior_rejection")}
            new = revised.get(key)
            if new is None:
                unmatched += 1
                claim.update(attribution_status="match_unresolved", candidate_site_ownership_supported=False,
                             candidate_site_ownership_reason="identity_requires_review_after_reattribution", attribution_version=ATTRIBUTION_VERSION)
            else:
                claim.update(new)
                claim.update({k: v for k, v in previous_flags.items() if v is not None})
                if previous_status in {"snippet_reference", "prior_rejection_review"} or (previous_status != "identity_supported" and claim["attribution_status"] == "identity_supported"):
                    claim["attribution_status"] = previous_status
            db.execute("UPDATE claims SET attribution_status=?,payload=? WHERE claim_id=?",
                       (claim["attribution_status"], json.dumps(claim, ensure_ascii=False), key))
            updated += 1
        checked += 1
        if checked % 25 == 0:
            db.commit()
            LOG.info("OWNERSHIP sources=%s claims=%s", checked, updated)
    db.commit()
    return {"attribution_version": ATTRIBUTION_VERSION, "sources_checked": checked, "claims_updated": updated, "unmatched_claims": unmatched}


def refresh_attribution(db, run, roster, contests, max_depth, *, reparse=False, ocr=True, force=False):
    from src.mayoral_source_expansion import parse_document
    refreshed = 0
    sources = [dict(r) for r in db.execute("SELECT s.*, c.result_json FROM sources s JOIN current c USING(source_id)")]
    for source in sources:
        result = json.loads(source.pop("result_json"))
        candidates = document_candidates(db, source["source_id"], roster, contests)
        if not result.get("parsed_path") and not (reparse and result.get("raw_path")):
            continue
        if not reparse and not force and result.get("attributed_context_hash") == candidate_context_hash(candidates):
            continue
        if reparse:
            content = gzip.decompress((Path(run)/result["raw_path"]).read_bytes())
            if hashlib.sha256(content).hexdigest() != result["raw_sha256"]:
                raise ValueError("Stored source hash mismatch")
            effective = source.get("cache_effective_url") or result["effective_url"]
            parsed = parse_document(content, result["content_type"], effective, ocr=ocr)
            parsed_rel, text_rel = save_parsed(run, parsed)
            result.update(parsed_path=parsed_rel, text_path=text_rel, effective_url=effective,
                          status=parsed_status(parsed.get("parse_status")), parse_status=parsed.get("parse_status"),
                          parser_version=parsed.get("parser_version"), error=parsed.get("error", ""),
                          text_characters=len(parsed.get("text", "")), link_count=len(parsed.get("links", [])),
                          unprocessed_pages=parsed.get("unprocessed_pages", []), ocr_used=parsed.get("ocr_used", False))
        else:
            parsed = json.loads(gzip.decompress((Path(run)/result["parsed_path"]).read_bytes()))
        result.update(content_retrieved_at=result.get("content_retrieved_at",result["ended_at"]), ended_at=now(),
                      request_count=0, used_cache=True, retryable=False, operation="reparse" if reparse else "reattribute")
        attempt = begin_attempt(db, source["source_id"])
        complete_attempt(db, attempt, source, result, parsed, roster, contests, max_depth)
        refreshed += 1
        if refreshed % 25 == 0:
            LOG.info("REPARSE refreshed=%s", refreshed)
    return refreshed


def pending_sources(db, max_attempts, cache_only=False):
    sql = """SELECT s.*,COALESCE(a.n,0) AS attempt_count, COALESCE(json_extract(c.result_json,'$.next_retry_at'),0) AS next_retry_at FROM sources s
        LEFT JOIN current c USING(source_id)
        LEFT JOIN (SELECT source_id,COUNT(*) AS n FROM attempts WHERE COALESCE(json_extract(result_json,'$.operation'),'') NOT IN ('reparse','reattribute','saved_search_snippet') GROUP BY source_id) a USING(source_id)
        WHERE s.disposition='pending' AND (c.source_id IS NULL OR c.retryable=1)
        AND COALESCE(a.n,0)<?"""
    if cache_only:
        sql += " AND s.cache_path<>''"
    sql += " ORDER BY COALESCE(a.n,0),s.priority,s.source_id"
    return [dict(r) for r in db.execute(sql, (max_attempts,))]


def restrict_unavailable_host(db,host):
    rows = [json.loads(r[0]) for r in db.execute("""SELECT a.result_json FROM attempts a JOIN sources s USING(source_id)
        WHERE s.host=? AND CAST(json_extract(a.result_json,'$.request_count') AS INTEGER)>0
        ORDER BY a.started_at DESC LIMIT 3""",(host,))]
    blocked = lambda r: r.get("http_status")==403 or (r.get("http_status")==202 and not r.get("content_bytes"))
    if len(rows)==3 and all(blocked(r) for r in rows):
        db.execute("INSERT OR REPLACE INTO host_restrictions VALUES(?,?,?,?)",
                   (host,"Three most recent requests returned HTTP403 or empty HTTP202; defer access review",
                    json.dumps([r["source_id"] for r in rows]),now()))
        db.execute("UPDATE sources SET disposition='host_deferred_access' WHERE host=? AND disposition='pending' AND source_id NOT IN (SELECT source_id FROM current)",(host,))
        db.commit()


def repair_empty_response_statuses(db,roster,contests,max_depth):
    rows = [dict(r) for r in db.execute("SELECT s.*,c.result_json FROM sources s JOIN current c USING(source_id) WHERE c.status='fetched' AND json_extract(c.result_json,'$.http_status')=202 AND json_extract(c.result_json,'$.content_bytes')=0")]
    for source in rows:
        result=json.loads(source.pop("result_json"))
        result.update(status="source_unavailable",retryable=False,error="Empty HTTP202 response; no source document received",operation="response_reclassification",request_count=0,ended_at=now())
        complete_attempt(db,begin_attempt(db,source["source_id"]),source,result,None,roster,contests,max_depth)
    for host in {r["host"] for r in rows}:
        restrict_unavailable_host(db,host)


def extract_saved_snippets(db, run, roster, contests, max_depth):
    from src.mayoral_source_expansion import parse_document, URL_RE
    marker = db.execute("SELECT value FROM meta WHERE key='snippets_extracted'").fetchone()
    if marker:
        return 0
    rows = [dict(r) for r in db.execute("SELECT c.*,s.url AS page_url FROM contexts c JOIN sources s USING(source_id) WHERE c.snippet<>'' AND c.origin<>'candidate_link_followup'")]
    count = 0
    for row in rows:
        if not URL_RE.search(row["snippet"]):
            continue
        source_id = sid("snippet_source", row["relation_id"])
        source = {"source_id": source_id, "url": "artifact://saved-search-snippet/"+row["relation_id"], "host": "", "title": row["title"], "depth": 0}
        db.execute("INSERT OR IGNORE INTO sources(source_id,url,host,title,priority,depth,disposition,created_at) VALUES(?,?,?,?,?,?,?,?)",
                   (source_id, source["url"], "", row["title"], 0, 0, "saved_search_snippet", now()))
        db.execute("INSERT OR IGNORE INTO contexts VALUES(?,?,?,?,?,?,?,?,?,?)",
                   (sid("snippet_relation",row["relation_id"]),source_id,row["candidate_year_id"],"saved_search_snippet",row["input_path"],row["input_row"],row["title"],row["snippet"],row["raw_url"],""))
        content = row["snippet"].encode()
        digest = hashlib.sha256(content).hexdigest()
        raw_path = f"raw/{digest}.gz"
        atomic_bytes(Path(run)/raw_path, gzip.compress(content,mtime=0))
        parsed = parse_document(content, "text/plain", row["page_url"], ocr=False)
        parsed_path,text_path = save_parsed(run, parsed)
        result = {"ended_at": now(), "status": parsed_status(parsed["parse_status"]), "retryable": False, "effective_url": row["page_url"],
                  "raw_sha256":digest,"raw_path":raw_path,"parsed_path":parsed_path,"text_path":text_path,"content_type":"text/plain",
                  "content_bytes":len(content),"request_count":0,"used_cache":True,"operation":"saved_search_snippet", "source_kind":"saved_search_snippet",
                  "input_path":row["input_path"],"input_row":row["input_row"],"parser_version":parsed["parser_version"],"error":parsed.get("error", "")}
        complete_attempt(db,begin_attempt(db,source_id),source,result,parsed,roster,contests,max_depth)
        count += 1
    db.execute("INSERT OR REPLACE INTO meta VALUES('snippets_extracted',?)",(str(count),))
    db.commit()
    return count


class TrancheBudget:
    """Persist precharges before dispatch. Interrupted requests keep their full charge."""

    def __init__(self, db, tranche_id, additional_bytes, max_sources):
        if not tranche_id or additional_bytes is None or additional_bytes <= 0 or max_sources is None or max_sources <= 0:
            raise ValueError("Tranche requires an ID and positive byte and source ceilings")
        self.db, self.key = db, "fetch_tranche:" + tranche_id
        row = db.execute("SELECT value FROM meta WHERE key=?", (self.key,)).fetchone()
        self.state = json.loads(row[0]) if row else {
            "tranche_id": tranche_id, "additional_bytes": additional_bytes, "max_sources": max_sources,
            "attempts_started": 0, "charged_bytes": 0, "reservations": {}, "created_at": now(),
            "baseline_attempts": db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0],
            "baseline_downloaded_raw_bytes": status(db)["downloaded_raw_bytes"],
        }
        if self.state["additional_bytes"] != additional_bytes or self.state["max_sources"] != max_sources:
            raise ValueError("Existing tranche ceilings differ; do not reset an approved tranche")
        self.save()

    def save(self):
        self.db.execute("INSERT OR REPLACE INTO meta VALUES(?,?)", (self.key, json.dumps(self.state)))
        self.db.commit()

    def reserve(self, amount):
        if self.state["attempts_started"] >= self.state["max_sources"]:
            return None, "bounded_run"
        if self.state["charged_bytes"] + amount > self.state["additional_bytes"]:
            return None, "tranche_byte_limit"
        token = str(uuid.uuid4())
        self.state["attempts_started"] += 1
        self.state["charged_bytes"] += amount
        self.state["reservations"][token] = amount
        self.save()
        return token, ""

    def settle(self, token, downloaded_bytes):
        amount = self.state["reservations"][token]
        if not 0 <= downloaded_bytes <= amount:
            raise ValueError("Downloaded bytes exceed the durable reservation")
        self.state["charged_bytes"] -= amount - downloaded_bytes
        del self.state["reservations"][token]
        self.save()


def run_fetch(root, run, *, workers=8, timeout=20, max_attempts=3, max_depth=2,
              max_bytes=25_000_000, total_bytes=8_000_000_000, min_free_bytes=5_000_000_000,
              host_delay=2., max_sources=None, cache_only=False, ocr=True,
              tranche_id=None, additional_bytes=None):
    run = Path(run)
    db = connect(run)
    if additional_bytes is not None and tranche_id is None:
        raise ValueError("--additional-bytes requires --tranche-id")
    budget = TrancheBudget(db, tranche_id, additional_bytes, max_sources) if tranche_id else None
    import_review_decisions(db, run)
    # Resume incomplete attempts without losing the fact that they were started.
    db.execute("UPDATE attempts SET status='interrupted',ended_at=? WHERE status='started'", (now(),))
    db.commit()
    roster = {r[0]: json.loads(r[1]) for r in db.execute("SELECT * FROM candidates")}
    contests = defaultdict(list)
    for key, value in roster.items():
        if 2010 <= int(float(value["year"])) <= 2021:
            contests[value.get("contest", value.get("contest_id", ""))].append(key)
    extract_saved_snippets(db,run,roster,contests,max_depth)
    repair_empty_response_statuses(db,roster,contests,max_depth)
    count, stopped, active_hosts = 0, "", set()
    ready_at = defaultdict(float)
    last_progress = 0.
    limiter = HostLimiter(host_delay)
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {}
        queue = deque(pending_sources(db, max_attempts, cache_only))
        while queue or futures:
            if max_sources is not None and count + len(futures) >= max_sources:
                queue.clear()
                stopped = "bounded_run"
            if not stopped and shutil.disk_usage(run).free < min_free_bytes:
                stopped = "storage_headroom"
                queue.clear()
            for _ in range(len(queue)):
                if stopped or len(futures) >= workers:
                    break
                if max_sources is not None and count + len(futures) >= max_sources:
                    stopped = "bounded_run"
                    queue.clear()
                    break
                item = queue.popleft()
                if db.execute("SELECT disposition FROM sources WHERE source_id=?",(item["source_id"],)).fetchone()[0] != "pending":
                    continue
                host = item["host"]
                if host in active_hosts or ready_at[host] > time.monotonic() or item.get("next_retry_at",0)>time.time():
                    queue.append(item)
                    continue
                token = None
                if budget:
                    # The stream can return one final chunk beyond the document limit.
                    token, reason = budget.reserve(max_bytes + 65536)
                    if reason:
                        queue.appendleft(item)
                        if futures and reason == "tranche_byte_limit":
                            break
                        stopped = reason
                        queue.clear()
                        break
                attempt = begin_attempt(db, item["source_id"])
                future = pool.submit(fetch_document, item, root, run, timeout=timeout, max_bytes=max_bytes, ocr=ocr,limiter=limiter)
                futures[future] = (attempt, item, token)
                active_hosts.add(host)
            if not futures:
                if queue:
                    time.sleep(.2)
                continue
            done, _ = wait(futures, timeout=1, return_when=FIRST_COMPLETED)
            for future in done:
                attempt, item, token = futures.pop(future)
                result, parsed = future.result()
                nclaims = complete_attempt(db, attempt, item, result, parsed, roster, contests, max_depth)
                if budget:
                    # A failed stream can retain an unread partial chunk in the client.
                    charged = (max_bytes + 65536 if result["status"] in {"request_error", "source_error"}
                               else result["downloaded_bytes"])
                    budget.settle(token, charged)
                restrict_unavailable_host(db,item["host"])
                active_hosts.discard(item["host"])
                ready_at[item["host"]] = time.monotonic() + (max(host_delay, 15) if result.get("retryable") else host_delay)
                count += 1
                LOG.info("SOURCE %s %s cache=%s links=%s claims=%s", item["source_id"], result["status"], result["used_cache"], result.get("link_count", 0), nclaims)
            if time.monotonic() - last_progress >= 30 or not queue and not futures:
                progress = status(db)
                progress["stop_reason"] = stopped
                if budget:
                    progress["tranche"] = budget.state
                atomic_json(run / "progress.json", progress)
                last_progress = time.monotonic()
                if not budget and progress["downloaded_raw_bytes"] > total_bytes and not stopped:
                    stopped = "total_byte_limit"
                    queue.clear()
                if time.monotonic() - getattr(run_fetch,"last_export",0) >= 1800:
                    export(run,db)
                    run_fetch.last_export = time.monotonic()
            if not queue and not futures and not stopped:
                if not cache_only:
                    refresh_attribution(db, run, roster, contests, max_depth)
                queue.extend(pending_sources(db, max_attempts, cache_only))
    final = export(run, db=db)
    final["stop_reason"] = stopped
    final["cache_only"] = cache_only
    if budget:
        final["tranche"] = budget.state
    atomic_json(run / "audit.json", final)
    db.close()
    return final


def status(db):
    states = dict(db.execute("SELECT status,COUNT(*) FROM current GROUP BY status").fetchall())
    dispositions = dict(db.execute("SELECT disposition,COUNT(*) FROM sources GROUP BY disposition").fetchall())
    totals = Counter()
    for row in db.execute("SELECT result_json FROM current"):
        r = json.loads(row[0])
        totals.update(processed_sources=1, cached_sources_used=int(r.get("used_cache", False)),
                      raw_bytes_saved=r.get("content_bytes", 0), unprocessed_ocr_pages=len(r.get("unprocessed_pages", [])))
    downloaded_bytes = db.execute("SELECT COALESCE(SUM(COALESCE(CAST(json_extract(result_json,'$.downloaded_bytes') AS INTEGER), CAST(json_extract(result_json,'$.content_bytes') AS INTEGER))),0) FROM attempts WHERE CAST(json_extract(result_json,'$.request_count') AS INTEGER)>0").fetchone()[0]
    pending = db.execute("SELECT COUNT(*) FROM sources s LEFT JOIN current c USING(source_id) WHERE s.disposition='pending' AND c.source_id IS NULL").fetchone()[0]
    retryable = db.execute("SELECT COUNT(*) FROM current WHERE retryable=1").fetchone()[0]
    return {"version": VERSION, "checked_at": now(), "source_rows": sum(dispositions.values()),
            "source_document_rows": sum(dispositions.values())-dispositions.get("saved_search_snippet",0),
            "processed_source_documents": db.execute("SELECT COUNT(*) FROM current JOIN sources USING(source_id) WHERE disposition<>'saved_search_snippet'").fetchone()[0],
            "processed_saved_snippets": db.execute("SELECT COUNT(*) FROM current JOIN sources USING(source_id) WHERE disposition='saved_search_snippet'").fetchone()[0],
            "source_relations": db.execute("SELECT COUNT(*) FROM contexts").fetchone()[0],
            "attempt_rows": db.execute("SELECT COUNT(*) FROM attempts").fetchone()[0],
            "processed_sources": totals["processed_sources"], "never_attempted_sources": pending,
            "retryable_sources": retryable, "source_status_counts": states, "source_dispositions": dispositions,
            "cached_sources_used": totals["cached_sources_used"],
            "downloaded_raw_bytes": downloaded_bytes,
            "tranches": [json.loads(row[0]) for row in db.execute("SELECT value FROM meta WHERE key LIKE 'fetch_tranche:%' ORDER BY key")],
            "raw_bytes_saved": totals["raw_bytes_saved"],
            "claim_rows": db.execute("SELECT COUNT(*) FROM claims").fetchone()[0],
            "candidate_years_with_extracted_claim": db.execute("SELECT COUNT(DISTINCT candidate_year_id) FROM claims").fetchone()[0],
            "deferred_followup_links": db.execute("SELECT COUNT(*) FROM deferred_links").fetchone()[0],
            "unprocessed_ocr_pages": totals["unprocessed_ocr_pages"],
            "stage": "source_pass_finished" if pending == 0 else "source_pass_incomplete", "paid_requests": 0}


def export(run, db=None):
    from src.mayoral_source_expansion import eligible_site_claim
    run = Path(run)
    owned = db is None
    db = db or connect(run)
    columns = dict.fromkeys(["claim_id", "source_id", "candidate_year_id", "exact_normalized_url", "attribution_status"])
    attribution_counts, site_candidates, site_claims = Counter(), set(), 0
    for record in db.execute("SELECT payload FROM claims ORDER BY claim_id"):
        claim = json.loads(record[0])
        columns.update(dict.fromkeys(claim))
        attribution_counts[claim.get("attribution_status", "")] += 1
        if eligible_site_claim(claim):
            site_claims += 1
            site_candidates.add(claim["candidate_year_id"])
    atomic_csv(run / "expanded_url_claims.csv", (json.loads(r[0]) for r in db.execute("SELECT payload FROM claims ORDER BY claim_id")), list(columns))
    rows = []
    for r in db.execute("SELECT s.*,c.status,c.retryable,c.result_json FROM sources s LEFT JOIN current c USING(source_id) ORDER BY source_id"):
        r = dict(r)
        payload = json.loads(r.pop("result_json") or "{}")
        r.update(payload)
        if r.get("status") is None:
            r["status"] = "not_attempted" if r["disposition"] == "pending" else r["disposition"]
        rows.append(r)
    atomic_csv(run / "source_dispositions.csv", rows)
    relations = []
    bysource = {r["source_id"]: r for r in rows}
    for row in db.execute("SELECT * FROM contexts ORDER BY relation_id"):
        item = dict(row)
        source = bysource[item["source_id"]]
        item.update(source_status=source["status"], retryable=source.get("retryable", False), source_disposition=source["disposition"])
        relations.append(item)
    atomic_csv(run / "source_candidate_dispositions.csv", relations)
    atomic_csv(run / "deferred_followup_links.csv", [dict(r) for r in db.execute("SELECT * FROM deferred_links")],
               ["link_id", "source_id", "candidate_year_id", "url", "reason", "depth"])
    atomic_csv(run / "host_restrictions.csv",[dict(r) for r in db.execute("SELECT * FROM host_restrictions")],
               ["host","reason","evidence_sources","observed_at"])
    result = status(db)
    result["exact_source_disposition_key_agreement"] = len({r["source_id"] for r in rows}) == result["source_rows"]
    result["exact_relation_disposition_key_agreement"] = len({r["relation_id"] for r in relations}) == result["source_relations"]
    result["attribution_status_counts"] = dict(attribution_counts)
    result["usable_site_claims"] = site_claims
    result["site_claim_candidate_years"] = len(site_candidates)
    result["retired_followup_contexts"] = db.execute("SELECT COUNT(*) FROM retired_contexts").fetchone()[0]
    atomic_json(run / "audit.json", result)
    if (run / "roster/candidate_roster.csv").is_file():
        from scripts.summarize_mayoral_source_expansion import summarize
        result["combined_roster"] = summarize(run)
    atomic_json(run / "audit.json", result)
    if owned:
        db.close()
    return result


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["prepare", "fetch", "reparse", "reattribute", "export", "status"])
    p.add_argument("--repo-root", type=Path, default=ROOT)
    p.add_argument("--run-dir", type=Path, default=DEFAULT_RUN)
    p.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    p.add_argument("--workers", type=int, default=8)
    p.add_argument("--timeout", type=int, default=20)
    p.add_argument("--max-attempts", type=int, default=3)
    p.add_argument("--max-depth", type=int, default=2)
    p.add_argument("--max-sources", type=int)
    p.add_argument("--cache-only", action="store_true")
    p.add_argument("--no-ocr", action="store_true")
    p.add_argument("--ownership-only", action="store_true", help="Reattribute fixed claims while preserving acquisition state")
    p.add_argument("--host-delay", type=float, default=2.)
    p.add_argument("--total-bytes", type=int, default=8_000_000_000)
    p.add_argument("--tranche-id", help="Stable approved tranche ID; reuse this ID after restart")
    p.add_argument("--additional-bytes", type=int, help="Persistent new response byte ceiling for this tranche")
    p.add_argument("--min-free-bytes", type=int, default=5_000_000_000)
    args = p.parse_args()
    if args.ownership_only and args.command != "reattribute":
        p.error("--ownership-only requires reattribute")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run = args.run_dir if args.run_dir.is_absolute() else args.repo_root / args.run_dir
    if args.command == "prepare":
        result = prepare(args.repo_root, run, args.manifest)
    elif args.command in {"fetch", "reparse", "reattribute"}:
        import fcntl
        run.mkdir(parents=True, exist_ok=True)
        with (run / ".writer.lock").open("a") as lock:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
            if args.command == "fetch":
                result = run_fetch(args.repo_root, run, workers=args.workers, timeout=args.timeout,
                                   max_attempts=args.max_attempts, max_depth=args.max_depth, max_sources=args.max_sources,
                                   cache_only=args.cache_only, ocr=not args.no_ocr, host_delay=args.host_delay, total_bytes=args.total_bytes,
                                   tranche_id=args.tranche_id, additional_bytes=args.additional_bytes, min_free_bytes=args.min_free_bytes)
            else:
                db = connect(run)
                import_review_decisions(db,run)
                roster = {r[0]: json.loads(r[1]) for r in db.execute("SELECT * FROM candidates")}
                contests = defaultdict(list)
                for key,row in roster.items():
                    if 2010<=int(float(row["year"]))<=2021:
                        contests[row.get("contest",row.get("contest_id", ""))].append(key)
                if args.ownership_only:
                    if args.command != "reattribute":
                        raise ValueError("--ownership-only requires reattribute")
                    atomic_json(run / "ownership_repair.json", repair_saved_ownership(db, run))
                else:
                    refresh_attribution(db,run,roster,contests,args.max_depth,reparse=args.command=="reparse",ocr=not args.no_ocr,force=True)
                result = export(run,db)
                db.close()
    elif args.command == "export":
        result = export(run)
    else:
        db = connect(run)
        result = status(db)
        db.close()
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
