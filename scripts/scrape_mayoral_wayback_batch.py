#!/usr/bin/env python3
"""Inventory and scrape a frozen batch of discovered mayoral campaign domains."""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import json
import logging
import os
import re
import tempfile
import threading
import time
from collections.abc import Iterable, Iterator
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from itertools import islice
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.freeze_mayoral_wayback_scrape_batch import (
    FROZEN_COLUMNS,
    FREEZE_VERSION,
    SCRAPE_METHOD_VERSION,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
BATCH_ROOT = REPO_ROOT / "data/municipal_archive_acquisition/openai_residual_100_20260826"
DEFAULT_TARGETS = BATCH_ROOT / "frozen/targets.csv"
DEFAULT_MANIFEST = BATCH_ROOT / "frozen/manifest.json"
DEFAULT_RUN_DIR = BATCH_ROOT / "runs/production"
CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; contact: hhilbig@ucdavis.edu)"
RUN_CONTRACT_VERSION = "mayoral-wayback-site-scrape-run-v1"
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
TERMINAL_INVENTORY = {"found", "no_capture"}
TERMINAL_FETCH = {
    "fetched", "fetched_timestamp_mismatch", "non_html", "no_content",
    "not_available", "redirect_loop",
}

VARIANT_ATTEMPT_COLUMNS = [
    "variant_attempt_id", "scrape_key", "target_id", "candidate_year_id",
    "variant_url", "window_start", "window_end", "inventory_status",
    "http_status", "request_count", "raw_response_path", "raw_response_sha256",
    "raw_response_bytes", "capture_rows", "retryable", "error", "attempted_at_utc",
]
CAPTURE_COLUMNS = [
    "capture_id", "variant_attempt_id", "scrape_key", "target_id",
    "candidate_year_id", "candidate_contest_id", "full_name", "geo_name",
    "state_abb", "year", "month", "registrable_domain", "timestamp",
    "original_url", "statuscode", "mimetype", "digest", "length",
    "replay_url", "election_reference_date", "capture_days_signed",
    "capture_days_absolute", "page_identity", "inventory_parser_version",
]
FETCH_QUEUE_COLUMNS = [
    "capture_id", "scrape_key", "target_id", "candidate_year_id",
    "candidate_contest_id", "full_name", "geo_name", "state_abb", "year",
    "month", "registrable_domain", "timestamp", "original_url", "digest",
    "replay_url", "election_reference_date", "capture_days_signed",
    "capture_days_absolute", "page_identity", "path_depth", "target_queue_rank",
    "selection_reason", "max_pages_per_target",
]
TARGET_STATUS_COLUMNS = [
    "scrape_key", "target_id", "candidate_year_id", "registrable_domain",
    "target_status", "variant_attempts_expected", "variant_attempts_recorded",
    "variant_found", "variant_no_capture", "variant_request_error",
    "capture_inventory_rows", "fetch_queue_rows", "retryable", "status_reason",
]
FETCH_ATTEMPT_COLUMNS = [
    "capture_id", "scrape_key", "target_id", "candidate_year_id", "timestamp",
    "original_url", "requested_replay_url", "effective_replay_url",
    "effective_timestamp", "fetch_status", "http_status", "request_count",
    "content_type", "raw_html_path", "raw_html_sha256", "raw_html_bytes",
    "text_path", "text_sha256", "text_characters", "retryable", "error",
    "attempted_at_utc",
]


class MayoralWaybackScrapeError(RuntimeError):
    pass


@dataclass
class RequestOutcome:
    response: requests.Response | None
    request_count: int
    error: str


class RateLimiter:
    def __init__(self, delay_seconds: float, sleep_fn: Callable[[float], None] = time.sleep):
        self.delay_seconds = max(0.0, delay_seconds)
        self.sleep_fn = sleep_fn
        self.last_request = 0.0
        self._lock = threading.Lock()

    def wait(self) -> None:
        with self._lock:
            remaining = self.delay_seconds - (time.monotonic() - self.last_request)
            if remaining > 0:
                self.sleep_fn(remaining)
            self.last_request = time.monotonic()


def _text(value: object) -> str:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return ""
    return str(value).strip()


def _as_bool(value: object) -> bool:
    return _text(value).casefold() in {"true", "1", "yes"}


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(_text(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False, lineterminator="\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _append_csv_row(row: Mapping[str, object], path: Path, columns: list[str]) -> None:
    """Durably append one result without rewriting the accumulated checkpoint."""
    path.parent.mkdir(parents=True, exist_ok=True)
    needs_header = not path.exists() or path.stat().st_size == 0
    with path.open("a", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore", lineterminator="\n")
        if needs_header:
            writer.writeheader()
        writer.writerow({column: row.get(column, "") for column in columns})
        handle.flush()
        os.fsync(handle.fileno())


def _read_fetch_checkpoint(base_path: Path, journal_path: Path) -> pd.DataFrame:
    """Read a compacted checkpoint plus later journal rows; latest row wins."""
    frames = []
    for path in (base_path, journal_path):
        if not path.exists() or path.stat().st_size == 0:
            continue
        frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
        if frame.columns.tolist() != FETCH_ATTEMPT_COLUMNS:
            raise MayoralWaybackScrapeError(f"fetch checkpoint schema differs: {path.name}")
        frames.append(frame)
    if not frames:
        return pd.DataFrame(columns=FETCH_ATTEMPT_COLUMNS)
    combined = pd.concat(frames, ignore_index=True)
    return combined.drop_duplicates("capture_id", keep="last").reset_index(drop=True)


def _compact_fetch_checkpoint(frame: pd.DataFrame, base_path: Path, journal_path: Path) -> None:
    """Atomically materialize current state; a stale journal is safe if a crash intervenes."""
    _atomic_csv(frame.sort_values("capture_id"), base_path, FETCH_ATTEMPT_COLUMNS)
    if journal_path.exists():
        journal_path.unlink()


def _merge_fetch_updates(
    frame: pd.DataFrame, updates: Iterable[Mapping[str, object]]
) -> pd.DataFrame:
    """Merge a bounded update batch into current state."""
    update_frame = pd.DataFrame(list(updates)).reindex(columns=FETCH_ATTEMPT_COLUMNS)
    if update_frame.empty:
        return frame
    combined = pd.concat([frame, update_frame], ignore_index=True)
    return combined.drop_duplicates("capture_id", keep="last").reset_index(drop=True)


def _fetch_queue_keys(queue_path: Path) -> set[str]:
    """Validate the queue and return its unique capture keys."""
    keys: set[str] = set()
    with queue_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FETCH_QUEUE_COLUMNS:
            raise MayoralWaybackScrapeError("fetch queue schema differs")
        for row in reader:
            capture_id = _text(row.get("capture_id"))
            if not capture_id or capture_id in keys:
                raise MayoralWaybackScrapeError("fetch queue keys are empty or duplicated")
            keys.add(capture_id)
    return keys


def _pending_fetch_rows(
    queue_path: Path, completed: set[str], request_budget: int | None
) -> Iterator[dict[str, str]]:
    """Stream pending queue rows without materializing the queue."""
    yielded = 0
    with queue_path.open(encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames != FETCH_QUEUE_COLUMNS:
            raise MayoralWaybackScrapeError("fetch queue schema differs")
        for row in reader:
            if row["capture_id"] in completed:
                continue
            if request_budget is not None and yielded >= request_budget:
                return
            yielded += 1
            yield row


def _atomic_json(value: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        json.dump(value, handle, indent=2, sort_keys=True)
        handle.write("\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_gzip(value: bytes, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        with gzip.GzipFile(fileobj=handle, mode="wb", mtime=0) as compressed:
            compressed.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _atomic_text(value: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(value)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def load_frozen(targets_path: Path, manifest_path: Path) -> pd.DataFrame:
    targets_path = targets_path.resolve()
    manifest_path = manifest_path.resolve()
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    if manifest.get("freeze_contract_version") != FREEZE_VERSION:
        raise MayoralWaybackScrapeError("freeze contract version differs")
    if manifest.get("scrape_method_version") != SCRAPE_METHOD_VERSION:
        raise MayoralWaybackScrapeError("scrape method version differs")
    output = manifest.get("output")
    if not isinstance(output, Mapping):
        raise MayoralWaybackScrapeError("freeze manifest lacks output record")
    if output.get("columns") != FROZEN_COLUMNS:
        raise MayoralWaybackScrapeError("frozen target column contract differs")
    if _sha256_file(targets_path) != output.get("sha256"):
        raise MayoralWaybackScrapeError("frozen target SHA-256 differs")
    targets = pd.read_csv(targets_path, keep_default_na=False, low_memory=False)
    if targets.columns.tolist() != FROZEN_COLUMNS or len(targets) != int(output.get("rows", -1)):
        raise MayoralWaybackScrapeError("frozen target shape differs")
    if targets["scrape_key"].duplicated().any() or targets["target_id"].duplicated().any():
        raise MayoralWaybackScrapeError("frozen target keys are duplicated")
    for row in targets.to_dict("records"):
        key = "\x1f".join([row["target_id"], row["candidate_year_id"], row["registrable_domain"]])
        if key != row["scrape_key"] or hashlib.sha256(key.encode()).hexdigest() != row["scrape_hash"]:
            raise MayoralWaybackScrapeError(f"frozen scrape key differs for {row['target_id']}")
    return targets


def expected_variant_attempts(targets: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target in targets.to_dict("records"):
        for variant in json.loads(target["url_variants_json"]):
            rows.append({
                "variant_attempt_id": _stable_id(
                    "mayoral_cdx_variant", RUN_CONTRACT_VERSION, target["scrape_key"], variant,
                    target["scrape_window_start"], target["scrape_window_end"],
                ),
                "scrape_key": target["scrape_key"],
                "target_id": target["target_id"],
                "candidate_year_id": target["candidate_year_id"],
                "variant_url": variant,
                "window_start": _text(target["scrape_window_start"]).replace("-", ""),
                "window_end": _text(target["scrape_window_end"]).replace("-", ""),
            })
    result = pd.DataFrame(rows)
    if result["variant_attempt_id"].duplicated().any():
        raise MayoralWaybackScrapeError("variant attempt keys are duplicated")
    return result


def _retry_after_seconds(response: requests.Response) -> float:
    value = _text(response.headers.get("Retry-After"))
    if not value:
        return 0.0
    try:
        return max(0.0, float(value))
    except ValueError:
        try:
            parsed = parsedate_to_datetime(value)
            now = datetime.now(parsed.tzinfo or timezone.utc)
            return max(0.0, (parsed - now).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return 0.0


def request_with_retries(
    session: requests.Session,
    url: str,
    *,
    params: dict[str, object] | None,
    timeout: tuple[int, int],
    attempts: int,
    rate_limiter: RateLimiter,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> RequestOutcome:
    error = ""
    response: requests.Response | None = None
    for attempt in range(1, attempts + 1):
        rate_limiter.wait()
        try:
            response = session.get(url, params=params, timeout=timeout, allow_redirects=True)
            if response.status_code not in RETRYABLE_HTTP:
                return RequestOutcome(response, attempt, "")
            error = f"retryable_http_{response.status_code}"
            retry_after = _retry_after_seconds(response)
        except requests.TooManyRedirects as exc:
            error = f"{type(exc).__name__}: {str(exc)[:500]}"
            return RequestOutcome(None, attempt, error)
        except requests.RequestException as exc:
            response = None
            error = f"{type(exc).__name__}: {str(exc)[:500]}"
            retry_after = 0.0
        if attempt < attempts:
            sleep_fn(max(retry_after, min(120.0, 5.0 * (3 ** (attempt - 1)))))
    return RequestOutcome(response, attempts, error)


def parse_cdx_response(content: bytes, variant_attempt: Mapping[str, object], target: Mapping[str, object]) -> pd.DataFrame:
    try:
        payload = json.loads(content.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise MayoralWaybackScrapeError(f"invalid CDX JSON: {exc}") from exc
    if not payload:
        return pd.DataFrame(columns=CAPTURE_COLUMNS)
    header = payload[0]
    expected = ["timestamp", "original", "statuscode", "mimetype", "digest", "length"]
    if header != expected:
        raise MayoralWaybackScrapeError(f"unexpected CDX header: {header!r}")
    reference = pd.Timestamp(target["election_reference_date"])
    rows = []
    for values in payload[1:]:
        if len(values) != len(expected):
            raise MayoralWaybackScrapeError("malformed CDX result row")
        record = dict(zip(expected, values))
        timestamp = _text(record["timestamp"])
        if not re.fullmatch(r"\d{14}", timestamp):
            raise MayoralWaybackScrapeError(f"invalid CDX timestamp: {timestamp!r}")
        capture_date = pd.to_datetime(timestamp[:8], format="%Y%m%d")
        signed = int((capture_date - reference).days)
        original = _text(record["original"])
        digest = _text(record["digest"])
        capture_id = _stable_id(
            "mayoral_capture", target["scrape_key"], timestamp, original, digest
        )
        parsed = urlsplit(original)
        page_identity = f"{parsed.path or '/'}" + (f"?{parsed.query}" if parsed.query else "")
        rows.append({
            "capture_id": capture_id,
            "variant_attempt_id": variant_attempt["variant_attempt_id"],
            "scrape_key": target["scrape_key"],
            "target_id": target["target_id"],
            "candidate_year_id": target["candidate_year_id"],
            "candidate_contest_id": target["candidate_contest_id"],
            "full_name": target["full_name"],
            "geo_name": target["geo_name"],
            "state_abb": target["state_abb"],
            "year": target["year"],
            "month": target["month"],
            "registrable_domain": target["registrable_domain"],
            "timestamp": timestamp,
            "original_url": original,
            "statuscode": record["statuscode"],
            "mimetype": record["mimetype"],
            "digest": digest,
            "length": record["length"],
            "replay_url": f"https://web.archive.org/web/{timestamp}id_/{original}",
            "election_reference_date": target["election_reference_date"],
            "capture_days_signed": signed,
            "capture_days_absolute": abs(signed),
            "page_identity": page_identity,
            "inventory_parser_version": RUN_CONTRACT_VERSION,
        })
    return pd.DataFrame(rows).reindex(columns=CAPTURE_COLUMNS)


def build_fetch_queue(captures: pd.DataFrame, targets: pd.DataFrame) -> tuple[pd.DataFrame, dict[str, object]]:
    if captures.empty:
        return pd.DataFrame(columns=FETCH_QUEUE_COLUMNS), {
            "inventory_rows": 0, "unique_page_rows": 0, "selected_rows": 0,
            "cap_drop_rows": 0, "cap_drop_fraction": 0.0,
        }
    captures = captures.drop_duplicates("capture_id").copy()
    best = (
        captures.sort_values(
            ["target_id", "page_identity", "capture_days_absolute", "timestamp", "capture_id"],
            kind="stable",
        )
        .drop_duplicates(["target_id", "page_identity"], keep="first")
    )
    target_lookup = targets.set_index("target_id", drop=False)
    selected_groups = []
    cap_drop = 0
    for target_id, group in best.groupby("target_id", sort=False):
        limit = int(target_lookup.loc[target_id, "max_pages_per_target"])
        group = group.copy()
        group["path_depth"] = group["page_identity"].map(
            lambda value: len([part for part in urlsplit(_text(value)).path.split("/") if part])
        )
        group = group.sort_values(
            ["path_depth", "capture_days_absolute", "page_identity", "timestamp"],
            kind="stable",
        )
        cap_drop += max(0, len(group) - limit)
        group = group.head(limit).copy()
        group["target_queue_rank"] = range(1, len(group) + 1)
        group["selection_reason"] = "nearest_election_capture_per_distinct_archived_path"
        group["max_pages_per_target"] = limit
        selected_groups.append(group)
    selected = pd.concat(selected_groups, ignore_index=True) if selected_groups else pd.DataFrame()
    queue = selected.reindex(columns=FETCH_QUEUE_COLUMNS).sort_values(
        ["scrape_key", "target_queue_rank", "capture_id"], kind="stable"
    ).reset_index(drop=True)
    if queue["capture_id"].duplicated().any():
        raise MayoralWaybackScrapeError("fetch queue capture IDs are duplicated")
    unique_pages = len(best)
    return queue, {
        "inventory_rows": len(captures),
        "unique_page_rows": unique_pages,
        "selected_rows": len(queue),
        "cap_drop_rows": cap_drop,
        "cap_drop_fraction": cap_drop / unique_pages if unique_pages else 0.0,
        "cap_drop_explanation": (
            "Per-target page ceiling; all inventory rows remain preserved."
            if cap_drop else "No target exceeded its page ceiling."
        ),
    }


def _read_checkpoint(path: Path, columns: list[str]) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
    if frame.columns.tolist() != columns:
        raise MayoralWaybackScrapeError(f"checkpoint schema differs: {path}")
    return frame


def _replace_row(frame: pd.DataFrame, key_column: str, row: dict[str, object]) -> pd.DataFrame:
    result = frame.loc[frame[key_column].map(_text).ne(_text(row[key_column]))].copy()
    result = pd.concat([result, pd.DataFrame([row])], ignore_index=True)
    if result[key_column].map(_text).duplicated().any():
        raise MayoralWaybackScrapeError(f"duplicate checkpoint key: {key_column}")
    return result


def _target_status(targets: pd.DataFrame, expected: pd.DataFrame, attempts: pd.DataFrame, captures: pd.DataFrame, queue: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for target in targets.to_dict("records"):
        target_id = target["target_id"]
        exp = expected.loc[expected.target_id.eq(target_id)]
        got = attempts.loc[attempts.target_id.eq(target_id)]
        statuses = got.inventory_status.value_counts().to_dict() if len(got) else {}
        capture_n = int(captures.target_id.eq(target_id).sum()) if len(captures) else 0
        queue_n = int(queue.target_id.eq(target_id).sum()) if len(queue) else 0
        missing = len(exp) - len(got)
        errors = int(statuses.get("request_error", 0))
        found = int(statuses.get("found", 0))
        if missing:
            status, retryable, reason = "incomplete", True, f"{missing}_variant_attempts_not_recorded"
        elif errors and found:
            status, retryable, reason = "found_with_partial_error", True, "captures_found_but_variant_errors_remain"
        elif errors:
            status, retryable, reason = "request_error", True, "all_or_some_variants_failed_without_capture"
        elif capture_n:
            status, retryable, reason = "found", False, "capture_inventory_nonempty"
        else:
            status, retryable, reason = "no_capture", False, "all_variants_checked_without_capture"
        rows.append({
            "scrape_key": target["scrape_key"], "target_id": target_id,
            "candidate_year_id": target["candidate_year_id"],
            "registrable_domain": target["registrable_domain"], "target_status": status,
            "variant_attempts_expected": len(exp), "variant_attempts_recorded": len(got),
            "variant_found": found, "variant_no_capture": int(statuses.get("no_capture", 0)),
            "variant_request_error": errors, "capture_inventory_rows": capture_n,
            "fetch_queue_rows": queue_n, "retryable": retryable, "status_reason": reason,
        })
    return pd.DataFrame(rows).reindex(columns=TARGET_STATUS_COLUMNS)


def run_inventory(
    targets: pd.DataFrame,
    run_dir: Path,
    *,
    session: requests.Session,
    rate_limiter: RateLimiter,
    attempts_per_request: int,
    timeout: tuple[int, int],
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    expected = expected_variant_attempts(targets)
    attempts_path = run_dir / "inventory_variant_attempts.csv"
    captures_path = run_dir / "capture_inventory.csv"
    queue_path = run_dir / "fetch_queue.csv"
    status_path = run_dir / "inventory_target_status.csv"
    attempts = _read_checkpoint(attempts_path, VARIANT_ATTEMPT_COLUMNS)
    captures = _read_checkpoint(captures_path, CAPTURE_COLUMNS)
    expected_keys = set(expected.variant_attempt_id)
    if not set(attempts.variant_attempt_id).issubset(expected_keys):
        raise MayoralWaybackScrapeError("inventory checkpoint contains unexpected attempts")
    target_lookup = targets.set_index("target_id", drop=False)
    completed = set(attempts.loc[attempts.inventory_status.isin(TERMINAL_INVENTORY), "variant_attempt_id"])
    for variant in expected.to_dict("records"):
        attempt_id = variant["variant_attempt_id"]
        if attempt_id in completed:
            continue
        target = target_lookup.loc[variant["target_id"]].to_dict()
        params = {
            "url": variant["variant_url"], "matchType": "prefix",
            "from": variant["window_start"], "to": variant["window_end"],
            "output": "json", "fl": "timestamp,original,statuscode,mimetype,digest,length",
            "filter": ["statuscode:200", "mimetype:text/html"], "collapse": "digest",
            "limit": 50000,
        }
        outcome = request_with_retries(
            session, CDX_ENDPOINT, params=params, timeout=timeout,
            attempts=attempts_per_request, rate_limiter=rate_limiter, sleep_fn=sleep_fn,
        )
        now = datetime.now(timezone.utc).isoformat()
        response = outcome.response
        http_status = response.status_code if response is not None else ""
        raw_path = run_dir / "cdx_raw" / f"{attempt_id}.json.gz"
        raw_sha = ""
        raw_bytes = 0
        capture_frame = pd.DataFrame(columns=CAPTURE_COLUMNS)
        error = outcome.error
        status = "request_error"
        if response is not None and response.status_code == 200:
            try:
                capture_frame = parse_cdx_response(response.content, variant, target)
                _atomic_gzip(response.content, raw_path)
                raw_sha = _sha256_bytes(response.content)
                raw_bytes = len(response.content)
                status = "found" if len(capture_frame) else "no_capture"
                error = ""
            except MayoralWaybackScrapeError as exc:
                error = str(exc)
        elif response is not None and response.status_code not in RETRYABLE_HTTP:
            error = f"http_{response.status_code}"
        attempts = _replace_row(attempts, "variant_attempt_id", {
            **variant, "inventory_status": status, "http_status": http_status,
            "request_count": outcome.request_count,
            "raw_response_path": str(raw_path.relative_to(run_dir)) if raw_sha else "",
            "raw_response_sha256": raw_sha, "raw_response_bytes": raw_bytes,
            "capture_rows": len(capture_frame), "retryable": status == "request_error",
            "error": error, "attempted_at_utc": now,
        })
        captures = captures.loc[captures.variant_attempt_id.ne(attempt_id)].copy()
        if len(capture_frame):
            captures = pd.concat([captures, capture_frame], ignore_index=True)
            captures = captures.drop_duplicates("capture_id", keep="first")
        _atomic_csv(attempts.sort_values("variant_attempt_id"), attempts_path, VARIANT_ATTEMPT_COLUMNS)
        _atomic_csv(captures.sort_values(["scrape_key", "timestamp", "capture_id"]), captures_path, CAPTURE_COLUMNS)
        logging.info("CDX %s %s captures=%d", target["registrable_domain"], status, len(capture_frame))
    queue, selection_audit = build_fetch_queue(captures, targets)
    target_status = _target_status(targets, expected, attempts, captures, queue)
    _atomic_csv(queue, queue_path, FETCH_QUEUE_COLUMNS)
    _atomic_csv(target_status, status_path, TARGET_STATUS_COLUMNS)
    exact_attempts = set(attempts.variant_attempt_id) == expected_keys and len(attempts) == len(expected)
    audit = {
        "run_contract_version": RUN_CONTRACT_VERSION,
        "stage": "inventory_complete" if exact_attempts else "inventory_partial",
        "targets": len(targets), "variant_attempts_expected": len(expected),
        "variant_attempts_recorded": len(attempts), "exact_variant_attempt_key_agreement": exact_attempts,
        "variant_status_counts": {str(k): int(v) for k, v in attempts.inventory_status.value_counts().sort_index().items()},
        "target_status_counts": {str(k): int(v) for k, v in target_status.target_status.value_counts().sort_index().items()},
        "capture_inventory_rows": len(captures), "capture_inventory_targets": int(captures.target_id.nunique()) if len(captures) else 0,
        "fetch_queue_rows": len(queue), "fetch_queue_targets": int(queue.target_id.nunique()) if len(queue) else 0,
        "selection": selection_audit,
    }
    _atomic_json(audit, run_dir / "inventory_audit.json")
    return audit


def _effective_timestamp(url: str) -> str:
    match = re.search(r"/web/(\d{14})(?:id_)?/", url)
    return match.group(1) if match else ""


def _visible_text(content: bytes) -> str:
    soup = BeautifulSoup(content, "html.parser")
    for tag in soup(["script", "style", "noscript", "svg"]):
        tag.decompose()
    return "\n".join(line.strip() for line in soup.get_text("\n").splitlines() if line.strip())


def _bounded_fetch_outcomes(
    captures: Iterable[dict[str, object]], *, workers: int, session: requests.Session,
    rate_limiter: RateLimiter, timeout: tuple[int, int], attempts_per_request: int,
    sleep_fn: Callable[[float], None],
):
    """Yield bounded batches; at most ``workers`` network requests are in flight."""
    if workers == 1:
        for capture in captures:
            yield capture, request_with_retries(
                session, capture["replay_url"], params=None, timeout=timeout,
                attempts=attempts_per_request, rate_limiter=rate_limiter, sleep_fn=sleep_fn,
            )
        return
    local = threading.local()

    def fetch(capture: dict[str, object]) -> RequestOutcome:
        worker_session = getattr(local, "session", None)
        if worker_session is None:
            worker_session = make_session()
            local.session = worker_session
        return request_with_retries(
            worker_session, capture["replay_url"], params=None, timeout=timeout,
            attempts=attempts_per_request, rate_limiter=rate_limiter, sleep_fn=sleep_fn,
        )

    with ThreadPoolExecutor(max_workers=workers) as executor:
        iterator = iter(captures)
        while batch := list(islice(iterator, workers)):
            outcomes = list(executor.map(fetch, batch))
            yield from zip(batch, outcomes)


def run_fetch(
    targets: pd.DataFrame,
    run_dir: Path,
    *,
    session: requests.Session,
    rate_limiter: RateLimiter,
    attempts_per_request: int,
    timeout: tuple[int, int],
    max_raw_bytes: int,
    circuit_breaker_failures: int,
    circuit_breaker_sleep: float,
    request_budget: int | None = None,
    checkpoint_interval: int = 250,
    workers: int = 1,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, object]:
    if request_budget is not None and request_budget < 1:
        raise MayoralWaybackScrapeError("fetch request budget must be positive")
    if checkpoint_interval < 1:
        raise MayoralWaybackScrapeError("checkpoint interval must be positive")
    if workers < 1:
        raise MayoralWaybackScrapeError("workers must be positive")
    queue_path = run_dir / "fetch_queue.csv"
    if not queue_path.exists():
        raise MayoralWaybackScrapeError("fetch queue is missing; run inventory first")
    queue_keys = _fetch_queue_keys(queue_path)
    attempts_path = run_dir / "fetch_attempts.csv"
    journal_path = run_dir / "fetch_attempts.journal.csv"
    fetches = _read_fetch_checkpoint(attempts_path, journal_path)
    if fetches.capture_id.duplicated().any() or not set(fetches.capture_id).issubset(queue_keys):
        raise MayoralWaybackScrapeError("fetch checkpoint keys differ")
    completed = set(fetches.loc[fetches.fetch_status.isin(TERMINAL_FETCH), "capture_id"])
    raw_used = int(pd.to_numeric(fetches.raw_html_bytes, errors="coerce").fillna(0).sum())
    consecutive_failures = 0
    requests_started = 0
    pending_updates: dict[str, dict[str, object]] = {}
    pending = _pending_fetch_rows(queue_path, completed, request_budget)
    for capture, outcome in _bounded_fetch_outcomes(
        pending, workers=workers, session=session, rate_limiter=rate_limiter,
        timeout=timeout, attempts_per_request=attempts_per_request, sleep_fn=sleep_fn,
    ):
        capture_id = capture["capture_id"]
        requests_started += 1
        response = outcome.response
        status = "request_error"
        retryable = True
        error = outcome.error
        http_status: object = ""
        content_type = ""
        effective_url = ""
        effective_timestamp = ""
        raw_path = run_dir / "raw_html" / f"{capture_id}.html.gz"
        text_path = run_dir / "text" / f"{capture_id}.txt"
        raw_sha = text_sha = ""
        raw_bytes = text_characters = 0
        if response is None and error.startswith("TooManyRedirects:"):
            status, retryable = "redirect_loop", False
        if response is not None:
            http_status = response.status_code
            content_type = _text(response.headers.get("Content-Type"))
            effective_url = _text(response.url)
            effective_timestamp = _effective_timestamp(effective_url)
            if response.status_code == 200:
                raw_bytes = len(response.content)
                if raw_used + raw_bytes > max_raw_bytes:
                    status, retryable, error = "disk_budget_exceeded", True, "raw byte ceiling reached"
                elif "html" not in content_type.casefold() and b"<html" not in response.content[:4096].lower():
                    status, retryable, error = "non_html", False, ""
                else:
                    text_value = _visible_text(response.content)
                    _atomic_gzip(response.content, raw_path)
                    raw_sha = _sha256_bytes(response.content)
                    raw_used += raw_bytes
                    if text_value:
                        _atomic_text(text_value, text_path)
                        text_sha = hashlib.sha256(text_value.encode("utf-8")).hexdigest()
                        text_characters = len(text_value)
                        requested_timestamp = _text(capture["timestamp"])
                        status = "fetched" if effective_timestamp in {"", requested_timestamp} else "fetched_timestamp_mismatch"
                        retryable, error = False, ""
                    else:
                        status, retryable, error = "no_content", False, ""
            elif response.status_code in {404, 410}:
                status, retryable, error = "not_available", False, f"http_{response.status_code}"
            elif response.status_code not in RETRYABLE_HTTP:
                status, retryable, error = "not_available", False, f"http_{response.status_code}"
        attempt_row = {
            "capture_id": capture_id, "scrape_key": capture["scrape_key"],
            "target_id": capture["target_id"], "candidate_year_id": capture["candidate_year_id"],
            "timestamp": capture["timestamp"], "original_url": capture["original_url"],
            "requested_replay_url": capture["replay_url"], "effective_replay_url": effective_url,
            "effective_timestamp": effective_timestamp, "fetch_status": status,
            "http_status": http_status, "request_count": outcome.request_count,
            "content_type": content_type,
            "raw_html_path": str(raw_path.relative_to(run_dir)) if raw_sha else "",
            "raw_html_sha256": raw_sha, "raw_html_bytes": raw_bytes if raw_sha else 0,
            "text_path": str(text_path.relative_to(run_dir)) if text_sha else "",
            "text_sha256": text_sha, "text_characters": text_characters,
            "retryable": retryable, "error": error,
            "attempted_at_utc": datetime.now(timezone.utc).isoformat(),
        }
        _append_csv_row(attempt_row, journal_path, FETCH_ATTEMPT_COLUMNS)
        pending_updates[capture_id] = attempt_row
        if requests_started % checkpoint_interval == 0:
            fetches = _merge_fetch_updates(fetches, pending_updates.values())
            _compact_fetch_checkpoint(fetches, attempts_path, journal_path)
            pending_updates.clear()
        if retryable:
            consecutive_failures += 1
            if status == "disk_budget_exceeded":
                logging.error("Raw byte ceiling reached; stopping fetch stage")
                break
            if consecutive_failures >= circuit_breaker_failures:
                logging.warning("Circuit breaker: sleeping %.0f seconds after %d retryable failures", circuit_breaker_sleep, consecutive_failures)
                sleep_fn(circuit_breaker_sleep)
                consecutive_failures = 0
        else:
            consecutive_failures = 0
        logging.info("FETCH %s %s", capture_id, status)
    fetches = _merge_fetch_updates(fetches, pending_updates.values())
    _compact_fetch_checkpoint(fetches, attempts_path, journal_path)
    fetch_keys = set(fetches.capture_id)
    fetched_mask = fetches.fetch_status.isin({"fetched", "fetched_timestamp_mismatch"})
    audit = {
        "run_contract_version": RUN_CONTRACT_VERSION,
        "stage": "fetch_complete" if fetch_keys == queue_keys else "fetch_partial",
        "queue_rows": len(queue_keys), "attempt_rows": len(fetches),
        "missing_attempt_rows": len(queue_keys - fetch_keys), "unexpected_attempt_rows": len(fetch_keys - queue_keys),
        "exact_queue_key_agreement": fetch_keys == queue_keys,
        "fetch_status_counts": {str(k): int(v) for k, v in fetches.fetch_status.value_counts().sort_index().items()},
        "fetched_rows": int(fetched_mask.sum()),
        "fetched_targets": int(fetches.loc[fetched_mask, "target_id"].nunique()),
        "retryable_rows": int(fetches.retryable.map(_as_bool).sum()),
        "raw_html_bytes": int(pd.to_numeric(fetches.raw_html_bytes, errors="coerce").fillna(0).sum()),
        "text_characters": int(pd.to_numeric(fetches.text_characters, errors="coerce").fillna(0).sum()),
        "max_raw_bytes": max_raw_bytes,
        "fetch_request_budget": request_budget,
        "fetch_requests_started_this_run": requests_started,
        "checkpoint_interval": checkpoint_interval,
        "checkpoint_journal_rows_at_completion": 0,
        "workers": workers,
    }
    _atomic_json(audit, run_dir / "fetch_audit.json")
    return audit


def make_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Encoding": "gzip, deflate"})
    return session


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=["inventory", "fetch", "run"])
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--run-dir", type=Path, default=DEFAULT_RUN_DIR)
    parser.add_argument("--target-id", action="append", default=[])
    parser.add_argument("--rate-limit-seconds", type=float, default=1.0)
    parser.add_argument("--attempts", type=int, default=5)
    parser.add_argument("--connect-timeout", type=int, default=30)
    parser.add_argument("--read-timeout", type=int, default=120)
    parser.add_argument("--max-raw-bytes", type=int, default=1_500_000_000)
    parser.add_argument("--circuit-breaker-failures", type=int, default=5)
    parser.add_argument("--circuit-breaker-sleep", type=float, default=900.0)
    parser.add_argument("--fetch-request-budget", type=int)
    parser.add_argument("--checkpoint-interval", type=int, default=250)
    parser.add_argument("--workers", type=int, default=1)
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.log_level), format="%(asctime)s %(levelname)s %(message)s")
    targets = load_frozen(args.targets, args.manifest)
    if args.target_id:
        unknown = set(args.target_id) - set(targets.target_id)
        if unknown:
            raise MayoralWaybackScrapeError(f"unknown target IDs: {sorted(unknown)}")
        targets = targets.loc[targets.target_id.isin(args.target_id)].copy()
    run_dir = args.run_dir.resolve()
    run_dir.mkdir(parents=True, exist_ok=True)
    session = make_session()
    limiter = RateLimiter(args.rate_limit_seconds)
    timeout = (args.connect_timeout, args.read_timeout)
    result: dict[str, object] = {}
    if args.command in {"inventory", "run"}:
        result["inventory"] = run_inventory(
            targets, run_dir, session=session, rate_limiter=limiter,
            attempts_per_request=args.attempts, timeout=timeout,
        )
    if args.command in {"fetch", "run"}:
        result["fetch"] = run_fetch(
            targets, run_dir, session=session, rate_limiter=limiter,
            attempts_per_request=args.attempts, timeout=timeout,
            max_raw_bytes=args.max_raw_bytes,
            circuit_breaker_failures=args.circuit_breaker_failures,
            circuit_breaker_sleep=args.circuit_breaker_sleep,
            request_budget=args.fetch_request_budget,
            checkpoint_interval=args.checkpoint_interval,
            workers=args.workers,
        )
    session.close()
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
