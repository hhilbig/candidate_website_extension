"""Select dated website observations and export coverage without dropping candidates.

Main pairs require a confirmed, usable homepage. Other pages remain in the corpus.
Event participation is a separate, sourced field; FEC registration does not establish it.
"""
from __future__ import annotations

import csv
import gzip
import io
import json
import os
import tempfile
from collections import Counter, defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

UTC = timezone.utc
NONPARTICIPANT = {"not_participating", "not_participant", "not_on_ballot", "withdrawn", "ineligible"}
FAILURES = {"http_error", "request_error", "blocked", "oversize"}
PAIR_STATUSES = ("paired", "pre_only", "post_only", "uncovered")
PAGE_COLUMNS = [
    "page_id", "candidate_id", "snapshot_id", "source", "original_url", "effective_url",
    "requested_timestamp", "captured_at", "fetched_at", "page_type", "depth", "status",
    "identity_status", "html_path", "text_path", "sha256", "n_char", "requested_event_id",
    "requested_side", "event_date", "errors", "render_used", "snapshot_complete",
]


def _datetime(value) -> datetime | None:
    if isinstance(value, datetime):
        parsed = value
    elif isinstance(value, str) and value.strip():
        try:
            parsed = (datetime.strptime(value, "%Y%m%d%H%M%S").replace(tzinfo=UTC)
                      if len(value) == 14 and value.isdigit()
                      else datetime.fromisoformat(value.replace("Z", "+00:00")))
        except ValueError:
            return None
    else:
        return None
    # Naive ISO timestamps do not establish which side of election day they belong to.
    return parsed.astimezone(UTC) if parsed.tzinfo else None


def _date(value) -> date | None:
    try:
        return date.fromisoformat(str(value)[:10])
    except (TypeError, ValueError):
        return None


def _local_dates(captured_at, event: dict) -> list[date]:
    stamp = _datetime(captured_at)
    if stamp is None:
        return []
    names = event.get("timezones") or ([event["timezone"]] if event.get("timezone") else [])
    if isinstance(names, str):
        names = [names]
    if names:
        try:
            return sorted({stamp.astimezone(ZoneInfo(name)).date() for name in names})
        except (KeyError, ValueError):
            return []
    # Without a jurisdiction timezone, require agreement across possible civil dates.
    return sorted({(stamp + timedelta(hours=-12)).date(), (stamp + timedelta(hours=14)).date()})


def capture_side(captured_at, event: dict) -> str:
    """Return a side only when every possible local date falls within that window."""
    election = _date(event.get("date"))
    dates = _local_dates(captured_at, event)
    if election is None or not dates:
        return ""
    differences = [(observed - election).days for observed in dates]
    if all(-30 <= difference <= -1 for difference in differences):
        return "pre"
    if all(1 <= difference <= 30 for difference in differences):
        return "post"
    return ""


def capture_distance_days(captured_at, event: dict) -> int | None:
    """Return the largest local calendar-day distance across valid event timezones."""
    if not capture_side(captured_at, event):
        return None
    election = _date(event["date"])
    return max(abs((observed - election).days) for observed in _local_dates(captured_at, event))


def _homepage(page: dict) -> bool:
    return page.get("page_type") in {"homepage", "home"} and str(page.get("depth", 0)) == "0"


def _usable(page: dict) -> bool:
    return page.get("status") == "usable" and page.get("identity_status") == "confirmed"


def _participation(event: dict) -> str:
    value = event.get("participation", event.get("participation_status", "unknown"))
    return str(value or "unknown")


def _event_rows(candidate: dict, events: list[dict]) -> list[dict]:
    from src.collection_2026_calendar import events_for_candidate
    return events_for_candidate(candidate, events)


def _validate_ids(rows: list[dict], key: str) -> None:
    values = [str(row.get(key) or "") for row in rows]
    if not all(values) or len(values) != len(set(values)):
        raise ValueError(f"Each input row requires a unique nonempty {key}")


def _snapshot_choice(pages: list[dict], event: dict, side: str) -> dict:
    eligible = [page for page in pages if _usable(page) and capture_side(page.get("captured_at"), event) == side]
    roots = [page for page in eligible if _homepage(page)
             and page.get("snapshot_complete", True) not in {False, "false", "False", "0", 0}]
    if not roots:
        return {}
    # Timestamp order breaks calendar-day ties. IDs provide deterministic final ties.
    root = min(roots, key=lambda page: (
        capture_distance_days(page.get("captured_at"), event),
        -_datetime(page["captured_at"]).timestamp() if side == "pre" else _datetime(page["captured_at"]).timestamp(),
        str(page.get("snapshot_id", "")), str(page.get("page_id", "")),
    ))
    snapshot_id = root.get("snapshot_id")
    selected = [page for page in eligible
                if page.get("snapshot_id") == snapshot_id] if snapshot_id else [root]
    selected.sort(key=lambda page: (str(page.get("captured_at", "")), str(page.get("page_id", ""))))
    times = [_datetime(page["captured_at"]) for page in selected]
    local_dates = _local_dates(root["captured_at"], event)
    sources = sorted({str(page.get("source", "")) for page in selected})
    return {
        "snapshot_id": snapshot_id or "", "homepage_page_id": root.get("page_id", ""),
        "captured_at": root["captured_at"], "source": root.get("source", ""),
        "distance_days": capture_distance_days(root["captured_at"], event),
        "local_date_min": min(local_dates).isoformat(), "local_date_max": max(local_dates).isoformat(),
        "page_ids": [page.get("page_id", "") for page in selected], "n_pages": len(selected),
        "page_types": sorted({str(page.get("page_type", "")) for page in selected}),
        "page_sources": sources, "page_time_min": min(times).isoformat(),
        "page_time_max": max(times).isoformat(), "page_span_hours": (max(times) - min(times)).total_seconds() / 3600,
        "snapshot_complete": root.get("snapshot_complete", True),
    }


def _missing_reason(candidate: dict, pages: list[dict], event: dict, side: str) -> str:
    matching = [page for page in pages
                if capture_side(page.get("captured_at"), event) == side
                or (page.get("requested_event_id") == event["event_id"] and page.get("requested_side") == side)]
    if any(_usable(page) and _homepage(page) and page.get("snapshot_complete", True) in {False, "false", "False", "0", 0}
           and capture_side(page.get("captured_at"), event) == side for page in matching):
        return "incomplete_snapshot"
    if any(_usable(page) and not _homepage(page) and capture_side(page.get("captured_at"), event) == side
           for page in matching):
        return "subpage_only"
    if any(page.get("status") == "identity_unresolved" or (
        page.get("status") == "usable" and page.get("identity_status") == "unresolved") for page in matching):
        return "identity_unresolved"
    if any(page.get("status") in FAILURES for page in matching):
        return "failed_requests"
    if any(page.get("status") == "timestamp_mismatch" for page in matching):
        return "timestamp_mismatch"
    if matching:
        return "unusable_content"
    if not candidate.get("website_url") and not pages:
        return "missing_website"
    return "no_eligible_capture" if pages else "not_collected"


def pairing_rows(candidates: list[dict], events: list[dict], pages: list[dict]) -> list[dict]:
    """Return one row per candidate and applicable event, including missing pairs."""
    _validate_ids(candidates, "candidate_id")
    _validate_ids(events, "event_id")
    by_candidate = defaultdict(list)
    for page in pages:
        by_candidate[str(page.get("candidate_id", ""))].append(page)
    output = []
    for candidate in candidates:
        candidate_pages = by_candidate[str(candidate["candidate_id"])]
        for event in _event_rows(candidate, events):
            status = _participation(event)
            eligible = status not in NONPARTICIPANT
            row = {key: candidate.get(key, "") for key in
                   ("candidate_id", "candidate", "office", "state", "district", "party", "year", "website_url")}
            row.update({
                "event_id": event["event_id"], "event_date": event["date"],
                "event_type": event.get("event_type", ""), "event_round": event.get("round", ""),
                "event_status": event.get("event_status", "scheduled"), "event_source_url": event.get("source_url", ""),
                "event_verified_source_url": event.get("verified_source_url", ""),
                "event_verification_status": event.get("verification_status", ""),
                "participation_status": status, "participation_source_url": event.get("participation_source_url", ""),
                "comparison_eligible": eligible, "timezones": event.get("timezones", []),
                "date_policy": "all_possible_local_dates", "homepage_required": True,
            })
            choices = {}
            for side in ("pre", "post"):
                choice = _snapshot_choice(candidate_pages, event, side) if eligible else {}
                choices[side] = choice
                row[f"{side}_available"] = bool(choice)
                row[f"{side}_missing_reason"] = "" if choice else (
                    _missing_reason(candidate, candidate_pages, event, side) if eligible else "confirmed_nonparticipant")
                for key in ("snapshot_id", "homepage_page_id", "captured_at", "source", "distance_days", "local_date_min",
                            "local_date_max", "page_ids", "n_pages", "page_types", "page_sources", "page_time_min",
                            "page_time_max", "page_span_hours", "snapshot_complete"):
                    row[f"{side}_{key}"] = choice.get(key, "")
            row["coverage_status"] = ("paired" if all(choices.values()) else "pre_only" if choices["pre"]
                                      else "post_only" if choices["post"] else "uncovered")
            row["mixed_source_pair"] = bool(all(choices.values()) and len(set(
                choices["pre"]["page_sources"] + choices["post"]["page_sources"])) > 1)
            row["pair_homepage_span_days"] = (
                (_datetime(choices["post"]["captured_at"]) - _datetime(choices["pre"]["captured_at"])).total_seconds() / 86400
                if all(choices.values()) else "")
            output.append(row)
    return output


def _regular_general_date(year: int) -> date:
    november_second = date(year, 11, 2)
    return november_second + timedelta(days=(1 - november_second.weekday()) % 7)


def live_cadence_due(events: list[dict], latest: datetime | None, stamp: datetime) -> bool:
    """Monthly (30-day) copies plus one copy on each local day immediately around a primary/runoff."""
    if latest is None or stamp - latest >= timedelta(days=30):
        return True
    for event in events:
        kind = event.get("event_type", "")
        if _participation(event) in NONPARTICIPANT or not ("primary" in kind or "runoff" in kind):
            continue
        election = _date(event.get("date"))
        if election is None:
            continue
        current = set(_local_dates(stamp, event))
        previous = set(_local_dates(latest, event))
        for offset in (-1, 1):
            target = election + timedelta(days=offset)
            if current == {target} and previous != {target}:
                return True
    return False


def due_live(candidate: dict, events: list[dict], pages: list[dict], now: datetime,
             end_date: date | None = None) -> bool:
    """Use monthly cadence and targeted pre/post copies until the collection endpoint.

    `end_date` caps the endpoint for every candidate. Hanno set it on
    2026-09-30 to 14 days after the general election, so later runoffs no
    longer extend the collection.
    """
    stamp = _datetime(now)
    if stamp is None:
        raise ValueError("now requires an explicit timezone")
    if not candidate.get("website_url"):
        return False
    relevant = _event_rows(candidate, events)
    dates = [_regular_general_date(int(candidate.get("year") or 2026))]
    dates.extend(_date(event["date"]) for event in relevant
                 if event.get("event_type") in {"general", "general_runoff", "special_general", "special_runoff"}
                 and _participation(event) not in NONPARTICIPANT)
    endpoint = max(dates) + timedelta(days=14)
    if end_date is not None:
        endpoint = min(endpoint, end_date)
    local_dates = [observed for event in relevant for observed in _local_dates(stamp, event)]
    if not local_dates:
        local_dates = _local_dates(stamp, {})
    if min(local_dates) > endpoint:
        return False
    completed = [_datetime(page.get("captured_at")) for page in pages
                 if str(page.get("candidate_id")) == str(candidate["candidate_id"])
                 and page.get("source") == "live" and _homepage(page) and _usable(page)
                 and page.get("snapshot_complete", True) not in {False, "false", "False", "0", 0}
                 and _datetime(page.get("captured_at")) is not None]
    if not completed:
        return True
    latest = max(completed)
    return live_cadence_due(relevant, latest, stamp)


def _value(value):
    return json.dumps(value, sort_keys=True, ensure_ascii=False) if isinstance(value, (list, dict, tuple)) else value


def _columns(rows: list[dict], defaults=()) -> list[str]:
    return list(dict.fromkeys(list(defaults) + [key for row in rows for key in row]))


def _atomic(path: Path, write) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    handle, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    os.close(handle)
    try:
        write(Path(temporary))
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def _write_csv(path: Path, rows: list[dict], defaults=()) -> None:
    columns = _columns(rows, defaults)
    def write(temporary):
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(handle, fieldnames=columns)
            writer.writeheader()
            writer.writerows({key: _value(value) for key, value in row.items()} for row in rows)
            handle.flush()
            os.fsync(handle.fileno())
    _atomic(path, write)


def _terminal_work(item: dict, by_key: dict) -> bool:
    seen = set()
    while item.get("status") == "superseded":
        if item.get("key") in seen:
            return False
        seen.add(item.get("key"))
        item = by_key.get(item.get("result", {}).get("superseded_by"), {})
    return item.get("status") in ({"complete", "empty"} if seen else {"complete", "empty", "failed", "blocked"})


def _work_reason(items: list[dict], event_id: str, side: str) -> str:
    by_key = {item.get("key"): item for item in items}
    relevant = [item for item in items if item.get("kind") in {"cdx_inventory", "archive_snapshot"}
                and not (item.get("status") == "superseded" and _terminal_work(item, by_key))
                and (not item.get("event_id") or item.get("event_id") == event_id)
                and (not item.get("side") or item.get("side") == side)]
    if any(item.get("status") in {"failed", "retryable", "blocked"} for item in relevant):
        return "failed_requests"
    inventories = [item for item in relevant if item.get("kind") == "cdx_inventory"]
    if inventories and all(item.get("status") == "empty" for item in inventories):
        return "missing_archives"
    if any(item.get("status") in {"pending", "running"} for item in relevant):
        return "pending_collection"
    return ""


def _prepare_outputs(root: Path, candidates: list[dict], events: list[dict], pages: list[dict],
                     work: list[dict], parquet_written: bool = False) -> tuple[dict, dict]:
    """Build rows and counts without reading HTML files or writing exports."""
    root = Path(root)
    pairs = pairing_rows(candidates, events, pages)
    by_work = defaultdict(list)
    by_pages = defaultdict(list)
    by_pairs = defaultdict(list)
    for item in work:
        by_work[str(item.get("candidate_id", ""))].append(item)
    for page in pages:
        by_pages[str(page.get("candidate_id", ""))].append(page)
    for pair in pairs:
        for side in ("pre", "post"):
            if pair[f"{side}_missing_reason"] in {"not_collected", "no_eligible_capture"}:
                reason = _work_reason(by_work[str(pair["candidate_id"])], pair["event_id"], side)
                if reason:
                    pair[f"{side}_missing_reason"] = reason
        by_pairs[str(pair["candidate_id"])].append(pair)
    coverage = []
    for candidate in candidates:
        candidate_id = str(candidate["candidate_id"])
        candidate_pages = by_pages[candidate_id]
        candidate_pairs = by_pairs[candidate_id]
        statuses = Counter(pair["coverage_status"] for pair in candidate_pairs)
        row = {key: candidate.get(key, "") for key in
               ("candidate_id", "candidate", "office", "state", "district", "party", "year", "website_url")}
        reasons = sorted({pair[f"{side}_missing_reason"] for pair in candidate_pairs for side in ("pre", "post")
                          if pair[f"{side}_missing_reason"]})
        row.update({"website_missing": not bool(candidate.get("website_url")),
                    "n_events": len(candidate_pairs), "n_pages": len(candidate_pages),
                    "n_usable_pages": sum(_usable(page) for page in candidate_pages),
                    "n_incomplete_snapshots": len({page.get("snapshot_id") for page in candidate_pages
                        if _homepage(page) and page.get("snapshot_complete", True) in {False, "false", "False", "0", 0}}),
                    "n_snapshots": len({page.get("snapshot_id") for page in candidate_pages if page.get("snapshot_id")}),
                    "n_complete_pairs": statuses["paired"], "n_pre_only": statuses["pre_only"],
                    "n_post_only": statuses["post_only"], "n_uncovered_events": statuses["uncovered"],
                    "n_participation_unknown": sum(pair["participation_status"] == "unknown" for pair in candidate_pairs),
                    "n_identity_unresolved": sum(page.get("status") == "identity_unresolved" for page in candidate_pages),
                    "coverage_reasons": reasons or (["no_applicable_event"] if not candidate_pairs else []),
                    "page_status_counts": dict(Counter(page.get("status", "missing_status") for page in candidate_pages)),
                    "work_status_counts": dict(Counter(item.get("status", "missing_status") for item in by_work[candidate_id]))})
        coverage.append(row)
    exports = {
        "candidate_roster": (candidates, ["candidate_id", "candidate", "website_url"]),
        "election_calendar": (events, ["event_id", "date", "event_type"]),
        "page_corpus": (pages, PAGE_COLUMNS),
        "candidate_event_pairs": (pairs, ["candidate_id", "event_id", "coverage_status"]),
        "candidate_coverage": (coverage, ["candidate_id", "n_events", "n_pages"]),
        "work_status": (work, ["key", "kind", "candidate_id", "event_id", "side", "status"]),
    }
    candidate_ids = {str(candidate["candidate_id"]) for candidate in candidates}
    work_counts = Counter(item.get("status", "missing_status") for item in work)
    work_by_key = {item.get("key"): item for item in work}
    pair_counts = Counter(pair["coverage_status"] for pair in pairs)
    summary = {
        "generated_at": datetime.now(UTC).isoformat(), "candidate_count": len(candidates),
        "event_count": len(events), "candidate_event_count": len(pairs), "page_count": len(pages),
        "snapshot_count": len({(page.get("candidate_id"), page.get("snapshot_id")) for page in pages if page.get("snapshot_id")}),
        "candidate_without_website_count": sum(not bool(candidate.get("website_url")) for candidate in candidates),
        "candidate_without_applicable_event_count": sum(not by_pairs[str(candidate["candidate_id"])] for candidate in candidates),
        "candidate_without_usable_homepage_count": sum(not any(_homepage(page) and _usable(page)
            for page in by_pages[str(candidate["candidate_id"])]) for candidate in candidates),
        "coverage_counts": {status: pair_counts[status] for status in PAIR_STATUSES},
        "distinct_candidates_by_event_coverage": {status: len({pair["candidate_id"] for pair in pairs
            if pair["coverage_status"] == status}) for status in PAIR_STATUSES},
        "candidate_coverage_count_note": "A candidate can appear in multiple categories across different events.",
        "mixed_source_pair_count": sum(pair["mixed_source_pair"] for pair in pairs),
        "same_source_pair_count": sum(pair["coverage_status"] == "paired" and not pair["mixed_source_pair"] for pair in pairs),
        "participation_unknown_event_count": sum(pair["participation_status"] == "unknown" for pair in pairs),
        "page_status_counts": dict(Counter(page.get("status", "missing_status") for page in pages)),
        "work_status_counts": dict(work_counts), "work_item_count": len(work),
        "work_terminal_count": sum(_terminal_work(item, work_by_key) for item in work),
        "work_pending_count": sum(work_counts[status] for status in ("pending", "running", "retryable")),
        "work_missing_status_count": work_counts["missing_status"],
        "every_work_item_has_status": all(bool(item.get("status")) for item in work),
        "every_work_item_terminal": all(_terminal_work(item, work_by_key) for item in work),
        "incomplete_snapshot_count": len({(page.get("candidate_id"), page.get("snapshot_id")) for page in pages
            if _homepage(page) and page.get("snapshot_complete", True) in {False, "false", "False", "0", 0}}),
        "orphan_page_count": sum(str(page.get("candidate_id", "")) not in candidate_ids for page in pages),
        "parquet_written": parquet_written, "parquet_required_for_production": True,
        "date_policy": "all possible local dates must be on the same side within 30 days; election day excluded",
        "pair_policy": "usable confirmed-identity homepage required; subpage dates checked separately",
        "participation_policy": "unknown participation retained; sourced nonparticipants excluded from main pairs",
        "outputs": {name: str(root / f"{name}.csv") for name in exports},
    }
    return exports, summary


def write_outputs(root: Path, candidates: list[dict], events: list[dict], pages: list[dict], work: list[dict]) -> dict:
    """Write CSV and optional Parquet exports, plus an audit of every input denominator."""
    root = Path(root)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pa = pq = None
    exports, summary = _prepare_outputs(root, candidates, events, pages, work, pa is not None)
    for name, (rows, defaults) in exports.items():
        _write_csv(root / f"{name}.csv", rows, defaults)
        if pa is not None:
            # String columns preserve heterogeneous metadata and empty export schemas.
            columns = _columns(rows, defaults)
            table = pa.table({key: pa.array(["" if row.get(key) is None else str(_value(row.get(key, "")))
                                            for row in rows], type=pa.string()) for key in columns})
            _atomic(root / f"{name}.parquet", lambda temporary, table=table: pq.write_table(table, temporary, compression="zstd"))
    _atomic(root / "coverage_report.json", lambda temporary: temporary.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"))
    return summary


def _store_record(table: str, record) -> dict:
    row = dict(record)
    if table in {"pages", "captures"}:
        return json.loads(row["data"])
    for key in ("payload", "result", "detail"):
        if key in row:
            row[key] = json.loads(row[key])
    return row


def _iter_store_rows(store, table: str):
    if table not in {"pages", "captures", "work", "attempts"}:
        raise ValueError(table)
    for record in store.db.execute(f"SELECT * FROM {table} ORDER BY rowid"):
        yield _store_record(table, record)


def _stream_export(root: Path, name: str, row_factory, defaults=(), *, parquet=True,
                   batch_size=1024, inspect=None, csv_gzip=False) -> dict:
    """Discover columns, then atomically stream CSV and optional Parquet batches."""
    columns = dict.fromkeys(defaults)
    count = 0
    for row in row_factory():
        columns.update(dict.fromkeys(row))
        count += 1
        if inspect is not None:
            inspect(row)
    columns = list(columns)
    try:
        import pyarrow as pa
        import pyarrow.parquet as pq
    except ImportError:
        pa = pq = None
    use_parquet = parquet and pa is not None
    temporary_paths = []
    writer = None
    try:
        csv_suffix = ".csv.gz" if csv_gzip else ".csv"
        fd, temporary = tempfile.mkstemp(prefix=f".{name}{csv_suffix}.", dir=root)
        csv_temporary = Path(temporary)
        temporary_paths.append(csv_temporary)
        if use_parquet:
            pq_fd, pq_temporary = tempfile.mkstemp(prefix=f".{name}.parquet.", dir=root)
            os.close(pq_fd)
            parquet_temporary = Path(pq_temporary)
            temporary_paths.append(parquet_temporary)
            schema = pa.schema([(key, pa.string()) for key in columns])
            writer = pq.ParquetWriter(parquet_temporary, schema, compression="zstd")

        def flush(batch):
            if writer is not None and batch:
                arrays = {key: pa.array([
                    "" if row.get(key) is None else str(_value(row.get(key, ""))) for row in batch
                ], type=pa.string()) for key in columns}
                writer.write_table(pa.table(arrays, schema=schema), row_group_size=batch_size)

        with os.fdopen(fd, "wb") as raw:
            compressed = gzip.GzipFile(filename="", mode="wb", fileobj=raw, mtime=0) if csv_gzip else None
            handle = io.TextIOWrapper(compressed or raw, encoding="utf-8", newline="")
            try:
                csv_writer = csv.DictWriter(handle, fieldnames=columns)
                csv_writer.writeheader()
                batch = []
                written = 0
                for row in row_factory():
                    csv_writer.writerow({key: _value(value) for key, value in row.items()})
                    if use_parquet:
                        batch.append(row)
                        if len(batch) >= batch_size:
                            flush(batch)
                            batch.clear()
                    written += 1
                flush(batch)
                if written != count:
                    raise RuntimeError(f"The {name} row count changed during export")
                handle.flush()
            finally:
                handle.detach()
                if compressed is not None:
                    compressed.close()
            raw.flush()
            os.fsync(raw.fileno())
        if writer is not None:
            writer.close()
            writer = None
        os.replace(csv_temporary, root / f"{name}{csv_suffix}")
        if use_parquet:
            os.replace(parquet_temporary, root / f"{name}.parquet")
    finally:
        if writer is not None:
            writer.close()
        for path in temporary_paths:
            if path.exists():
                path.unlink()
    return {"row_count": count, "parquet_written": use_parquet,
            "csv_path": str(root / f"{name}{csv_suffix}"), "csv_compressed": csv_gzip}


def _snapshot_count(store, *, incomplete=False) -> int:
    # SQLite performs the distinct count without a Python set of all snapshots.
    if incomplete:
        condition = """
            json_extract(data,'$.page_type') IN ('homepage','home')
            AND (json_type(data,'$.depth') IS NULL OR
                (json_type(data,'$.depth') IN ('text','integer')
                 AND CAST(json_extract(data,'$.depth') AS TEXT)='0'))
            AND json_extract(data,'$.snapshot_complete') IN (0,'false','False','0')
        """
    else:
        condition = """
            json_type(data,'$.snapshot_id') IS NOT NULL
            AND json_type(data,'$.snapshot_id') != 'null'
            AND json_extract(data,'$.snapshot_id') NOT IN ('',0)
        """
    return store.db.execute(f"""
        SELECT COUNT(*) FROM (
            SELECT json_extract(data,'$.candidate_id'),json_extract(data,'$.snapshot_id')
            FROM pages WHERE {condition}
            GROUP BY json_extract(data,'$.candidate_id'),json_extract(data,'$.snapshot_id')
        )
    """).fetchone()[0]


def write_store_outputs(root: Path, candidates: list[dict], events: list[dict], store,
                        *, batch_size: int = 1024) -> dict:
    """Export SQLite metadata without materializing the full page or capture tables.

    Pair calculations hold one candidate's metadata at a time. CSV and Parquet
    writes use two database passes so optional fields cannot disappear.
    The caller must pause collection while this export reads the store.
    """
    root = Path(root)
    root.mkdir(parents=True, exist_ok=True)
    if batch_size < 1:
        raise ValueError("batch_size must be positive")
    _validate_ids(candidates, "candidate_id")
    _validate_ids(events, "event_id")
    exports, summary = _prepare_outputs(root, [], events, [], [])
    candidate_ids = {str(candidate["candidate_id"]) for candidate in candidates}
    sum_fields = (
        "candidate_count", "candidate_event_count", "candidate_without_website_count",
        "candidate_without_applicable_event_count", "candidate_without_usable_homepage_count",
        "mixed_source_pair_count", "same_source_pair_count", "participation_unknown_event_count",
    )

    # Only pair and coverage rows use temporary files. The large metadata tables
    # stream directly from SQLite and do not need duplicate temporary JSON files.
    with tempfile.TemporaryDirectory(prefix=".collection-export-", dir=root) as scratch:
        scratch = Path(scratch)
        pair_path, coverage_path = scratch / "pairs.jsonl", scratch / "coverage.jsonl"
        with pair_path.open("w", encoding="utf-8") as pair_handle, coverage_path.open("w", encoding="utf-8") as coverage_handle:
            for candidate in candidates:
                candidate_id = str(candidate["candidate_id"])
                candidate_pages = [json.loads(record[0]) for record in store.db.execute(
                    "SELECT data FROM pages WHERE json_extract(data,'$.candidate_id')=? ORDER BY rowid",
                    (candidate_id,))]
                candidate_work = [_store_record("work", record) for record in store.db.execute(
                    "SELECT * FROM work WHERE candidate_id=? ORDER BY rowid", (candidate_id,))]
                prepared, counts = _prepare_outputs(root, [candidate], events, candidate_pages, candidate_work)
                for name, handle in (("candidate_event_pairs", pair_handle), ("candidate_coverage", coverage_handle)):
                    for row in prepared[name][0]:
                        handle.write(json.dumps(row, ensure_ascii=False) + "\n")
                for field in sum_fields:
                    summary[field] += counts[field]
                for status in PAIR_STATUSES:
                    summary["coverage_counts"][status] += counts["coverage_counts"][status]
                    # A candidate can contribute to several event coverage categories.
                    summary["distinct_candidates_by_event_coverage"][status] += counts["distinct_candidates_by_event_coverage"][status]
                del prepared, counts, candidate_pages, candidate_work

        def json_rows(path):
            with path.open(encoding="utf-8") as handle:
                for line in handle:
                    yield json.loads(line)

        page_counts, work_counts = Counter(), Counter()
        summary.update(page_count=0, orphan_page_count=0, work_item_count=0, work_terminal_count=0,
                       every_work_item_has_status=True, every_work_item_terminal=True)

        class WorkLookup:
            def get(self, key, default=None):
                record = store.db.execute("SELECT * FROM work WHERE key=?", (key,)).fetchone()
                return _store_record("work", record) if record is not None else default

        work_lookup = WorkLookup()

        def inspect_page(row):
            summary["page_count"] += 1
            page_counts[row.get("status", "missing_status")] += 1
            summary["orphan_page_count"] += str(row.get("candidate_id", "")) not in candidate_ids

        def inspect_work(row):
            summary["work_item_count"] += 1
            work_counts[row.get("status", "missing_status")] += 1
            terminal = _terminal_work(row, work_lookup)
            summary["work_terminal_count"] += terminal
            summary["every_work_item_has_status"] &= bool(row.get("status"))
            summary["every_work_item_terminal"] &= terminal

        row_sources = {
            "candidate_roster": lambda: iter(candidates),
            "election_calendar": lambda: iter(events),
            "page_corpus": lambda: _iter_store_rows(store, "pages"),
            "candidate_event_pairs": lambda: json_rows(pair_path),
            "candidate_coverage": lambda: json_rows(coverage_path),
            "work_status": lambda: _iter_store_rows(store, "work"),
        }
        parquet_written = True
        for name, row_factory in row_sources.items():
            result = _stream_export(root, name, row_factory, exports[name][1], batch_size=batch_size,
                                    inspect=inspect_page if name == "page_corpus" else inspect_work if name == "work_status" else None)
            parquet_written &= result["parquet_written"]

    inventory = _stream_export(root, "capture_inventory", lambda: _iter_store_rows(store, "captures"),
                               ("capture_id", "inventory_key", "timestamp", "original"),
                               parquet=False, batch_size=batch_size, csv_gzip=True)
    attempts = store.db.execute("""
        SELECT COUNT(*), COUNT(DISTINCT attempt_id),
               COALESCE(SUM(CASE WHEN status='started' THEN 1 ELSE 0 END),0) FROM attempts
    """).fetchone()
    summary.update(
        generated_at=datetime.now(UTC).isoformat(), parquet_written=parquet_written,
        snapshot_count=_snapshot_count(store), incomplete_snapshot_count=_snapshot_count(store, incomplete=True),
        page_status_counts=dict(page_counts), work_status_counts=dict(work_counts),
        work_pending_count=sum(work_counts[status] for status in ("pending", "running", "retryable")),
        work_missing_status_count=work_counts["missing_status"], capture_inventory_count=inventory["row_count"],
        attempts=attempts[0], attempt_ids_unique=attempts[0] == attempts[1], unfinished_attempts=attempts[2],
    )
    summary["outputs"]["capture_inventory"] = str(root / "capture_inventory.csv.gz")
    summary["obsolete_generated_exports"] = [str(root / name) for name in ("capture_inventory.csv", "capture_inventory.parquet")
                                                if (root / name).exists()]
    _atomic(root / "coverage_report.json", lambda temporary: temporary.write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8"))
    return summary
