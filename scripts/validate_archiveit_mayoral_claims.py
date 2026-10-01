#!/usr/bin/env python3
"""Validate Archive-It mayoral URL claims against collection-local captures.

Archive-It seed metadata creates a grade-C lead. This stage upgrades a claim to
grade A only when an election-period capture of the exact URL identifies the
named candidate, a mayoral campaign, and the city. Archive-It alone never
creates grade B evidence.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import re
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import quote, urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.discover_archiveit_mayoral_urls import normalize_exact_url, unwrap_archive_url


USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; public archive validation)"
VALIDATION_METHOD_VERSION = "archive_it_exact_content_windows_v2"
HEADLINE_WINDOW_DAYS = 365
SENSITIVITY_MONTHS_BEFORE = 18
SENSITIVITY_DAYS_AFTER = 120
REUSE_MONTHS_EACH_SIDE = 24
MAX_REUSE_PROBES = 2


VALIDATION_KEY = ["claim_id", "source_id", "exact_normalized_url"]
VALIDATION_COLUMNS = [
    "claim_id", "source_id", "exact_normalized_url", "validation_context_hash",
    "validation_method_version", "collection_id", "candidate_year_id",
    "candidate_contest_id", "contest_id", "full_name", "geo_name", "state_abb",
    "year", "month", "url_type", "archive_query_target", "archive_timemap_url",
    "archive_query_status", "archive_capture_count", "eligible_capture_count",
    "election_reference_date", "headline_window_days", "sensitivity_window_start",
    "sensitivity_window_end", "reuse_window_start", "reuse_window_end",
    "nearest_capture_timestamp", "nearest_capture_original_url",
    "nearest_capture_days_signed", "nearest_capture_days_absolute",
    "within_headline_window", "within_sensitivity_window", "capture_replay_url",
    "capture_fetch_status", "capture_content_sha256", "candidate_identity_match",
    "mayoral_campaign_match", "city_match", "content_signal_summary",
    "reuse_captures_before", "reuse_captures_after", "reuse_probe_timestamps",
    "reuse_probe_count", "reuse_conflict_count", "domain_reuse_status",
    "proposed_evidence_grade", "strict_coverage_eligible",
    "temporal_validation_status", "validation_reason", "error", "validated_on",
]


REVIEW_COLUMNS_ADDED = [
    "archive_validation_method_version", "archive_query_status",
    "archive_validation_reason", "archive_nearest_capture_timestamp",
    "archive_nearest_capture_days_absolute", "archive_within_headline_window",
    "archive_within_sensitivity_window", "archive_capture_replay_url",
    "archive_candidate_identity_match", "archive_mayoral_campaign_match",
    "archive_city_match", "archive_domain_reuse_status",
]


@dataclass(frozen=True)
class Capture:
    timestamp: str
    original: str
    mimetype: str
    statuscode: int
    digest: str

    @property
    def captured_at(self) -> datetime:
        return datetime.strptime(self.timestamp[:14], "%Y%m%d%H%M%S")


class ArchiveItValidationRequestError(RuntimeError):
    """A collection-local query or replay request failed after retries."""


def _write_csv_atomic(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False,
        encoding="utf-8", newline="",
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False)
    temporary.replace(path)


def _normalize_text(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value)).encode(
        "ascii", "ignore"
    ).decode()
    return " ".join(re.findall(r"[a-z0-9]+", ascii_value.casefold()))


def _stable_context_hash(row: Mapping[str, Any]) -> str:
    values = [
        row.get("claim_id", ""), row.get("source_id", ""),
        row.get("exact_normalized_url", ""), row.get("collection_id", ""),
        row.get("full_name", ""), row.get("geo_name", ""), row.get("year", ""),
        "" if pd.isna(row.get("month", pd.NA)) else row.get("month", ""),
        row.get("url_type", ""),
    ]
    payload = "\x1f".join(str(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def shift_months(value: date, months: int) -> date:
    month_index = value.year * 12 + value.month - 1 + months
    year, zero_month = divmod(month_index, 12)
    month = zero_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def collection_timemap_url(collection_id: int, exact_url: str) -> str:
    target = normalize_exact_url(unwrap_archive_url(exact_url))
    if not target:
        raise ValueError(f"Invalid Archive-It validation URL: {exact_url!r}")
    return (
        f"https://wayback.archive-it.org/{int(collection_id)}/timemap/json"
        f"?url={quote(target, safe='')}"
    )


def collection_replay_url(collection_id: int, capture: Capture) -> str:
    return (
        f"https://wayback.archive-it.org/{int(collection_id)}/"
        f"{capture.timestamp}id_/{capture.original}"
    )


def _request(
    url: str,
    timeout: int,
    attempts: int,
    delay_seconds: float,
    session: Any,
) -> Any:
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_error = "Archive-It request failed"
    retry_after_seconds = 0.0
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=timeout,
            )
            status = int(response.status_code)
            if status == 429 or status >= 500:
                last_error = f"HTTP {status}"
                headers = getattr(response, "headers", {}) or {}
                try:
                    retry_after_seconds = max(
                        0.0, float(headers.get("Retry-After", 0) or 0)
                    )
                except (TypeError, ValueError):
                    retry_after_seconds = 0.0
            elif status >= 400:
                raise ArchiveItValidationRequestError(f"HTTP {status} for {url}")
            else:
                return response
        except ArchiveItValidationRequestError:
            raise
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < attempts:
            exponential_delay = delay_seconds * (2 ** (attempt - 1))
            time.sleep(min(60.0, max(exponential_delay, retry_after_seconds)))
    raise ArchiveItValidationRequestError(f"{last_error} for {url}")


def query_collection_timemap(
    collection_id: int,
    exact_url: str,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> list[Capture]:
    """Query only the configured Archive-It collection's timemap."""
    url = collection_timemap_url(collection_id, exact_url)
    response = _request(url, timeout, attempts, delay_seconds, session)
    try:
        payload = response.json()
    except ValueError as error:
        raise ArchiveItValidationRequestError(
            f"Invalid timemap JSON for collection {collection_id}: {error}"
        ) from error
    return parse_timemap(payload)


def parse_timemap(payload: Any) -> list[Capture]:
    if not isinstance(payload, list) or not payload:
        return []
    captures: list[Capture] = []
    if isinstance(payload[0], list):
        header = [str(value) for value in payload[0]]
        for raw in payload[1:]:
            if not isinstance(raw, list):
                continue
            item = dict(zip(header, raw))
            try:
                captures.append(Capture(
                    timestamp=str(item.get("timestamp", "")),
                    original=str(item.get("original", "")),
                    mimetype=str(item.get("mimetype", "")),
                    statuscode=int(item.get("statuscode", 0)),
                    digest=str(item.get("digest", "")),
                ))
            except (TypeError, ValueError):
                continue
    else:
        for item in payload:
            if not isinstance(item, Mapping):
                continue
            try:
                captures.append(Capture(
                    timestamp=str(item.get("timestamp", "")),
                    original=str(item.get("original", "")),
                    mimetype=str(item.get("mimetype", "")),
                    statuscode=int(item.get("statuscode", 0)),
                    digest=str(item.get("digest", "")),
                ))
            except (TypeError, ValueError):
                continue
    valid = []
    seen = set()
    for capture in captures:
        try:
            capture.captured_at
        except ValueError:
            continue
        key = (capture.timestamp, capture.original, capture.digest)
        if key not in seen:
            valid.append(capture)
            seen.add(key)
    return sorted(valid, key=lambda capture: capture.timestamp)


def eligible_html_captures(captures: list[Capture]) -> list[Capture]:
    allowed_mimetypes = {"text/html", "application/xhtml+xml", "warc/revisit"}
    return [
        capture for capture in captures
        if capture.statuscode == 200 and capture.mimetype.casefold() in allowed_mimetypes
    ]


def nearest_capture(captures: list[Capture], reference: date) -> tuple[Capture | None, int | None]:
    if not captures:
        return None, None
    reference_dt = datetime.combine(reference, datetime.min.time())
    ranked = sorted(
        ((abs((capture.captured_at - reference_dt).days), capture) for capture in captures),
        key=lambda item: (item[0], item[1].timestamp),
    )
    return ranked[0][1], ranked[0][0]


def fetch_collection_capture(
    collection_id: int,
    capture: Capture,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> str:
    response = _request(
        collection_replay_url(collection_id, capture), timeout, attempts,
        delay_seconds, session,
    )
    content_type = str(response.headers.get("content-type", "")).casefold()
    if content_type and "html" not in content_type and "xhtml" not in content_type:
        raise ArchiveItValidationRequestError(
            f"Replay is not HTML ({content_type}) for {capture.timestamp}"
        )
    return response.text


NICKNAME_GROUPS = (
    {"bill", "billy", "will", "william"},
    {"bob", "bobby", "rob", "robert"},
    {"chris", "christopher"},
    {"jim", "jimmy", "james"},
    {"mike", "michael"},
    {"tony", "anthony"},
)


def _given_name_present(given_tokens: list[str], page_tokens: set[str]) -> bool:
    for token in given_tokens:
        if len(token) > 1 and token in page_tokens:
            return True
        if any(token in group and group & page_tokens for group in NICKNAME_GROUPS):
            return True
    return False


def page_content_signals(html: str, candidate_name: str, city: str) -> dict[str, Any]:
    soup = BeautifulSoup(str(html), "html.parser")
    for node in soup(["script", "style", "noscript", "svg"]):
        node.decompose()
    metadata_parts: list[str] = []
    if soup.title:
        metadata_parts.append(soup.title.get_text(" ", strip=True))
    for tag in soup.find_all("meta"):
        name = str(tag.get("name") or tag.get("property") or "").casefold()
        if name in {"description", "og:title", "og:description", "twitter:title", "twitter:description"}:
            metadata_parts.append(str(tag.get("content", "")))
    visible = soup.get_text(" ", strip=True)
    combined = " ".join(metadata_parts + [visible])
    normalized = _normalize_text(combined)
    page_tokens = set(normalized.split())

    name_tokens = [token for token in _normalize_text(candidate_name).split() if token not in {"jr", "sr", "ii", "iii", "iv"}]
    surname = name_tokens[-1] if name_tokens else ""
    identity_match = bool(
        surname and surname in page_tokens
        and _given_name_present(name_tokens[:-1], page_tokens)
    )

    mayoral_patterns = (
        r"\bfor mayor\b", r"\bmayoral campaign\b", r"\bcampaign for mayor\b",
        r"\bmayor[\s-]+president\b",
        r"\b(?:elect|reelect|vote for)\b.{0,80}\bmayor\b",
    )
    mayoral_match = any(re.search(pattern, normalized) for pattern in mayoral_patterns)

    normalized_city = _normalize_text(city)
    if normalized_city == "los angeles":
        city_match = bool(
            re.search(r"\blos angeles\b", normalized)
            or re.search(r"\bl a\b", normalized)
        )
    else:
        city_match = bool(normalized_city and re.search(
            rf"\b{re.escape(normalized_city)}\b", normalized,
        ))
    summary = (
        f"identity={'yes' if identity_match else 'no'}; "
        f"mayoral_campaign={'yes' if mayoral_match else 'no'}; "
        f"city={'yes' if city_match else 'no'}"
    )
    return {
        "candidate_identity_match": identity_match,
        "mayoral_campaign_match": mayoral_match,
        "city_match": city_match,
        "content_signal_summary": summary,
        "content_sha256": hashlib.sha256(str(html).encode("utf-8")).hexdigest(),
    }


def _reuse_probe_candidates(
    captures: list[Capture],
    nearest: Capture,
    window_start: date,
    window_end: date,
    limit: int = MAX_REUSE_PROBES,
) -> list[Capture]:
    in_window = [
        capture for capture in captures
        if window_start <= capture.captured_at.date() <= window_end
        and capture.timestamp != nearest.timestamp
        and (not nearest.digest or capture.digest != nearest.digest)
    ]
    if not in_window or limit <= 0:
        return []
    ordered = sorted(in_window, key=lambda capture: capture.timestamp)
    selected = [ordered[0]]
    if limit > 1 and ordered[-1].timestamp != ordered[0].timestamp:
        selected.append(ordered[-1])
    return selected[:limit]


def _base_result(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "claim_id": row["claim_id"],
        "source_id": row["source_id"],
        "exact_normalized_url": row["exact_normalized_url"],
        "validation_context_hash": _stable_context_hash(row),
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "collection_id": int(row["collection_id"]),
        "candidate_year_id": row.get("candidate_year_id", ""),
        "candidate_contest_id": row.get("candidate_contest_id", ""),
        "contest_id": row.get("contest_id", ""),
        "full_name": row.get("full_name", ""),
        "geo_name": row.get("geo_name", ""),
        "state_abb": row.get("state_abb", ""),
        "year": int(row["year"]),
        "month": row.get("month", pd.NA),
        "url_type": row.get("url_type", ""),
        "archive_query_target": normalize_exact_url(unwrap_archive_url(str(row["exact_normalized_url"]))),
        "archive_timemap_url": "",
        "archive_query_status": "not_attempted",
        "archive_capture_count": 0,
        "eligible_capture_count": 0,
        "election_reference_date": "",
        "headline_window_days": HEADLINE_WINDOW_DAYS,
        "sensitivity_window_start": "",
        "sensitivity_window_end": "",
        "reuse_window_start": "",
        "reuse_window_end": "",
        "nearest_capture_timestamp": "",
        "nearest_capture_original_url": "",
        "nearest_capture_days_signed": pd.NA,
        "nearest_capture_days_absolute": pd.NA,
        "within_headline_window": False,
        "within_sensitivity_window": False,
        "capture_replay_url": "",
        "capture_fetch_status": "not_attempted",
        "capture_content_sha256": "",
        "candidate_identity_match": False,
        "mayoral_campaign_match": False,
        "city_match": False,
        "content_signal_summary": "",
        "reuse_captures_before": 0,
        "reuse_captures_after": 0,
        "reuse_probe_timestamps": "",
        "reuse_probe_count": 0,
        "reuse_conflict_count": 0,
        "domain_reuse_status": "not_inspected",
        "proposed_evidence_grade": "C",
        "strict_coverage_eligible": False,
        "temporal_validation_status": "unresolved",
        "validation_reason": "not_validated",
        "error": "",
        "validated_on": date.today().isoformat(),
    }


def _completed_validation(row: Mapping[str, Any]) -> bool:
    if row.get("archive_query_status") == "request_error":
        return False
    if row.get("capture_fetch_status") == "request_error":
        return False
    if row.get("domain_reuse_status") == "request_error":
        return False
    return row.get("archive_query_status") in {
        "checked", "not_attempted_missing_election_month",
    }


def _validation_reason(result: Mapping[str, Any]) -> str:
    if str(result.get("url_type", "")).startswith("excluded_"):
        return "excluded_url_type"
    if not result.get("nearest_capture_timestamp"):
        return "no_collection_capture"
    if not result.get("within_headline_window"):
        return (
            "outside_headline_365_within_sensitivity"
            if result.get("within_sensitivity_window")
            else "outside_headline_and_sensitivity_windows"
        )
    if result.get("capture_fetch_status") == "request_error":
        return "capture_request_error_retryable"
    if not result.get("candidate_identity_match"):
        return "candidate_identity_not_confirmed"
    if not result.get("mayoral_campaign_match"):
        return "mayoral_campaign_not_confirmed"
    if not result.get("city_match"):
        return "city_not_confirmed"
    if result.get("domain_reuse_status") == "request_error":
        return "reuse_inspection_request_error_retryable"
    if result.get("domain_reuse_status") == "conflicting_content_detected":
        return "domain_reuse_conflict"
    return "grade_a_exact_url_election_period_content_confirmed"


def validate_one_claim(
    row: Mapping[str, Any],
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    reuse_probe_limit: int = MAX_REUSE_PROBES,
    query_cache: dict[tuple[int, str], list[Capture]] | None = None,
    capture_cache: dict[str, str] | None = None,
) -> dict[str, Any]:
    result = _base_result(row)
    month = pd.to_numeric(pd.Series([row.get("month", pd.NA)]), errors="coerce").iloc[0]
    if pd.isna(month):
        result.update({
            "month": pd.NA,
            "archive_query_status": "not_attempted_missing_election_month",
            "capture_fetch_status": "not_attempted_missing_election_month",
            "temporal_validation_status": "missing_election_month",
            "validation_reason": "missing_election_month_no_imputation",
            "error": "Recorded election month is missing; no month was imputed",
        })
        return result
    if not 1 <= int(month) <= 12:
        raise ValueError(f"Invalid election month for claim {row['claim_id']}: {month}")
    result["month"] = int(month)
    reference = date(int(row["year"]), int(month), 15)
    sensitivity_start = shift_months(reference, -SENSITIVITY_MONTHS_BEFORE)
    sensitivity_end = reference + timedelta(days=SENSITIVITY_DAYS_AFTER)
    reuse_start = shift_months(reference, -REUSE_MONTHS_EACH_SIDE)
    reuse_end = shift_months(reference, REUSE_MONTHS_EACH_SIDE)
    result.update({
        "election_reference_date": reference.isoformat(),
        "sensitivity_window_start": sensitivity_start.isoformat(),
        "sensitivity_window_end": sensitivity_end.isoformat(),
        "reuse_window_start": reuse_start.isoformat(),
        "reuse_window_end": reuse_end.isoformat(),
        "archive_timemap_url": collection_timemap_url(
            int(row["collection_id"]), result["archive_query_target"],
        ),
    })

    cache_key = (int(row["collection_id"]), result["archive_query_target"])
    try:
        if query_cache is not None and cache_key in query_cache:
            captures = query_cache[cache_key]
        else:
            captures = query_collection_timemap(
                int(row["collection_id"]), result["archive_query_target"],
                timeout=timeout, attempts=attempts, delay_seconds=delay_seconds,
            )
            if query_cache is not None:
                query_cache[cache_key] = captures
        result["archive_query_status"] = "checked"
    except (ArchiveItValidationRequestError, ValueError) as error:
        result.update({
            "archive_query_status": "request_error",
            "capture_fetch_status": "not_attempted_query_error",
            "temporal_validation_status": "request_error_retryable",
            "validation_reason": "archive_query_request_error_retryable",
            "error": f"{type(error).__name__}: {error}"[:1000],
        })
        return result

    result["archive_capture_count"] = len(captures)
    eligible = eligible_html_captures(captures)
    result["eligible_capture_count"] = len(eligible)
    nearest, _ = nearest_capture(eligible, reference)
    if nearest is None:
        result.update({
            "capture_fetch_status": "not_attempted_no_capture",
            "domain_reuse_status": "not_inspected_no_capture",
            "temporal_validation_status": "no_collection_capture",
            "validation_reason": "no_collection_capture",
        })
        return result

    signed_days = (nearest.captured_at.date() - reference).days
    absolute_days = abs(signed_days)
    within_headline = absolute_days <= HEADLINE_WINDOW_DAYS
    within_sensitivity = sensitivity_start <= nearest.captured_at.date() <= sensitivity_end
    result.update({
        "nearest_capture_timestamp": nearest.timestamp,
        "nearest_capture_original_url": nearest.original,
        "nearest_capture_days_signed": signed_days,
        "nearest_capture_days_absolute": absolute_days,
        "within_headline_window": within_headline,
        "within_sensitivity_window": within_sensitivity,
        "capture_replay_url": collection_replay_url(int(row["collection_id"]), nearest),
    })

    captures_in_reuse_window = [
        capture for capture in eligible
        if reuse_start <= capture.captured_at.date() <= reuse_end
    ]
    result["reuse_captures_before"] = sum(
        capture.captured_at.date() < reference for capture in captures_in_reuse_window
    )
    result["reuse_captures_after"] = sum(
        capture.captured_at.date() > reference for capture in captures_in_reuse_window
    )

    replay_url = result["capture_replay_url"]
    try:
        if capture_cache is not None and replay_url in capture_cache:
            html = capture_cache[replay_url]
        else:
            html = fetch_collection_capture(
                int(row["collection_id"]), nearest, timeout=timeout,
                attempts=attempts, delay_seconds=delay_seconds,
            )
            if capture_cache is not None:
                capture_cache[replay_url] = html
        signals = page_content_signals(html, str(row["full_name"]), str(row["geo_name"]))
        result.update({
            "capture_fetch_status": "fetched",
            "capture_content_sha256": signals["content_sha256"],
            "candidate_identity_match": signals["candidate_identity_match"],
            "mayoral_campaign_match": signals["mayoral_campaign_match"],
            "city_match": signals["city_match"],
            "content_signal_summary": signals["content_signal_summary"],
        })
    except ArchiveItValidationRequestError as error:
        result.update({
            "capture_fetch_status": "request_error",
            "domain_reuse_status": "not_inspected_capture_error",
            "temporal_validation_status": "request_error_retryable",
            "validation_reason": "capture_request_error_retryable",
            "error": f"{type(error).__name__}: {error}"[:1000],
        })
        return result

    probes = _reuse_probe_candidates(
        eligible, nearest, reuse_start, reuse_end, reuse_probe_limit,
    )
    result["reuse_probe_timestamps"] = " | ".join(probe.timestamp for probe in probes)
    result["reuse_probe_count"] = len(probes)
    reuse_conflicts = 0
    try:
        for probe in probes:
            probe_url = collection_replay_url(int(row["collection_id"]), probe)
            if capture_cache is not None and probe_url in capture_cache:
                probe_html = capture_cache[probe_url]
            else:
                probe_html = fetch_collection_capture(
                    int(row["collection_id"]), probe, timeout=timeout,
                    attempts=attempts, delay_seconds=delay_seconds,
                )
                if capture_cache is not None:
                    capture_cache[probe_url] = probe_html
            probe_signals = page_content_signals(
                probe_html, str(row["full_name"]), str(row["geo_name"]),
            )
            if not probe_signals["candidate_identity_match"]:
                reuse_conflicts += 1
        result["reuse_conflict_count"] = reuse_conflicts
        if reuse_conflicts:
            result["domain_reuse_status"] = "conflicting_content_detected"
        elif probes:
            result["domain_reuse_status"] = "same_candidate_only"
        elif len(captures_in_reuse_window) > 1:
            result["domain_reuse_status"] = "no_distinct_content_to_probe"
        else:
            result["domain_reuse_status"] = "no_additional_capture"
    except ArchiveItValidationRequestError as error:
        result.update({
            "domain_reuse_status": "request_error",
            "error": f"{type(error).__name__}: {error}"[:1000],
        })

    reason = _validation_reason(result)
    result["validation_reason"] = reason
    if reason == "grade_a_exact_url_election_period_content_confirmed":
        result.update({
            "proposed_evidence_grade": "A",
            "strict_coverage_eligible": True,
            "temporal_validation_status": "grade_a_content_confirmed_pending_review",
        })
    elif result["domain_reuse_status"] == "request_error":
        result["temporal_validation_status"] = "request_error_retryable"
    elif not within_headline:
        result["temporal_validation_status"] = reason
    else:
        result["temporal_validation_status"] = "content_validation_unresolved"
    if str(result["url_type"]).startswith("excluded_"):
        result["strict_coverage_eligible"] = False
        result["proposed_evidence_grade"] = "C"
    if result["proposed_evidence_grade"] not in {"A", "C"}:
        raise AssertionError("Archive-It validator may only emit grade A or C")
    return result


def _validate_input_contract(evidence: pd.DataFrame) -> pd.DataFrame:
    required = {
        "claim_id", "source_id", "exact_normalized_url", "collection_id",
        "candidate_year_id", "full_name", "geo_name", "state_abb", "year",
        "month", "url_type",
    }
    missing = required - set(evidence.columns)
    if missing:
        raise ValueError(f"Archive-It evidence missing columns: {sorted(missing)}")
    result = evidence.copy()
    for column in VALIDATION_KEY:
        invalid = result[column].isna() | result[column].astype(str).str.strip().eq("")
        if invalid.any():
            raise ValueError(f"Archive-It evidence has missing {column}")
    if result.duplicated(VALIDATION_KEY).any():
        raise ValueError("Archive-It evidence has duplicate validation keys")
    result["month"] = pd.to_numeric(result["month"], errors="coerce").astype("Int64")
    return result


def _target_key_set(frame: pd.DataFrame) -> set[tuple[str, str, str]]:
    return set(zip(*(frame[column].astype(str) for column in VALIDATION_KEY)))


def build_review_queue(evidence: pd.DataFrame, validation: pd.DataFrame) -> pd.DataFrame:
    if validation.duplicated(VALIDATION_KEY).any():
        raise ValueError("Archive-It validation has duplicate keys")
    validation_fields = VALIDATION_KEY + [
        "validation_method_version", "archive_query_status", "validation_reason",
        "nearest_capture_timestamp", "nearest_capture_days_absolute",
        "within_headline_window", "within_sensitivity_window", "capture_replay_url",
        "candidate_identity_match", "mayoral_campaign_match", "city_match",
        "domain_reuse_status", "proposed_evidence_grade",
        "strict_coverage_eligible", "temporal_validation_status",
    ]
    merged = evidence.merge(
        validation[validation_fields], on=VALIDATION_KEY, how="left",
        validate="one_to_one", suffixes=("", "_archive_validation"),
    )
    grade = merged["proposed_evidence_grade"].fillna("C")
    if not set(grade).issubset({"A", "C"}):
        raise AssertionError("Archive-It review queue cannot contain grade B")
    merged["evidence_grade"] = grade
    merged["strict_coverage_eligible"] = merged[
        "strict_coverage_eligible_archive_validation"
    ].fillna(False).astype(bool)
    merged["temporal_validation_status"] = merged[
        "temporal_validation_status_archive_validation"
    ].fillna("validation_not_completed")
    completed_capture = merged["nearest_capture_timestamp"].fillna("").astype(str)
    if "source_snapshot_timestamp" not in merged:
        merged["source_snapshot_timestamp"] = ""
    merged["source_snapshot_timestamp"] = merged[
        "source_snapshot_timestamp"
    ].fillna("").astype("string")
    merged.loc[completed_capture.ne(""), "source_snapshot_timestamp"] = completed_capture
    merged["archive_validation_method_version"] = merged["validation_method_version"]
    merged["archive_validation_reason"] = merged["validation_reason"]
    merged["archive_nearest_capture_timestamp"] = merged["nearest_capture_timestamp"]
    merged["archive_nearest_capture_days_absolute"] = merged["nearest_capture_days_absolute"]
    merged["archive_within_headline_window"] = merged["within_headline_window"]
    merged["archive_within_sensitivity_window"] = merged["within_sensitivity_window"]
    merged["archive_capture_replay_url"] = merged["capture_replay_url"]
    merged["archive_candidate_identity_match"] = merged["candidate_identity_match"]
    merged["archive_mayoral_campaign_match"] = merged["mayoral_campaign_match"]
    merged["archive_city_match"] = merged["city_match"]
    merged["archive_domain_reuse_status"] = merged["domain_reuse_status"]
    drop_columns = [
        "validation_method_version", "validation_reason", "nearest_capture_timestamp",
        "nearest_capture_days_absolute", "within_headline_window",
        "within_sensitivity_window", "capture_replay_url", "candidate_identity_match",
        "mayoral_campaign_match", "city_match", "domain_reuse_status",
        "proposed_evidence_grade", "strict_coverage_eligible_archive_validation",
        "temporal_validation_status_archive_validation",
    ]
    merged = merged.drop(columns=[column for column in drop_columns if column in merged])
    original = evidence.columns.tolist()
    ordered = original + [column for column in REVIEW_COLUMNS_ADDED if column not in original]
    return merged.reindex(columns=ordered)


def validate_claims(
    evidence: pd.DataFrame,
    validation_path: Path,
    review_queue_path: Path,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    reuse_probe_limit: int = MAX_REUSE_PROBES,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    targets = _validate_input_contract(evidence)
    target_keys = _target_key_set(targets)
    contexts = {
        tuple(str(row[column]) for column in VALIDATION_KEY): _stable_context_hash(row)
        for row in targets.to_dict("records")
    }

    prior = (
        pd.read_csv(validation_path, low_memory=False)
        if validation_path.exists() else pd.DataFrame(columns=VALIDATION_COLUMNS)
    )
    for column in VALIDATION_COLUMNS:
        if column not in prior:
            prior[column] = pd.NA
    if not prior.empty:
        prior_keys = list(zip(*(prior[column].astype(str) for column in VALIDATION_KEY)))
        keep = [
            key in target_keys
            and str(prior.iloc[index]["validation_method_version"]) == VALIDATION_METHOD_VERSION
            and str(prior.iloc[index]["validation_context_hash"]) == contexts.get(key, "")
            for index, key in enumerate(prior_keys)
        ]
        prior = prior.loc[keep].drop_duplicates(VALIDATION_KEY, keep="last")
    rows = prior.reindex(columns=VALIDATION_COLUMNS).to_dict("records")
    _write_csv_atomic(pd.DataFrame(rows), validation_path, VALIDATION_COLUMNS)

    completed = {
        tuple(str(row[column]) for column in VALIDATION_KEY)
        for row in rows if _completed_validation(row)
    }
    query_cache: dict[tuple[int, str], list[Capture]] = {}
    capture_cache: dict[str, str] = {}
    for target in targets.to_dict("records"):
        key = tuple(str(target[column]) for column in VALIDATION_KEY)
        if key in completed:
            continue
        rows = [
            row for row in rows
            if tuple(str(row[column]) for column in VALIDATION_KEY) != key
        ]
        result = validate_one_claim(
            target, timeout=timeout, attempts=attempts,
            delay_seconds=delay_seconds, reuse_probe_limit=reuse_probe_limit,
            query_cache=query_cache, capture_cache=capture_cache,
        )
        rows.append(result)
        _write_csv_atomic(pd.DataFrame(rows), validation_path, VALIDATION_COLUMNS)
        time.sleep(delay_seconds)

    validation = pd.DataFrame(rows).reindex(columns=VALIDATION_COLUMNS)
    result_keys = _target_key_set(validation)
    if result_keys != target_keys or len(validation) != len(target_keys):
        raise ValueError(
            "Archive-It validation/evidence key mismatch: "
            f"missing={len(target_keys - result_keys)}, "
            f"unexpected={len(result_keys - target_keys)}, "
            f"duplicate_rows={len(validation) - len(result_keys)}"
        )
    validation = validation.sort_values(VALIDATION_KEY).reset_index(drop=True)
    _write_csv_atomic(validation, validation_path, VALIDATION_COLUMNS)
    review = build_review_queue(targets, validation)
    _write_csv_atomic(review, review_queue_path, review.columns.tolist())
    return validation, review


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--evidence",
        default="data/municipal_discovery/archiveit/mayoral_url_evidence_archiveit.csv",
    )
    parser.add_argument(
        "--validation-output",
        default="data/municipal_discovery/archiveit/mayoral_url_archiveit_validation.csv",
    )
    parser.add_argument(
        "--review-output",
        default="data/municipal_discovery/archiveit/mayoral_url_evidence_archiveit_review.csv",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--reuse-probe-limit", type=int, default=MAX_REUSE_PROBES)
    args = parser.parse_args()
    if not 0 <= args.reuse_probe_limit <= MAX_REUSE_PROBES:
        parser.error(f"--reuse-probe-limit must be between 0 and {MAX_REUSE_PROBES}")

    validation, review = validate_claims(
        pd.read_csv(args.evidence, low_memory=False),
        Path(args.validation_output), Path(args.review_output),
        timeout=args.timeout, attempts=args.attempts,
        delay_seconds=args.delay_seconds, reuse_probe_limit=args.reuse_probe_limit,
    )
    summary = {
        "validation_rows": len(validation),
        "proposed_grades": {
            str(key): int(value)
            for key, value in validation["proposed_evidence_grade"].value_counts().sort_index().items()
        },
        "validation_reasons": {
            str(key): int(value)
            for key, value in validation["validation_reason"].value_counts().sort_index().items()
        },
        "retryable_request_errors": int((
            validation["archive_query_status"].eq("request_error")
            | validation["capture_fetch_status"].eq("request_error")
            | validation["domain_reuse_status"].eq("request_error")
        ).sum()),
        "review_rows": len(review),
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
