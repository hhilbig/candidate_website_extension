#!/usr/bin/env python3
"""Validate frozen search URL leads with archive.org's availability API.

The module is deliberately downstream of the separately frozen ownership
gate.  It never expands or recomputes candidate ownership, never imputes a
missing election month, and never treats capture metadata as content proof.
"""

from __future__ import annotations

import argparse
import calendar
import hashlib
import json
import os
import re
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import urlencode, urlsplit, urlunsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup


REPO_ROOT = Path(__file__).resolve().parents[1]
SEARCH_ROOT = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/search_source_expansion"
)
VALIDATION_ROOT = SEARCH_ROOT / "temporal_validation"
DEFAULT_FROZEN_LEADS = VALIDATION_ROOT / "frozen/eligible_leads.csv"
DEFAULT_FROZEN_MANIFEST = VALIDATION_ROOT / "frozen/manifest.json"
DEFAULT_SHARD_DIR = VALIDATION_ROOT / "shards"

FREEZE_CONTRACT_VERSION = "search_lead_wayback_available_freeze_v1"
VALIDATION_METHOD_VERSION = "search_lead_wayback_available_content_v1"
CONTENT_SIGNAL_VERSION = "mayoral_content_signals_v2"
FREEZE_MANIFEST_VERSION = "search-lead-wayback-available-manifest-v1"
ARCHIVE_AVAILABLE_ENDPOINT = "https://archive.org/wayback/available"
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; archive validation)"

ELIGIBLE_URL_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}
IDENTITY_SUPPORTED_STATUS = "identity_supported"
HEADLINE_WINDOW_DAYS = 365
SENSITIVITY_MONTHS_BEFORE = 18
SENSITIVITY_DAYS_AFTER = 120
REUSE_MONTHS_EACH_SIDE = 24
HEX64 = re.compile(r"[0-9a-f]{64}")

FROZEN_LEAD_COLUMNS = [
    "freeze_contract_version",
    "validation_method_version",
    "validation_key",
    "validation_hash",
    "lead_record_hash",
    "candidate_context_hash",
    "validation_shard_id",
    "validation_shard_count",
    "eligible_for_temporal_validation",
    "eligibility_reason",
    "contract_version",
    "url_claim_id",
    "url_entity_id",
    "result_claim_id",
    "query_attempt_id",
    "page_link_id",
    "page_id",
    "source_page_url",
    "discovery_candidate_year_id",
    "discovery_candidate_contest_id",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "in_candidate_screen",
    "raw_url",
    "exact_normalized_url",
    "site_root_url",
    "registrable_domain",
    "url_type",
    "candidate_attribution_status",
    "candidate_attribution_reason",
    "matched_candidate_ids_json",
    "source_locator",
    "extraction_method",
    "evidence_text",
    "evidence_grade",
    "strict_coverage_eligible",
    "review_status",
    "acceptance",
    "parser_version",
]

VALIDATION_COLUMNS = FROZEN_LEAD_COLUMNS + [
    "archive_query_target",
    "availability_api_url",
    "availability_query_timestamp",
    "availability_status",
    "availability_http_requests",
    "availability_probe_summary_json",
    "nearest_capture_available",
    "nearest_capture_timestamp",
    "nearest_capture_url",
    "nearest_capture_http_status",
    "election_reference_date",
    "headline_window_days",
    "sensitivity_window_start",
    "sensitivity_window_end",
    "reuse_window_start",
    "reuse_window_end",
    "nearest_capture_days_signed",
    "nearest_capture_days_absolute",
    "within_headline_window",
    "within_sensitivity_window",
    "capture_fetch_status",
    "capture_requested_timestamp",
    "capture_primary_requested_url",
    "capture_requested_url",
    "capture_effective_timestamp",
    "capture_effective_url",
    "capture_replay_source",
    "capture_primary_error",
    "capture_content_bytes",
    "capture_content_sha256",
    "timing_capture_timestamp",
    "timing_capture_basis",
    "candidate_identity_match",
    "mayoral_campaign_match",
    "city_match",
    "content_signal_summary",
    "reuse_capture_timestamps",
    "reuse_fetch_summary_json",
    "reuse_capture_count",
    "reuse_fetched_count",
    "reuse_conflict_count",
    "domain_reuse_status",
    "proposed_evidence_grade",
    "strict_coverage_eligible_result",
    "temporal_validation_status",
    "validation_reason",
    "retryable",
    "error",
    "validated_on",
]


class SearchLeadWaybackValidationError(ValueError):
    """A frozen lead or validation checkpoint violates the fixed contract."""


class ArchiveAvailableRequestError(RuntimeError):
    """The public archive availability or replay request failed."""


@dataclass(frozen=True)
class AvailableCapture:
    timestamp: str
    url: str
    status: int

    @property
    def captured_at(self) -> datetime:
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S")


@dataclass(frozen=True)
class FetchedArchiveContent:
    html: str
    requested_timestamp: str
    primary_requested_url: str
    requested_url: str
    effective_timestamp: str
    effective_url: str
    replay_source: str
    content_bytes: int
    content_sha256: str
    primary_error: str = ""

    @property
    def effective_captured_at(self) -> datetime:
        return datetime.strptime(self.effective_timestamp, "%Y%m%d%H%M%S")


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _as_bool(value: object, label: str) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).casefold()
    if token in {"true", "1", "yes"}:
        return True
    if token in {"false", "0", "no", ""}:
        return False
    raise SearchLeadWaybackValidationError(
        f"{label} has invalid boolean value {value!r}"
    )


def _as_int(value: object, label: str, *, allow_blank: bool = False) -> int | None:
    token = _text(value)
    if not token and allow_blank:
        return None
    if not re.fullmatch(r"-?\d+(?:\.0+)?", token):
        raise SearchLeadWaybackValidationError(
            f"{label} has invalid integer value {value!r}"
        )
    return int(float(token))


def _frozen_value_equal(column: str, left: object, right: object) -> bool:
    """Compare frozen values without treating CSV integer formatting as drift."""
    if column == "month":
        return _as_int(left, column, allow_blank=True) == _as_int(
            right, column, allow_blank=True
        )
    return _text(left) == _text(right)


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != columns:
        raise SearchLeadWaybackValidationError(
            f"{label} column contract drift; "
            f"missing={sorted(set(columns) - set(actual))}, "
            f"unexpected={sorted(set(actual) - set(columns))}, "
            f"order_matches={actual == columns}"
        )


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=VALIDATION_COLUMNS).to_csv(
            handle, index=False, lineterminator="\n"
        )
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def validation_key(
    url_claim_id: object, candidate_year_id: object, exact_normalized_url: object
) -> str:
    return "\x1f".join(
        map(_text, [url_claim_id, candidate_year_id, exact_normalized_url])
    )


def validation_hash(key: object) -> str:
    return hashlib.sha256(_text(key).encode("utf-8")).hexdigest()


def _resolve_manifest_path(value: object) -> Path:
    path = Path(_text(value))
    return path if path.is_absolute() else REPO_ROOT / path


def _validate_frozen_frame(
    frozen: pd.DataFrame, *, shard_id: int | None = None
) -> pd.DataFrame:
    _require_exact_columns(frozen, FROZEN_LEAD_COLUMNS, "frozen search leads")
    if frozen.empty:
        raise SearchLeadWaybackValidationError("Frozen search leads are empty")
    if frozen["validation_key"].map(_text).duplicated().any():
        raise SearchLeadWaybackValidationError(
            "Frozen search leads contain duplicate validation keys"
        )
    if frozen[
        ["url_claim_id", "candidate_year_id", "exact_normalized_url"]
    ].astype(str).duplicated().any():
        raise SearchLeadWaybackValidationError(
            "Frozen search leads contain duplicate tuple keys"
        )
    shard_counts = {
        int(_as_int(value, "validation_shard_count"))
        for value in frozen["validation_shard_count"]
    }
    if len(shard_counts) != 1 or next(iter(shard_counts)) <= 0:
        raise SearchLeadWaybackValidationError(
            "Frozen search leads must carry one positive shard count"
        )
    shard_count = next(iter(shard_counts))
    for row in frozen.to_dict("records"):
        claim_id = _text(row["url_claim_id"])
        key = validation_key(
            row["url_claim_id"],
            row["candidate_year_id"],
            row["exact_normalized_url"],
        )
        digest = validation_hash(key)
        if _text(row["validation_key"]) != key or _text(row["validation_hash"]) != digest:
            raise SearchLeadWaybackValidationError(
                f"Frozen validation key/hash drift for {claim_id}"
            )
        if _text(row["freeze_contract_version"]) != FREEZE_CONTRACT_VERSION:
            raise SearchLeadWaybackValidationError(
                f"Freeze contract drift for {claim_id}"
            )
        if _text(row["validation_method_version"]) != VALIDATION_METHOD_VERSION:
            raise SearchLeadWaybackValidationError(
                f"Validation method drift for {claim_id}"
            )
        for field in ("lead_record_hash", "candidate_context_hash"):
            if not HEX64.fullmatch(_text(row[field])):
                raise SearchLeadWaybackValidationError(
                    f"Invalid {field} for {claim_id}"
                )
        row_shard = int(_as_int(row["validation_shard_id"], "validation_shard_id"))
        if row_shard != int(digest, 16) % shard_count:
            raise SearchLeadWaybackValidationError(
                f"Frozen shard assignment drift for {claim_id}"
            )
        if not _as_bool(
            row["eligible_for_temporal_validation"],
            "eligible_for_temporal_validation",
        ):
            raise SearchLeadWaybackValidationError(
                f"Frozen eligible manifest contains a gated-out lead: {claim_id}"
            )
        if not _text(row["eligibility_reason"]):
            raise SearchLeadWaybackValidationError(
                f"Frozen lead lacks an explicit eligibility reason: {claim_id}"
            )
        if _text(row["candidate_attribution_status"]) != IDENTITY_SUPPORTED_STATUS:
            raise SearchLeadWaybackValidationError(
                f"Frozen lead is not identity-supported: {claim_id}"
            )
        if _text(row["url_type"]) not in ELIGIBLE_URL_TYPES:
            raise SearchLeadWaybackValidationError(
                f"Frozen lead has an ineligible URL type: {claim_id}"
            )
        if _text(row["evidence_grade"]) != "D":
            raise SearchLeadWaybackValidationError(
                f"Frozen search lead is not grade D: {claim_id}"
            )
        if _as_bool(row["strict_coverage_eligible"], "strict_coverage_eligible"):
            raise SearchLeadWaybackValidationError(
                f"Frozen source lead already claims strict coverage: {claim_id}"
            )
        if _text(row["review_status"]) != "pending" or _text(row["acceptance"]):
            raise SearchLeadWaybackValidationError(
                f"Frozen source lead is not pending and unapproved: {claim_id}"
            )
        exact_url = _text(row["exact_normalized_url"])
        parsed = urlsplit(exact_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise SearchLeadWaybackValidationError(
                f"Frozen lead has invalid exact URL: {claim_id}"
            )
        for field in (
            "url_claim_id",
            "result_claim_id",
            "query_attempt_id",
            "discovery_candidate_year_id",
            "discovery_candidate_contest_id",
            "candidate_year_id",
            "candidate_contest_id",
            "full_name",
            "geo_name",
            "year",
            "parser_version",
        ):
            if not _text(row[field]):
                raise SearchLeadWaybackValidationError(
                    f"Frozen lead lacks {field}: {claim_id}"
                )
        month = _as_int(row["month"], f"{claim_id}.month", allow_blank=True)
        if month is not None and not 1 <= month <= 12:
            raise SearchLeadWaybackValidationError(
                f"Frozen lead has invalid election month: {claim_id}"
            )
    if shard_id is not None:
        if not 0 <= shard_id < shard_count:
            raise SearchLeadWaybackValidationError(
                f"shard_id must be between 0 and {shard_count - 1}"
            )
        frozen = frozen.loc[
            pd.to_numeric(frozen["validation_shard_id"], errors="raise").astype(int)
            == shard_id
        ]
    return frozen.sort_values(
        ["validation_hash", "url_claim_id"], kind="stable"
    ).reset_index(drop=True)


def load_frozen_targets(
    leads_path: Path | str,
    manifest_path: Path | str,
    shard_id: int | None = None,
) -> pd.DataFrame:
    """Load exact hash-verified eligible search leads from the root freeze."""
    leads_path = Path(leads_path).resolve()
    manifest_path = Path(manifest_path).resolve()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise SearchLeadWaybackValidationError(
            f"Cannot read frozen lead manifest: {error}"
        ) from error
    expected_scalars = {
        "manifest_version": FREEZE_MANIFEST_VERSION,
        "freeze_contract_version": FREEZE_CONTRACT_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "required_identity_status": IDENTITY_SUPPORTED_STATUS,
    }
    for field, expected in expected_scalars.items():
        if manifest.get(field) != expected:
            raise SearchLeadWaybackValidationError(
                f"Frozen lead manifest {field} drift"
            )
    if manifest.get("key_definition") != (
        "url_claim_id + U+001F + candidate_year_id + U+001F + "
        "exact_normalized_url"
    ):
        raise SearchLeadWaybackValidationError(
            "Frozen lead manifest key definition drift"
        )
    record = manifest.get("eligible_output")
    if not isinstance(record, Mapping):
        raise SearchLeadWaybackValidationError(
            "Frozen lead manifest lacks eligible_output"
        )
    if record.get("columns") != FROZEN_LEAD_COLUMNS:
        raise SearchLeadWaybackValidationError(
            "Frozen lead manifest column contract drift"
        )
    if set(manifest.get("coverage_url_types", [])) != ELIGIBLE_URL_TYPES:
        raise SearchLeadWaybackValidationError(
            "Frozen lead manifest eligible URL types drift"
        )
    shard_count = _as_int(
        manifest.get("validation_shard_count"), "manifest.validation_shard_count"
    )
    if shard_count is None or shard_count <= 0:
        raise SearchLeadWaybackValidationError(
            "Frozen lead manifest has invalid shard count"
        )
    if _resolve_manifest_path(record.get("path", "")).resolve() != leads_path:
        raise SearchLeadWaybackValidationError(
            "Frozen eligible-leads path differs from the manifest"
        )
    if not leads_path.is_file():
        raise SearchLeadWaybackValidationError(
            f"Frozen eligible leads are missing: {leads_path}"
        )
    if leads_path.stat().st_size != int(record.get("bytes", -1)):
        raise SearchLeadWaybackValidationError("Frozen eligible-leads byte count drift")
    if _sha256_file(leads_path) != record.get("sha256"):
        raise SearchLeadWaybackValidationError("Frozen eligible-leads SHA-256 drift")
    frozen = pd.read_csv(leads_path, keep_default_na=False, low_memory=False)
    if len(frozen) != int(record.get("rows", -1)):
        raise SearchLeadWaybackValidationError("Frozen eligible-leads row count drift")
    if len(frozen) != int(manifest.get("eligible_rows", -1)):
        raise SearchLeadWaybackValidationError("Frozen eligible-leads summary drift")
    if set(pd.to_numeric(frozen["validation_shard_count"], errors="raise")) != {
        shard_count
    }:
        raise SearchLeadWaybackValidationError(
            "Frozen lead rows disagree with manifest shard count"
        )
    targets = _validate_frozen_frame(frozen, shard_id=shard_id)
    if shard_id is not None:
        expected_rows = int(
            manifest.get("validation_shard_rows", {}).get(str(shard_id), 0)
        )
        if len(targets) != expected_rows:
            raise SearchLeadWaybackValidationError(
                f"Frozen shard {shard_id} row-count drift"
            )
    return targets


def shift_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + months
    year, zero_month = divmod(absolute, 12)
    month = zero_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def _timestamp(value: date) -> str:
    return f"{value.strftime('%Y%m%d')}120000"


def archive_available_url(exact_url: str, target_timestamp: str) -> str:
    parsed = urlsplit(str(exact_url))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise SearchLeadWaybackValidationError(
            f"Invalid exact URL for archive availability query: {exact_url!r}"
        )
    if not re.fullmatch(r"\d{14}", str(target_timestamp)):
        raise SearchLeadWaybackValidationError(
            f"Invalid archive availability timestamp: {target_timestamp!r}"
        )
    return f"{ARCHIVE_AVAILABLE_ENDPOINT}?{urlencode([('url', exact_url), ('timestamp', target_timestamp)])}"


def _clean_error(error: BaseException) -> str:
    message = re.sub(r"0x[0-9a-fA-F]+", "0x...", str(error))
    return f"{type(error).__name__}: {message}"[:1000]


def _request(
    url: str,
    *,
    timeout: int,
    attempts: int,
    delay_seconds: float,
    session: Any,
) -> Any:
    if attempts < 1 or timeout <= 0 or delay_seconds < 0:
        raise SearchLeadWaybackValidationError(
            "attempts/timeout must be positive and delay nonnegative"
        )
    last_error = "archive request failed"
    retry_after = 0.0
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=timeout
            )
            status = int(response.status_code)
            if status in {408, 425, 429} or status >= 500:
                last_error = f"HTTP {status}"
                try:
                    retry_after = max(
                        0.0,
                        float((getattr(response, "headers", {}) or {}).get("Retry-After", 0) or 0),
                    )
                except (TypeError, ValueError):
                    retry_after = 0.0
            elif status >= 400:
                raise ArchiveAvailableRequestError(f"HTTP {status} for {url}")
            else:
                if delay_seconds:
                    time.sleep(delay_seconds)
                return response
        except ArchiveAvailableRequestError:
            raise
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < attempts:
            wait = max(delay_seconds * (2 ** (attempt - 1)), retry_after)
            if wait:
                time.sleep(min(wait, 60.0))
    raise ArchiveAvailableRequestError(f"{last_error} for {url}")


def _parse_available_payload(payload: object) -> AvailableCapture | None:
    if not isinstance(payload, Mapping):
        raise ArchiveAvailableRequestError("Archive availability JSON is not an object")
    snapshots = payload.get("archived_snapshots", {})
    if not isinstance(snapshots, Mapping):
        raise ArchiveAvailableRequestError(
            "Archive availability JSON has invalid archived_snapshots"
        )
    closest = snapshots.get("closest")
    if not closest:
        return None
    if not isinstance(closest, Mapping):
        raise ArchiveAvailableRequestError(
            "Archive availability JSON has invalid closest capture"
        )
    available = closest.get("available", False)
    if isinstance(available, str):
        available = available.casefold() == "true"
    if not available:
        return None
    timestamp = _text(closest.get("timestamp"))
    url = _text(closest.get("url"))
    try:
        status = int(closest.get("status", 0))
        capture = AvailableCapture(timestamp=timestamp, url=url, status=status)
        capture.captured_at
    except (TypeError, ValueError) as error:
        raise ArchiveAvailableRequestError(
            "Archive availability JSON has invalid capture metadata"
        ) from error
    if not url or urlsplit(url).scheme not in {"http", "https"} or status != 200:
        return None
    return capture


def query_archive_available(
    exact_url: str,
    target_timestamp: str,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> AvailableCapture | None:
    response = _request(
        archive_available_url(exact_url, target_timestamp),
        timeout=timeout,
        attempts=attempts,
        delay_seconds=delay_seconds,
        session=session,
    )
    try:
        payload = response.json()
    except ValueError as error:
        raise ArchiveAvailableRequestError(
            f"Invalid archive availability JSON: {error}"
        ) from error
    return _parse_available_payload(payload)


def _capture_fetch_url(capture: AvailableCapture) -> str:
    parsed = urlsplit(capture.url)
    scheme = "https" if parsed.scheme in {"http", "https"} else parsed.scheme
    path = parsed.path
    match = re.match(r"^/web/(\d{14})(?:id_)?/(.+)$", path)
    if match:
        path = f"/web/{match.group(1)}id_/{match.group(2)}"
    return urlunsplit((scheme, parsed.netloc, path, parsed.query, parsed.fragment))


def _capture_original_url(capture: AvailableCapture) -> str:
    parsed = urlsplit(capture.url)
    match = re.match(r"^/web/\d{14}(?:id_)?/(.+)$", parsed.path)
    if not match:
        raise ArchiveAvailableRequestError(
            f"Cannot recover original URL from availability replay {capture.url}"
        )
    original = match.group(1)
    if parsed.query:
        original += f"?{parsed.query}"
    return original


def archive_it_fallback_url(capture: AvailableCapture) -> str:
    original = _capture_original_url(capture)
    return (
        "https://wayback.archive-it.org/all/"
        f"{capture.timestamp}id_/{original}"
    )


def _effective_archive_timestamp(url: str) -> str:
    path = urlsplit(url).path
    match = re.search(r"/(?:web|all)/(\d{14})(?:id_)?/", path)
    if not match:
        raise ArchiveAvailableRequestError(
            f"Effective archive replay URL lacks a capture timestamp: {url}"
        )
    return match.group(1)


def archive_response_error(html: str) -> str:
    """Identify the Archive-It service's HTTP-200 rate-limit response.

    Generic historical error pages are not service errors. Require both the
    service heading and its archive.org contact or request-limit explanation.
    This helper also supports auditing already-saved response bodies.
    """
    soup = BeautifulSoup(str(html), "html.parser")
    headings = " ".join(
        tag.get_text(" ", strip=True) for tag in soup.find_all(["title", "h1"])
    )
    text = soup.get_text(" ", strip=True).casefold()
    if "rate limit reached" in headings.casefold() and (
        "aitratelimit@archive.org" in str(html).casefold()
        or "number of requests that can be made in a short period of time" in text
    ):
        return "archive_service_rate_limit_page"
    return ""


def _fetched_content(
    response: Any,
    *,
    capture: AvailableCapture,
    primary_requested_url: str,
    requested_url: str,
    replay_source: str,
    primary_error: str = "",
) -> FetchedArchiveContent:
    content_type = _text(
        (getattr(response, "headers", {}) or {}).get("content-type")
    ).casefold()
    if content_type and "html" not in content_type and "xhtml" not in content_type:
        raise ArchiveAvailableRequestError(
            f"Archived capture is not HTML ({content_type})"
        )
    effective_url = _text(getattr(response, "url", "")) or requested_url
    effective_timestamp = _effective_archive_timestamp(effective_url)
    raw_content = bytes(getattr(response, "content", b""))
    html = str(getattr(response, "text", ""))
    service_error = archive_response_error(html)
    if service_error:
        raise ArchiveAvailableRequestError(service_error)
    if not raw_content:
        raw_content = html.encode("utf-8")
    return FetchedArchiveContent(
        html=html,
        requested_timestamp=capture.timestamp,
        primary_requested_url=primary_requested_url,
        requested_url=requested_url,
        effective_timestamp=effective_timestamp,
        effective_url=effective_url,
        replay_source=replay_source,
        content_bytes=len(raw_content),
        content_sha256=hashlib.sha256(raw_content).hexdigest(),
        primary_error=primary_error,
    )


def fetch_archive_capture(
    capture: AvailableCapture,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> FetchedArchiveContent:
    """Fetch web.archive replay, then bounded Archive-It all fallback on error."""
    requested_url = _capture_fetch_url(capture)
    try:
        response = _request(
            requested_url,
            timeout=timeout,
            attempts=attempts,
            delay_seconds=delay_seconds,
            session=session,
        )
        return _fetched_content(
            response,
            capture=capture,
            primary_requested_url=requested_url,
            requested_url=requested_url,
            replay_source="web_archive",
        )
    except ArchiveAvailableRequestError as primary_error:
        fallback_url = archive_it_fallback_url(capture)
        try:
            response = _request(
                fallback_url,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
                session=session,
            )
        except ArchiveAvailableRequestError as fallback_error:
            raise ArchiveAvailableRequestError(
                "primary_web_archive_error="
                f"{_clean_error(primary_error)}; "
                "archive_it_all_fallback_error="
                f"{_clean_error(fallback_error)}"
            ) from fallback_error
        return _fetched_content(
            response,
            capture=capture,
            primary_requested_url=requested_url,
            requested_url=fallback_url,
            replay_source="archive_it_all_fallback",
            primary_error=_clean_error(primary_error),
        )


def _repair_name(value: object) -> str:
    raw = _text(value)
    if any(marker in raw for marker in ("Ã", "Â", "â")):
        try:
            raw = raw.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError):
            pass
    return " ".join(raw.replace("\u00a0", " ").split())


def _normalize_text(value: object) -> str:
    ascii_value = unicodedata.normalize("NFKD", _text(value)).encode(
        "ascii", "ignore"
    ).decode()
    return " ".join(re.findall(r"[a-z0-9]+", ascii_value.casefold()))


def _candidate_aliases(name: object) -> list[tuple[str, ...]]:
    value = _repair_name(name).replace("“", '"').replace("”", '"')
    value = value.replace("‘", "'").replace("’", "'")
    nickname_pattern = re.compile(
        r'''(?<![A-Za-z0-9])'([^']+)'(?![A-Za-z0-9])|"([^"]+)"'''
    )
    nicknames = [
        _text(single or double)
        for single, double in nickname_pattern.findall(value)
        if _text(single or double)
    ]
    legal = " ".join(nickname_pattern.sub(" ", value).split())
    tokens = _normalize_text(legal).split()
    aliases: list[tuple[str, ...]] = []
    if len(tokens) >= 2:
        aliases.append(tuple(tokens))
        aliases.append((tokens[0], tokens[-1]))
        # A shortened first name still requires the supplied candidate surname.
        first_name_aliases = {"kenneth": "ken", "ken": "kenneth"}
        if tokens[0] in first_name_aliases:
            aliases.append((first_name_aliases[tokens[0]], tokens[-1]))
        aliases.extend(
            tuple(_normalize_text(f"{nickname} {tokens[-1]}").split())
            for nickname in nicknames
        )
    return list(dict.fromkeys(alias for alias in aliases if len(alias) >= 2))


def _city_aliases(city: str) -> list[str]:
    normalized = _normalize_text(city)
    if not normalized:
        return []
    aliases = [normalized]
    if normalized.startswith("st "):
        aliases.append("saint " + normalized[3:])
    elif normalized.startswith("saint "):
        aliases.append("st " + normalized[6:])
    if normalized == "nashville davidson":
        aliases.append("nashville")
    return aliases


def content_signals(html: str, candidate_name: str, city: str) -> dict[str, object]:
    soup = BeautifulSoup(str(html), "html.parser")
    for node in soup(["script", "style", "noscript", "svg"]):
        node.decompose()
    pieces: list[str] = []
    if soup.title:
        pieces.append(soup.title.get_text(" ", strip=True))
    for tag in soup.find_all("meta"):
        name = _text(tag.get("name") or tag.get("property")).casefold()
        if name in {
            "description",
            "og:title",
            "og:description",
            "twitter:title",
            "twitter:description",
        }:
            pieces.append(_text(tag.get("content")))
    pieces.append(soup.get_text(" ", strip=True))
    normalized = _normalize_text(" ".join(pieces))
    tokens = set(normalized.split())
    identity = any(set(alias).issubset(tokens) for alias in _candidate_aliases(candidate_name))
    city_aliases = _city_aliases(city)
    city_patterns = "|".join(re.escape(alias) for alias in city_aliases)
    campaign_patterns = [
        r"\bfor mayor\b",
        r"\bmayoral campaign\b",
        r"\bcampaign for mayor\b",
        r"\b(?:elect|reelect|re elect|vote for)\b.{0,80}\bmayor\b",
    ]
    if city_patterns:
        campaign_patterns.extend([
            rf"\bfor (?:{city_patterns}) mayor\b",
            rf"\bthe mayor (?:{city_patterns}) needs\b",
        ])
    for alias in _candidate_aliases(candidate_name):
        campaign_patterns.append(rf"\b{re.escape(' '.join(alias))} 4 mayor\b")
        if city_patterns:
            campaign_patterns.append(
                rf"\b{re.escape(' '.join(alias))} 4 (?:{city_patterns}) mayor\b"
            )
    mayoral = any(
        re.search(pattern, normalized)
        for pattern in campaign_patterns
    )
    # Employment/project headings can expose past candidacies on a personal CV.
    # Keep campaign sites with a resume section when the page also presents an
    # affirmative campaign identity or campaign participation/payment context.
    heading_text = {
        _normalize_text(tag.get_text(" ", strip=True))
        for tag in soup.find_all(re.compile(r"^h[1-6]$"))
    }
    cv_sections = {
        "company experience", "project experience", "education experience",
        "specialized experience", "employment experience", "work experience",
    }
    resume_structure = len(heading_text & cv_sections) >= 2 or bool(
        heading_text & {"resume", "curriculum vitae"}
    )
    branding = _normalize_text(" ".join(
        tag.get_text(" ", strip=True) for tag in soup.find_all(["title", "h1"])
    ))
    campaign_context = any(re.search(pattern, branding) for pattern in campaign_patterns) or bool(
        re.search(
            r"\bpaid for by\b|\bjoin (?:our|the) campaign\b|"
            r"\b(?:donate|volunteer) (?:to|for|with) (?:the |our )?campaign\b",
            normalized,
        )
    )
    resume_only = resume_structure and not campaign_context
    if resume_only:
        mayoral = False
    city_match = any(
        re.search(rf"\b{re.escape(alias)}\b", normalized) is not None
        for alias in city_aliases
    )
    return {
        "content_signal_version": CONTENT_SIGNAL_VERSION,
        "resume_only_campaign_history": resume_only,
        "candidate_identity_match": identity,
        "mayoral_campaign_match": mayoral,
        "city_match": city_match,
        "content_signal_summary": (
            f"version={CONTENT_SIGNAL_VERSION}; "
            f"identity={'yes' if identity else 'no'}; "
            f"mayoral_campaign={'yes' if mayoral else 'no'}; "
            f"city={'yes' if city_match else 'no'}; "
            f"resume_only={'yes' if resume_only else 'no'}"
        ),
        "content_sha256": hashlib.sha256(str(html).encode("utf-8")).hexdigest(),
    }


def _coerce_fetched_content(
    value: FetchedArchiveContent | str, capture: AvailableCapture
) -> FetchedArchiveContent:
    if isinstance(value, FetchedArchiveContent):
        return value
    if not isinstance(value, str):
        raise ArchiveAvailableRequestError(
            f"Archive fetch returned unsupported content type {type(value).__name__}"
        )
    raw = value.encode("utf-8")
    requested_url = _capture_fetch_url(capture)
    return FetchedArchiveContent(
        html=value,
        requested_timestamp=capture.timestamp,
        primary_requested_url=requested_url,
        requested_url=requested_url,
        effective_timestamp=capture.timestamp,
        effective_url=requested_url,
        replay_source="web_archive",
        content_bytes=len(raw),
        content_sha256=hashlib.sha256(raw).hexdigest(),
    )


def _apply_capture_timing(
    result: dict[str, object],
    timestamp: str,
    reference: date,
    sensitivity_start: date,
    sensitivity_end: date,
    *,
    basis: str,
) -> None:
    try:
        captured = datetime.strptime(timestamp, "%Y%m%d%H%M%S").date()
    except ValueError as error:
        raise ArchiveAvailableRequestError(
            f"Invalid timing capture timestamp {timestamp!r}"
        ) from error
    signed_days = (captured - reference).days
    result.update(
        {
            "timing_capture_timestamp": timestamp,
            "timing_capture_basis": basis,
            "nearest_capture_days_signed": signed_days,
            "nearest_capture_days_absolute": abs(signed_days),
            "within_headline_window": abs(signed_days) <= HEADLINE_WINDOW_DAYS,
            "within_sensitivity_window": sensitivity_start <= captured <= sensitivity_end,
        }
    )


def _base_result(row: Mapping[str, object]) -> dict[str, object]:
    result = {column: "" for column in VALIDATION_COLUMNS}
    for column in FROZEN_LEAD_COLUMNS:
        result[column] = row.get(column, "")
    result.update(
        {
            "archive_query_target": _text(row["exact_normalized_url"]),
            "availability_status": "not_attempted_missing_election_month",
            "availability_http_requests": 0,
            "availability_probe_summary_json": "[]",
            "nearest_capture_available": False,
            "headline_window_days": HEADLINE_WINDOW_DAYS,
            "nearest_capture_days_signed": pd.NA,
            "nearest_capture_days_absolute": pd.NA,
            "within_headline_window": False,
            "within_sensitivity_window": False,
            "capture_fetch_status": "not_attempted_missing_election_month",
            "capture_content_bytes": 0,
            "candidate_identity_match": False,
            "mayoral_campaign_match": False,
            "city_match": False,
            "reuse_capture_count": 0,
            "reuse_fetch_summary_json": "[]",
            "reuse_fetched_count": 0,
            "reuse_conflict_count": 0,
            "domain_reuse_status": "not_inspected_missing_election_month",
            "proposed_evidence_grade": "C",
            "strict_coverage_eligible_result": False,
            "temporal_validation_status": "missing_election_month",
            "validation_reason": "missing_election_month_no_imputation",
            "retryable": False,
            "validated_on": date.today().isoformat(),
        }
    )
    return result


def _probe_record(
    label: str,
    requested_timestamp: str,
    status: str,
    capture: AvailableCapture | None = None,
    error: str = "",
    *,
    response_statuses: Iterable[str] = (),
    disagreement: bool = False,
    source: str = "live_request",
) -> dict[str, object]:
    statuses = list(response_statuses)
    return {
        "label": label,
        "requested_timestamp": requested_timestamp,
        "status": status,
        "response_count": len(statuses),
        "response_statuses": statuses,
        "disagreement": bool(disagreement),
        "source": source,
        "capture_timestamp": capture.timestamp if capture else "",
        "capture_url": capture.url if capture else "",
        "capture_http_status": capture.status if capture else "",
        "error": error,
    }


def _canonical_probe_json(records: Iterable[Mapping[str, object]]) -> str:
    return json.dumps(list(records), ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _decision(result: Mapping[str, object]) -> tuple[str, str, str, bool, bool]:
    availability = _text(result["availability_status"])
    if availability == "not_attempted_missing_election_month":
        return "missing_election_month_no_imputation", "missing_election_month", "C", False, False
    if availability == "request_error":
        return "availability_request_error_retryable", "request_error_retryable", "C", False, True
    if not _as_bool(result["nearest_capture_available"], "nearest_capture_available"):
        return "no_archive_capture", "no_archive_capture", "C", False, False
    if _text(result["capture_fetch_status"]) == "request_error":
        return "capture_request_error_retryable", "request_error_retryable", "C", False, True
    if not _as_bool(result["within_headline_window"], "within_headline_window"):
        reason = (
            "outside_headline_365_within_sensitivity"
            if _as_bool(result["within_sensitivity_window"], "within_sensitivity_window")
            else "outside_headline_and_sensitivity_windows"
        )
        return reason, reason, "C", False, availability == "checked_with_partial_error"
    for field, reason in (
        ("candidate_identity_match", "candidate_identity_not_confirmed"),
        ("mayoral_campaign_match", "mayoral_campaign_not_confirmed"),
        ("city_match", "city_not_confirmed"),
    ):
        if not _as_bool(result[field], field):
            return reason, "content_validation_unresolved", "C", False, availability == "checked_with_partial_error"
    if availability == "checked_with_partial_error" or _text(result["domain_reuse_status"]) == "request_error":
        return "reuse_inspection_request_error_retryable", "request_error_retryable", "C", False, True
    if _text(result["domain_reuse_status"]) == "conflicting_content_detected":
        return "domain_reuse_conflict", "content_validation_unresolved", "C", False, False
    return (
        "grade_a_exact_url_election_period_content_confirmed",
        "grade_a_content_confirmed_pending_review",
        "A",
        True,
        False,
    )


def validate_one_lead(
    row: Mapping[str, object],
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    availability_cache: dict[tuple[str, str], AvailableCapture | None] | None = None,
    capture_cache: dict[str, FetchedArchiveContent | str] | None = None,
) -> dict[str, object]:
    """Validate one frozen lead while retaining every request error as retryable."""
    result = _base_result(row)
    month = _as_int(row.get("month"), "month", allow_blank=True)
    if month is None:
        result["month"] = ""
        return result
    if not 1 <= month <= 12:
        raise SearchLeadWaybackValidationError(f"Invalid election month: {month}")
    year = int(_as_int(row["year"], "year"))
    reference = date(year, month, 15)
    sensitivity_start = shift_months(reference, -SENSITIVITY_MONTHS_BEFORE)
    sensitivity_end = reference + timedelta(days=SENSITIVITY_DAYS_AFTER)
    reuse_start = shift_months(reference, -REUSE_MONTHS_EACH_SIDE)
    reuse_end = shift_months(reference, REUSE_MONTHS_EACH_SIDE)
    exact_url = _text(row["exact_normalized_url"])
    primary_timestamp = _timestamp(reference)
    result.update(
        {
            "month": month,
            "availability_api_url": archive_available_url(exact_url, primary_timestamp),
            "availability_query_timestamp": primary_timestamp,
            "election_reference_date": reference.isoformat(),
            "sensitivity_window_start": sensitivity_start.isoformat(),
            "sensitivity_window_end": sensitivity_end.isoformat(),
            "reuse_window_start": reuse_start.isoformat(),
            "reuse_window_end": reuse_end.isoformat(),
            "capture_fetch_status": "not_attempted_no_capture",
            "domain_reuse_status": "not_inspected_no_capture",
        }
    )
    probes: list[dict[str, object]] = []
    errors: list[str] = []
    requests_made = 0

    def query(label: str, requested: str) -> AvailableCapture | None:
        nonlocal requests_made
        cache_key = (exact_url, requested)
        response_statuses: list[str] = []
        try:
            if availability_cache is not None and cache_key in availability_cache:
                capture = availability_cache[cache_key]
                probes.append(
                    _probe_record(
                        label,
                        requested,
                        "checked" if capture else "no_capture",
                        capture,
                        response_statuses=(
                            "cached_capture" if capture else "cached_confirmed_no_capture",
                        ),
                        source="validated_run_cache",
                    )
                )
                return capture
            else:
                requests_made += 1
                capture = query_archive_available(
                    exact_url,
                    requested,
                    timeout=timeout,
                    attempts=attempts,
                    delay_seconds=delay_seconds,
                )
                response_statuses.append("capture" if capture else "no_capture")
                if capture is None:
                    # archive.org occasionally returns HTTP 200 with no closest
                    # snapshot for a URL that resolves on an identical request.
                    # Absence therefore requires two concordant live responses.
                    requests_made += 1
                    capture = query_archive_available(
                        exact_url,
                        requested,
                        timeout=timeout,
                        attempts=attempts,
                        delay_seconds=delay_seconds,
                    )
                    response_statuses.append("capture" if capture else "no_capture")
                if availability_cache is not None:
                    availability_cache[cache_key] = capture
            probes.append(
                _probe_record(
                    label,
                    requested,
                    "checked" if capture else "no_capture",
                    capture,
                    response_statuses=response_statuses,
                    disagreement=len(set(response_statuses)) > 1,
                )
            )
            return capture
        except (ArchiveAvailableRequestError, ValueError) as error:
            clean = _clean_error(error)
            errors.append(f"{label}:{clean}")
            response_statuses.append("request_error")
            probes.append(
                _probe_record(
                    label,
                    requested,
                    "request_error",
                    error=clean,
                    response_statuses=response_statuses,
                    disagreement=len(set(response_statuses)) > 1,
                )
            )
            return None

    nearest = query("election_reference", primary_timestamp)
    result["availability_http_requests"] = requests_made
    if probes[-1]["status"] == "request_error":
        result.update(
            {
                "availability_status": "request_error",
                "capture_fetch_status": "not_attempted_query_error",
                "domain_reuse_status": "not_inspected_query_error",
                "availability_probe_summary_json": _canonical_probe_json(probes),
                "error": " | ".join(errors),
            }
        )
        reason, status, grade, strict, retryable = _decision(result)
        result.update(
            validation_reason=reason,
            temporal_validation_status=status,
            proposed_evidence_grade=grade,
            strict_coverage_eligible_result=strict,
            retryable=retryable,
        )
        return result
    if nearest is None:
        result.update(
            {
                "availability_status": "no_capture",
                "availability_probe_summary_json": _canonical_probe_json(probes),
                "validation_reason": "no_archive_capture",
                "temporal_validation_status": "no_archive_capture",
            }
        )
        return result

    result.update(
        {
            "availability_status": "checked",
            "nearest_capture_available": True,
            "nearest_capture_timestamp": nearest.timestamp,
            "nearest_capture_url": nearest.url,
            "nearest_capture_http_status": nearest.status,
        }
    )
    _apply_capture_timing(
        result,
        nearest.timestamp,
        reference,
        sensitivity_start,
        sensitivity_end,
        basis="availability_metadata_capture_unfetched",
    )

    side_queries = [
        ("reuse_before_24m", _timestamp(reuse_start)),
        ("reuse_before_12m", _timestamp(shift_months(reference, -12))),
        ("reuse_after_12m", _timestamp(shift_months(reference, 12))),
        ("reuse_after_24m", _timestamp(reuse_end)),
    ]
    side_captures: list[AvailableCapture] = []
    side_error = False
    for label, requested in side_queries:
        capture = query(label, requested)
        if probes[-1]["status"] == "request_error":
            side_error = True
        elif capture is not None:
            side_captures.append(capture)
    result["availability_http_requests"] = requests_made
    result["availability_status"] = (
        "checked_with_partial_error" if side_error else "checked"
    )
    result["availability_probe_summary_json"] = _canonical_probe_json(probes)
    result.update(
        {
            "capture_requested_timestamp": nearest.timestamp,
            "capture_primary_requested_url": _capture_fetch_url(nearest),
            "capture_requested_url": _capture_fetch_url(nearest),
        }
    )

    try:
        cache_key = nearest.url
        if capture_cache is not None and cache_key in capture_cache:
            fetched_value = capture_cache[cache_key]
        else:
            fetched_value = fetch_archive_capture(
                nearest,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
            )
            if capture_cache is not None:
                capture_cache[cache_key] = fetched_value
        fetched_content = _coerce_fetched_content(fetched_value, nearest)
        signals = content_signals(
            fetched_content.html,
            _text(row["full_name"]),
            _text(row["geo_name"]),
        )
        _apply_capture_timing(
            result,
            fetched_content.effective_timestamp,
            reference,
            sensitivity_start,
            sensitivity_end,
            basis="effective_replay_capture",
        )
        result.update(
            {
                "capture_fetch_status": (
                    "fetched_archive_it_fallback"
                    if fetched_content.replay_source == "archive_it_all_fallback"
                    else "fetched"
                ),
                "capture_requested_timestamp": fetched_content.requested_timestamp,
                "capture_primary_requested_url": fetched_content.primary_requested_url,
                "capture_requested_url": fetched_content.requested_url,
                "capture_effective_timestamp": fetched_content.effective_timestamp,
                "capture_effective_url": fetched_content.effective_url,
                "capture_replay_source": fetched_content.replay_source,
                "capture_primary_error": fetched_content.primary_error,
                "capture_content_bytes": fetched_content.content_bytes,
                "capture_content_sha256": fetched_content.content_sha256,
                "candidate_identity_match": signals["candidate_identity_match"],
                "mayoral_campaign_match": signals["mayoral_campaign_match"],
                "city_match": signals["city_match"],
                "content_signal_summary": signals["content_signal_summary"],
            }
        )
    except ArchiveAvailableRequestError as error:
        errors.append(f"nearest_capture:{_clean_error(error)}")
        result.update(
            {
                "capture_fetch_status": "request_error",
                "domain_reuse_status": "not_inspected_capture_error",
                "error": " | ".join(errors),
            }
        )
        reason, status, grade, strict, retryable = _decision(result)
        result.update(
            validation_reason=reason,
            temporal_validation_status=status,
            proposed_evidence_grade=grade,
            strict_coverage_eligible_result=strict,
            retryable=retryable,
        )
        return result

    unique_side = {
        (capture.timestamp, capture.url): capture for capture in side_captures
    }
    before = sorted(
        (
            capture
            for capture in unique_side.values()
            if reuse_start <= capture.captured_at.date() < reference
            and capture.timestamp != nearest.timestamp
        ),
        key=lambda capture: capture.timestamp,
    )
    after = sorted(
        (
            capture
            for capture in unique_side.values()
            if reference < capture.captured_at.date() <= reuse_end
            and capture.timestamp != nearest.timestamp
        ),
        key=lambda capture: capture.timestamp,
    )
    selected_reuse = ([before[0]] if before else []) + ([after[-1]] if after else [])
    result["reuse_capture_timestamps"] = " | ".join(
        capture.timestamp for capture in selected_reuse
    )
    result["reuse_capture_count"] = len(selected_reuse)
    fetched = 0
    conflicts = 0
    reuse_fetch_error = False
    reuse_summaries: list[dict[str, object]] = []
    for capture in selected_reuse:
        try:
            if capture_cache is not None and capture.url in capture_cache:
                fetched_value = capture_cache[capture.url]
            else:
                fetched_value = fetch_archive_capture(
                    capture,
                    timeout=timeout,
                    attempts=attempts,
                    delay_seconds=delay_seconds,
                )
                if capture_cache is not None:
                    capture_cache[capture.url] = fetched_value
            fetched_content = _coerce_fetched_content(fetched_value, capture)
            fetched += 1
            signals = content_signals(
                fetched_content.html,
                _text(row["full_name"]),
                _text(row["geo_name"]),
            )
            if not signals["candidate_identity_match"]:
                conflicts += 1
            reuse_summaries.append(
                {
                    "requested_timestamp": fetched_content.requested_timestamp,
                    "requested_url": fetched_content.requested_url,
                    "effective_timestamp": fetched_content.effective_timestamp,
                    "effective_url": fetched_content.effective_url,
                    "replay_source": fetched_content.replay_source,
                    "fetch_status": "fetched",
                    "candidate_identity_match": bool(
                        signals["candidate_identity_match"]
                    ),
                    "primary_error": fetched_content.primary_error,
                }
            )
        except ArchiveAvailableRequestError as error:
            reuse_fetch_error = True
            clean = _clean_error(error)
            errors.append(f"reuse_capture:{capture.timestamp}:{clean}")
            reuse_summaries.append(
                {
                    "requested_timestamp": capture.timestamp,
                    "requested_url": _capture_fetch_url(capture),
                    "effective_timestamp": "",
                    "effective_url": "",
                    "replay_source": "",
                    "fetch_status": "request_error",
                    "candidate_identity_match": False,
                    "primary_error": clean,
                }
            )
    result.update(
        {
            "reuse_fetched_count": fetched,
            "reuse_conflict_count": conflicts,
            "reuse_fetch_summary_json": _canonical_probe_json(reuse_summaries),
            "error": " | ".join(errors),
        }
    )
    if side_error or reuse_fetch_error:
        result["domain_reuse_status"] = "request_error"
    elif conflicts:
        result["domain_reuse_status"] = "conflicting_content_detected"
    elif selected_reuse:
        result["domain_reuse_status"] = "same_candidate_only"
    else:
        result["domain_reuse_status"] = "no_qualifying_side_capture"

    reason, status, grade, strict, retryable = _decision(result)
    result.update(
        validation_reason=reason,
        temporal_validation_status=status,
        proposed_evidence_grade=grade,
        strict_coverage_eligible_result=strict,
        retryable=retryable,
    )
    if grade not in {"A", "C"}:
        raise AssertionError("Search-lead validator may emit only A or C")
    if strict and not (
        _text(result["capture_fetch_status"]).startswith("fetched")
        and _as_bool(result["candidate_identity_match"], "candidate_identity_match")
        and _as_bool(result["mayoral_campaign_match"], "mayoral_campaign_match")
        and _as_bool(result["city_match"], "city_match")
    ):
        raise AssertionError("Strict coverage requires all fetched archive content signals")
    return result


def _completed(row: Mapping[str, object]) -> bool:
    return bool(
        _text(row.get("temporal_validation_status"))
        and not _as_bool(row.get("retryable"), "retryable")
    )


def validate_leads(
    frozen: pd.DataFrame,
    validation_path: Path | str,
    shard_id: int,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
) -> pd.DataFrame:
    """Validate one shard with stale-row pruning and atomic checkpoints."""
    validation_path = Path(validation_path)
    targets = _validate_frozen_frame(frozen, shard_id=shard_id)
    target_lookup = {
        _text(row["validation_key"]): row for row in targets.to_dict("records")
    }
    prior = (
        pd.read_csv(validation_path, keep_default_na=False, low_memory=False)
        if validation_path.exists()
        else pd.DataFrame(columns=VALIDATION_COLUMNS)
    )
    _require_exact_columns(prior, VALIDATION_COLUMNS, "search-lead validation checkpoint")
    if prior["validation_key"].map(_text).duplicated().any():
        raise SearchLeadWaybackValidationError(
            "Search-lead validation checkpoint has duplicate keys"
        )
    rows: list[dict[str, object]] = []
    for prior_row in prior.to_dict("records"):
        target = target_lookup.get(_text(prior_row["validation_key"]))
        if target is None:
            continue
        if all(
            _frozen_value_equal(column, prior_row[column], target[column])
            for column in FROZEN_LEAD_COLUMNS
        ):
            rows.append(prior_row)
    _atomic_csv(pd.DataFrame(rows), validation_path)
    completed = {
        _text(row["validation_key"]) for row in rows if _completed(row)
    }
    availability_cache: dict[tuple[str, str], AvailableCapture | None] = {}
    capture_cache: dict[str, FetchedArchiveContent | str] = {}
    for target in targets.to_dict("records"):
        key = _text(target["validation_key"])
        if key in completed:
            continue
        rows = [row for row in rows if _text(row["validation_key"]) != key]
        rows.append(
            validate_one_lead(
                target,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
                availability_cache=availability_cache,
                capture_cache=capture_cache,
            )
        )
        rows.sort(
            key=lambda row: (
                _text(row["validation_hash"]),
                _text(row["url_claim_id"]),
            )
        )
        _atomic_csv(pd.DataFrame(rows), validation_path)
    result = pd.DataFrame(rows).reindex(columns=VALIDATION_COLUMNS)
    expected_keys = set(target_lookup)
    actual_keys = set(result["validation_key"].map(_text))
    if actual_keys != expected_keys or len(result) != len(expected_keys):
        raise SearchLeadWaybackValidationError(
            "Search-lead validation/freeze key mismatch: "
            f"missing={sorted(expected_keys - actual_keys)}, "
            f"unexpected={sorted(actual_keys - expected_keys)}, "
            f"duplicate_rows={len(result) - len(actual_keys)}"
        )
    result = result.sort_values(
        ["validation_hash", "url_claim_id"], kind="stable"
    ).reset_index(drop=True)
    _atomic_csv(result, validation_path)
    return result


def _summary(result: pd.DataFrame, output: Path) -> dict[str, object]:
    return {
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "output": str(output),
        "rows": int(len(result)),
        "availability_status_counts": {
            str(key): int(value)
            for key, value in result["availability_status"].value_counts().sort_index().items()
        },
        "temporal_validation_status_counts": {
            str(key): int(value)
            for key, value in result["temporal_validation_status"].value_counts().sort_index().items()
        },
        "proposed_grade_counts": {
            str(key): int(value)
            for key, value in result["proposed_evidence_grade"].value_counts().sort_index().items()
        },
        "strict_coverage_eligible": int(
            result["strict_coverage_eligible_result"].map(
                lambda value: _as_bool(value, "strict_coverage_eligible_result")
            ).sum()
        ),
        "retryable_rows": int(
            result["retryable"].map(lambda value: _as_bool(value, "retryable")).sum()
        ),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--leads", type=Path, default=DEFAULT_FROZEN_LEADS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_FROZEN_MANIFEST)
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    targets = load_frozen_targets(args.leads, args.manifest, args.shard_id)
    output = args.output or DEFAULT_SHARD_DIR / f"validation_shard_{args.shard_id}.csv"
    result = validate_leads(
        targets,
        output,
        args.shard_id,
        timeout=args.timeout,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
    )
    print(json.dumps(_summary(result, output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
