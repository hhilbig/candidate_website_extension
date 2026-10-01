#!/usr/bin/env python3
"""Validate frozen municipal candidate URLs with the public Wayback Machine.

The validator queries captures for the exact frozen URL, including its path,
query, and case.  It proposes grade A only when an election-period capture
identifies the candidate, a mayoral campaign, and the city, and when the
two-year domain-reuse inspection finds no conflicting candidate content.
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
from typing import Any, Mapping
from urllib.parse import urlencode, urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.freeze_municipal_temporal_validation_claims import (
    ELIGIBLE_URL_TYPES,
    FREEZE_CONTRACT_VERSION,
    FROZEN_CLAIM_COLUMNS,
    VALIDATION_METHOD_VERSION,
    VALIDATION_SHARD_COUNT,
    validation_evidence_hash,
    validation_key,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
VALIDATION_ROOT = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_wayback_validation"
)
DEFAULT_FROZEN_CLAIMS = VALIDATION_ROOT / "frozen/eligible_claims.csv"
DEFAULT_FROZEN_MANIFEST = VALIDATION_ROOT / "frozen/manifest.json"
DEFAULT_SHARD_DIR = VALIDATION_ROOT / "shards"

USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; public archive validation)"
WAYBACK_CDX_ENDPOINT = "https://web.archive.org/cdx/search/cdx"
WAYBACK_REPLAY_ROOT = "https://web.archive.org/web"
FREEZE_MANIFEST_VERSION = "municipal-curated-wayback-freeze-manifest-v1"
HEADLINE_WINDOW_DAYS = 365
SENSITIVITY_MONTHS_BEFORE = 18
SENSITIVITY_DAYS_AFTER = 120
REUSE_MONTHS_EACH_SIDE = 24
MAX_REUSE_PROBES = 2

VALIDATION_COLUMNS = [
    "freeze_contract_version",
    "validation_method_version",
    "validation_key",
    "evidence_hash",
    "evidence_record_hash",
    "context_hash",
    "identity_decision_hash",
    "validation_shard_id",
    "validation_shard_count",
    "evidence_id",
    "source_id",
    "target_id",
    "candidate_year_id",
    "contest",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "url_type",
    "raw_url",
    "exact_normalized_url",
    "archive_query_target",
    "archive_cdx_url",
    "archive_query_status",
    "archive_capture_count",
    "eligible_capture_count",
    "election_reference_date",
    "headline_window_days",
    "sensitivity_window_start",
    "sensitivity_window_end",
    "reuse_window_start",
    "reuse_window_end",
    "nearest_capture_timestamp",
    "nearest_capture_original_url",
    "nearest_capture_days_signed",
    "nearest_capture_days_absolute",
    "within_headline_window",
    "within_sensitivity_window",
    "capture_replay_url",
    "capture_fetch_status",
    "capture_content_sha256",
    "candidate_identity_match",
    "mayoral_campaign_match",
    "city_match",
    "content_signal_summary",
    "reuse_captures_before",
    "reuse_captures_after",
    "reuse_probe_timestamps",
    "reuse_probe_count",
    "reuse_conflict_count",
    "domain_reuse_status",
    "proposed_evidence_grade",
    "strict_coverage_eligible",
    "temporal_validation_status",
    "validation_reason",
    "error",
    "validated_on",
]

STATIC_ECHO_COLUMNS = VALIDATION_COLUMNS[:22]
HEX64 = re.compile(r"[0-9a-f]{64}")


class MunicipalWaybackValidationError(ValueError):
    """The frozen input or a validation checkpoint violates its contract."""


class PublicWaybackRequestError(RuntimeError):
    """A public Wayback query or replay request failed."""


@dataclass(frozen=True)
class Capture:
    timestamp: str
    original: str
    mimetype: str
    statuscode: int
    digest: str

    @property
    def captured_at(self) -> datetime:
        return datetime.strptime(self.timestamp, "%Y%m%d%H%M%S")


def _text(value: object) -> str:
    if pd.isna(value):
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
    raise MunicipalWaybackValidationError(f"{label} has invalid boolean {value!r}")


def _as_int(value: object, label: str, *, allow_blank: bool = False) -> int | None:
    token = _text(value)
    if not token and allow_blank:
        return None
    if not re.fullmatch(r"-?\d+", token):
        raise MunicipalWaybackValidationError(f"{label} has invalid integer {value!r}")
    return int(token)


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    actual = frame.columns.tolist()
    if actual != columns:
        raise MunicipalWaybackValidationError(
            f"{label} columns differ from the frozen contract; "
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
    os.replace(temporary, path)


def _resolve_manifest_path(value: object) -> Path:
    path = Path(_text(value))
    return path if path.is_absolute() else REPO_ROOT / path


def _validate_frozen_frame(
    frozen: pd.DataFrame,
    *,
    shard_id: int | None = None,
    expected_eligible_rows: int | None = None,
) -> pd.DataFrame:
    """Validate the full frozen frame, then select eligible rows and a shard."""
    _require_exact_columns(frozen, FROZEN_CLAIM_COLUMNS, "frozen claims")
    if frozen.empty:
        raise MunicipalWaybackValidationError("Frozen claims are empty")
    if frozen["validation_key"].map(_text).duplicated().any():
        raise MunicipalWaybackValidationError("Frozen claims contain duplicate validation keys")
    if frozen[["evidence_id", "candidate_year_id", "exact_normalized_url"]].astype(
        str
    ).duplicated().any():
        raise MunicipalWaybackValidationError("Frozen claims contain duplicate tuple keys")

    identity_status_required = "identity_supported"
    for row in frozen.itertuples(index=False):
        evidence_id = _text(row.evidence_id)
        key = validation_key(
            row.evidence_id, row.candidate_year_id, row.exact_normalized_url
        )
        digest = validation_evidence_hash(key)
        if _text(row.validation_key) != key or _text(row.evidence_hash) != digest:
            raise MunicipalWaybackValidationError(f"Frozen key/hash drift for {evidence_id}")
        if _text(row.freeze_contract_version) != FREEZE_CONTRACT_VERSION:
            raise MunicipalWaybackValidationError(
                f"Unexpected freeze contract for {evidence_id}"
            )
        if _text(row.validation_method_version) != VALIDATION_METHOD_VERSION:
            raise MunicipalWaybackValidationError(
                f"Unexpected validation method for {evidence_id}"
            )
        shard_count = _as_int(
            row.validation_shard_count, f"{evidence_id}.validation_shard_count"
        )
        row_shard = _as_int(
            row.validation_shard_id, f"{evidence_id}.validation_shard_id"
        )
        if shard_count != VALIDATION_SHARD_COUNT:
            raise MunicipalWaybackValidationError(
                f"Frozen shard count drift for {evidence_id}"
            )
        if row_shard != int(digest, 16) % int(shard_count):
            raise MunicipalWaybackValidationError(
                f"Frozen shard assignment drift for {evidence_id}"
            )
        for field in ["evidence_record_hash", "context_hash", "identity_decision_hash"]:
            if not HEX64.fullmatch(_text(getattr(row, field))):
                raise MunicipalWaybackValidationError(
                    f"Invalid {field} for {evidence_id}"
                )
        coverage = _as_bool(
            row.coverage_url_type_eligible,
            f"{evidence_id}.coverage_url_type_eligible",
        )
        identity_gate = _as_bool(
            row.identity_gate_pass, f"{evidence_id}.identity_gate_pass"
        )
        eligible = _as_bool(
            row.eligible_for_temporal_validation,
            f"{evidence_id}.eligible_for_temporal_validation",
        )
        expected_coverage = _text(row.url_type) in ELIGIBLE_URL_TYPES
        if coverage != expected_coverage:
            raise MunicipalWaybackValidationError(
                f"Coverage URL-type gate drift for {evidence_id}"
            )
        expected_identity = _text(row.identity_attribution_status) == identity_status_required
        if identity_gate != expected_identity:
            raise MunicipalWaybackValidationError(
                f"Independent identity gate drift for {evidence_id}"
            )
        if eligible != (coverage and identity_gate):
            raise MunicipalWaybackValidationError(
                f"Temporal eligibility gate drift for {evidence_id}"
            )
        if not _text(row.identity_attribution_reason):
            raise MunicipalWaybackValidationError(
                f"Missing identity attribution reason for {evidence_id}"
            )
        exact_url = _text(row.exact_normalized_url)
        parsed = urlsplit(exact_url)
        if parsed.scheme not in {"http", "https"} or not parsed.hostname:
            raise MunicipalWaybackValidationError(
                f"Invalid exact normalized URL for {evidence_id}: {exact_url!r}"
            )
        for field in ["full_name", "geo_name", "year", "candidate_year_id"]:
            if not _text(getattr(row, field)):
                raise MunicipalWaybackValidationError(
                    f"Missing candidate context {field} for {evidence_id}"
                )
        month = _as_int(row.month, f"{evidence_id}.month", allow_blank=True)
        if month is not None and not 1 <= month <= 12:
            raise MunicipalWaybackValidationError(
                f"Invalid election month for {evidence_id}: {month}"
            )

    eligible = frozen.loc[
        frozen["eligible_for_temporal_validation"].map(
            lambda value: _as_bool(value, "eligible_for_temporal_validation")
        )
    ].copy()
    if expected_eligible_rows is not None and len(eligible) != expected_eligible_rows:
        raise MunicipalWaybackValidationError(
            f"Eligible row count drift: expected {expected_eligible_rows}, found {len(eligible)}"
        )
    if shard_id is not None:
        if not 0 <= shard_id < VALIDATION_SHARD_COUNT:
            raise MunicipalWaybackValidationError(
                f"shard_id must be between 0 and {VALIDATION_SHARD_COUNT - 1}"
            )
        eligible = eligible.loc[
            pd.to_numeric(eligible["validation_shard_id"], errors="raise").astype(int)
            == shard_id
        ]
    return eligible.sort_values(
        ["evidence_hash", "evidence_id"], kind="stable"
    ).reset_index(drop=True)


def load_frozen_targets(
    claims_path: Path | str,
    manifest_path: Path | str,
    shard_id: int | None = None,
) -> pd.DataFrame:
    """Load hash-verified frozen claims, optionally selecting one shard."""
    claims_path = Path(claims_path).resolve()
    manifest_path = Path(manifest_path).resolve()
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as error:
        raise MunicipalWaybackValidationError(
            f"Cannot read frozen manifest: {error}"
        ) from error
    if manifest.get("manifest_version") != FREEZE_MANIFEST_VERSION:
        raise MunicipalWaybackValidationError("Unexpected frozen manifest version")
    if manifest.get("freeze_contract_version") != FREEZE_CONTRACT_VERSION:
        raise MunicipalWaybackValidationError("Frozen manifest contract drift")
    if manifest.get("validation_method_version") != VALIDATION_METHOD_VERSION:
        raise MunicipalWaybackValidationError("Frozen manifest validation method drift")
    if int(manifest.get("validation_shard_count", -1)) != VALIDATION_SHARD_COUNT:
        raise MunicipalWaybackValidationError("Frozen manifest shard count drift")
    if manifest.get("validation_key_fields") != [
        "evidence_id",
        "candidate_year_id",
        "exact_normalized_url",
    ]:
        raise MunicipalWaybackValidationError("Frozen manifest key fields drift")
    if manifest.get("validation_key_separator") != "U+001F":
        raise MunicipalWaybackValidationError("Frozen manifest key separator drift")
    if set(manifest.get("eligible_url_types", [])) != set(ELIGIBLE_URL_TYPES):
        raise MunicipalWaybackValidationError("Frozen manifest eligible URL types drift")
    if manifest.get("identity_gate_pass_status") != "identity_supported":
        raise MunicipalWaybackValidationError("Frozen manifest identity gate drift")
    if manifest.get("frozen_claim_columns") != FROZEN_CLAIM_COLUMNS:
        raise MunicipalWaybackValidationError("Frozen manifest column contract drift")

    records = manifest.get("outputs", {})
    matched_label = ""
    record: Mapping[str, object] | None = None
    for label in ("all_claims", "eligible_claims"):
        candidate = records.get(label)
        if isinstance(candidate, Mapping) and _resolve_manifest_path(
            candidate.get("path", "")
        ).resolve() == claims_path:
            matched_label = label
            record = candidate
            break
    if record is None:
        raise MunicipalWaybackValidationError(
            "Claims path is not one of the manifest's frozen outputs"
        )
    if not claims_path.is_file():
        raise MunicipalWaybackValidationError(f"Frozen claims file is missing: {claims_path}")
    if claims_path.stat().st_size != int(record.get("bytes", -1)):
        raise MunicipalWaybackValidationError("Frozen claims bytes count drift")
    if _sha256_file(claims_path) != record.get("sha256"):
        raise MunicipalWaybackValidationError("Frozen claims SHA-256 drift")
    frozen = pd.read_csv(claims_path, keep_default_na=False, low_memory=False)
    if len(frozen) != int(record.get("rows", -1)):
        raise MunicipalWaybackValidationError("Frozen claims row count drift")
    if matched_label == "eligible_claims" and "identity_gate_pass" in frozen:
        bad_url_types = ~frozen["url_type"].isin(ELIGIBLE_URL_TYPES)
        if bad_url_types.any():
            raise MunicipalWaybackValidationError(
                "Frozen eligible claims contain an ineligible URL type"
            )
        identity_failures = ~frozen["identity_gate_pass"].map(
            lambda value: _as_bool(value, "identity_gate_pass")
        )
        if identity_failures.any():
            raise MunicipalWaybackValidationError(
                "Frozen eligible claims contain an identity-gate failure"
            )
    expected_eligible = int(manifest.get("summary", {}).get("eligible_claims", -1))
    targets = _validate_frozen_frame(
        frozen,
        shard_id=shard_id,
        expected_eligible_rows=expected_eligible,
    )
    if shard_id is not None:
        expected_shard_counts = manifest.get("summary", {}).get(
            "validation_shard_counts", {}
        )
        expected_shard_rows = int(expected_shard_counts.get(str(shard_id), 0))
        if len(targets) != expected_shard_rows:
            raise MunicipalWaybackValidationError(
                f"Shard {shard_id} row count drift: expected {expected_shard_rows}, "
                f"found {len(targets)} from {matched_label}"
            )
    return targets


def shift_months(value: date, months: int) -> date:
    absolute = value.year * 12 + value.month - 1 + months
    year, zero_month = divmod(absolute, 12)
    month = zero_month + 1
    day = min(value.day, calendar.monthrange(year, month)[1])
    return date(year, month, day)


def public_cdx_url(exact_url: str, start: date, end: date) -> str:
    """Build a public CDX query for one exact URL and bounded date window."""
    parsed = urlsplit(str(exact_url))
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise MunicipalWaybackValidationError(
            f"Invalid exact URL for public CDX query: {exact_url!r}"
        )
    params = [
        ("url", str(exact_url)),
        ("matchType", "exact"),
        ("from", start.strftime("%Y%m%d")),
        ("to", end.strftime("%Y%m%d")),
        ("output", "json"),
        ("fl", "timestamp,original,mimetype,statuscode,digest"),
        ("filter", "statuscode:200"),
        ("collapse", "digest"),
    ]
    return f"{WAYBACK_CDX_ENDPOINT}?{urlencode(params)}"


def public_replay_url(capture: Capture) -> str:
    return f"{WAYBACK_REPLAY_ROOT}/{capture.timestamp}id_/{capture.original}"


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
    if attempts < 1:
        raise MunicipalWaybackValidationError("attempts must be at least 1")
    if timeout <= 0 or delay_seconds < 0:
        raise MunicipalWaybackValidationError("timeout must be positive and delay nonnegative")
    last_error = "public Wayback request failed"
    retry_after = 0.0
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                url, headers={"User-Agent": USER_AGENT}, timeout=timeout
            )
            status = int(response.status_code)
            if status == 429 or status >= 500:
                last_error = f"HTTP {status}"
                headers = getattr(response, "headers", {}) or {}
                try:
                    retry_after = max(
                        0.0, float(headers.get("Retry-After", 0) or 0)
                    )
                except (TypeError, ValueError):
                    retry_after = 0.0
            elif status >= 400:
                raise PublicWaybackRequestError(f"HTTP {status} for {url}")
            else:
                if delay_seconds:
                    time.sleep(delay_seconds)
                return response
        except PublicWaybackRequestError:
            raise
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < attempts:
            wait = max(delay_seconds * (2 ** (attempt - 1)), retry_after)
            if wait:
                time.sleep(min(60.0, wait))
    raise PublicWaybackRequestError(f"{last_error} for {url}")


def parse_cdx(payload: Any) -> list[Capture]:
    """Parse the public CDX JSON table into unique, ordered captures."""
    if not isinstance(payload, list) or not payload:
        return []
    if not isinstance(payload[0], list):
        raise PublicWaybackRequestError("Public CDX JSON lacks a header row")
    header = [str(value) for value in payload[0]]
    required = {"timestamp", "original", "mimetype", "statuscode", "digest"}
    if not required.issubset(header):
        raise PublicWaybackRequestError(
            f"Public CDX header lacks fields: {sorted(required - set(header))}"
        )
    captures: list[Capture] = []
    seen: set[tuple[str, str, str]] = set()
    for values in payload[1:]:
        if not isinstance(values, list):
            continue
        item = dict(zip(header, values))
        try:
            capture = Capture(
                timestamp=str(item.get("timestamp", "")),
                original=str(item.get("original", "")),
                mimetype=str(item.get("mimetype", "")),
                statuscode=int(item.get("statuscode", 0)),
                digest=str(item.get("digest", "")),
            )
            capture.captured_at
        except (TypeError, ValueError):
            continue
        key = (capture.timestamp, capture.original, capture.digest)
        if key not in seen:
            seen.add(key)
            captures.append(capture)
    return sorted(captures, key=lambda capture: capture.timestamp)


def query_public_wayback(
    exact_url: str,
    start: date,
    end: date,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> list[Capture]:
    url = public_cdx_url(exact_url, start, end)
    response = _request(
        url,
        timeout=timeout,
        attempts=attempts,
        delay_seconds=delay_seconds,
        session=session,
    )
    try:
        payload = response.json()
    except ValueError as error:
        raise PublicWaybackRequestError(
            f"Invalid public CDX JSON: {error}"
        ) from error
    return parse_cdx(payload)


def fetch_public_capture(
    capture: Capture,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> str:
    response = _request(
        public_replay_url(capture),
        timeout=timeout,
        attempts=attempts,
        delay_seconds=delay_seconds,
        session=session,
    )
    content_type = str(response.headers.get("content-type", "")).casefold()
    if content_type and "html" not in content_type and "xhtml" not in content_type:
        raise PublicWaybackRequestError(
            f"Replay is not HTML ({content_type}) for {capture.timestamp}"
        )
    return response.text


def _eligible_exact_html_captures(
    captures: list[Capture], exact_url: str
) -> list[Capture]:
    allowed = {"text/html", "application/xhtml+xml", "warc/revisit"}
    return [
        capture
        for capture in captures
        if capture.statuscode == 200
        and capture.mimetype.casefold() in allowed
        and capture.original == exact_url
    ]


def _nearest_capture(
    captures: list[Capture], reference: date
) -> Capture | None:
    if not captures:
        return None
    reference_dt = datetime.combine(reference, datetime.min.time())
    return min(
        captures,
        key=lambda capture: (
            abs((capture.captured_at - reference_dt).days),
            capture.timestamp,
        ),
    )


def _normalize_text(value: str) -> str:
    ascii_value = unicodedata.normalize("NFKD", str(value)).encode(
        "ascii", "ignore"
    ).decode()
    return " ".join(re.findall(r"[a-z0-9]+", ascii_value.casefold()))


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
    metadata: list[str] = []
    if soup.title:
        metadata.append(soup.title.get_text(" ", strip=True))
    for tag in soup.find_all("meta"):
        name = str(tag.get("name") or tag.get("property") or "").casefold()
        if name in {
            "description",
            "og:title",
            "og:description",
            "twitter:title",
            "twitter:description",
        }:
            metadata.append(str(tag.get("content", "")))
    normalized = _normalize_text(" ".join(metadata + [soup.get_text(" ", strip=True)]))
    page_tokens = set(normalized.split())
    name_tokens = [
        token
        for token in _normalize_text(candidate_name).split()
        if token not in {"jr", "sr", "ii", "iii", "iv"}
    ]
    surname = name_tokens[-1] if name_tokens else ""
    identity = bool(
        surname
        and surname in page_tokens
        and _given_name_present(name_tokens[:-1], page_tokens)
    )
    mayoral_patterns = (
        r"\bfor mayor\b",
        r"\bmayoral campaign\b",
        r"\bcampaign for mayor\b",
        r"\bmayor[\s-]+president\b",
        r"\b(?:elect|reelect|re-elect|vote for)\b.{0,80}\bmayor\b",
    )
    mayoral = any(re.search(pattern, normalized) for pattern in mayoral_patterns)
    normalized_city = _normalize_text(city)
    city_match = bool(
        normalized_city
        and re.search(rf"\b{re.escape(normalized_city)}\b", normalized)
    )
    return {
        "candidate_identity_match": identity,
        "mayoral_campaign_match": mayoral,
        "city_match": city_match,
        "content_signal_summary": (
            f"identity={'yes' if identity else 'no'}; "
            f"mayoral_campaign={'yes' if mayoral else 'no'}; "
            f"city={'yes' if city_match else 'no'}"
        ),
        "content_sha256": hashlib.sha256(str(html).encode("utf-8")).hexdigest(),
    }


def _reuse_probes(
    captures: list[Capture],
    nearest: Capture,
    window_start: date,
    window_end: date,
    limit: int,
) -> list[Capture]:
    candidates = [
        capture
        for capture in captures
        if window_start <= capture.captured_at.date() <= window_end
        and capture.timestamp != nearest.timestamp
        and (not nearest.digest or capture.digest != nearest.digest)
    ]
    if not candidates or limit <= 0:
        return []
    ordered = sorted(candidates, key=lambda capture: capture.timestamp)
    selected = [ordered[0]]
    if limit > 1 and ordered[-1].timestamp != ordered[0].timestamp:
        selected.append(ordered[-1])
    return selected[:limit]


def _base_result(row: Mapping[str, Any]) -> dict[str, Any]:
    result = {column: "" for column in VALIDATION_COLUMNS}
    for column in STATIC_ECHO_COLUMNS:
        result[column] = row.get(column, "")
    result.update(
        {
            "archive_query_target": _text(row["exact_normalized_url"]),
            "archive_query_status": "not_attempted_missing_election_month",
            "archive_capture_count": 0,
            "eligible_capture_count": 0,
            "headline_window_days": HEADLINE_WINDOW_DAYS,
            "nearest_capture_days_signed": pd.NA,
            "nearest_capture_days_absolute": pd.NA,
            "within_headline_window": False,
            "within_sensitivity_window": False,
            "capture_fetch_status": "not_attempted_missing_election_month",
            "candidate_identity_match": False,
            "mayoral_campaign_match": False,
            "city_match": False,
            "reuse_captures_before": 0,
            "reuse_captures_after": 0,
            "reuse_probe_count": 0,
            "reuse_conflict_count": 0,
            "domain_reuse_status": "not_inspected_missing_election_month",
            "proposed_evidence_grade": "C",
            "strict_coverage_eligible": False,
            "temporal_validation_status": "missing_election_month",
            "validation_reason": "missing_election_month_no_imputation",
            "validated_on": date.today().isoformat(),
        }
    )
    return result


def _decision(result: Mapping[str, Any]) -> tuple[str, str, str, bool]:
    if result["archive_query_status"] == "not_attempted_missing_election_month":
        return "missing_election_month_no_imputation", "missing_election_month", "C", False
    if result["archive_query_status"] == "request_error":
        return "archive_query_request_error_retryable", "request_error_retryable", "C", False
    if not result["nearest_capture_timestamp"]:
        return "no_public_wayback_capture", "no_public_wayback_capture", "C", False
    if result["capture_fetch_status"] == "request_error":
        return "capture_request_error_retryable", "request_error_retryable", "C", False
    if not result["within_headline_window"]:
        reason = (
            "outside_headline_365_within_sensitivity"
            if result["within_sensitivity_window"]
            else "outside_headline_and_sensitivity_windows"
        )
        return reason, reason, "C", False
    if not result["candidate_identity_match"]:
        return "candidate_identity_not_confirmed", "content_validation_unresolved", "C", False
    if not result["mayoral_campaign_match"]:
        return "mayoral_campaign_not_confirmed", "content_validation_unresolved", "C", False
    if not result["city_match"]:
        return "city_not_confirmed", "content_validation_unresolved", "C", False
    if result["domain_reuse_status"] == "request_error":
        return "reuse_inspection_request_error_retryable", "request_error_retryable", "C", False
    if result["domain_reuse_status"] == "conflicting_content_detected":
        return "domain_reuse_conflict", "content_validation_unresolved", "C", False
    return (
        "grade_a_exact_url_election_period_content_confirmed",
        "grade_a_content_confirmed_pending_review",
        "A",
        True,
    )


def validate_one_claim(
    row: Mapping[str, Any],
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    reuse_probe_limit: int = MAX_REUSE_PROBES,
    query_cache: dict[tuple[str, date, date], list[Capture]] | None = None,
    capture_cache: dict[str, str] | None = None,
) -> dict[str, Any]:
    """Validate one already-frozen claim; all request failures remain retryable."""
    if not 0 <= reuse_probe_limit <= MAX_REUSE_PROBES:
        raise MunicipalWaybackValidationError(
            f"reuse_probe_limit must be between 0 and {MAX_REUSE_PROBES}"
        )
    result = _base_result(row)
    month = _as_int(row.get("month", ""), "month", allow_blank=True)
    if month is None:
        result["month"] = ""
        return result
    if not 1 <= month <= 12:
        raise MunicipalWaybackValidationError(f"Invalid election month: {month}")
    year = int(_as_int(row["year"], "year"))
    result["month"] = month
    reference = date(year, month, 15)
    sensitivity_start = shift_months(reference, -SENSITIVITY_MONTHS_BEFORE)
    sensitivity_end = reference + timedelta(days=SENSITIVITY_DAYS_AFTER)
    reuse_start = shift_months(reference, -REUSE_MONTHS_EACH_SIDE)
    reuse_end = shift_months(reference, REUSE_MONTHS_EACH_SIDE)
    exact_url = result["archive_query_target"]
    result.update(
        {
            "archive_cdx_url": public_cdx_url(exact_url, reuse_start, reuse_end),
            "election_reference_date": reference.isoformat(),
            "sensitivity_window_start": sensitivity_start.isoformat(),
            "sensitivity_window_end": sensitivity_end.isoformat(),
            "reuse_window_start": reuse_start.isoformat(),
            "reuse_window_end": reuse_end.isoformat(),
        }
    )
    cache_key = (exact_url, reuse_start, reuse_end)
    try:
        if query_cache is not None and cache_key in query_cache:
            captures = query_cache[cache_key]
        else:
            captures = query_public_wayback(
                exact_url,
                reuse_start,
                reuse_end,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
            )
            if query_cache is not None:
                query_cache[cache_key] = captures
        result["archive_query_status"] = "checked"
    except (PublicWaybackRequestError, ValueError) as error:
        result.update(
            {
                "archive_query_status": "request_error",
                "capture_fetch_status": "not_attempted_query_error",
                "domain_reuse_status": "not_inspected_query_error",
                "error": _clean_error(error),
            }
        )
        reason, status, grade, strict = _decision(result)
        result.update(
            validation_reason=reason,
            temporal_validation_status=status,
            proposed_evidence_grade=grade,
            strict_coverage_eligible=strict,
        )
        return result

    result["archive_capture_count"] = len(captures)
    eligible = _eligible_exact_html_captures(captures, exact_url)
    result["eligible_capture_count"] = len(eligible)
    nearest = _nearest_capture(eligible, reference)
    if nearest is None:
        result.update(
            {
                "capture_fetch_status": "not_attempted_no_capture",
                "domain_reuse_status": "not_inspected_no_capture",
                "validation_reason": "no_public_wayback_capture",
                "temporal_validation_status": "no_public_wayback_capture",
            }
        )
        return result

    signed_days = (nearest.captured_at.date() - reference).days
    absolute_days = abs(signed_days)
    result.update(
        {
            "nearest_capture_timestamp": nearest.timestamp,
            "nearest_capture_original_url": nearest.original,
            "nearest_capture_days_signed": signed_days,
            "nearest_capture_days_absolute": absolute_days,
            "within_headline_window": absolute_days <= HEADLINE_WINDOW_DAYS,
            "within_sensitivity_window": (
                sensitivity_start <= nearest.captured_at.date() <= sensitivity_end
            ),
            "capture_replay_url": public_replay_url(nearest),
        }
    )
    in_reuse_window = [
        capture
        for capture in eligible
        if reuse_start <= capture.captured_at.date() <= reuse_end
    ]
    result["reuse_captures_before"] = sum(
        capture.captured_at.date() < reference for capture in in_reuse_window
    )
    result["reuse_captures_after"] = sum(
        capture.captured_at.date() > reference for capture in in_reuse_window
    )

    nearest_url = result["capture_replay_url"]
    try:
        if capture_cache is not None and nearest_url in capture_cache:
            html = capture_cache[nearest_url]
        else:
            html = fetch_public_capture(
                nearest,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
            )
            if capture_cache is not None:
                capture_cache[nearest_url] = html
        signals = page_content_signals(
            html, _text(row["full_name"]), _text(row["geo_name"])
        )
        result.update(
            {
                "capture_fetch_status": "fetched",
                "capture_content_sha256": signals["content_sha256"],
                "candidate_identity_match": signals["candidate_identity_match"],
                "mayoral_campaign_match": signals["mayoral_campaign_match"],
                "city_match": signals["city_match"],
                "content_signal_summary": signals["content_signal_summary"],
            }
        )
    except PublicWaybackRequestError as error:
        result.update(
            {
                "capture_fetch_status": "request_error",
                "domain_reuse_status": "not_inspected_capture_error",
                "error": _clean_error(error),
            }
        )
        reason, status, grade, strict = _decision(result)
        result.update(
            validation_reason=reason,
            temporal_validation_status=status,
            proposed_evidence_grade=grade,
            strict_coverage_eligible=strict,
        )
        return result

    probes = _reuse_probes(
        eligible, nearest, reuse_start, reuse_end, reuse_probe_limit
    )
    result["reuse_probe_timestamps"] = " | ".join(
        probe.timestamp for probe in probes
    )
    result["reuse_probe_count"] = len(probes)
    conflicts = 0
    try:
        for probe in probes:
            replay_url = public_replay_url(probe)
            if capture_cache is not None and replay_url in capture_cache:
                probe_html = capture_cache[replay_url]
            else:
                probe_html = fetch_public_capture(
                    probe,
                    timeout=timeout,
                    attempts=attempts,
                    delay_seconds=delay_seconds,
                )
                if capture_cache is not None:
                    capture_cache[replay_url] = probe_html
            probe_signals = page_content_signals(
                probe_html, _text(row["full_name"]), _text(row["geo_name"])
            )
            if not probe_signals["candidate_identity_match"]:
                conflicts += 1
        result["reuse_conflict_count"] = conflicts
        if conflicts:
            result["domain_reuse_status"] = "conflicting_content_detected"
        elif probes:
            result["domain_reuse_status"] = "same_candidate_only"
        elif len(in_reuse_window) > 1:
            result["domain_reuse_status"] = "no_distinct_content_to_probe"
        else:
            result["domain_reuse_status"] = "no_additional_capture"
    except PublicWaybackRequestError as error:
        result.update(
            {
                "reuse_conflict_count": conflicts,
                "domain_reuse_status": "request_error",
                "error": _clean_error(error),
            }
        )

    reason, status, grade, strict = _decision(result)
    result.update(
        validation_reason=reason,
        temporal_validation_status=status,
        proposed_evidence_grade=grade,
        strict_coverage_eligible=strict,
    )
    if grade not in {"A", "C"}:
        raise AssertionError("Public Wayback validator may emit only grade A or C")
    return result


def _is_completed(row: Mapping[str, Any]) -> bool:
    if _text(row.get("archive_query_status")) == "request_error":
        return False
    if _text(row.get("capture_fetch_status")) == "request_error":
        return False
    if _text(row.get("domain_reuse_status")) == "request_error":
        return False
    return _text(row.get("archive_query_status")) in {
        "checked",
        "not_attempted_missing_election_month",
    }


def validate_claims(
    frozen: pd.DataFrame,
    validation_path: Path | str,
    shard_id: int,
    *,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    reuse_probe_limit: int = MAX_REUSE_PROBES,
) -> pd.DataFrame:
    """Validate one shard with atomic checkpoints and exact key equality."""
    validation_path = Path(validation_path)
    targets = _validate_frozen_frame(frozen, shard_id=shard_id)
    target_lookup = {
        _text(row["validation_key"]): row for row in targets.to_dict("records")
    }
    target_keys = set(target_lookup)
    prior = (
        pd.read_csv(validation_path, keep_default_na=False, low_memory=False)
        if validation_path.exists()
        else pd.DataFrame(columns=VALIDATION_COLUMNS)
    )
    _require_exact_columns(prior, VALIDATION_COLUMNS, "validation checkpoint")
    if prior["validation_key"].map(_text).duplicated().any():
        raise MunicipalWaybackValidationError(
            "Validation checkpoint contains duplicate keys"
        )
    kept: list[dict[str, Any]] = []
    for prior_row in prior.to_dict("records"):
        key = _text(prior_row.get("validation_key"))
        target = target_lookup.get(key)
        if target is None:
            continue
        if all(_text(prior_row[column]) == _text(target[column]) for column in STATIC_ECHO_COLUMNS):
            kept.append(prior_row)
    rows = kept
    _atomic_csv(pd.DataFrame(rows), validation_path)
    completed = {
        _text(row["validation_key"]) for row in rows if _is_completed(row)
    }
    query_cache: dict[tuple[str, date, date], list[Capture]] = {}
    capture_cache: dict[str, str] = {}
    for target in targets.to_dict("records"):
        key = _text(target["validation_key"])
        if key in completed:
            continue
        rows = [row for row in rows if _text(row["validation_key"]) != key]
        rows.append(
            validate_one_claim(
                target,
                timeout=timeout,
                attempts=attempts,
                delay_seconds=delay_seconds,
                reuse_probe_limit=reuse_probe_limit,
                query_cache=query_cache,
                capture_cache=capture_cache,
            )
        )
        rows.sort(key=lambda row: (_text(row["evidence_hash"]), _text(row["evidence_id"])))
        _atomic_csv(pd.DataFrame(rows), validation_path)

    result = pd.DataFrame(rows).reindex(columns=VALIDATION_COLUMNS)
    result_keys = set(result["validation_key"].map(_text))
    if result_keys != target_keys or len(result) != len(target_keys):
        raise MunicipalWaybackValidationError(
            "Validation/frozen key mismatch: "
            f"missing={sorted(target_keys - result_keys)}, "
            f"unexpected={sorted(result_keys - target_keys)}, "
            f"duplicate_rows={len(result) - len(result_keys)}"
        )
    result = result.sort_values(
        ["evidence_hash", "evidence_id"], kind="stable"
    ).reset_index(drop=True)
    _atomic_csv(result, validation_path)
    return result


def _summary(result: pd.DataFrame, output: Path) -> dict[str, Any]:
    retryable = (
        result["archive_query_status"].eq("request_error")
        | result["capture_fetch_status"].eq("request_error")
        | result["domain_reuse_status"].eq("request_error")
    )
    return {
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "output": str(output),
        "rows": int(len(result)),
        "archive_query_status_counts": {
            str(key): int(value)
            for key, value in result["archive_query_status"].value_counts().sort_index().items()
        },
        "temporal_validation_status_counts": {
            str(key): int(value)
            for key, value in result["temporal_validation_status"].value_counts().sort_index().items()
        },
        "proposed_grade_counts": {
            str(key): int(value)
            for key, value in result["proposed_evidence_grade"].value_counts().sort_index().items()
        },
        "strict_coverage_eligible": int(result["strict_coverage_eligible"].astype(bool).sum()),
        "retryable_rows": int(retryable.sum()),
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--claims", type=Path, default=DEFAULT_FROZEN_CLAIMS)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_FROZEN_MANIFEST)
    parser.add_argument("--shard-id", type=int, required=True)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument("--reuse-probe-limit", type=int, default=MAX_REUSE_PROBES)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if not 0 <= args.shard_id < VALIDATION_SHARD_COUNT:
        raise SystemExit(
            f"--shard-id must be between 0 and {VALIDATION_SHARD_COUNT - 1}"
        )
    if not 0 <= args.reuse_probe_limit <= MAX_REUSE_PROBES:
        raise SystemExit(
            f"--reuse-probe-limit must be between 0 and {MAX_REUSE_PROBES}"
        )
    output = args.output or DEFAULT_SHARD_DIR / f"validation_shard_{args.shard_id}.csv"
    targets = load_frozen_targets(args.claims, args.manifest, args.shard_id)
    result = validate_claims(
        targets,
        output,
        args.shard_id,
        timeout=args.timeout,
        attempts=args.attempts,
        delay_seconds=args.delay_seconds,
        reuse_probe_limit=args.reuse_probe_limit,
    )
    print(json.dumps(_summary(result, output), indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
