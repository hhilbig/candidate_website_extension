#!/usr/bin/env python3
"""Enumerate public Archive-It collections that may contain mayoral websites.

The Archive-It Partner API documents ``/api/collection`` and ``limit=-1`` for
public collection metadata.  This script fetches that catalog once, then runs a
deterministic local query grid over every in-scope city, mayoral/election terms,
and collection subjects.  A local grid avoids hundreds of repetitive public API
requests while retaining an explicit terminal status for every logical query.

This is a source-discovery stage only.  A collection row is a lead, not evidence
that a candidate owned a URL during an election.  Selected collections must be
promoted into the seed adapter and temporal validator before they can contribute
to coverage.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import tempfile
import time
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import urlencode

import pandas as pd
import requests


COLLECTION_API_URL = "https://partner.archive-it.org/api/collection"
COLLECTION_API_PARAMS = {"limit": -1}
COLLECTION_API_REQUEST_URL = f"{COLLECTION_API_URL}?{urlencode(COLLECTION_API_PARAMS)}"
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; public metadata only)"
PARSER_VERSION = "archive_it_collection_enumerator_v1"
SOURCE_ID = "archive_it_public_collection_catalog"
TERMINAL_ATTEMPT_STATUSES = {
    "found", "no_result", "source_unavailable", "request_error", "match_unresolved",
}
KNOWN_COLLECTION_IDS = {5903, 16766}
TARGET_START_YEAR = 2000
TARGET_END_YEAR = 2021


@dataclass(frozen=True)
class LogicalQuery:
    query_id: str
    query_kind: str
    query_term: str
    query_city: str = ""
    query_state: str = ""


class ArchiveItCollectionRequestError(RuntimeError):
    """The public collection-catalog request failed after bounded retries."""


class ArchiveItCollectionSourceUnavailable(RuntimeError):
    """The public collection catalog is unavailable to this client."""


ATTEMPT_COLUMNS = [
    "attempt_id", "source_id", "query_id", "query_kind", "query_term",
    "query_city", "query_state", "api_endpoint", "api_request_url",
    "request_params_json", "source_status", "raw_records_considered",
    "matched_collections", "error", "retrieved_at", "parser_version",
]


CANDIDATE_COLUMNS = [
    "collection_candidate_id", "source_id", "query_id", "query_kind",
    "query_term", "query_city", "query_state", "collection_id",
    "collection_name", "collection_access_url", "account_id", "state",
    "publicly_visible", "deleted", "created_date", "last_updated_date",
    "last_crawl_date", "num_active_seeds", "num_inactive_seeds", "topics",
    "metadata_title", "metadata_subject", "metadata_description",
    "metadata_coverage", "metadata_creator", "source_metadata_json",
    "source_collection_json", "matched_target_locations", "year_hints",
    "target_period_hint", "relevance_flags", "priority_score",
    "known_collection", "review_status", "promotion_status",
    "api_endpoint", "api_request_url", "retrieved_at", "parser_version",
]


PRIORITY_COLUMNS = [
    "collection_priority_id", "source_id", "collection_id", "collection_name",
    "collection_access_url", "account_id", "state", "publicly_visible",
    "deleted", "created_date", "last_updated_date", "last_crawl_date",
    "num_active_seeds", "num_inactive_seeds", "topics", "metadata_title",
    "metadata_subject", "metadata_description", "metadata_coverage",
    "metadata_creator", "source_metadata_json", "source_collection_json",
    "query_claim_count", "matched_query_ids", "matched_query_kinds",
    "matched_query_terms", "matched_queries_json", "matched_target_locations",
    "year_hints", "target_period_hint", "relevance_flags", "priority_score",
    "known_collection", "collection_novelty", "review_status",
    "promotion_status", "api_endpoint", "api_request_url", "retrieved_at",
    "parser_version",
]


MAYORAL_TERMS = ("mayor", "mayoral")
ELECTION_TERMS = ("election", "campaign", "candidate")
SUBJECT_QUERIES = (
    ("metadata_subject", "mayor"),
    ("metadata_subject", "mayoral"),
    ("metadata_subject", "election"),
    ("metadata_subject", "campaign"),
    ("collection_topic", "politicsAndElections"),
    ("collection_topic", "government-Cities"),
)
POLITICAL_PATTERN = re.compile(
    r"\b(?:mayors?|mayoral|elections?|campaigns?|candidates?|politics|political|"
    r"ballot|city\s+council|municipal|race)\b",
    re.IGNORECASE,
)
LOCAL_PATTERN = re.compile(
    r"\b(?:city|cities|municipal|local|mayors?|mayoral|city\s+council)\b",
    re.IGNORECASE,
)


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(str(value) for value in values)
    digest = hashlib.sha256(payload.encode("utf-8")).hexdigest()[:24]
    return f"{prefix}_{digest}"


def _normalized_text(value: object) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return re.sub(r"[^a-z0-9]+", " ", text.casefold()).strip()


def _contains_phrase(text: str, phrase: str) -> bool:
    normalized_phrase = _normalized_text(phrase)
    if not normalized_phrase:
        return False
    return f" {normalized_phrase} " in f" {text} "


def _metadata_values(record: Mapping[str, Any], field: str) -> list[str]:
    metadata = record.get("metadata")
    if not isinstance(metadata, Mapping):
        return []
    values = metadata.get(field, [])
    if not isinstance(values, list):
        values = [values]
    result: list[str] = []
    for item in values:
        if isinstance(item, Mapping):
            value = item.get("value", "")
        else:
            value = item
        if value is not None and str(value).strip():
            result.append(str(value))
    return result


def _joined_metadata(record: Mapping[str, Any], field: str) -> str:
    return " | ".join(_metadata_values(record, field))


def _record_text(record: Mapping[str, Any]) -> tuple[str, str, str]:
    name = str(record.get("name", ""))
    metadata_text = " ".join(
        str(value)
        for field in ("Title", "Subject", "Description", "Coverage", "Creator")
        for value in _metadata_values(record, field)
    )
    topics = str(record.get("topics", "") or "")
    return (
        _normalized_text(f"{name} {metadata_text} {topics}"),
        _normalized_text(_joined_metadata(record, "Subject")),
        _normalized_text(topics),
    )


def _scope_mask(universe: pd.DataFrame) -> pd.Series:
    if "in_discovery_scope" not in universe:
        return pd.Series(True, index=universe.index)
    values = universe["in_discovery_scope"]
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    return values.astype("string").str.casefold().isin({"1", "true", "yes"})


def target_locations(universe: pd.DataFrame) -> list[tuple[str, str]]:
    required = {"geo_name", "state_abb"}
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Candidate universe missing columns: {sorted(missing)}")
    selected = universe.loc[_scope_mask(universe), ["geo_name", "state_abb"]].copy()
    selected["geo_name"] = selected["geo_name"].astype("string").str.strip().str.casefold()
    selected["state_abb"] = selected["state_abb"].astype("string").str.strip().str.upper()
    selected = selected.loc[selected.geo_name.ne("") & selected.state_abb.ne("")]
    return sorted(set(map(tuple, selected.itertuples(index=False, name=None))))


def build_logical_queries(universe: pd.DataFrame) -> list[LogicalQuery]:
    queries: list[LogicalQuery] = []
    for city, state in target_locations(universe):
        queries.append(LogicalQuery(
            query_id=_stable_id("archiveit_query", "city", city, state),
            query_kind="city",
            query_term=f"{city}, {state}",
            query_city=city,
            query_state=state,
        ))
    for term in MAYORAL_TERMS:
        queries.append(LogicalQuery(
            query_id=_stable_id("archiveit_query", "mayoral_term", term),
            query_kind="mayoral_term",
            query_term=term,
        ))
    for term in ELECTION_TERMS:
        queries.append(LogicalQuery(
            query_id=_stable_id("archiveit_query", "election_term", term),
            query_kind="election_term",
            query_term=term,
        ))
    for kind, term in SUBJECT_QUERIES:
        queries.append(LogicalQuery(
            query_id=_stable_id("archiveit_query", kind, term),
            query_kind=kind,
            query_term=term,
        ))
    if len({query.query_id for query in queries}) != len(queries):
        raise AssertionError("Logical Archive-It query IDs are not unique")
    return queries


def fetch_collection_catalog(
    timeout: int = 60,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> list[dict[str, Any]]:
    """Fetch all public collection metadata with bounded transient retries."""
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_error = "Archive-It public collection catalog request failed"
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                COLLECTION_API_URL,
                params=COLLECTION_API_PARAMS,
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
            )
            status_code = int(response.status_code)
            if status_code in {401, 403, 404}:
                raise ArchiveItCollectionSourceUnavailable(
                    f"Archive-It collection catalog returned HTTP {status_code}"
                )
            if status_code == 429 or status_code >= 500:
                last_error = f"Archive-It collection catalog returned HTTP {status_code}"
            elif status_code >= 400:
                raise ArchiveItCollectionRequestError(
                    f"Archive-It collection catalog returned HTTP {status_code}"
                )
            else:
                try:
                    payload = response.json()
                except ValueError as error:
                    last_error = f"Archive-It collection catalog returned invalid JSON: {error}"
                else:
                    if not isinstance(payload, list):
                        raise ArchiveItCollectionRequestError(
                            "Archive-It collection catalog did not return a JSON list"
                        )
                    return [dict(item) for item in payload if isinstance(item, Mapping)]
        except ArchiveItCollectionSourceUnavailable:
            raise
        except ArchiveItCollectionRequestError:
            raise
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < attempts:
            time.sleep(delay_seconds)
    raise ArchiveItCollectionRequestError(last_error)


def load_collection_catalog(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError("Saved Archive-It collection catalog must be a JSON list")
    return [dict(item) for item in payload if isinstance(item, Mapping)]


def _is_public_record(record: Mapping[str, Any]) -> bool:
    return bool(record.get("publicly_visible", True)) and not bool(record.get("deleted", False))


def _all_location_matches(
    normalized_record_text: str,
    locations: Sequence[tuple[str, str]],
) -> list[tuple[str, str]]:
    return [
        (city, state)
        for city, state in locations
        if _contains_phrase(normalized_record_text, city)
    ]


def _prepare_record_analysis(
    record: Mapping[str, Any],
    locations: Sequence[tuple[str, str]],
) -> dict[str, Any]:
    all_text, subject_text, topic_text = _record_text(record)
    return {
        "all_text": all_text,
        "subject_text": subject_text,
        "topic_text": topic_text,
        "location_matches": _all_location_matches(all_text, locations),
        "explicit_mayoral": bool(re.search(r"\b(?:mayors?|mayoral)\b", all_text)),
        "political": bool(POLITICAL_PATTERN.search(all_text)),
        "local": bool(LOCAL_PATTERN.search(all_text)),
        "politics_topic": _contains_phrase(topic_text, "politicsAndElections"),
        "city_topic": _contains_phrase(topic_text, "government-Cities"),
    }


def _term_pattern(term: str) -> re.Pattern[str]:
    normalized = _normalized_text(term)
    if normalized == "mayor":
        # Word boundaries prevent "Sotomayor" from becoming a mayoral hit.
        return re.compile(r"\bmayors?\b", re.IGNORECASE)
    if normalized.endswith("s"):
        return re.compile(rf"\b{re.escape(normalized)}\b", re.IGNORECASE)
    return re.compile(rf"\b{re.escape(normalized)}s?\b", re.IGNORECASE)


def _query_matches(
    query: LogicalQuery,
    record: Mapping[str, Any],
    locations: Sequence[tuple[str, str]],
    analysis: Mapping[str, Any] | None = None,
) -> tuple[bool, list[tuple[str, str]], list[str], int]:
    prepared = dict(analysis or _prepare_record_analysis(record, locations))
    all_text = str(prepared["all_text"])
    subject_text = str(prepared["subject_text"])
    topic_text = str(prepared["topic_text"])
    location_matches = list(prepared["location_matches"])
    flags: list[str] = []

    explicit_mayoral = bool(prepared["explicit_mayoral"])
    political = bool(prepared["political"])
    local = bool(prepared["local"])
    politics_topic = bool(prepared["politics_topic"])
    city_topic = bool(prepared["city_topic"])

    if explicit_mayoral:
        flags.append("explicit_mayoral_term")
    if politics_topic:
        flags.append("politics_and_elections_topic")
    if city_topic:
        flags.append("government_cities_topic")
    if location_matches:
        flags.append("target_city_mentioned")

    if query.query_kind == "city":
        query_city_matches = _contains_phrase(all_text, query.query_city)
        matched = query_city_matches and political
    elif query.query_kind == "mayoral_term":
        matched = bool(_term_pattern(query.query_term).search(all_text))
    elif query.query_kind == "election_term":
        has_term = bool(_term_pattern(query.query_term).search(all_text))
        matched = has_term and (bool(location_matches) or politics_topic or local)
    elif query.query_kind == "metadata_subject":
        has_term = bool(_term_pattern(query.query_term).search(subject_text))
        matched = has_term and (
            explicit_mayoral or bool(location_matches) or politics_topic or local
        )
    elif query.query_kind == "collection_topic":
        has_topic = _contains_phrase(topic_text, query.query_term)
        matched = has_topic and (explicit_mayoral or bool(location_matches) or local)
    else:
        raise ValueError(f"Unknown logical query kind: {query.query_kind}")

    if not matched:
        return False, location_matches, flags, 0

    score = 0
    if explicit_mayoral:
        score += 5
    if location_matches:
        score += 4
    if politics_topic:
        score += 2
    if city_topic:
        score += 1
    if political:
        score += 1
    collection_id = pd.to_numeric(record.get("id"), errors="coerce")
    if pd.notna(collection_id) and int(collection_id) in KNOWN_COLLECTION_IDS:
        score += 2
        flags.append("known_adapter_collection")
    return True, location_matches, sorted(set(flags)), score


def _year_hints(record: Mapping[str, Any]) -> list[int]:
    values = [
        record.get("name", ""),
        _joined_metadata(record, "Title"),
        _joined_metadata(record, "Subject"),
        _joined_metadata(record, "Description"),
        _joined_metadata(record, "Coverage"),
        record.get("created_date", ""),
        record.get("last_crawl_date", ""),
    ]
    years: set[int] = set()
    for value in values:
        for token in re.findall(r"(?<!\d)(?:19|20)\d{2}(?!\d)", str(value or "")):
            years.add(int(token))
    return sorted(years)


def _target_period_hint(years: Sequence[int]) -> str:
    if not years:
        return "unknown"
    if any(TARGET_START_YEAR <= year <= TARGET_END_YEAR for year in years):
        return "overlaps_2000_2021"
    return "outside_2000_2021"


def enumerate_collection_candidates(
    universe: pd.DataFrame,
    catalog: Sequence[Mapping[str, Any]],
    *,
    retrieved_at: str | None = None,
    catalog_error: tuple[str, str] | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply the logical grid and return query-level collection leads/attempts."""
    queries = build_logical_queries(universe)
    timestamp = retrieved_at or datetime.now(timezone.utc).isoformat()
    locations = target_locations(universe)
    records = [dict(record) for record in catalog if _is_public_record(record)]

    if catalog_error is not None:
        status, error = catalog_error
        if status not in {"source_unavailable", "request_error"}:
            raise ValueError(f"Invalid catalog error status: {status}")
        attempts = pd.DataFrame([{
            "attempt_id": _stable_id("archiveit_attempt", query.query_id, timestamp),
            "source_id": SOURCE_ID,
            "query_id": query.query_id,
            "query_kind": query.query_kind,
            "query_term": query.query_term,
            "query_city": query.query_city,
            "query_state": query.query_state,
            "api_endpoint": COLLECTION_API_URL,
            "api_request_url": COLLECTION_API_REQUEST_URL,
            "request_params_json": json.dumps(COLLECTION_API_PARAMS, sort_keys=True),
            "source_status": status,
            "raw_records_considered": 0,
            "matched_collections": 0,
            "error": error,
            "retrieved_at": timestamp,
            "parser_version": PARSER_VERSION,
        } for query in queries]).reindex(columns=ATTEMPT_COLUMNS)
        return pd.DataFrame(columns=CANDIDATE_COLUMNS), attempts

    prepared_records = [
        (record, _prepare_record_analysis(record, locations)) for record in records
    ]
    candidate_rows: list[dict[str, Any]] = []
    attempt_rows: list[dict[str, Any]] = []
    for query in queries:
        matches = 0
        for record, analysis in prepared_records:
            matched, matched_locations, flags, score = _query_matches(
                query, record, locations, analysis,
            )
            if not matched:
                continue
            collection_number = pd.to_numeric(record.get("id"), errors="coerce")
            if pd.isna(collection_number):
                continue
            collection_id = int(collection_number)
            years = _year_hints(record)
            matches += 1
            metadata = record.get("metadata") if isinstance(record.get("metadata"), Mapping) else {}
            candidate_rows.append({
                "collection_candidate_id": _stable_id(
                    "archiveit_collection_candidate", query.query_id, collection_id,
                ),
                "source_id": SOURCE_ID,
                "query_id": query.query_id,
                "query_kind": query.query_kind,
                "query_term": query.query_term,
                "query_city": query.query_city,
                "query_state": query.query_state,
                "collection_id": collection_id,
                "collection_name": str(record.get("name", "")),
                "collection_access_url": f"https://archive-it.org/collections/{collection_id}",
                "account_id": record.get("account", ""),
                "state": str(record.get("state", "")),
                "publicly_visible": bool(record.get("publicly_visible", True)),
                "deleted": bool(record.get("deleted", False)),
                "created_date": str(record.get("created_date", "")),
                "last_updated_date": str(record.get("last_updated_date", "")),
                "last_crawl_date": str(record.get("last_crawl_date", "")),
                "num_active_seeds": record.get("num_active_seeds", ""),
                "num_inactive_seeds": record.get("num_inactive_seeds", ""),
                "topics": str(record.get("topics", "") or ""),
                "metadata_title": _joined_metadata(record, "Title"),
                "metadata_subject": _joined_metadata(record, "Subject"),
                "metadata_description": _joined_metadata(record, "Description"),
                "metadata_coverage": _joined_metadata(record, "Coverage"),
                "metadata_creator": _joined_metadata(record, "Creator"),
                "source_metadata_json": json.dumps(
                    metadata, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                ),
                "source_collection_json": json.dumps(
                    record, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
                ),
                "matched_target_locations": json.dumps([
                    {"geo_name": city, "state_abb": state}
                    for city, state in matched_locations
                ], sort_keys=True, separators=(",", ":")),
                "year_hints": ";".join(map(str, years)),
                "target_period_hint": _target_period_hint(years),
                "relevance_flags": ";".join(flags),
                "priority_score": score,
                "known_collection": collection_id in KNOWN_COLLECTION_IDS,
                "review_status": "pending",
                "promotion_status": (
                    "already_in_seed_adapter" if collection_id in KNOWN_COLLECTION_IDS
                    else "not_promoted"
                ),
                "api_endpoint": COLLECTION_API_URL,
                "api_request_url": COLLECTION_API_REQUEST_URL,
                "retrieved_at": timestamp,
                "parser_version": PARSER_VERSION,
            })

        attempt_rows.append({
            "attempt_id": _stable_id("archiveit_attempt", query.query_id, timestamp),
            "source_id": SOURCE_ID,
            "query_id": query.query_id,
            "query_kind": query.query_kind,
            "query_term": query.query_term,
            "query_city": query.query_city,
            "query_state": query.query_state,
            "api_endpoint": COLLECTION_API_URL,
            "api_request_url": COLLECTION_API_REQUEST_URL,
            "request_params_json": json.dumps(COLLECTION_API_PARAMS, sort_keys=True),
            "source_status": "found" if matches else "no_result",
            "raw_records_considered": len(records),
            "matched_collections": matches,
            "error": "",
            "retrieved_at": timestamp,
            "parser_version": PARSER_VERSION,
        })

    candidates = pd.DataFrame(candidate_rows).reindex(columns=CANDIDATE_COLUMNS)
    attempts = pd.DataFrame(attempt_rows).reindex(columns=ATTEMPT_COLUMNS)
    if not candidates.empty:
        candidates = candidates.sort_values(
            ["priority_score", "collection_id", "query_kind", "query_term"],
            ascending=[False, True, True, True],
            kind="stable",
        ).reset_index(drop=True)
    attempts = attempts.sort_values(
        ["query_kind", "query_state", "query_city", "query_term"], kind="stable",
    ).reset_index(drop=True)
    return candidates, attempts


def _single_collection_value(group: pd.DataFrame, column: str) -> Any:
    """Return one collection-stable value and reject conflicting provenance."""
    values = group[column].dropna().tolist()
    distinct: list[Any] = []
    for value in values:
        if not any(value == existing for existing in distinct):
            distinct.append(value)
    if len(distinct) > 1:
        collection_id = group["collection_id"].iloc[0]
        raise ValueError(
            f"Collection {collection_id} has conflicting {column} values"
        )
    return distinct[0] if distinct else ""


def _json_list_union(values: Iterable[object]) -> str:
    members: set[tuple[str, str]] = set()
    for raw in values:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        try:
            parsed = json.loads(str(raw))
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid aggregated JSON list: {error}") from error
        if not isinstance(parsed, list):
            raise ValueError("Aggregated JSON provenance must contain a list")
        for item in parsed:
            canonical = json.dumps(
                item, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
            )
            members.add((canonical.casefold(), canonical))
    return "[" + ",".join(value for _, value in sorted(members)) + "]"


def _semicolon_union(values: Iterable[object], *, numeric: bool = False) -> str:
    members: set[str] = set()
    for raw in values:
        if raw is None or (isinstance(raw, float) and pd.isna(raw)):
            continue
        for value in str(raw).split(";"):
            if value.strip():
                members.add(value.strip())
    if numeric:
        return ";".join(sorted(members, key=lambda value: int(value)))
    return ";".join(sorted(members, key=str.casefold))


def _boolean_value(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return False
    return str(value).strip().casefold() in {"1", "true", "yes"}


def build_collection_priority(candidates: pd.DataFrame) -> pd.DataFrame:
    """Build one deterministic, provenance-preserving row per collection."""
    required = set(CANDIDATE_COLUMNS)
    missing = required - set(candidates.columns)
    if missing:
        raise ValueError(f"Collection candidate claims missing columns: {sorted(missing)}")
    if candidates.empty:
        return pd.DataFrame(columns=PRIORITY_COLUMNS)
    if candidates["collection_id"].isna().any():
        raise ValueError("Collection candidate claims contain missing collection IDs")
    if candidates["collection_candidate_id"].duplicated().any():
        raise ValueError("Collection candidate claim IDs are duplicated")

    stable_columns = [
        "source_id", "collection_name", "collection_access_url", "account_id",
        "state", "publicly_visible", "deleted", "created_date",
        "last_updated_date", "last_crawl_date", "num_active_seeds",
        "num_inactive_seeds", "topics", "metadata_title", "metadata_subject",
        "metadata_description", "metadata_coverage", "metadata_creator",
        "source_metadata_json", "source_collection_json", "target_period_hint",
        "known_collection", "review_status", "api_endpoint", "api_request_url",
        "retrieved_at", "parser_version",
    ]
    rows: list[dict[str, Any]] = []
    numeric_ids = pd.to_numeric(candidates["collection_id"], errors="raise").astype(int)
    working = candidates.assign(collection_id=numeric_ids)
    for collection_id, group in working.groupby("collection_id", sort=True):
        values = {
            column: _single_collection_value(group, column)
            for column in stable_columns
        }
        known = _boolean_value(values["known_collection"])
        detailed_queries = [
            {
                "query_id": str(row.query_id),
                "query_kind": str(row.query_kind),
                "query_term": str(row.query_term),
                "query_city": "" if pd.isna(row.query_city) else str(row.query_city),
                "query_state": "" if pd.isna(row.query_state) else str(row.query_state),
            }
            for row in group[[
                "query_id", "query_kind", "query_term", "query_city", "query_state",
            ]].itertuples(index=False)
        ]
        detailed_queries = sorted(
            {item["query_id"]: item for item in detailed_queries}.values(),
            key=lambda item: item["query_id"],
        )
        row = {
            "collection_priority_id": _stable_id(
                "archiveit_collection_priority", SOURCE_ID, collection_id,
            ),
            **values,
            "collection_id": int(collection_id),
            "query_claim_count": len(group),
            "matched_query_ids": json.dumps(
                sorted(set(group.query_id.astype(str))), separators=(",", ":"),
            ),
            "matched_query_kinds": json.dumps(
                sorted(set(group.query_kind.astype(str))), separators=(",", ":"),
            ),
            "matched_query_terms": json.dumps(
                sorted(set(group.query_term.astype(str)), key=str.casefold),
                ensure_ascii=False, separators=(",", ":"),
            ),
            "matched_queries_json": json.dumps(
                detailed_queries, sort_keys=True, ensure_ascii=False, separators=(",", ":"),
            ),
            "matched_target_locations": _json_list_union(group.matched_target_locations),
            "year_hints": _semicolon_union(group.year_hints, numeric=True),
            "relevance_flags": _semicolon_union(group.relevance_flags),
            "priority_score": int(pd.to_numeric(group.priority_score, errors="raise").max()),
            "known_collection": known,
            "collection_novelty": "known_adapter_collection" if known else "new_collection",
            "promotion_status": (
                "already_in_seed_adapter" if known else "pending_promotion_review"
            ),
        }
        rows.append(row)
    priority = pd.DataFrame(rows).reindex(columns=PRIORITY_COLUMNS)
    priority = priority.sort_values(
        ["priority_score", "collection_id"], ascending=[False, True], kind="stable",
    ).reset_index(drop=True)
    if priority.collection_id.duplicated().any():
        raise AssertionError("Deduplicated Archive-It priority output has duplicate collections")
    if set(priority.collection_id) != set(working.collection_id):
        raise AssertionError("Priority output and query claims disagree on collection IDs")
    return priority


def _write_csv_atomic(frame: pd.DataFrame, path: Path, columns: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False,
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False)
    temporary.replace(path)


def _write_json_atomic(payload: Mapping[str, Any], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False,
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True, ensure_ascii=False)
        handle.write("\n")
    temporary.replace(path)


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe",
        default="data/municipal_discovery/final/mayoral_candidate_universe.csv",
    )
    parser.add_argument(
        "--output-dir", default="data/municipal_discovery/archiveit",
    )
    parser.add_argument(
        "--catalog-json",
        help="Use a saved public /api/collection response instead of a live request.",
    )
    parser.add_argument(
        "--candidate-claims",
        help=(
            "Rebuild only the deduplicated priority output from an existing "
            "query-claim CSV; this performs no public request."
        ),
    )
    parser.add_argument(
        "--fetch-live", action="store_true",
        help="Fetch the public Archive-It collection catalog (no API key or paid service).",
    )
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    args = parser.parse_args()

    input_modes = sum(bool(value) for value in (
        args.catalog_json, args.fetch_live, args.candidate_claims,
    ))
    if input_modes != 1:
        parser.error(
            "Supply exactly one of --catalog-json, --fetch-live, or --candidate-claims"
        )

    output_dir = Path(args.output_dir)
    candidate_path = output_dir / "mayoral_archiveit_collection_candidates.csv"
    attempt_path = output_dir / "mayoral_archiveit_collection_query_attempts.csv"
    priority_path = output_dir / "mayoral_archiveit_collection_priority.csv"
    summary_path = output_dir / "mayoral_archiveit_collection_enumeration_summary.json"
    if args.candidate_claims:
        candidates = pd.read_csv(args.candidate_claims, low_memory=False)
        priority = build_collection_priority(candidates)
        _write_csv_atomic(priority, priority_path, PRIORITY_COLUMNS)
        priority_known = int(priority.known_collection.map(_boolean_value).sum())
        if summary_path.exists():
            summary = json.loads(summary_path.read_text(encoding="utf-8"))
        else:
            summary = {
                "api_endpoint": COLLECTION_API_URL,
                "api_request_url": COLLECTION_API_REQUEST_URL,
                "query_collection_claims": len(candidates),
                "unique_candidate_collections": int(candidates.collection_id.nunique()),
                "strict_urls_added": 0,
                "parser_version": PARSER_VERSION,
            }
        summary.update({
            "deduplicated_priority_rows": len(priority),
            "priority_known_collections": priority_known,
            "priority_new_collections": len(priority) - priority_known,
            "priority_output_file": priority_path.name,
            "priority_built_from_claims": str(args.candidate_claims),
            "priority_built_at": datetime.now(timezone.utc).isoformat(),
        })
        _write_json_atomic(summary, summary_path)
        print(json.dumps(summary, indent=2, sort_keys=True))
        return

    universe = pd.read_csv(args.universe, low_memory=False)
    retrieved_at = datetime.now(timezone.utc).isoformat()
    catalog: list[dict[str, Any]] = []
    catalog_error: tuple[str, str] | None = None
    if args.catalog_json:
        try:
            catalog = load_collection_catalog(Path(args.catalog_json))
        except (OSError, json.JSONDecodeError, ValueError) as error:
            catalog_error = ("request_error", f"{type(error).__name__}: {error}")
    else:
        try:
            catalog = fetch_collection_catalog(
                timeout=args.timeout,
                attempts=args.attempts,
                delay_seconds=args.delay_seconds,
            )
        except ArchiveItCollectionSourceUnavailable as error:
            catalog_error = ("source_unavailable", str(error))
        except ArchiveItCollectionRequestError as error:
            catalog_error = ("request_error", str(error))

    candidates, attempts = enumerate_collection_candidates(
        universe, catalog, retrieved_at=retrieved_at, catalog_error=catalog_error,
    )
    priority = build_collection_priority(candidates)
    _write_csv_atomic(candidates, candidate_path, CANDIDATE_COLUMNS)
    _write_csv_atomic(attempts, attempt_path, ATTEMPT_COLUMNS)
    _write_csv_atomic(priority, priority_path, PRIORITY_COLUMNS)
    unique_collections = (
        int(candidates.collection_id.nunique()) if not candidates.empty else 0
    )
    new_collections = (
        int(candidates.loc[~candidates.known_collection, "collection_id"].nunique())
        if not candidates.empty else 0
    )
    priority_known = int(priority.known_collection.map(_boolean_value).sum())
    summary = {
        "api_endpoint": COLLECTION_API_URL,
        "api_request_url": COLLECTION_API_REQUEST_URL,
        "catalog_records_received": len(catalog),
        "logical_queries": len(attempts),
        "query_statuses": {
            str(status): int(count)
            for status, count in attempts.source_status.value_counts().sort_index().items()
        },
        "query_collection_claims": len(candidates),
        "unique_candidate_collections": unique_collections,
        "new_candidate_collections": new_collections,
        "known_adapter_collections_found": sorted(
            set(candidates.loc[candidates.known_collection, "collection_id"].astype(int))
        ) if not candidates.empty else [],
        "deduplicated_priority_rows": len(priority),
        "priority_known_collections": priority_known,
        "priority_new_collections": len(priority) - priority_known,
        "priority_output_file": priority_path.name,
        "priority_built_from_claims": "",
        "priority_built_at": retrieved_at,
        "strict_urls_added": 0,
        "note": (
            "Collection candidates are discovery leads. Promote selected collections to "
            "seed extraction and temporal ownership validation before coverage use."
        ),
        "retrieved_at": retrieved_at,
        "parser_version": PARSER_VERSION,
    }
    _write_json_atomic(summary, summary_path)
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
