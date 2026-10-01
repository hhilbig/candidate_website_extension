#!/usr/bin/env python3
"""Discover mayoral campaign URL leads from public Archive-It seed metadata.

The Archive-It Partner API is public and free for these collection/seed queries.
This adapter deliberately stops at a reviewable evidence lead: Archive-It seed
metadata is source support (grade C), while a later archive-content validator
must establish election-period ownership before a URL can enter strict coverage.
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
from datetime import date
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable, Mapping
from urllib.parse import parse_qs, unquote, urlsplit, urlunsplit

import pandas as pd
import requests


PARTNER_API_SEED_URL = "https://partner.archive-it.org/api/seed"
USER_AGENT = "CandidateWebsiteExtension/1.0 (academic research; public metadata only)"
PARSER_VERSION = "archive_it_mayoral_v1"
TERMINAL_ATTEMPT_STATUSES = {
    "found", "no_result", "source_unavailable", "request_error", "match_unresolved",
}


@dataclass(frozen=True)
class CollectionConfig:
    collection_id: int
    source_id: str
    collection_name: str
    geo_name: str
    state_abb: str
    start_year: int
    end_year: int
    default_year: int | None = None
    collection_scoped_mayoral_seeds: bool = False

    @property
    def collection_access_url(self) -> str:
        return f"https://archive-it.org/collections/{self.collection_id}"


COLLECTION_CONFIGS: dict[int, CollectionConfig] = {
    5903: CollectionConfig(
        collection_id=5903,
        source_id="archive_it_collection_5903",
        collection_name="UCLA Online Campaign Literature Web Archive",
        geo_name="los angeles",
        state_abb="CA",
        start_year=2001,
        end_year=2017,
    ),
    16766: CollectionConfig(
        collection_id=16766,
        source_id="archive_it_collection_16766",
        collection_name="Birmingham Mayoral Election 2021 Web Archive",
        geo_name="birmingham",
        state_abb="AL",
        start_year=2021,
        end_year=2021,
        default_year=2021,
    ),
    9517: CollectionConfig(
        collection_id=9517,
        source_id="archive_it_collection_9517",
        collection_name="Birmingham Mayoral Race 2017",
        geo_name="birmingham",
        state_abb="AL",
        start_year=2017,
        end_year=2017,
        default_year=2017,
    ),
    13056: CollectionConfig(
        collection_id=13056,
        source_id="archive_it_collection_13056",
        collection_name="City of Grand Rapids elections - November 2019",
        geo_name="grand rapids",
        state_abb="MI",
        start_year=2019,
        end_year=2019,
        default_year=2019,
        collection_scoped_mayoral_seeds=True,
    ),
    17435: CollectionConfig(
        collection_id=17435,
        source_id="archive_it_collection_17435",
        collection_name="Cleveland Ohio 2021 Mayoral Race",
        geo_name="cleveland",
        state_abb="OH",
        start_year=2021,
        end_year=2021,
        default_year=2021,
        collection_scoped_mayoral_seeds=True,
    ),
}


EVIDENCE_COLUMNS = [
    "claim_id", "source_id", "parser_version",
    "candidate_year_id", "candidate_contest_id", "contest_id", "ledb_candid",
    "full_name", "geo_name", "state_abb", "year", "month",
    "candidate_source", "candidate_source_doi",
    "collection_id", "collection_name", "collection_access_url",
    "seed_id", "seed_created_date", "seed_updated_date", "seed_type",
    "seed_active", "seed_deleted", "seed_publicly_visible",
    "url_source", "source_profile_url", "archive_calendar_url",
    "source_snapshot_timestamp", "source_date", "source_election_date",
    "source_election_month", "source_election_stage", "retrieved_on",
    "raw_url", "exact_normalized_url", "evidence_url", "site_root_url",
    "registrable_domain", "url_type", "target_url_source",
    "source_candidate_name", "identity_match", "identity_match_score",
    "identity_match_method", "temporal_match", "evidence_grade",
    "strict_coverage_eligible", "temporal_validation_status", "review_status",
    "acceptance", "evidence_text", "discovery_method", "confidence",
    "source_metadata_json", "source_seed_json",
]


ATTEMPT_COLUMNS = [
    "attempt_id", "source_id", "collection_id", "collection_name",
    "contest_id", "geo_name", "state_abb", "year", "month",
    "source_status", "mayoral_seeds_considered", "candidate_matches",
    "eligible_url_leads", "excluded_url_leads", "error", "retrieved_on",
    "parser_version",
]


SEED_RECORD_COLUMNS = [
    "source_id", "collection_id", "collection_name", "collection_access_url",
    "seed_id", "seed_created_date", "seed_updated_date", "seed_type",
    "seed_active", "seed_deleted", "seed_publicly_visible",
    "raw_url", "exact_normalized_url", "site_root_url", "registrable_domain",
    "url_type", "target_url_source", "archive_calendar_url",
    "source_geo_name", "source_state_abb", "source_years",
    "source_election_dates", "source_election_stages", "candidate_name_hints",
    "metadata_title", "metadata_creator", "metadata_subject",
    "metadata_coverage", "metadata_description", "source_metadata_json",
    "source_seed_json",
    "considered_contest_ids", "matched_candidate_year_ids", "match_status",
    "parser_version",
]


SOCIAL_HOSTS = {
    "facebook.com", "instagram.com", "linkedin.com", "tiktok.com",
    "twitter.com", "x.com", "youtube.com",
}
FUNDRAISING_HOSTS = {
    "actblue.com", "anedot.com", "donorbox.org", "gofundme.com",
    "paypal.com", "winred.com",
}
SHARED_HOSTS = {
    "blogspot.com", "digital.library.ucla.edu", "geocities.com", "github.io",
    "nationbuilder.com", "squarespace.com", "tumblr.com", "web.mac.com",
    "weebly.com", "wixsite.com", "wordpress.com",
}
ARCHIVE_WRAPPER_HOSTS = {
    "archive-it.org", "web.archive.org", "wayback.archive-it.org",
}
COMMON_SECOND_LEVEL_SUFFIXES = {
    "ac.uk", "co.nz", "co.uk", "com.au", "com.br", "com.mx", "gov.uk",
    "net.au", "org.au", "org.uk",
}
NICKNAME_GROUPS = (
    {"bill", "billy", "will", "william"},
    {"bob", "bobby", "rob", "robert"},
    {"chris", "christopher"},
    {"dick", "rich", "richard", "rick"},
    {"ed", "eddie", "edward"},
    {"jim", "jimmy", "james"},
    {"mike", "michael"},
    {"tony", "anthony"},
)


class ArchiveItRequestError(RuntimeError):
    """A public Partner API request failed after bounded retries."""


class ArchiveItSourceUnavailable(RuntimeError):
    """A configured collection is unavailable to a public API client."""


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(str(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _host_matches(host: str, suffixes: Iterable[str]) -> bool:
    return any(host == suffix or host.endswith(f".{suffix}") for suffix in suffixes)


def unwrap_archive_url(value: str) -> str:
    """Extract an original target from Wayback or UCLA wrapper syntax.

    UCLA copied many early sites to a collection-local path. A local copy with
    no embedded original host cannot be reverse engineered and is intentionally
    retained as a shared-host path. URLs whose path embeds the old host are
    unwrapped (for example ``.../2009_999_006/web.mac.com/...``).
    """
    if not isinstance(value, str):
        return ""
    current = value.strip().replace("&amp;", "&")
    for _ in range(4):
        parsed = urlsplit(current)
        host = (parsed.hostname or "").casefold().removeprefix("www.")

        query = parse_qs(parsed.query)
        query_target = next(
            (items[0] for key in ("url", "target", "original") if (items := query.get(key))),
            "",
        )
        if host in ARCHIVE_WRAPPER_HOSTS and query_target.startswith(("http://", "https://")):
            current = unquote(query_target)
            continue

        if host in ARCHIVE_WRAPPER_HOSTS:
            target_match = re.search(r"/(https?://.+)$", unquote(parsed.path), re.I)
            if target_match:
                current = target_match.group(1)
                if parsed.query:
                    current += f"?{parsed.query}"
                continue

        if host == "digital.library.ucla.edu":
            match = re.match(r"^/websites/[^/]+/(.+)$", unquote(parsed.path), re.I)
            if match:
                tail = match.group(1).lstrip("/")
                if tail.startswith(("http://", "https://")):
                    current = tail
                    if parsed.query:
                        current += f"?{parsed.query}"
                    continue
                first_segment = tail.split("/", 1)[0]
                if "." in first_segment and not first_segment.startswith("."):
                    current = f"http://{tail}"
                    if parsed.query:
                        current += f"?{parsed.query}"
                    continue
        break
    return current


def normalize_exact_url(value: str) -> str:
    """Normalize scheme/host while retaining the exact path, query, and case."""
    target = unwrap_archive_url(value)
    if not target.startswith(("http://", "https://")):
        return ""
    parsed = urlsplit(target)
    if not parsed.hostname:
        return ""
    scheme = parsed.scheme.casefold()
    host = parsed.hostname.casefold()
    if ":" in host and not host.startswith("["):
        host = f"[{host}]"
    port = f":{parsed.port}" if parsed.port else ""
    userinfo = ""
    if parsed.username:
        userinfo = parsed.username
        if parsed.password:
            userinfo += f":{parsed.password}"
        userinfo += "@"
    return urlunsplit((scheme, f"{userinfo}{host}{port}", parsed.path, parsed.query, ""))


def site_root(value: str) -> str:
    normalized = normalize_exact_url(value)
    if not normalized:
        return ""
    parsed = urlsplit(normalized)
    return urlunsplit((parsed.scheme, parsed.netloc, "/", "", ""))


def registrable_domain(value: str) -> str:
    normalized = normalize_exact_url(value)
    host = (urlsplit(normalized).hostname or "").casefold().removeprefix("www.")
    if not host or re.fullmatch(r"\d+(?:\.\d+){3}", host) or ":" in host:
        return host
    labels = host.split(".")
    if len(labels) <= 2:
        return host
    suffix2 = ".".join(labels[-2:])
    return ".".join(labels[-3:]) if suffix2 in COMMON_SECOND_LEVEL_SUFFIXES else suffix2


def classify_url(value: str, metadata: Mapping[str, Any] | None = None) -> str:
    normalized = normalize_exact_url(value)
    if not normalized:
        return "excluded_noncampaign_lead"
    host = (urlsplit(normalized).hostname or "").casefold().removeprefix("www.")
    metadata_text = json.dumps(metadata or {}, ensure_ascii=False).casefold()
    if "opposition site" in metadata_text:
        return "excluded_opposition_lead"
    if _host_matches(host, SOCIAL_HOSTS):
        return "excluded_social_lead"
    if _host_matches(host, FUNDRAISING_HOSTS):
        return "excluded_fundraising_lead"
    if _host_matches(host, ARCHIVE_WRAPPER_HOSTS):
        return "candidate_subdomain_or_shared_host_path"
    if _host_matches(host, SHARED_HOSTS):
        return "candidate_subdomain_or_shared_host_path"
    registered = registrable_domain(normalized)
    if host != registered and not host.startswith("www."):
        return "candidate_subdomain_or_shared_host_path"
    return "candidate_owned_custom_domain"


def _metadata_values(seed: Mapping[str, Any], field: str) -> list[str]:
    metadata = seed.get("metadata") or {}
    values: list[str] = []
    for key, items in metadata.items():
        if str(key).casefold() != field.casefold():
            continue
        if not isinstance(items, list):
            items = [items]
        for item in items:
            value = item.get("value", "") if isinstance(item, Mapping) else item
            if str(value).strip():
                values.append(str(value).strip())
    return values


def _joined_metadata(seed: Mapping[str, Any], field: str) -> str:
    return " | ".join(_metadata_values(seed, field))


def is_mayoral_seed(seed: Mapping[str, Any]) -> bool:
    """Use structured subject headings, not accidental URL/title substrings."""
    return any(re.search(r"\bmayors?\b", value, re.I) for value in _metadata_values(seed, "Subject"))


def _metadata_original_url(seed: Mapping[str, Any]) -> str:
    for value in _metadata_values(seed, "Identifier"):
        match = re.search(r"URL\s+at\s+time\s+of\s+capture\s*:\s*(https?://\S+)", value, re.I)
        if match:
            return match.group(1).rstrip(".,;)")
    return ""


def _source_geo_name(seed: Mapping[str, Any], config: CollectionConfig) -> str:
    if config.collection_id == 16766:
        return config.geo_name
    for value in _metadata_values(seed, "Coverage"):
        match = re.search(r"\bCity of (.+)$", value, re.I)
        if match:
            city = re.sub(r"\s*\(Proposed\)\s*$", "", match.group(1), flags=re.I)
            return " ".join(city.casefold().split())
    subjects = " | ".join(_metadata_values(seed, "Subject"))
    if re.search(r"\bLos Angeles Mayor\b", subjects, re.I):
        return "los angeles"
    return config.geo_name


def _source_dates(seed: Mapping[str, Any]) -> list[str]:
    dates: list[str] = []
    for value in _metadata_values(seed, "Date of Election"):
        dates.extend(re.findall(r"\b(?:19|20)\d{2}-\d{2}-\d{2}\b", value))
    return list(dict.fromkeys(dates))


def _source_years(seed: Mapping[str, Any], config: CollectionConfig) -> list[int]:
    dates = _source_dates(seed)
    years = [int(value[:4]) for value in dates]
    if not years:
        for field in ("Subject", "Title", "Description"):
            for value in _metadata_values(seed, field):
                years.extend(int(year) for year in re.findall(r"\b(?:19|20)\d{2}\b", value))
    if not years and config.default_year is not None:
        years = [config.default_year]
    return sorted({year for year in years if config.start_year <= year <= config.end_year})


def _source_stages(seed: Mapping[str, Any]) -> list[str]:
    stages: list[str] = []
    text = " | ".join(_metadata_values(seed, "Subject"))
    for token, label in (
        ("primary election", "primary"), ("general election", "general"),
        ("runoff", "runoff"), ("special election", "special"),
    ):
        if token in text.casefold():
            stages.append(label)
    return stages


def _ascii_words(value: str) -> list[str]:
    ascii_value = unicodedata.normalize("NFKD", str(value)).encode(
        "ascii", "ignore"
    ).decode()
    return re.findall(r"[a-z]+", ascii_value.casefold())


def _clean_name_hint(value: str) -> str:
    value = re.sub(r"\[[^]]+\]", " ", str(value))
    value = re.sub(r"\((?:19|20)\d{2}(?:-\d{2}-\d{2})?\)", " ", value)
    value = re.sub(r"\((?:Ala\.?|California)\)", " ", value, flags=re.I)
    value = re.sub(
        r"^(?:committee|friends|people)\s+(?:to\s+)?(?:re-?elect\s+|elect\s+)?",
        "", value, flags=re.I,
    )
    value = re.sub(r"^(?:elect|re-?elect)\s+", "", value, flags=re.I)
    value = re.sub(r"\s+for\s+mayor(?:\s+of\s+.+)?$", "", value, flags=re.I)
    value = re.sub(r"\s+(?:bhm\s+)?mayor\s*(?:20\d{2})?$", "", value, flags=re.I)
    value = " ".join(value.strip(" -:?,.").split())
    if "," in value:
        surname, rest = value.split(",", 1)
        if surname.strip() and rest.strip():
            value = f"{rest.strip()} {surname.strip()}"
    return value


def candidate_name_hints(seed: Mapping[str, Any], config: CollectionConfig) -> list[str]:
    fields = (
        ("Subject", "Creator", "Title", "Publisher", "Contributor")
        if config.collection_id == 16766
        else ("Creator", "Title", "Subject", "Contributor", "Publisher")
    )
    hints: list[str] = []
    for field in fields:
        for raw in _metadata_values(seed, field):
            lowered = raw.casefold()
            if " -- " in raw or any(term in lowered for term in (
                "politics and government", "archived website", "unknown",
            )):
                continue
            cleaned = _clean_name_hint(raw)
            tokens = _ascii_words(cleaned)
            if not 2 <= len(tokens) <= 7:
                continue
            if any(token in tokens for token in ("committee", "website", "home", "district")):
                continue
            hints.append(cleaned)
    deduplicated: list[str] = []
    seen: set[str] = set()
    for hint in hints:
        key = " ".join(_ascii_words(hint))
        if key and key not in seen:
            deduplicated.append(hint)
            seen.add(key)
    return deduplicated


def _normalized_name_tokens(value: str) -> list[str]:
    tokens = _ascii_words(_clean_name_hint(value))
    suffixes = {"ii", "iii", "iv", "jr", "sr"}
    return [token for token in tokens if token not in suffixes]


def _nickname_equivalent(left: str, right: str) -> bool:
    return any(left in group and right in group for group in NICKNAME_GROUPS)


def _name_score(source_name: str, candidate_name: str) -> tuple[float, str]:
    source = _normalized_name_tokens(source_name)
    candidate = _normalized_name_tokens(candidate_name)
    if not source or not candidate:
        return 0.0, "empty_name"
    if source == candidate:
        return 1.0, "exact_normalized_name"

    source_compact = [token for token in source if len(token) > 1]
    candidate_compact = [token for token in candidate if len(token) > 1]
    if source_compact == candidate_compact:
        return 0.99, "exact_name_ignoring_initials"

    source_surname, candidate_surname = source[-1], candidate[-1]
    source_given, candidate_given = source[:-1], candidate[:-1]
    if source_surname == candidate_surname:
        source_full = {token for token in source_given if len(token) > 1}
        candidate_full = {token for token in candidate_given if len(token) > 1}
        if source_full & candidate_full:
            return 0.98, "same_surname_given_name"
        if any(_nickname_equivalent(a, b) for a in source_full for b in candidate_full):
            return 0.96, "same_surname_nickname"
        if source_given and candidate_given and source_given[0][0] == candidate_given[0][0]:
            return 0.93, "same_surname_given_initial"

    source_text = " ".join(source_compact)
    candidate_text = " ".join(candidate_compact)
    ratio = SequenceMatcher(None, source_text, candidate_text).ratio()
    first_initial_matches = bool(
        source_given and candidate_given and source_given[0][0] == candidate_given[0][0]
    )
    if ratio >= 0.88 and first_initial_matches:
        return ratio, "fuzzy_name_with_given_initial"
    return ratio, "fuzzy_name"


def _url_candidate_score(value: str, candidate_name: str) -> tuple[float, str]:
    """Score candidate-shaped hostnames without treating article paths as identity evidence."""
    normalized = normalize_exact_url(value)
    host = (urlsplit(normalized).hostname or "").casefold().removeprefix("www.")
    label = re.sub(r"[^a-z0-9]+", "", host.split(".")[0])
    tokens = _normalized_name_tokens(candidate_name)
    if not label or len(tokens) < 2:
        return 0.0, "no_candidate_shaped_hostname"
    given, surname = tokens[0], tokens[-1]
    campaign_marker = any(marker in label for marker in ("elect", "for", "mayor", "vote"))
    if len(surname) >= 4 and surname in label:
        if given in label or campaign_marker:
            return 0.995, "candidate_surname_in_campaign_hostname"
        return 0.94, "candidate_surname_in_hostname"
    if len(given) >= 4 and given in label and campaign_marker:
        return 0.92, "candidate_given_name_in_campaign_hostname"
    return 0.0, "no_candidate_shaped_hostname"


def match_candidate(
    hints: list[str], candidates: pd.DataFrame, url: str = "",
) -> tuple[pd.Series | None, float, str, str]:
    """Return one high-score, separated candidate match or an unresolved result."""
    if candidates.empty:
        return None, 0.0, "", "no_candidate_name_hint"
    best_by_candidate: dict[int, tuple[float, str, str]] = {}
    for position, candidate in candidates.reset_index(drop=True).iterrows():
        scored = [(*_name_score(hint, candidate["full_name"]), hint) for hint in hints]
        url_score, url_method = _url_candidate_score(url, candidate["full_name"])
        scored.append((url_score, url_method, url))
        score, method, hint = max(scored, key=lambda item: item[0])
        best_by_candidate[position] = (score, method, hint)
    ranking = sorted(best_by_candidate.items(), key=lambda item: (-item[1][0], item[0]))
    best_position, (best_score, best_method, best_hint) = ranking[0]
    second_score = ranking[1][1][0] if len(ranking) > 1 else 0.0
    sufficiently_separated = best_score >= 0.985 or best_score - second_score >= 0.08
    if best_score < 0.90 or not sufficiently_separated:
        return None, best_score, best_hint, "candidate_match_unresolved"
    return candidates.reset_index(drop=True).iloc[best_position], best_score, best_hint, best_method


def _payload_records(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        records = payload
    elif isinstance(payload, Mapping):
        records = next(
            (payload[key] for key in ("results", "objects", "seeds", "data") if isinstance(payload.get(key), list)),
            None,
        )
        if records is None:
            raise ValueError("Archive-It seed JSON has no recognized record array")
    else:
        raise ValueError("Archive-It seed JSON must be an array or object containing an array")
    if not all(isinstance(record, Mapping) for record in records):
        raise ValueError("Archive-It seed JSON contains a non-object record")
    return [dict(record) for record in records]


def parse_collection_seeds(payload: Any, config: CollectionConfig) -> pd.DataFrame:
    """Return auditable seed rows from a structured or explicitly mayoral collection."""
    rows: list[dict[str, Any]] = []
    for seed in _payload_records(payload):
        record_collection = seed.get("collection")
        if record_collection not in (None, "", config.collection_id, str(config.collection_id)):
            continue
        if not is_mayoral_seed(seed) and not config.collection_scoped_mayoral_seeds:
            continue
        raw_url = str(seed.get("url") or seed.get("canonical_url") or "").strip()
        metadata_original = _metadata_original_url(seed)
        preferred_url = metadata_original or raw_url
        unwrapped = unwrap_archive_url(preferred_url)
        normalized = normalize_exact_url(unwrapped)
        target_source = "seed_url"
        if metadata_original:
            target_source = "metadata_url_at_time_of_capture"
        elif unwrapped != raw_url:
            target_source = "archive_wrapper_path"
        metadata = seed.get("metadata") or {}
        dates = _source_dates(seed)
        years = _source_years(seed, config)
        stages = _source_stages(seed)
        hints = candidate_name_hints(seed, config)
        seed_id = str(seed.get("id", ""))
        calendar_target = raw_url or normalized
        rows.append({
            "source_id": config.source_id,
            "collection_id": config.collection_id,
            "collection_name": config.collection_name,
            "collection_access_url": config.collection_access_url,
            "seed_id": seed_id,
            "seed_created_date": str(seed.get("created_date", "")),
            "seed_updated_date": str(seed.get("last_updated_date", "")),
            "seed_type": str(seed.get("seed_type", "")),
            "seed_active": seed.get("active", ""),
            "seed_deleted": seed.get("deleted", ""),
            "seed_publicly_visible": seed.get("publicly_visible", ""),
            "raw_url": raw_url,
            "exact_normalized_url": normalized,
            "site_root_url": site_root(normalized),
            "registrable_domain": registrable_domain(normalized),
            "url_type": classify_url(normalized, metadata),
            "target_url_source": target_source,
            "archive_calendar_url": f"https://wayback.archive-it.org/{config.collection_id}/*/{calendar_target}",
            "source_geo_name": _source_geo_name(seed, config),
            "source_state_abb": config.state_abb,
            "source_years": " | ".join(map(str, years)),
            "source_election_dates": " | ".join(dates),
            "source_election_stages": " | ".join(stages),
            "candidate_name_hints": " | ".join(hints),
            "metadata_title": _joined_metadata(seed, "Title"),
            "metadata_creator": _joined_metadata(seed, "Creator"),
            "metadata_subject": _joined_metadata(seed, "Subject"),
            "metadata_coverage": _joined_metadata(seed, "Coverage"),
            "metadata_description": _joined_metadata(seed, "Description"),
            "source_metadata_json": json.dumps(metadata, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
            "source_seed_json": json.dumps(seed, sort_keys=True, ensure_ascii=False, separators=(",", ":")),
            "considered_contest_ids": "",
            "matched_candidate_year_ids": "",
            "match_status": "not_attempted",
            "parser_version": PARSER_VERSION,
        })
    return pd.DataFrame(rows).reindex(columns=SEED_RECORD_COLUMNS)


def fetch_partner_seed_json(
    collection_id: int,
    timeout: int = 30,
    attempts: int = 3,
    delay_seconds: float = 1.0,
    session: Any = requests,
) -> Any:
    """Fetch all public seeds for one collection with bounded retries."""
    if attempts < 1:
        raise ValueError("attempts must be at least 1")
    last_error = "Archive-It Partner API request failed"
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(
                PARTNER_API_SEED_URL,
                params={"collection": int(collection_id), "limit": -1},
                headers={"User-Agent": USER_AGENT},
                timeout=timeout,
            )
            status_code = int(response.status_code)
            if status_code in {401, 403, 404}:
                raise ArchiveItSourceUnavailable(
                    f"Archive-It collection {collection_id} returned HTTP {status_code}"
                )
            if status_code == 429 or status_code >= 500:
                last_error = f"Archive-It collection {collection_id} returned HTTP {status_code}"
            elif status_code >= 400:
                raise ArchiveItRequestError(
                    f"Archive-It collection {collection_id} returned HTTP {status_code}"
                )
            else:
                try:
                    return response.json()
                except ValueError as error:
                    last_error = f"Archive-It collection {collection_id} returned invalid JSON: {error}"
        except ArchiveItSourceUnavailable:
            raise
        except ArchiveItRequestError:
            raise
        except requests.RequestException as error:
            last_error = f"{type(error).__name__}: {error}"
        if attempt < attempts:
            time.sleep(delay_seconds)
    raise ArchiveItRequestError(last_error)


def load_seed_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def load_collection_configs(path: Path) -> dict[int, CollectionConfig]:
    """Load explicit collection targets for a reproducible promoted batch."""
    payload = json.loads(path.read_text(encoding="utf-8"))
    records = payload.get("collections") if isinstance(payload, Mapping) else payload
    if not isinstance(records, list) or not records:
        raise ValueError("Archive-It config JSON must contain a nonempty collections list")
    required = {
        "collection_id", "source_id", "collection_name", "geo_name",
        "state_abb", "start_year", "end_year",
    }
    configs: dict[int, CollectionConfig] = {}
    for record in records:
        if not isinstance(record, Mapping):
            raise ValueError("Every Archive-It collection config must be an object")
        missing = required - set(record)
        if missing:
            raise ValueError(f"Archive-It collection config missing: {sorted(missing)}")
        collection_id = int(record["collection_id"])
        if collection_id in configs:
            raise ValueError(f"Duplicate Archive-It collection config: {collection_id}")
        config = CollectionConfig(
            collection_id=collection_id,
            source_id=str(record["source_id"]),
            collection_name=str(record["collection_name"]),
            geo_name=str(record["geo_name"]).strip().casefold(),
            state_abb=str(record["state_abb"]).strip().upper(),
            start_year=int(record["start_year"]),
            end_year=int(record["end_year"]),
            default_year=(
                int(record["default_year"])
                if record.get("default_year") not in (None, "") else None
            ),
            collection_scoped_mayoral_seeds=bool(
                record.get("collection_scoped_mayoral_seeds", False)
            ),
        )
        if config.start_year > config.end_year:
            raise ValueError(f"Invalid year range for Archive-It collection {collection_id}")
        configs[collection_id] = config
    source_ids = [config.source_id for config in configs.values()]
    if len(source_ids) != len(set(source_ids)):
        raise ValueError("Archive-It collection source_id values must be unique")
    return configs


def _scope_mask(universe: pd.DataFrame) -> pd.Series:
    if "in_discovery_scope" not in universe:
        return pd.Series(True, index=universe.index)
    values = universe["in_discovery_scope"]
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    return values.astype("string").str.casefold().isin({"1", "true", "yes"})


def _prepared_universe(universe: pd.DataFrame, config: CollectionConfig) -> pd.DataFrame:
    required = {"candidate_year_id", "full_name", "geo_name", "state_abb", "year"}
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Candidate universe missing columns: {sorted(missing)}")
    selected = universe.loc[
        _scope_mask(universe)
        & universe["geo_name"].astype("string").str.casefold().eq(config.geo_name)
        & universe["state_abb"].astype("string").str.upper().eq(config.state_abb)
        & pd.to_numeric(universe["year"], errors="coerce").between(config.start_year, config.end_year)
    ].copy()
    if "contest_id" in selected:
        selected["_contest_id"] = selected["contest_id"].astype("string")
    elif "contest" in selected:
        selected["_contest_id"] = selected["contest"].astype("string")
    elif "candidate_contest_id" in selected:
        selected["_contest_id"] = selected["candidate_contest_id"].astype("string").str.rsplit("__", n=1).str[0]
    else:
        month = selected["month"].astype("string") if "month" in selected else ""
        selected["_contest_id"] = (
            selected["geo_name"].astype("string") + "__" + selected["state_abb"].astype("string")
            + "__" + selected["year"].astype("Int64").astype("string") + "__" + month
        )
    if "month" not in selected:
        selected["month"] = pd.NA
    if "candidate_contest_id" not in selected:
        selected["candidate_contest_id"] = selected["_contest_id"] + "__" + selected["candidate_year_id"].astype("string")
    for column, default in (
        ("ledb_candid", ""), ("source_dataset", "American Local Government Elections Database"),
        ("source_doi", "10.17605/OSF.IO/MV5E6"),
    ):
        if column not in selected:
            selected[column] = default
    return selected


def _pipe_values(value: object) -> list[str]:
    if pd.isna(value) or not str(value).strip():
        return []
    return [item.strip() for item in str(value).split(" | ") if item.strip()]


def _seed_relevant_to_contest(seed: pd.Series, contest: pd.Series) -> bool:
    if str(seed["source_geo_name"]).casefold() != str(contest["geo_name"]).casefold():
        return False
    target_year = int(contest["year"])
    years = {int(value) for value in _pipe_values(seed["source_years"])}
    if target_year not in years:
        return False
    dates = [value for value in _pipe_values(seed["source_election_dates"]) if value.startswith(f"{target_year:04d}-")]
    month = contest.get("month")
    if dates and pd.notna(month):
        return any(int(value[5:7]) == int(float(month)) for value in dates)
    return True


def _contest_rows(candidates: pd.DataFrame) -> pd.DataFrame:
    columns = ["_contest_id", "geo_name", "state_abb", "year", "month"]
    return candidates[columns].drop_duplicates("_contest_id").sort_values(
        ["year", "month", "_contest_id"], na_position="last"
    ).reset_index(drop=True)


def match_seed_records(
    seed_records: pd.DataFrame, universe: pd.DataFrame, config: CollectionConfig,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Match seeds within city/year/contest and retain unresolved seed audit rows."""
    candidates = _prepared_universe(universe, config)
    contests = _contest_rows(candidates)
    evidence: list[dict[str, Any]] = []
    audited = seed_records.copy()

    for seed_index, seed in audited.iterrows():
        relevant_contests = contests[
            contests.apply(lambda contest: _seed_relevant_to_contest(seed, contest), axis=1)
        ]
        contest_ids = relevant_contests["_contest_id"].astype(str).tolist()
        audited.at[seed_index, "considered_contest_ids"] = " | ".join(contest_ids)
        if relevant_contests.empty:
            audited.at[seed_index, "match_status"] = "outside_target_contests"
            continue
        if len(relevant_contests) > 1 and not _pipe_values(seed["source_election_dates"]):
            audited.at[seed_index, "match_status"] = "temporal_match_unresolved"
            continue
        if seed["url_type"] == "excluded_opposition_lead":
            audited.at[seed_index, "match_status"] = "excluded_opposition_lead"
            continue

        matched_ids: list[str] = []
        best_unresolved_score = 0.0
        for _, contest in relevant_contests.iterrows():
            contest_id = str(contest["_contest_id"])
            contest_candidates = candidates[candidates["_contest_id"].astype(str).eq(contest_id)]
            # Broad election collections often attach candidate names to news
            # article titles.  For collections promoted on collection-level
            # mayoral scope, require the candidate identity in the hostname so
            # an article about a candidate cannot become a candidate URL claim.
            hints = (
                [] if config.collection_scoped_mayoral_seeds
                else _pipe_values(seed["candidate_name_hints"])
            )
            matched, score, source_name, method = match_candidate(
                hints, contest_candidates, str(seed["exact_normalized_url"]),
            )
            best_unresolved_score = max(best_unresolved_score, score)
            if matched is None:
                continue
            matched_ids.append(str(matched["candidate_year_id"]))
            target_year = int(matched["year"])
            target_month = matched["month"]
            relevant_dates = [
                value for value in _pipe_values(seed["source_election_dates"])
                if value.startswith(f"{target_year:04d}-")
                and (pd.isna(target_month) or int(value[5:7]) == int(float(target_month)))
            ]
            temporal_match = "exact_election_month" if relevant_dates else "year_only"
            source_date = str(seed["seed_created_date"])[:10]
            source_election_date = " | ".join(relevant_dates)
            source_election_month = (
                int(float(target_month)) if relevant_dates and pd.notna(target_month) else pd.NA
            )
            claim_id = _stable_id(
                "archiveit_claim", config.source_id, seed["seed_id"],
                matched["candidate_year_id"], seed["exact_normalized_url"],
            )
            excluded = str(seed["url_type"]).startswith("excluded_")
            evidence.append({
                "claim_id": claim_id,
                "source_id": config.source_id,
                "parser_version": PARSER_VERSION,
                "candidate_year_id": matched["candidate_year_id"],
                "candidate_contest_id": matched["candidate_contest_id"],
                "contest_id": contest_id,
                "ledb_candid": matched["ledb_candid"],
                "full_name": matched["full_name"],
                "geo_name": matched["geo_name"],
                "state_abb": matched["state_abb"],
                "year": target_year,
                "month": target_month,
                "candidate_source": matched["source_dataset"],
                "candidate_source_doi": matched["source_doi"],
                "collection_id": config.collection_id,
                "collection_name": config.collection_name,
                "collection_access_url": config.collection_access_url,
                "seed_id": seed["seed_id"],
                "seed_created_date": seed["seed_created_date"],
                "seed_updated_date": seed["seed_updated_date"],
                "seed_type": seed["seed_type"],
                "seed_active": seed["seed_active"],
                "seed_deleted": seed["seed_deleted"],
                "seed_publicly_visible": seed["seed_publicly_visible"],
                "url_source": f"Archive-It collection {config.collection_id} seed metadata",
                "source_profile_url": seed["archive_calendar_url"],
                "archive_calendar_url": seed["archive_calendar_url"],
                "source_snapshot_timestamp": "",
                "source_date": source_date,
                "source_election_date": source_election_date,
                "source_election_month": source_election_month,
                "source_election_stage": seed["source_election_stages"],
                "retrieved_on": date.today().isoformat(),
                "raw_url": seed["raw_url"],
                "exact_normalized_url": seed["exact_normalized_url"],
                "evidence_url": seed["exact_normalized_url"],
                "site_root_url": seed["site_root_url"],
                "registrable_domain": seed["registrable_domain"],
                "url_type": seed["url_type"],
                "target_url_source": seed["target_url_source"],
                "source_candidate_name": source_name,
                "identity_match": True,
                "identity_match_score": round(float(score), 6),
                "identity_match_method": method,
                "temporal_match": temporal_match,
                "evidence_grade": "C",
                "strict_coverage_eligible": False,
                "temporal_validation_status": "pending_archive_validation",
                "review_status": "pending",
                "acceptance": "",
                "evidence_text": " | ".join(filter(None, [
                    str(seed["metadata_title"]), str(seed["metadata_creator"]),
                    str(seed["metadata_subject"]),
                ]))[:1000],
                "discovery_method": "archive_it_partner_api_seed_metadata",
                "confidence": "excluded_lead" if excluded else "source_backed_pending_temporal_validation",
                "source_metadata_json": seed["source_metadata_json"],
                "source_seed_json": seed["source_seed_json"],
            })
        audited.at[seed_index, "matched_candidate_year_ids"] = " | ".join(dict.fromkeys(matched_ids))
        if matched_ids:
            audited.at[seed_index, "match_status"] = "matched_pending_archive_validation"
        else:
            audited.at[seed_index, "match_status"] = (
                f"candidate_match_unresolved_score_{best_unresolved_score:.3f}"
            )

    evidence_frame = pd.DataFrame(evidence).reindex(columns=EVIDENCE_COLUMNS)
    if not evidence_frame.empty:
        evidence_frame = evidence_frame.sort_values(
            ["collection_id", "year", "contest_id", "candidate_year_id", "seed_id", "claim_id"]
        ).reset_index(drop=True)
    return evidence_frame, audited.reindex(columns=SEED_RECORD_COLUMNS)


def _attempt_rows_for_collection(
    universe: pd.DataFrame,
    config: CollectionConfig,
    seed_records: pd.DataFrame,
    evidence: pd.DataFrame,
    collection_status: str | None = None,
    error: str = "",
) -> pd.DataFrame:
    candidates = _prepared_universe(universe, config)
    contests = _contest_rows(candidates)
    rows: list[dict[str, Any]] = []
    for _, contest in contests.iterrows():
        contest_id = str(contest["_contest_id"])
        relevant_seed_mask = seed_records["considered_contest_ids"].fillna("").map(
            lambda value: contest_id in _pipe_values(value)
        ) if not seed_records.empty else pd.Series(dtype=bool)
        relevant_seeds = seed_records.loc[relevant_seed_mask] if not seed_records.empty else seed_records
        relevant_evidence = evidence[evidence["contest_id"].astype(str).eq(contest_id)] if not evidence.empty else evidence
        if collection_status in {"source_unavailable", "request_error"}:
            status = collection_status
        elif not relevant_evidence.empty:
            status = "found"
        elif not relevant_seeds.empty and relevant_seeds["match_status"].str.contains("unresolved", na=False).any():
            status = "match_unresolved"
        else:
            status = "no_result"
        if status not in TERMINAL_ATTEMPT_STATUSES:
            raise AssertionError(f"Non-terminal Archive-It attempt status: {status}")
        url_types = relevant_evidence["url_type"].astype(str) if not relevant_evidence.empty else pd.Series(dtype=str)
        rows.append({
            "attempt_id": _stable_id("source_attempt", config.source_id, contest_id),
            "source_id": config.source_id,
            "collection_id": config.collection_id,
            "collection_name": config.collection_name,
            "contest_id": contest_id,
            "geo_name": contest["geo_name"],
            "state_abb": contest["state_abb"],
            "year": int(contest["year"]),
            "month": contest["month"],
            "source_status": status,
            "mayoral_seeds_considered": len(relevant_seeds),
            "candidate_matches": len(relevant_evidence),
            "eligible_url_leads": int((~url_types.str.startswith("excluded_", na=False)).sum()),
            "excluded_url_leads": int(url_types.str.startswith("excluded_", na=False).sum()),
            "error": str(error)[:500],
            "retrieved_on": date.today().isoformat(),
            "parser_version": PARSER_VERSION,
        })
    return pd.DataFrame(rows).reindex(columns=ATTEMPT_COLUMNS)


def discover_archiveit(
    universe: pd.DataFrame,
    payloads: Mapping[int, Any],
    collection_errors: Mapping[int, tuple[str, str]] | None = None,
    configs: Mapping[int, CollectionConfig] = COLLECTION_CONFIGS,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Build evidence, terminal source attempts, and a lossless seed audit."""
    errors = collection_errors or {}
    all_evidence: list[pd.DataFrame] = []
    all_attempts: list[pd.DataFrame] = []
    all_seeds: list[pd.DataFrame] = []
    for collection_id, config in configs.items():
        if collection_id in errors:
            status, message = errors[collection_id]
            if status not in {"source_unavailable", "request_error"}:
                raise ValueError(f"Invalid collection error status for {collection_id}: {status}")
            empty_evidence = pd.DataFrame(columns=EVIDENCE_COLUMNS)
            empty_seeds = pd.DataFrame(columns=SEED_RECORD_COLUMNS)
            all_attempts.append(_attempt_rows_for_collection(
                universe, config, empty_seeds, empty_evidence, status, message,
            ))
            continue
        if collection_id not in payloads:
            empty_evidence = pd.DataFrame(columns=EVIDENCE_COLUMNS)
            empty_seeds = pd.DataFrame(columns=SEED_RECORD_COLUMNS)
            all_attempts.append(_attempt_rows_for_collection(
                universe, config, empty_seeds, empty_evidence,
                "source_unavailable", "No saved JSON or live response supplied",
            ))
            continue
        try:
            parsed = parse_collection_seeds(payloads[collection_id], config)
            evidence, audited = match_seed_records(parsed, universe, config)
            all_evidence.append(evidence)
            all_seeds.append(audited)
            all_attempts.append(_attempt_rows_for_collection(
                universe, config, audited, evidence,
            ))
        except (TypeError, ValueError, KeyError) as exc:
            empty_evidence = pd.DataFrame(columns=EVIDENCE_COLUMNS)
            empty_seeds = pd.DataFrame(columns=SEED_RECORD_COLUMNS)
            all_attempts.append(_attempt_rows_for_collection(
                universe, config, empty_seeds, empty_evidence,
                "request_error", f"{type(exc).__name__}: {exc}",
            ))

    evidence_result = pd.concat(all_evidence, ignore_index=True) if all_evidence else pd.DataFrame(columns=EVIDENCE_COLUMNS)
    attempts_result = pd.concat(all_attempts, ignore_index=True) if all_attempts else pd.DataFrame(columns=ATTEMPT_COLUMNS)
    seeds_result = pd.concat(all_seeds, ignore_index=True) if all_seeds else pd.DataFrame(columns=SEED_RECORD_COLUMNS)
    if attempts_result.duplicated(["source_id", "contest_id"]).any():
        raise AssertionError("Archive-It emitted duplicate contest/source attempts")
    if not set(attempts_result["source_status"]).issubset(TERMINAL_ATTEMPT_STATUSES):
        raise AssertionError("Archive-It emitted a non-terminal source attempt")
    return (
        evidence_result.reindex(columns=EVIDENCE_COLUMNS),
        attempts_result.reindex(columns=ATTEMPT_COLUMNS),
        seeds_result.reindex(columns=SEED_RECORD_COLUMNS),
    )


def _write_csv_atomic(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False,
        encoding="utf-8", newline="",
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False)
    temporary.replace(path)


def _parse_collection_ids(value: str) -> list[int]:
    ids = [int(item.strip()) for item in value.split(",") if item.strip()]
    unknown = sorted(set(ids) - set(COLLECTION_CONFIGS))
    if unknown:
        raise ValueError(f"No Archive-It collection configuration for: {unknown}")
    return list(dict.fromkeys(ids))


def _parse_seed_json_arguments(values: list[str]) -> dict[int, Path]:
    result: dict[int, Path] = {}
    for value in values:
        if "=" not in value:
            raise ValueError("--seed-json must use COLLECTION_ID=PATH")
        collection, path = value.split("=", 1)
        collection_id = int(collection)
        if collection_id in result:
            raise ValueError(f"Duplicate --seed-json for collection {collection_id}")
        result[collection_id] = Path(path)
    return result


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--universe", default="data/municipal_discovery/final/mayoral_candidate_universe.csv",
    )
    parser.add_argument("--output-dir", default="data/municipal_discovery")
    parser.add_argument("--collections", default="5903,16766")
    parser.add_argument(
        "--config-json",
        help="Explicit promoted-collection config JSON; supplements built-in fixtures.",
    )
    parser.add_argument(
        "--seed-json", action="append", default=[], metavar="COLLECTION_ID=PATH",
        help="Consume a saved Partner API response; repeat for multiple collections.",
    )
    parser.add_argument(
        "--fetch-live", action="store_true",
        help="Fetch any selected collection not supplied with --seed-json from the free public API.",
    )
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--attempts", type=int, default=3)
    parser.add_argument("--delay-seconds", type=float, default=1.0)
    parser.add_argument(
        "--save-seed-json-dir",
        help="Optionally freeze each successful public API response as COLLECTION_ID.json.",
    )
    args = parser.parse_args()

    available_configs = dict(COLLECTION_CONFIGS)
    if args.config_json:
        try:
            available_configs.update(load_collection_configs(Path(args.config_json)))
        except (OSError, json.JSONDecodeError, ValueError) as error:
            parser.error(str(error))
    try:
        selected_ids = [int(item.strip()) for item in args.collections.split(",") if item.strip()]
        unknown = sorted(set(selected_ids) - set(available_configs))
        if unknown:
            raise ValueError(f"No Archive-It collection configuration for: {unknown}")
        selected_ids = list(dict.fromkeys(selected_ids))
        saved_paths = _parse_seed_json_arguments(args.seed_json)
    except ValueError as error:
        parser.error(str(error))
    unselected_saved = sorted(set(saved_paths) - set(selected_ids))
    if unselected_saved:
        parser.error(f"--seed-json supplied for unselected collections: {unselected_saved}")
    if not args.fetch_live and any(collection_id not in saved_paths for collection_id in selected_ids):
        parser.error("Supply --seed-json for every selected collection or add --fetch-live")

    payloads: dict[int, Any] = {}
    errors: dict[int, tuple[str, str]] = {}
    for collection_id in selected_ids:
        if collection_id in saved_paths:
            try:
                payloads[collection_id] = load_seed_json(saved_paths[collection_id])
            except (OSError, json.JSONDecodeError) as error:
                errors[collection_id] = ("request_error", f"{type(error).__name__}: {error}")
            continue
        try:
            payloads[collection_id] = fetch_partner_seed_json(
                collection_id, timeout=args.timeout, attempts=args.attempts,
                delay_seconds=args.delay_seconds,
            )
        except ArchiveItSourceUnavailable as error:
            errors[collection_id] = ("source_unavailable", str(error))
        except ArchiveItRequestError as error:
            errors[collection_id] = ("request_error", str(error))

    if args.save_seed_json_dir:
        seed_dir = Path(args.save_seed_json_dir)
        seed_dir.mkdir(parents=True, exist_ok=True)
        for collection_id, payload in payloads.items():
            target = seed_dir / f"{collection_id}.json"
            with tempfile.NamedTemporaryFile(
                mode="w", suffix=".json", dir=seed_dir, delete=False, encoding="utf-8",
            ) as handle:
                temporary = Path(handle.name)
                json.dump(payload, handle, ensure_ascii=False, sort_keys=True, indent=2)
                handle.write("\n")
            temporary.replace(target)

    configs = {collection_id: available_configs[collection_id] for collection_id in selected_ids}
    universe = pd.read_csv(args.universe, low_memory=False)
    evidence, attempts, seeds = discover_archiveit(
        universe, payloads, collection_errors=errors, configs=configs,
    )
    output_dir = Path(args.output_dir)
    _write_csv_atomic(evidence, output_dir / "mayoral_url_evidence_archiveit.csv", EVIDENCE_COLUMNS)
    _write_csv_atomic(attempts, output_dir / "mayoral_source_attempts_archiveit.csv", ATTEMPT_COLUMNS)
    _write_csv_atomic(seeds, output_dir / "mayoral_archiveit_seed_records.csv", SEED_RECORD_COLUMNS)
    summary = {
        "evidence_claims": len(evidence),
        "matched_candidate_years": int(evidence["candidate_year_id"].nunique()) if not evidence.empty else 0,
        "source_attempts": len(attempts),
        "attempt_statuses": {str(key): int(value) for key, value in attempts["source_status"].value_counts().sort_index().items()},
        "mayoral_seed_records_by_collection": {
            str(key): int(value) for key, value in seeds["collection_id"].value_counts().sort_index().items()
        } if not seeds.empty else {},
    }
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
