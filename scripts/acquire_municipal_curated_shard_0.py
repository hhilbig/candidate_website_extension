#!/usr/bin/env python3
"""Acquire and parse the frozen municipal-curated acquisition shard 0.

This runner is deliberately narrow.  It accepts only the frozen shard-0
contract, writes the exact four-entry shard interface, and never uses a paid
service.  A physical response is downloaded and parsed once even when it is
relevant to several contest targets.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import shutil
import time
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Mapping, Sequence
from urllib.parse import urlencode, urljoin, urlsplit

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.build_municipal_curated_acquisition_contract import (
    ATTEMPT_COLUMNS,
    CONTRACT_VERSION,
    EVIDENCE_COLUMNS,
    FILE_COLUMNS,
)
from scripts.extract_municipal_mayoral_urls import (
    SourceRecord,
    candidate_match,
    classify_url,
    deduplicate_records,
    normalize_exact_url,
    normalize_person_name,
    parse_local_file,
    registrable_domain,
    site_root_url,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
AUDIT_DIR = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/municipal_curated_acquisition/audit"
)
DEFAULT_TARGETS = AUDIT_DIR / "shard_0_targets.csv"
DEFAULT_CANDIDATES = AUDIT_DIR / "shard_0_candidate_targets.csv"
DEFAULT_OUTPUT = AUDIT_DIR.parent / "shard_0"

SHARD_ID = 0
EXPECTED_SOURCE_IDS = {
    "minneapolis_official_candidate_filings",
    "nycvotes_2021_official_profiles",
    "orange_county_candidate_statements",
    "los_angeles_county_past_candidate_statements",
}
PARSER_VERSION = "municipal_curated_shard_0_v1"
PARSER_IDS = {
    "html": "html_links_text_v1",
    "pdf": "pdf_text_annotations_v1",
    "csv": "tabular_url_fields_v1",
    "xls": "tabular_url_fields_v1",
    "xlsx": "tabular_url_fields_v1",
    "json": "json_url_fields_v1",
}
RETRYABLE_HTTP = {408, 425, 429, 500, 502, 503, 504}
SOURCE_HOSTS = {
    "vote.minneapolismn.gov",
    "web.archive.org",
    "www.nycvotes.org",
    "nycvotes.org",
    "ocvote.gov",
    "www.lavote.gov",
    "apps.lavote.gov",
}

# Orange County uses stable contest numbers in its public filing-log endpoint.
OC_CONTEST_IDS = {
    "anaheim": "5001",
    "garden grove": "5081",
    "irvine": "5101",
    "orange": "5181",
    "santa ana": "5221",
}


class AcquisitionError(RuntimeError):
    """The frozen shard contract or output invariant was violated."""


@dataclass(frozen=True)
class RequestSpec:
    request_id: str
    source_id: str
    url: str
    target_ids: tuple[str, ...]
    parse_group_id: str
    claim_eligible: bool
    status_required: bool
    label: str
    discovered_from: str = ""


@dataclass(frozen=True)
class FetchResult:
    spec: RequestSpec
    status: str
    http_status: int | None
    final_url: str
    content_type: str
    body: bytes
    retrieved_at_utc: str
    retry_count: int
    error: str


@dataclass(frozen=True)
class FrozenFile:
    file_id: str
    sha256: str
    file_type: str
    local_path: str
    parser_id: str
    result: FetchResult


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def _stable_id(prefix: str, *parts: object) -> str:
    payload = "\x1f".join(str(part) for part in parts).encode("utf-8")
    return f"{prefix}_{hashlib.sha256(payload).hexdigest()[:24]}"


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _json_list(value: object) -> list[str]:
    try:
        parsed = json.loads(_text(value))
    except json.JSONDecodeError as error:
        raise AcquisitionError(f"Invalid JSON list {value!r}") from error
    if not isinstance(parsed, list) or not all(isinstance(item, str) for item in parsed):
        raise AcquisitionError(f"Expected a JSON string list, got {value!r}")
    return parsed


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    temporary = path.with_suffix(path.suffix + ".tmp")
    frame.to_csv(temporary, index=False)
    temporary.replace(path)


def _read_contract(path: Path, candidate_path: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    targets = pd.read_csv(path, dtype=str, keep_default_na=False)
    candidates = pd.read_csv(candidate_path, dtype=str, keep_default_na=False)
    if len(targets) != 25 or targets["target_id"].nunique() != 25:
        raise AcquisitionError("Shard 0 must contain exactly 25 unique targets")
    if set(targets["source_id"]) != EXPECTED_SOURCE_IDS:
        raise AcquisitionError(
            f"Unexpected shard-0 source IDs: {sorted(set(targets['source_id']) - EXPECTED_SOURCE_IDS)}"
        )
    if set(pd.to_numeric(targets["shard_id"], errors="raise")) != {SHARD_ID}:
        raise AcquisitionError("The input contains a nonzero shard assignment")
    if targets["attempt_key"].duplicated().any() or targets["target_id"].duplicated().any():
        raise AcquisitionError("The shard target contract has duplicate keys")
    expected_pairs = {
        (str(row.target_id), candidate_id)
        for row in targets.itertuples(index=False)
        for candidate_id in _json_list(row.acquisition_candidate_ids_json)
    }
    actual_pairs = set(zip(candidates["target_id"], candidates["candidate_year_id"]))
    if actual_pairs != expected_pairs:
        raise AcquisitionError("Candidate-target keys differ from the frozen target contract")
    return targets, candidates


def _full_contest_candidates(
    target: Mapping[str, object], candidate_contract: pd.DataFrame
) -> pd.DataFrame:
    """Return every unresolved candidate frozen for one contest-source target."""
    selected = candidate_contract[
        candidate_contract["target_id"].astype(str).eq(str(target["target_id"]))
    ].copy()
    if selected.empty:
        raise AcquisitionError(f"No full-contest candidate rows for {target['target_id']}")
    if selected["candidate_year_id"].duplicated().any():
        raise AcquisitionError(f"Duplicate full-contest candidate keys for {target['target_id']}")
    expected_ids = set(_json_list(target["acquisition_candidate_ids_json"]))
    actual_ids = set(selected["candidate_year_id"].astype(str))
    if actual_ids != expected_ids or len(selected) != int(target["unresolved_candidate_n"]):
        raise AcquisitionError(
            f"Full-contest candidate contract drift for {target['target_id']}: "
            f"expected={int(target['unresolved_candidate_n'])}, actual={len(selected)}"
        )
    if set(selected["contest"].astype(str)) != {str(target["contest"])}:
        raise AcquisitionError(f"Candidate contest mismatch for {target['target_id']}")
    return selected.sort_values("candidate_year_id").reset_index(drop=True)


def _request_key(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()[:20]


def _merge_specs(specs: Iterable[RequestSpec]) -> list[RequestSpec]:
    """Merge identical physical GETs and retain every target association."""
    merged: dict[str, RequestSpec] = {}
    for spec in specs:
        key = spec.url
        if key not in merged:
            merged[key] = spec
            continue
        old = merged[key]
        if old.source_id != spec.source_id:
            raise AcquisitionError(f"A physical URL crosses sources: {spec.url}")
        merged[key] = replace(
            old,
            target_ids=tuple(sorted(set(old.target_ids) | set(spec.target_ids))),
            claim_eligible=old.claim_eligible or spec.claim_eligible,
            status_required=old.status_required or spec.status_required,
        )
    return sorted(merged.values(), key=lambda item: (item.source_id, item.url))


def build_request_specs(targets: pd.DataFrame) -> list[RequestSpec]:
    specs: list[RequestSpec] = []
    for source_id, group in targets.groupby("source_id", sort=True):
        group = group.sort_values(["year", "month", "target_id"])
        all_targets = tuple(group["target_id"])
        first_group = str(group.iloc[0]["parse_group_id"])
        entries: list[str] = []
        for value in group["entry_urls_json"]:
            entries.extend(_json_list(value))
        for url in dict.fromkeys(entries):
            explicit_years = {str(year) for year in group["year"] if str(year) in url}
            if source_id == "minneapolis_official_candidate_filings" and "web.archive.org" in url:
                target_ids = tuple(group.loc[group["year"].eq("2013"), "target_id"])
                claim_eligible = True
            elif explicit_years:
                target_ids = tuple(group.loc[group["year"].isin(explicit_years), "target_id"])
                claim_eligible = True
            else:
                target_ids = all_targets
                claim_eligible = source_id == "los_angeles_county_past_candidate_statements" and "past-elections" in url
            if not target_ids:
                continue
            parse_group = str(group.loc[group["target_id"].eq(target_ids[0]), "parse_group_id"].iloc[0])
            specs.append(RequestSpec(
                request_id=f"entry_{_request_key(url)}",
                source_id=source_id,
                url=url,
                target_ids=target_ids,
                parse_group_id=parse_group or first_group,
                claim_eligible=claim_eligible,
                status_required=(
                    url == str(group.iloc[0]["exact_source_url"])
                    or (
                        source_id == "minneapolis_official_candidate_filings"
                        and "web.archive.org" in url
                    )
                ),
                label="contract_entry",
            ))

    nyc = targets[targets["source_id"].eq("nycvotes_2021_official_profiles")]
    if not nyc.empty:
        target = nyc.iloc[0]
        tid = str(target["target_id"])
        base = "https://www.nycvotes.org"
        mayor_url = base + "/whats-on-the-ballot/2021-general-election/mayor/"
        api_url = base + "/generalelectionfinder/getcandidates?" + urlencode({
            "language": "en-US", "electionCycle": "2021", "pageId": "2973",
            "boroughName": "", "houseNumber": "", "street": "", "districtId": "",
            "boroughCode": "", "officeType": "", "officelevel": "",
            "geoAreaAbbr": "", "umbracoPageId": "2972", "cctRequest": "",
        })
        for label, url in (("nyc_mayor", mayor_url), ("nyc_candidate_payload", api_url)):
            specs.append(RequestSpec(
                request_id=f"{label}_{_request_key(url)}", source_id=str(target["source_id"]),
                url=url, target_ids=(tid,), parse_group_id=str(target["parse_group_id"]),
                claim_eligible=True, status_required=True, label=label,
            ))

    orange = targets[targets["source_id"].eq("orange_county_candidate_statements")]
    endpoint = "https://ocvote.gov/bin/voting/candidate_filing_log_submit.php"
    for target in orange.sort_values("target_id").to_dict("records"):
        city = str(target["geo_name"]).casefold()
        contest_number = OC_CONTEST_IDS.get(city)
        if contest_number is None:
            raise AcquisitionError(f"No Orange County contest mapping for {city}")
        url = endpoint + "?" + urlencode({
            "contest": contest_number,
            "elecabbr": f"GEN{target['year']}",
            "lang": "",
        })
        specs.append(RequestSpec(
            request_id=f"oc_contest_{target['target_id']}",
            source_id=str(target["source_id"]), url=url,
            target_ids=(str(target["target_id"]),), parse_group_id=str(target["parse_group_id"]),
            claim_eligible=True, status_required=True, label="orange_contest_response",
        ))
    return _merge_specs(specs)


def _default_fetch(spec: RequestSpec, timeout: float = 12.0, max_attempts: int = 3) -> FetchResult:
    headers = {"User-Agent": "municipal-candidate-url-research/1.0 (+academic research)"}
    final_error = ""
    retry_count = 0
    for attempt in range(1, max_attempts + 1):
        retrieved = _utc_now()
        try:
            response = requests.get(spec.url, headers=headers, timeout=timeout, allow_redirects=True)
            status_code = int(response.status_code)
            body = bytes(response.content)
            content_type = response.headers.get("Content-Type", "").split(";", 1)[0].strip().casefold()
            if status_code in RETRYABLE_HTTP and attempt < max_attempts:
                retry_count += 1
                time.sleep(min(2 ** (attempt - 1), 4))
                continue
            if status_code in RETRYABLE_HTTP:
                return FetchResult(spec, "request_error", status_code, response.url, content_type,
                                   body, retrieved, retry_count, f"retryable HTTP {status_code}")
            if not (200 <= status_code < 400):
                return FetchResult(spec, "source_unavailable", status_code, response.url, content_type,
                                   body, retrieved, retry_count, f"HTTP {status_code}")
            if not body:
                return FetchResult(spec, "source_unavailable", status_code, response.url, content_type,
                                   body, retrieved, retry_count, "empty response body")
            return FetchResult(spec, "downloaded", status_code, response.url, content_type,
                               body, retrieved, retry_count, "")
        except requests.RequestException as error:
            final_error = f"{type(error).__name__}: {_text(error)}"
            if attempt < max_attempts:
                retry_count += 1
                time.sleep(min(2 ** (attempt - 1), 4))
                continue
            return FetchResult(spec, "request_error", None, "", "", b"", retrieved,
                               retry_count, final_error)
    raise AssertionError("unreachable fetch loop")


def _detect_file_type(result: FetchResult) -> str:
    content_type = result.content_type
    body = result.body
    path = urlsplit(result.final_url or result.spec.url).path.casefold()
    if body.startswith(b"%PDF-"):
        return "pdf"
    if body.startswith(b"PK\x03\x04") and path.endswith(".xlsx"):
        return "xlsx"
    if body.startswith(b"\xd0\xcf\x11\xe0"):
        return "xls"
    if "json" in content_type or path.endswith(".json"):
        return "json"
    if path.endswith(".csv") or content_type in {"text/csv", "application/csv"}:
        return "csv"
    return "html"


def _freeze_downloads(
    results: Sequence[FetchResult], output_dir: Path
) -> tuple[list[FrozenFile], dict[str, FrozenFile]]:
    files_dir = output_dir / "files"
    files_dir.mkdir(parents=True, exist_ok=True)
    frozen_by_hash: dict[str, FrozenFile] = {}
    request_map: dict[str, FrozenFile] = {}
    for result in results:
        if result.status != "downloaded":
            continue
        digest = hashlib.sha256(result.body).hexdigest()
        if digest in frozen_by_hash:
            request_map[result.spec.request_id] = frozen_by_hash[digest]
            continue
        file_type = _detect_file_type(result)
        parser_id = PARSER_IDS[file_type]
        relative = f"files/{digest}.{file_type}"
        path = output_dir / relative
        temporary = path.with_suffix(path.suffix + ".tmp")
        temporary.write_bytes(result.body)
        temporary.replace(path)
        frozen = FrozenFile(
            file_id=f"sha256:{digest}", sha256=digest, file_type=file_type,
            local_path=relative, parser_id=parser_id, result=result,
        )
        frozen_by_hash[digest] = frozen
        request_map[result.spec.request_id] = frozen
    return sorted(frozen_by_hash.values(), key=lambda item: item.file_id), request_map


def _discover_orange_statements(
    results: Sequence[FetchResult], targets: pd.DataFrame
) -> list[RequestSpec]:
    target_lookup = targets.set_index("target_id")
    specs: list[RequestSpec] = []
    pattern = re.compile(r"/var/www/html/(fileadmin/user_upload/elections/[^<'\"\s]+\.pdf)", re.I)
    for result in results:
        if result.status != "downloaded" or result.spec.label != "orange_contest_response":
            continue
        paths = sorted(set(pattern.findall(result.body.decode("latin-1", errors="replace"))))
        for relative in paths:
            url = "https://ocvote.gov/" + relative.lstrip("/")
            target_id = result.spec.target_ids[0]
            target = target_lookup.loc[target_id]
            specs.append(RequestSpec(
                request_id=f"oc_statement_{_request_key(url)}",
                source_id=result.spec.source_id, url=url, target_ids=(target_id,),
                parse_group_id=str(target["parse_group_id"]), claim_eligible=True,
                status_required=True, label="orange_candidate_statement",
                discovered_from=result.spec.request_id,
            ))
    return _merge_specs(specs)


def _source_domain(url: str) -> bool:
    exact = normalize_exact_url(url)
    host = (urlsplit(exact).hostname or "").casefold().removeprefix("www.")
    normalized_sources = {item.removeprefix("www.") for item in SOURCE_HOSTS}
    return any(host == item or host.endswith(f".{item}") for item in normalized_sources)


def _contract_url_type(url: str) -> str:
    value = classify_url(url)
    return {
        "social_profile": "social",
        "fundraising_profile": "fundraising",
        "officeholder_or_government_profile": "officeholder",
        "shared_host_root": "other_lead",
    }.get(value, value if value in {
        "candidate_subdomain", "custom_domain", "shared_host_path"
    } else "other_lead")


def _candidate_context_hit_n(context: str, candidates: pd.DataFrame) -> int:
    """Count plausible candidate identities in a URL's local source context."""
    normalized_context = f" {normalize_person_name(context)} "
    hits = 0
    for name in candidates["full_name"].astype(str):
        normalized_name = normalize_person_name(name)
        tokens = normalized_name.split()
        exact = bool(normalized_name and f" {normalized_name} " in normalized_context)
        first_last = bool(
            len(tokens) >= 2
            and (
                f" {tokens[0]} {tokens[-1]} " in normalized_context
                or f" {tokens[-1]} {tokens[0]} " in normalized_context
            )
        )
        if exact or first_last:
            hits += 1
    return hits


def _snapshot_timestamp(url: str, fallback: str) -> str:
    match = re.search(r"web\.archive\.org/web/(\d{8,14})", url)
    return match.group(1) if match else fallback


def _parse_frozen_files(
    frozen: Sequence[FrozenFile], output_dir: Path
) -> dict[str, tuple[SourceRecord, ...]]:
    """Parse each immutable physical file exactly once."""
    parsed: dict[str, tuple[SourceRecord, ...]] = {}
    for item in frozen:
        path = output_dir / item.local_path
        result = parse_local_file(path, item.file_type)
        parsed[item.file_id] = tuple(deduplicate_records(result.records))
    if len(parsed) != len(frozen):
        raise AcquisitionError("A physical file was parsed more than once or skipped")
    return parsed


def _evidence_rows(
    targets: pd.DataFrame,
    contract_candidates: pd.DataFrame,
    frozen: Sequence[FrozenFile],
    parsed: Mapping[str, tuple[SourceRecord, ...]],
) -> tuple[list[dict[str, object]], dict[str, dict[str, int]]]:
    rows: list[dict[str, object]] = []
    stats: dict[str, dict[str, int]] = {}
    frozen_by_id = {item.file_id: item for item in frozen}
    for target in targets.sort_values("target_id").to_dict("records"):
        target_id = str(target["target_id"])
        full = _full_contest_candidates(target, contract_candidates)
        allowed = full
        allowed_ids = set(allowed["candidate_year_id"])
        matched_external = 0
        ambiguous_external = 0
        target_rows: list[dict[str, object]] = []
        for file_id, records in parsed.items():
            frozen_file = frozen_by_id[file_id]
            spec = frozen_file.result.spec
            if target_id not in spec.target_ids or not spec.claim_eligible:
                continue
            for record in records:
                exact = normalize_exact_url(record.raw_url)
                if not exact or _source_domain(exact):
                    continue
                candidate, score, method = candidate_match(record.context, full)
                if candidate is None:
                    # Generic navigation and social-share URLs are not match
                    # problems.  A row is unresolved only when its local
                    # context actually names one or more contest candidates.
                    if _candidate_context_hit_n(record.context, full):
                        ambiguous_external += 1
                    continue
                matched_external += 1
                candidate_id = str(candidate["candidate_year_id"])
                if candidate_id not in allowed_ids:
                    continue
                evidence_id = _stable_id(
                    "municipal_evidence", target_id, candidate_id, file_id, exact
                )
                target_rows.append({
                    "contract_version": CONTRACT_VERSION,
                    "evidence_id": evidence_id,
                    "attempt_key": target["attempt_key"],
                    "target_id": target_id,
                    "source_id": target["source_id"],
                    "parse_group_id": target["parse_group_id"],
                    "shard_id": SHARD_ID,
                    "candidate_year_id": candidate_id,
                    "contest": target["contest"],
                    "raw_url": record.raw_url,
                    "exact_normalized_url": exact,
                    "site_root_url": site_root_url(exact),
                    "registrable_domain": registrable_domain(exact),
                    "url_type": _contract_url_type(exact),
                    "source_url": frozen_file.result.spec.url,
                    "source_date": frozen_file.result.retrieved_at_utc[:10],
                    "source_file_id": file_id,
                    "source_file_sha256": frozen_file.sha256,
                    "source_snapshot_timestamp": _snapshot_timestamp(
                        frozen_file.result.spec.url, frozen_file.result.retrieved_at_utc
                    ),
                    "evidence_text": _text(record.context)[:1000] or _text(candidate["full_name"]),
                    "expected_website_field": target["expected_website_field"],
                    "parser_id": frozen_file.parser_id,
                    "parser_version": PARSER_VERSION,
                    "identity_match": f"{method}:{score:.2f}",
                    "temporal_match": "pending_archive_validation",
                    "evidence_grade": "C",
                    "review_status": "pending",
                })
        unique = {str(row["evidence_id"]): row for row in target_rows}
        rows.extend(sorted(unique.values(), key=lambda row: str(row["evidence_id"])))
        stats[target_id] = {
            "full_contest_candidate_n": int(full["candidate_year_id"].nunique()),
            "contract_candidate_n": int(allowed["candidate_year_id"].nunique()),
            "candidate_screen_n": int(allowed["in_candidate_screen"].str.casefold().isin({"true", "1", "yes"}).sum()),
            "complete_contest_screen_n": int(allowed["in_complete_contest_screen"].str.casefold().isin({"true", "1", "yes"}).sum()),
            "matched_external_record_n": matched_external,
            "ambiguous_external_record_n": ambiguous_external,
            "evidence_n": len(unique),
        }
    return rows, stats


def _file_rows(frozen: Sequence[FrozenFile]) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for item in frozen:
        result = item.result
        rows.append({
            "contract_version": CONTRACT_VERSION,
            "file_id": item.file_id,
            "source_id": result.spec.source_id,
            "parse_group_id": result.spec.parse_group_id,
            "shard_id": SHARD_ID,
            "partition_hostname": {
                "minneapolis_official_candidate_filings": "vote.minneapolismn.gov",
                "nycvotes_2021_official_profiles": "www.nycvotes.org",
                "orange_county_candidate_statements": "ocvote.gov",
                "los_angeles_county_past_candidate_statements": "www.lavote.gov",
            }[result.spec.source_id],
            "request_url": result.spec.url,
            "final_url": result.final_url,
            "request_hostname": (urlsplit(result.spec.url).hostname or "").casefold(),
            "final_hostname": (urlsplit(result.final_url).hostname or "").casefold(),
            "local_path": item.local_path,
            "file_type": item.file_type,
            "content_type": result.content_type or "application/octet-stream",
            "http_status": result.http_status,
            "bytes": len(result.body),
            "sha256": item.sha256,
            "retrieved_at_utc": result.retrieved_at_utc,
            "parser_id": item.parser_id,
            "parser_version": PARSER_VERSION,
        })
    return sorted(rows, key=lambda row: str(row["file_id"]))


def _attempt_rows(
    targets: pd.DataFrame,
    results: Sequence[FetchResult],
    request_map: Mapping[str, FrozenFile],
    evidence: Sequence[Mapping[str, object]],
    stats: Mapping[str, Mapping[str, int]],
) -> list[dict[str, object]]:
    evidence_counts: dict[str, int] = {}
    for row in evidence:
        evidence_counts[str(row["target_id"])] = evidence_counts.get(str(row["target_id"]), 0) + 1
    rows: list[dict[str, object]] = []
    for target in targets.sort_values("target_id").to_dict("records"):
        target_id = str(target["target_id"])
        relevant = [result for result in results if target_id in result.spec.target_ids]
        required = [result for result in relevant if result.spec.status_required]
        claim_downloads = [
            result for result in relevant
            if result.spec.claim_eligible and result.status == "downloaded"
        ]
        retry_errors = [result for result in required if result.status == "request_error"]
        unavailable = [result for result in required if result.status == "source_unavailable"]
        target_stats = stats[target_id]
        evidence_n = evidence_counts.get(target_id, 0)
        if evidence_n:
            terminal = "found"
            driver = next(result for result in relevant if result.spec.request_id in request_map)
            error = ""
        elif retry_errors:
            terminal = "request_error"
            driver = retry_errors[-1]
            error = " | ".join(
                f"{item.spec.label}: {item.error}" for item in retry_errors
            )[:2000]
        elif unavailable:
            terminal = "source_unavailable"
            driver = unavailable[-1]
            error = " | ".join(
                f"{item.spec.label}: {item.error}" for item in unavailable
            )[:2000]
        elif not claim_downloads:
            terminal = "source_unavailable"
            driver = relevant[-1] if relevant else None
            error = "No target-period source document was available for parsing"
        elif int(target_stats["ambiguous_external_record_n"]) > 0:
            terminal = "match_unresolved"
            driver = claim_downloads[-1]
            error = ""
        else:
            terminal = "no_result"
            driver = claim_downloads[-1]
            error = ""
        target_files = {
            request_map[result.spec.request_id].file_id
            for result in relevant if result.spec.request_id in request_map
        }
        parser_ids = {
            request_map[result.spec.request_id].parser_id
            for result in relevant if result.spec.request_id in request_map
        }
        match_note = (
            f"full_contest_candidates={target_stats['full_contest_candidate_n']}; "
            f"contract_candidates={target_stats['contract_candidate_n']}; "
            f"candidate_screen={target_stats['candidate_screen_n']}; "
            f"complete_contest_screen={target_stats['complete_contest_screen_n']}; "
            f"matched_external_records={target_stats['matched_external_record_n']}; "
            f"ambiguous_external_records={target_stats['ambiguous_external_record_n']}; "
            f"screen_evidence={target_stats['evidence_n']}"
        )
        rows.append({
            "contract_version": CONTRACT_VERSION,
            "attempt_key": target["attempt_key"],
            "target_id": target_id,
            "source_id": target["source_id"],
            "parse_group_id": target["parse_group_id"],
            "shard_id": SHARD_ID,
            "partition_hostname": target["partition_hostname"],
            "terminal_status": terminal,
            "attempted_at_utc": max((item.retrieved_at_utc for item in relevant), default=_utc_now()),
            "retry_count": sum(item.retry_count for item in relevant),
            "http_status": "" if driver is None or driver.http_status is None else driver.http_status,
            "request_url": "" if driver is None else driver.spec.url,
            "final_url": "" if driver is None else driver.final_url,
            "downloaded_file_n": len(target_files),
            "downloaded_file_ids_json": json.dumps(sorted(target_files)),
            "evidence_row_n": evidence_n,
            "parser_ids_json": json.dumps(sorted(parser_ids)),
            "error": error,
            "match_note": match_note,
        })
    return rows


def acquire(
    targets_path: Path = DEFAULT_TARGETS,
    candidates_path: Path = DEFAULT_CANDIDATES,
    output_dir: Path = DEFAULT_OUTPUT,
    fetcher: Callable[[RequestSpec], FetchResult] = _default_fetch,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    targets, contract_candidates = _read_contract(targets_path, candidates_path)
    for target in targets.to_dict("records"):
        _full_contest_candidates(target, contract_candidates)

    # Build into a sibling staging directory, then replace the exact interface.
    staging = output_dir.with_name(output_dir.name + ".staging")
    if staging.exists():
        shutil.rmtree(staging)
    staging.mkdir(parents=True)
    (staging / "files").mkdir()

    fixed_specs = build_request_specs(targets)
    fixed_results = [fetcher(spec) for spec in fixed_specs]
    dynamic_specs = _discover_orange_statements(fixed_results, targets)
    dynamic_results = [fetcher(spec) for spec in dynamic_specs]
    results = fixed_results + dynamic_results

    frozen, request_map = _freeze_downloads(results, staging)
    parsed = _parse_frozen_files(frozen, staging)
    evidence_rows, stats = _evidence_rows(
        targets, contract_candidates, frozen, parsed
    )
    attempt_rows = _attempt_rows(targets, results, request_map, evidence_rows, stats)

    attempts = pd.DataFrame(attempt_rows).reindex(columns=ATTEMPT_COLUMNS)
    files = pd.DataFrame(_file_rows(frozen)).reindex(columns=FILE_COLUMNS)
    evidence = pd.DataFrame(evidence_rows).reindex(columns=EVIDENCE_COLUMNS)
    if set(attempts["attempt_key"]) != set(targets["attempt_key"]):
        raise AcquisitionError("Assigned and terminal attempt keys differ")
    if attempts["attempt_key"].duplicated().any() or len(attempts) != len(targets):
        raise AcquisitionError("Terminal attempts are not one-to-one with assigned targets")
    if not evidence.empty and evidence["evidence_id"].duplicated().any():
        raise AcquisitionError("Duplicate evidence IDs")

    _atomic_csv(attempts, staging / "attempts.csv")
    _atomic_csv(files, staging / "downloaded_files.csv")
    _atomic_csv(evidence, staging / "evidence.csv")
    if output_dir.exists():
        backup = output_dir.with_name(output_dir.name + ".previous")
        if backup.exists():
            shutil.rmtree(backup)
        output_dir.replace(backup)
        staging.replace(output_dir)
        shutil.rmtree(backup)
    else:
        staging.replace(output_dir)
    return attempts, files, evidence


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--targets", type=Path, default=DEFAULT_TARGETS)
    parser.add_argument("--candidates", type=Path, default=DEFAULT_CANDIDATES)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT)
    args = parser.parse_args()
    attempts, files, evidence = acquire(
        args.targets, args.candidates, args.output_dir
    )
    screen_candidates = pd.read_csv(args.candidates, dtype=str, keep_default_na=False)
    evidence_ids = set(evidence["candidate_year_id"]) if len(evidence) else set()
    print(json.dumps({
        "assigned_targets": len(attempts),
        "attempt_key_equality": len(attempts) == 25 and attempts["attempt_key"].nunique() == 25,
        "terminal_statuses": attempts["terminal_status"].value_counts().sort_index().to_dict(),
        "downloaded_files": len(files),
        "evidence_rows": len(evidence),
        "screen_candidate_claims": int(screen_candidates["candidate_year_id"].isin(evidence_ids).sum()),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
