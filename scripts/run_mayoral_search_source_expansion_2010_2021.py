#!/usr/bin/env python3
"""Run the bounded 480-case Serper source-page expansion.

This channel issues one and only one primary query for each candidate in the
frozen 2010--2021 screen. Search results and extracted URLs are leads only;
this module never validates or approves a campaign-website claim.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import tempfile
import threading
import time
import unicodedata
from collections import deque
from concurrent.futures import FIRST_COMPLETED, ThreadPoolExecutor, wait
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping
from urllib.parse import urlsplit
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

from scripts.discover_mayoral_search_urls import is_candidate_shaped_domain
from scripts.extract_municipal_mayoral_urls import (
    classify_url,
    extract_urls,
    normalize_exact_url,
    registrable_domain,
    site_root_url,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCREEN_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_SCREEN = SCREEN_DIR / "candidate_screening_sample_2010_2021.csv"
DEFAULT_SAMPLING_MANIFEST = SCREEN_DIR / "sampling_manifest.json"
DEFAULT_ROSTER = (
    SCREEN_DIR
    / "search_source_expansion"
    / "frozen_inputs"
    / "current_coverage_roster.csv"
)
DEFAULT_OUTPUT_DIR = SCREEN_DIR / "search_source_expansion"

CONTRACT_VERSION = "mayoral-search-source-expansion-2010-2021-v1"
QUERY_VERSION = "serper-one-primary-campaign-or-website-v2"
RESULT_CLASSIFIER_VERSION = "serper-result-source-page-classifier-v2"
SOURCE_PAGE_PARSER_VERSION = "search-source-page-parser-v2"
EXPECTED_CANDIDATES = 480
MAX_SERPER_REQUESTS = 480
SERPER_RESULTS_PER_QUERY = 10
MAX_SOURCE_PAGES_PER_CANDIDATE = 3
SERPER_COST_PER_1000_USD = 1.0
EXPECTED_SCREEN_SHA256 = "646e2688005ca7c4f0f20ed027aeb7709351adbdfa9864af26e143f1caf3eb19"
EXPECTED_ROSTER_SHA256 = "589daf9be2eb4f90824d178d760556c73c51f3cb262042f4f4e37b3e5407f377"

SCREEN_REQUIRED_COLUMNS = [
    "sample_order",
    "sample_kind",
    "sample_seed",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "ledb_candid",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "covered",
    "period",
]

QUERY_MANIFEST_COLUMNS = [
    "contract_version",
    "query_version",
    "query_attempt_id",
    "query_attempt_hash",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "ledb_candid",
    "full_name",
    "query_name",
    "query_name_repaired",
    "query_name_repair_method",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "sample_order",
    "period",
    "query_variant",
    "query",
    "query_sha256",
    "provider",
    "country",
    "language",
    "results_requested",
    "screen_sha256",
    "roster_sha256",
]

REQUEST_LEDGER_COLUMNS = [
    "contract_version",
    "query_attempt_id",
    "candidate_year_id",
    "request_sequence",
    "query_sha256",
    "provider",
    "endpoint",
    "request_status",
    "request_sent",
    "terminal_for_run",
    "http_status",
    "results_returned",
    "raw_response_sha256",
    "error",
    "attempted_at",
    "completed_at",
    "retryable",
    "complete_for_wave",
]

RANKED_QUEUE_COLUMNS = [
    "contract_version",
    "result_claim_id",
    "query_attempt_id",
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "result_rank",
    "result_title",
    "result_url",
    "result_snippet",
    "result_date",
    "raw_result_sha256",
    "exact_normalized_result_url",
    "result_host",
    "candidate_attribution_status",
    "candidate_attribution_reason",
    "candidate_identity_signal",
    "mayoral_election_signal",
    "location_signal",
    "exact_year_signal",
    "temporal_conflict",
    "source_credibility_class",
    "source_score",
    "result_classification",
    "url_exclusion_reason",
    "eligible_for_page_fetch",
    "selected_for_page_fetch",
    "page_fetch_rank",
    "page_selection_reason",
    "classifier_version",
]

DOWNLOADED_PAGE_COLUMNS = [
    "contract_version",
    "page_id",
    "page_url",
    "result_claim_ids_json",
    "fetch_status",
    "http_status",
    "final_url",
    "content_type",
    "content_bytes",
    "content_sha256",
    "local_file_path",
    "extracted_text_path",
    "extracted_text_sha256",
    "text_characters",
    "link_count",
    "fetched_at",
    "retryable",
    "error",
    "parser_version",
]

PAGE_LINK_COLUMNS = [
    "contract_version",
    "page_link_id",
    "page_id",
    "page_url",
    "source_locator",
    "extraction_method",
    "anchor_text",
    "surrounding_context",
    "raw_url",
    "exact_normalized_url",
    "site_root_url",
    "registrable_domain",
    "url_type",
    "is_external_to_source",
    "parser_version",
]

URL_LEAD_COLUMNS = [
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


class SearchExpansionError(ValueError):
    """The frozen input or a generated output violates the channel contract."""


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return " ".join(str(value).split())


def _as_bool(value: object, label: str) -> bool:
    if isinstance(value, bool):
        return value
    token = _text(value).casefold()
    if token in {"true", "1", "yes", "y"}:
        return True
    if token in {"false", "0", "no", "n", ""}:
        return False
    raise SearchExpansionError(f"{label} has invalid boolean {value!r}")


def stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(str(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def stable_hash(*values: object) -> str:
    payload = "\x1f".join(str(value) for value in values)
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _display_path(path: Path) -> str:
    """Use a repository-relative audit path, or an absolute temporary-test path."""
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(REPO_ROOT.resolve()))
    except ValueError:
        return str(resolved)


def canonical_json_hash(value: object) -> str:
    encoded = json.dumps(
        value, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False, lineterminator="\n")
    os.replace(temporary, path)


def _atomic_json(payload: Mapping[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temporary, path)


def _require_columns(frame: pd.DataFrame, columns: Iterable[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise SearchExpansionError(f"{label} is missing columns: {missing}")


def _require_exact_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    if frame.columns.tolist() != columns:
        raise SearchExpansionError(
            f"{label} column contract drift; "
            f"missing={sorted(set(columns)-set(frame.columns))}, "
            f"unexpected={sorted(set(frame.columns)-set(columns))}, "
            f"order_matches={frame.columns.tolist() == columns}"
        )


def _manifest_output_path(manifest_path: Path, value: object) -> Path:
    path = Path(_text(value))
    if path.is_absolute():
        return path
    return manifest_path.parent / path


def load_frozen_inputs(
    screen_path: Path = DEFAULT_SCREEN,
    sampling_manifest_path: Path = DEFAULT_SAMPLING_MANIFEST,
    roster_path: Path = DEFAULT_ROSTER,
    *,
    expected_candidates: int = EXPECTED_CANDIDATES,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, object]]:
    """Verify the authoritative screen and full-contest roster before querying."""
    screen_path = screen_path.resolve()
    roster_path = roster_path.resolve()
    sampling_manifest_path = sampling_manifest_path.resolve()
    sampling_manifest = json.loads(sampling_manifest_path.read_text(encoding="utf-8"))
    candidate_record = sampling_manifest.get("outputs", {}).get("candidate_sample", {})
    recorded_path = _manifest_output_path(
        sampling_manifest_path, candidate_record.get("path", "")
    ).resolve()
    if recorded_path != screen_path:
        raise SearchExpansionError("Candidate screen path differs from sampling manifest")
    screen_hash = sha256_file(screen_path)
    roster_hash = sha256_file(roster_path)
    if screen_hash != candidate_record.get("sha256") or screen_hash != EXPECTED_SCREEN_SHA256:
        raise SearchExpansionError("Candidate screen SHA-256 differs from the frozen contract")
    roster_record = sampling_manifest.get("inputs", {}).get("coverage_roster", {})
    if roster_hash != roster_record.get("sha256") or roster_hash != EXPECTED_ROSTER_SHA256:
        raise SearchExpansionError("Full roster SHA-256 differs from the frozen contract")

    screen = pd.read_csv(screen_path, keep_default_na=False, low_memory=False)
    roster = pd.read_csv(roster_path, keep_default_na=False, low_memory=False)
    _require_columns(screen, SCREEN_REQUIRED_COLUMNS, "candidate screen")
    _require_columns(
        roster,
        [
            "candidate_year_id",
            "candidate_contest_id",
            "contest",
            "ledb_candid",
            "full_name",
            "geo_name",
            "state_abb",
            "year",
            "month",
            "contest_n_candidates",
        ],
        "full candidate roster",
    )
    if len(screen) != expected_candidates or screen["candidate_year_id"].nunique() != len(screen):
        raise SearchExpansionError(
            f"Candidate screen must contain {expected_candidates} unique candidate-years"
        )
    if screen["candidate_contest_id"].duplicated().any():
        raise SearchExpansionError("Candidate screen has duplicate candidate-contest keys")
    if roster["candidate_year_id"].duplicated().any() or roster[
        "candidate_contest_id"
    ].duplicated().any():
        raise SearchExpansionError("Full roster candidate keys are not unique")
    if any(_as_bool(value, "screen.covered") for value in screen["covered"]):
        raise SearchExpansionError("Frozen candidate screen contains covered cases")
    if set(screen["sample_kind"].map(_text)) != {
        "candidate_year_screen_2010_2021"
    }:
        raise SearchExpansionError("Candidate screen sample_kind drifted")
    if set(pd.to_numeric(screen["sample_seed"], errors="raise").astype(int)) != {
        20260825
    }:
        raise SearchExpansionError("Candidate screen seed drifted")
    expected_orders = list(range(1, expected_candidates + 1))
    if sorted(pd.to_numeric(screen["sample_order"], errors="raise").astype(int)) != expected_orders:
        raise SearchExpansionError("Candidate screen sample_order is not exactly 1..480")
    for column in [
        "candidate_year_id",
        "candidate_contest_id",
        "contest",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
    ]:
        if screen[column].map(_text).eq("").any():
            raise SearchExpansionError(f"Candidate screen has blank {column}")

    roster_lookup = roster.set_index("candidate_year_id", drop=False)
    missing = set(screen["candidate_year_id"].map(_text)) - set(
        roster["candidate_year_id"].map(_text)
    )
    if missing:
        raise SearchExpansionError(f"Screen candidates absent from full roster: {sorted(missing)}")
    echo_columns = [
        "candidate_contest_id",
        "contest",
        "ledb_candid",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
    ]
    for item in screen.to_dict("records"):
        candidate_id = _text(item["candidate_year_id"])
        reference = roster_lookup.loc[candidate_id]
        for column in echo_columns:
            if _text(item[column]) != _text(reference[column]):
                raise SearchExpansionError(
                    f"Screen/full-roster mismatch for {candidate_id}.{column}"
                )
    for contest, group in roster.groupby("contest", sort=False):
        declared = pd.to_numeric(group["contest_n_candidates"], errors="raise").astype(int)
        if declared.nunique() != 1 or int(declared.iloc[0]) != len(group):
            raise SearchExpansionError(f"Full contest roster is incomplete for {contest}")
    audit = {
        "screen_rows": int(len(screen)),
        "screen_candidate_years": int(screen["candidate_year_id"].nunique()),
        "screen_contests": int(screen["contest"].nunique()),
        "screen_missing_election_month": int(screen["month"].map(_text).eq("").sum()),
        "full_roster_rows": int(len(roster)),
        "full_roster_contests": int(roster["contest"].nunique()),
        "screen_sha256": screen_hash,
        "roster_sha256": roster_hash,
        "exact_screen_to_roster_key_agreement": True,
        "full_contest_rosters_verified": True,
    }
    return screen, roster, audit


def _escape_phrase(value: str) -> str:
    return _text(value).replace("\\", " ").replace('"', "").strip()


MOJIBAKE_MARKERS = ("Ã", "Â", "â", "ð")


def repair_query_name(full_name: object) -> tuple[str, bool, str]:
    """Repair UTF-8-as-Latin-1 mojibake only in the derived query identity."""
    # Preserve a trailing mojibake NBSP byte pair until after decoding. Python's
    # str.strip() removes U+00A0 first, which turns ``Â\u00a0`` into an invalid
    # lone Latin-1 byte and makes an otherwise deterministic repair fail.
    source = "" if full_name is None or pd.isna(full_name) else str(full_name)
    raw = source.strip()
    repair_source = source if any(marker in source for marker in MOJIBAKE_MARKERS) else raw
    repaired = repair_source
    if any(marker in repaired for marker in MOJIBAKE_MARKERS):
        try:
            repaired = repaired.encode("latin-1").decode("utf-8")
        except (UnicodeEncodeError, UnicodeDecodeError) as error:
            raise SearchExpansionError(
                f"Cannot deterministically repair query name {raw!r}"
            ) from error
        repaired = _text(repaired.replace("\u00a0", " "))
    if any(marker in repaired for marker in MOJIBAKE_MARKERS):
        raise SearchExpansionError(f"Mojibake remains in query name {repaired!r}")
    changed = repaired != raw
    return repaired, changed, "latin1_bytes_decoded_as_utf8" if changed else "none"


def candidate_identity_clause(full_name: object) -> str:
    """Build one quoted identity clause, expanding quoted nicknames in-place."""
    name = (
        _text(full_name)
        .replace("‘", "'")
        .replace("’", "'")
        .replace("“", '"')
        .replace("”", '"')
    )
    nickname_pattern = re.compile(
        r'''(?<![A-Za-z0-9])'([^']+)'(?![A-Za-z0-9])|"([^"]+)"'''
    )
    nicknames = [
        _text(single or double)
        for single, double in nickname_pattern.findall(name)
        if _text(single or double)
    ]
    legal = _text(nickname_pattern.sub(" ", name))
    tokens = legal.split()
    aliases = [legal]
    # One paid query must cover common middle-name and middle-initial variants.
    # Keep the full legal phrase for precision, then add legal first + surname.
    if len(tokens) > 2:
        aliases.append(f"{tokens[0]} {tokens[-1]}")
    if nicknames and tokens:
        surname = tokens[-1]
        aliases.extend(f"{nickname} {surname}" for nickname in nicknames)
    aliases = list(dict.fromkeys(alias for alias in aliases if alias))
    quoted = [f'"{_escape_phrase(alias)}"' for alias in aliases]
    return quoted[0] if len(quoted) == 1 else f"({' OR '.join(quoted)})"


def primary_query(row: Mapping[str, object]) -> str:
    query_name = row.get("query_name")
    if not _text(query_name):
        query_name = repair_query_name(row["full_name"])[0]
    identity = candidate_identity_clause(query_name)
    city = _escape_phrase(_text(row["geo_name"]))
    state = _escape_phrase(_text(row["state_abb"]))
    year = int(float(_text(row["year"])))
    return f'{identity} mayor "{city}" {state} {year} (campaign OR website)'


def build_query_manifest(
    screen: pd.DataFrame,
    *,
    screen_sha256: str,
    roster_sha256: str,
) -> pd.DataFrame:
    """Create exactly one stable Serper query for every frozen candidate-year."""
    if len(screen) > MAX_SERPER_REQUESTS:
        raise SearchExpansionError(
            f"Screen has {len(screen)} rows, above the {MAX_SERPER_REQUESTS}-request cap"
        )
    rows: list[dict[str, object]] = []
    ordered = screen.sort_values("sample_order", kind="stable")
    for item in ordered.to_dict("records"):
        query_name, name_repaired, repair_method = repair_query_name(item["full_name"])
        item = {**item, "query_name": query_name}
        query = primary_query(item)
        query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
        attempt_key_values = (
            screen_sha256,
            item["candidate_year_id"],
            item["candidate_contest_id"],
            query,
            QUERY_VERSION,
            "serper",
            "us",
            "en",
            SERPER_RESULTS_PER_QUERY,
        )
        attempt_hash = stable_hash(*attempt_key_values)
        attempt_id = f"serper_query_{attempt_hash[:24]}"
        rows.append(
            {
                "contract_version": CONTRACT_VERSION,
                "query_version": QUERY_VERSION,
                "query_attempt_id": attempt_id,
                "query_attempt_hash": attempt_hash,
                "candidate_year_id": _text(item["candidate_year_id"]),
                "candidate_contest_id": _text(item["candidate_contest_id"]),
                "contest": _text(item["contest"]),
                "ledb_candid": _text(item["ledb_candid"]),
                "full_name": str(item["full_name"]),
                "query_name": query_name,
                "query_name_repaired": name_repaired,
                "query_name_repair_method": repair_method,
                "geo_name": _text(item["geo_name"]),
                "state_abb": _text(item["state_abb"]),
                "year": int(float(_text(item["year"]))),
                "month": (
                    int(float(_text(item["month"]))) if _text(item["month"]) else ""
                ),
                "sample_order": int(float(_text(item["sample_order"]))),
                "period": _text(item["period"]),
                "query_variant": "one_primary_campaign_website",
                "query": query,
                "query_sha256": query_hash,
                "provider": "serper",
                "country": "us",
                "language": "en",
                "results_requested": SERPER_RESULTS_PER_QUERY,
                "screen_sha256": screen_sha256,
                "roster_sha256": roster_sha256,
            }
        )
    manifest = pd.DataFrame(rows).reindex(columns=QUERY_MANIFEST_COLUMNS)
    _require_exact_columns(manifest, QUERY_MANIFEST_COLUMNS, "query manifest")
    if len(manifest) != len(screen):
        raise SearchExpansionError("Query manifest lost candidate rows")
    if manifest["candidate_year_id"].duplicated().any():
        raise SearchExpansionError("Query manifest has multiple queries per candidate")
    if manifest["query_attempt_id"].duplicated().any():
        raise SearchExpansionError("Query manifest has duplicate attempt IDs")
    if not manifest["query"].str.endswith(" (campaign OR website)").all():
        raise SearchExpansionError("Primary query contract drifted")
    return manifest


def prepare_wave(
    *,
    screen_path: Path = DEFAULT_SCREEN,
    sampling_manifest_path: Path = DEFAULT_SAMPLING_MANIFEST,
    roster_path: Path = DEFAULT_ROSTER,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
) -> dict[str, object]:
    """Freeze the 480 exact queries and initialize empty downstream ledgers."""
    screen, roster, input_audit = load_frozen_inputs(
        screen_path, sampling_manifest_path, roster_path
    )
    query_manifest = build_query_manifest(
        screen,
        screen_sha256=input_audit["screen_sha256"],
        roster_sha256=input_audit["roster_sha256"],
    )
    output_dir.mkdir(parents=True, exist_ok=True)
    query_path = output_dir / "query_manifest.csv"
    ledger_path = output_dir / "request_ledger.csv"
    raw_path = output_dir / "raw_responses.jsonl"
    has_search_state = bool(
        (ledger_path.exists() and len(pd.read_csv(ledger_path, keep_default_na=False)))
        or (raw_path.exists() and raw_path.stat().st_size)
    )
    if query_path.exists() and has_search_state:
        prior_manifest = pd.read_csv(query_path, keep_default_na=False, low_memory=False)
        _validate_query_manifest(prior_manifest, query_manifest)
    _atomic_csv(query_manifest, query_path, QUERY_MANIFEST_COLUMNS)
    initialized = {
        "request_ledger.csv": REQUEST_LEDGER_COLUMNS,
        "ranked_result_page_queue.csv": RANKED_QUEUE_COLUMNS,
        "downloaded_pages.csv": DOWNLOADED_PAGE_COLUMNS,
        "page_links.csv": PAGE_LINK_COLUMNS,
        "url_leads.csv": URL_LEAD_COLUMNS,
    }
    for filename, columns in initialized.items():
        path = output_dir / filename
        if path.exists():
            prior = pd.read_csv(path, keep_default_na=False, low_memory=False)
            if prior.empty and prior.columns.tolist() != columns:
                _atomic_csv(pd.DataFrame(columns=columns), path, columns)
            elif not prior.empty:
                _require_exact_columns(prior, columns, filename)
        else:
            _atomic_csv(pd.DataFrame(columns=columns), path, columns)
    if not raw_path.exists():
        raw_path.write_text("", encoding="utf-8")
    audit = {
        "audit_version": "mayoral-search-source-expansion-audit-v1",
        "contract_version": CONTRACT_VERSION,
        "stage": "queries_prepared_search_not_started",
        "verdict": "partial_key_required",
        **input_audit,
        "query_rows": int(len(query_manifest)),
        "unique_query_attempt_ids": int(query_manifest["query_attempt_id"].nunique()),
        "unique_query_candidate_years": int(query_manifest["candidate_year_id"].nunique()),
        "one_query_per_candidate": True,
        "maximum_serper_requests": MAX_SERPER_REQUESTS,
        "expected_serper_requests": int(len(query_manifest)),
        "estimated_maximum_cost_usd": (
            len(query_manifest) * SERPER_COST_PER_1000_USD / 1000
        ),
        "fallback_queries_allowed": 0,
        "maximum_source_pages_per_candidate": MAX_SOURCE_PAGES_PER_CANDIDATE,
        "query_manifest": {
            "path": _display_path(query_path),
            "rows": int(len(query_manifest)),
            "bytes": query_path.stat().st_size,
            "sha256": sha256_file(query_path),
        },
        "large_filter_audit": {
            "triggered": False,
            "before": int(len(screen)),
            "after": int(len(query_manifest)),
            "dropped": 0,
            "drop_fraction": 0.0,
            "rule": "Every frozen screen candidate receives exactly one primary query.",
        },
        "claims_validated_or_approved": 0,
    }
    _atomic_json(audit, output_dir / "audit.json")
    return audit


def load_serper_api_key(
    *,
    environ: Mapping[str, str] | None = None,
    env_path: Path | None = None,
) -> str:
    """Load only SERPER_API_KEY from the process environment or project .env."""
    environment = os.environ if environ is None else environ
    key = _text(environment.get("SERPER_API_KEY", ""))
    if key:
        return key
    path = (REPO_ROOT / ".env") if env_path is None else Path(env_path)
    if path.is_file():
        for line in path.read_text(encoding="utf-8").splitlines():
            match = re.match(r"^\s*(?:export\s+)?SERPER_API_KEY\s*=\s*(.*?)\s*$", line)
            if not match:
                continue
            value = match.group(1).strip()
            if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
                value = value[1:-1]
            if _text(value):
                return _text(value)
    raise SearchExpansionError(
        "SERPER_API_KEY is missing from the environment and project .env; "
        "no search request was sent"
    )


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _validate_query_manifest(
    manifest: pd.DataFrame, expected: pd.DataFrame
) -> pd.DataFrame:
    _require_exact_columns(manifest, QUERY_MANIFEST_COLUMNS, "query manifest")
    if len(manifest) != len(expected):
        raise SearchExpansionError(
            f"Query manifest row drift: expected {len(expected)}, found {len(manifest)}"
        )
    for column in QUERY_MANIFEST_COLUMNS:
        actual_values = manifest[column].map(_text).tolist()
        expected_values = expected[column].map(_text).tolist()
        if actual_values != expected_values:
            raise SearchExpansionError(f"Query manifest differs in {column}")
    if manifest["query_attempt_id"].duplicated().any():
        raise SearchExpansionError("Query manifest has duplicate attempt IDs")
    return manifest.copy()


def _read_csv_contract(path: Path, columns: list[str], label: str) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=columns)
    frame = pd.read_csv(path, keep_default_na=False, low_memory=False)
    _require_exact_columns(frame, columns, label)
    return frame


def read_raw_responses(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line_number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        try:
            item = json.loads(line)
        except json.JSONDecodeError as error:
            raise SearchExpansionError(
                f"Raw response JSONL line {line_number} is invalid"
            ) from error
        if not isinstance(item, dict):
            raise SearchExpansionError(
                f"Raw response JSONL line {line_number} is not an object"
            )
        records.append(item)
    keys = [_text(item.get("query_attempt_id")) for item in records]
    if any(not key for key in keys) or len(keys) != len(set(keys)):
        raise SearchExpansionError("Raw responses have blank or duplicate query keys")
    return records


def _atomic_jsonl_records(
    path: Path, records: Iterable[Mapping[str, object]]
) -> None:
    """Atomically replace the response checkpoint with complete JSONL records."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        for record in records:
            payload = json.dumps(
                record, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            )
            handle.write(payload + "\n")
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _response_json(response: Any) -> tuple[object, str]:
    try:
        return response.json(), ""
    except (TypeError, ValueError) as error:
        text = str(getattr(response, "text", ""))
        return {"non_json_response_text": text}, f"invalid_json:{type(error).__name__}"


def request_serper_once(
    query: str,
    api_key: str,
    *,
    results_requested: int = SERPER_RESULTS_PER_QUERY,
    timeout: int = 30,
    session: Any = requests,
) -> tuple[str, int | str, object, str, str, str]:
    """Issue exactly one paid HTTP request; this function never retries."""
    try:
        response = session.post(
            "https://google.serper.dev/search",
            headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
            json={"q": query, "gl": "us", "hl": "en", "num": results_requested},
            timeout=timeout,
        )
    except requests.RequestException as error:
        empty_hash = hashlib.sha256(b"").hexdigest()
        return (
            "request_error",
            "",
            {},
            "",
            empty_hash,
            f"{type(error).__name__}: {error}"[:1000],
        )
    status_code = int(response.status_code)
    raw_bytes = bytes(getattr(response, "content", b""))
    if not raw_bytes:
        raw_body_fallback = str(getattr(response, "text", ""))
        raw_bytes = raw_body_fallback.encode("utf-8")
    raw_body = raw_bytes.decode("utf-8", errors="replace")
    response_hash = hashlib.sha256(raw_bytes).hexdigest()
    payload, json_error = _response_json(response)
    if status_code != 200:
        status = "provider_error" if status_code in {400, 401, 403} else "request_error"
        error = json_error or f"HTTP {status_code}"
        return status, status_code, payload, raw_body, response_hash, error[:1000]
    if json_error or not isinstance(payload, Mapping):
        return (
            "provider_error",
            status_code,
            payload,
            raw_body,
            response_hash,
            json_error or "invalid_response_object",
        )
    organic = payload.get("organic", [])
    if not isinstance(organic, list):
        return (
            "provider_error",
            status_code,
            payload,
            raw_body,
            response_hash,
            "organic_results_not_a_list",
        )
    return (
        "found" if organic else "no_result",
        status_code,
        payload,
        raw_body,
        response_hash,
        "",
    )


def _provisional_ledger_row(
    query_row: Mapping[str, object], request_sequence: int
) -> dict[str, object]:
    now = _utc_now()
    return {
        "contract_version": CONTRACT_VERSION,
        "query_attempt_id": query_row["query_attempt_id"],
        "candidate_year_id": query_row["candidate_year_id"],
        "request_sequence": request_sequence,
        "query_sha256": query_row["query_sha256"],
        "provider": "serper",
        "endpoint": "https://google.serper.dev/search",
        "request_status": "request_error",
        "request_sent": True,
        "terminal_for_run": True,
        "http_status": "",
        "results_returned": 0,
        "raw_response_sha256": "",
        "error": "request_started_without_response_checkpoint",
        "attempted_at": now,
        "completed_at": now,
        "retryable": True,
        "complete_for_wave": False,
    }


def _organic_results(payload: object) -> list[Mapping[str, object]]:
    if not isinstance(payload, Mapping):
        return []
    organic = payload.get("organic", [])
    return [item for item in organic if isinstance(item, Mapping)] if isinstance(organic, list) else []


def _reconcile_ledger_with_raw(
    ledger: pd.DataFrame,
    raw_records: list[dict[str, Any]],
) -> pd.DataFrame:
    if ledger.empty or not raw_records:
        return ledger
    raw_by_key = {_text(item["query_attempt_id"]): item for item in raw_records}
    rows: list[dict[str, object]] = []
    for item in ledger.to_dict("records"):
        raw = raw_by_key.get(_text(item["query_attempt_id"]))
        if raw is not None:
            item.update(
                {
                    "request_status": _text(raw.get("request_status")),
                    "http_status": raw.get("http_status", ""),
                    "results_returned": len(_organic_results(raw.get("raw_response"))),
                    "raw_response_sha256": _text(raw.get("raw_response_sha256")),
                    "error": _text(raw.get("error")),
                    "completed_at": _text(raw.get("retrieved_at")),
                    "retryable": _text(raw.get("request_status")) == "request_error",
                    "complete_for_wave": _text(raw.get("request_status"))
                    in {"found", "no_result"},
                }
            )
        rows.append(item)
    return pd.DataFrame(rows).reindex(columns=REQUEST_LEDGER_COLUMNS)


def _normalize_search_text(value: object) -> str:
    ascii_value = unicodedata.normalize("NFKD", _text(value)).encode(
        "ascii", "ignore"
    ).decode()
    return " ".join(re.findall(r"[a-z0-9]+", ascii_value.casefold()))


def _candidate_aliases(name: object) -> list[tuple[str, ...]]:
    clause = candidate_identity_clause(repair_query_name(name)[0])
    phrases = re.findall(r'"([^"]+)"', clause)
    return [tuple(_normalize_search_text(phrase).split()) for phrase in phrases]


def _candidate_is_named(context: str, name: object) -> bool:
    normalized = f" {_normalize_search_text(context)} "
    for alias in _candidate_aliases(name):
        if len(alias) < 2:
            continue
        if f" {' '.join(alias)} " in normalized:
            return True
        if f" {alias[0]} " in normalized and f" {alias[-1]} " in normalized:
            return True
    return False


def _attribution_from_context(
    context: str,
    target_candidate_year_id: str,
    contest_roster: pd.DataFrame,
) -> tuple[str, str, list[str]]:
    matched = sorted(
        _text(row.candidate_year_id)
        for row in contest_roster.itertuples(index=False)
        if _candidate_is_named(context, row.full_name)
    )
    if matched == [target_candidate_year_id]:
        return "identity_supported", "full_name_alias_uniquely_matches_target", matched
    if len(matched) == 1:
        return "reject_identity", "full_name_alias_matches_other_contest_candidate", matched
    if len(matched) > 1:
        return "match_unresolved", "multiple_full_name_aliases_in_context", matched
    return "match_unresolved", "no_full_name_alias_in_context", []


EXCLUDED_RESULT_HOSTS = {
    "facebook.com",
    "instagram.com",
    "linkedin.com",
    "pinterest.com",
    "reddit.com",
    "tiktok.com",
    "twitter.com",
    "x.com",
    "youtube.com",
}
KNOWN_ELECTION_HOSTS = {
    "ballotpedia.org",
    "vote411.org",
    "lwv.org",
    "ourcampaigns.com",
}
NEWS_HOST_MARKERS = {
    "abc",
    "cbs",
    "fox",
    "gazette",
    "herald",
    "journal",
    "nbc",
    "news",
    "patch",
    "post",
    "press",
    "record",
    "review",
    "star",
    "sun",
    "times",
    "tribune",
}
EVIDENCE_PROFILE_HOSTS = {
    *KNOWN_ELECTION_HOSTS,
    "wikipedia.org",
}
SEARCH_SOCIAL_OR_SHARE_HOSTS = {
    "pinterest.com",
    "reddit.com",
    "telegram.me",
    "youtu.be",
}


def _host_matches(host: str, domains: Iterable[str]) -> bool:
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)


def _source_credibility(host: str) -> str:
    if host.endswith(".gov"):
        return "official_government"
    if host.endswith(".edu"):
        return "educational_institution"
    if _host_matches(host, KNOWN_ELECTION_HOSTS):
        return "structured_election_source"
    labels = set(re.findall(r"[a-z]+", host))
    if labels & NEWS_HOST_MARKERS or any(marker in host for marker in NEWS_HOST_MARKERS):
        return "news_media"
    return "other_web_source"


def classify_search_lead_url(value: str) -> str:
    """Classify profile/evidence hosts without calling them candidate domains."""
    exact = normalize_exact_url(value)
    host = (urlsplit(exact).hostname or "").casefold().removeprefix("www.")
    base = classify_url(exact)
    if not host:
        return base
    if "%" in host:
        return "other_lead"
    if _host_matches(host, EVIDENCE_PROFILE_HOSTS):
        return "evidence_profile"
    if _host_matches(host, SEARCH_SOCIAL_OR_SHARE_HOSTS):
        return "social_profile"
    if host.endswith(".gov") or re.search(
        r"(?:^|\.)(?:sos\.)?state\.[a-z]{2}\.us$", host
    ):
        return "officeholder_or_government_profile"
    if host.endswith("tumblr.com") and urlsplit(exact).path.casefold().startswith(
        "/share/"
    ):
        return "social_profile"
    credibility = _source_credibility(host)
    if credibility in {"news_media", "educational_institution"}:
        return "evidence_source_page"
    return base


def classify_result(
    result: Mapping[str, object],
    query_row: Mapping[str, object],
    contest_roster: pd.DataFrame,
) -> dict[str, object]:
    raw_url = _text(result.get("link", result.get("url", "")))
    exact_url = normalize_exact_url(raw_url)
    host = (urlsplit(exact_url).hostname or "").casefold().removeprefix("www.")
    title = _text(result.get("title", ""))
    snippet = _text(result.get("snippet", ""))
    result_date = _text(result.get("date", ""))
    context = " ".join([title, snippet, raw_url])
    target_id = _text(query_row["candidate_year_id"])
    attribution, attribution_reason, _ = _attribution_from_context(
        context, target_id, contest_roster
    )
    identity_signal = attribution == "identity_supported"
    normalized_context = _normalize_search_text(context)
    mayoral_signal = bool(
        re.search(r"\b(?:mayor|mayoral|campaign|election|candidate)\b", normalized_context)
    )
    city = _normalize_search_text(query_row["geo_name"])
    raw_state = _text(query_row["state_abb"])
    state_signal = bool(
        raw_state
        and re.search(
            rf"(?<![A-Za-z]){re.escape(raw_state)}(?![A-Za-z])", context
        )
    )
    location_signal = bool(
        (city and re.search(rf"\b{re.escape(city)}\b", normalized_context))
        or state_signal
    )
    year = str(int(float(_text(query_row["year"]))))
    mentioned_years = sorted(set(re.findall(r"\b(?:19|20)\d{2}\b", context)))
    exact_year = year in mentioned_years
    temporal_conflict = bool(mentioned_years and not exact_year)
    excluded_reason = ""
    if not exact_url:
        excluded_reason = "invalid_or_non_http_url"
    elif _host_matches(host, EXCLUDED_RESULT_HOSTS):
        excluded_reason = "excluded_social_or_video_host"
    credibility = _source_credibility(host) if host else "invalid_url"
    candidate_site = bool(
        exact_url
        and credibility == "other_web_source"
        and is_candidate_shaped_domain(
            exact_url,
            _text(query_row.get("query_name", query_row["full_name"])),
            _text(query_row["geo_name"]),
        )
    )
    score = (
        (30 if identity_signal else 0)
        + (20 if mayoral_signal else 0)
        + (15 if location_signal else 0)
        + (25 if exact_year else 0)
        + (
            25
            if credibility == "official_government"
            else 20
            if credibility in {
                "structured_election_source",
                "news_media",
                "educational_institution",
            }
            else 5
            if credibility == "other_web_source"
            else 0
        )
        - (50 if temporal_conflict else 0)
        - (100 if excluded_reason else 0)
        - (40 if candidate_site else 0)
    )
    credible_evidence_host = credibility in {
        "official_government",
        "structured_election_source",
        "news_media",
        "educational_institution",
    }
    source_identity_context = bool(
        not excluded_reason
        and not candidate_site
        and identity_signal
        and mayoral_signal
        and location_signal
        and credible_evidence_host
    )
    eligible_source = bool(
        source_identity_context
        and not temporal_conflict
    )
    if excluded_reason:
        classification = "excluded_url"
    elif candidate_site and identity_signal:
        classification = "direct_candidate_site"
    elif eligible_source:
        classification = "credible_source_page"
    elif source_identity_context and temporal_conflict:
        classification = "credible_source_page_temporal_conflict"
    elif attribution != "identity_supported":
        classification = "identity_unresolved"
    else:
        classification = "noise"
    return {
        "exact_normalized_result_url": exact_url,
        "result_host": host,
        "candidate_attribution_status": attribution,
        "candidate_attribution_reason": attribution_reason,
        "candidate_identity_signal": identity_signal,
        "mayoral_election_signal": mayoral_signal,
        "location_signal": location_signal,
        "exact_year_signal": exact_year,
        "temporal_conflict": temporal_conflict,
        "source_credibility_class": credibility,
        "source_score": score,
        "result_classification": classification,
        "url_exclusion_reason": excluded_reason,
        "eligible_for_page_fetch": eligible_source,
    }


def build_ranked_result_page_queue(
    query_manifest: pd.DataFrame,
    raw_records: list[dict[str, Any]],
    full_roster: pd.DataFrame,
) -> pd.DataFrame:
    """Retain every organic result, marking rather than deleting selections."""
    query_lookup = {
        _text(row["query_attempt_id"]): row
        for row in query_manifest.to_dict("records")
    }
    roster_groups = {
        _text(contest): group.reset_index(drop=True)
        for contest, group in full_roster.groupby("contest", sort=False)
    }
    rows: list[dict[str, object]] = []
    for record in raw_records:
        attempt_id = _text(record.get("query_attempt_id"))
        query_row = query_lookup.get(attempt_id)
        if query_row is None:
            raise SearchExpansionError(
                f"Raw response references unexpected query attempt {attempt_id}"
            )
        organic = _organic_results(record.get("raw_response"))
        for enumeration, result in enumerate(organic, 1):
            raw_url = _text(result.get("link", result.get("url", "")))
            try:
                rank = int(result.get("position", enumeration))
            except (TypeError, ValueError):
                rank = enumeration
            raw_hash = canonical_json_hash(result)
            result_id = stable_id(
                "serper_result", CONTRACT_VERSION, attempt_id, rank, raw_url, raw_hash
            )
            classification = classify_result(
                result,
                query_row,
                roster_groups[_text(query_row["contest"])],
            )
            rows.append(
                {
                    "contract_version": CONTRACT_VERSION,
                    "result_claim_id": result_id,
                    "query_attempt_id": attempt_id,
                    "candidate_year_id": query_row["candidate_year_id"],
                    "candidate_contest_id": query_row["candidate_contest_id"],
                    "contest": query_row["contest"],
                    "full_name": query_row["full_name"],
                    "geo_name": query_row["geo_name"],
                    "state_abb": query_row["state_abb"],
                    "year": query_row["year"],
                    "month": query_row["month"],
                    "result_rank": rank,
                    "result_title": _text(result.get("title", "")),
                    "result_url": raw_url,
                    "result_snippet": _text(result.get("snippet", "")),
                    "result_date": _text(result.get("date", "")),
                    "raw_result_sha256": raw_hash,
                    **classification,
                    "selected_for_page_fetch": False,
                    "page_fetch_rank": "",
                    "page_selection_reason": (
                        "temporal_conflict_retained_for_review"
                        if classification["result_classification"]
                        == "credible_source_page_temporal_conflict"
                        else
                        "ineligible_result_classification"
                        if not classification["eligible_for_page_fetch"]
                        else "eligible_not_yet_ranked"
                    ),
                    "classifier_version": RESULT_CLASSIFIER_VERSION,
                }
            )
    queue = pd.DataFrame(rows).reindex(columns=RANKED_QUEUE_COLUMNS)
    if queue.empty:
        return queue
    if queue["result_claim_id"].duplicated().any():
        raise SearchExpansionError("Ranked queue has duplicate result claim IDs")
    if len(queue) != sum(
        len(_organic_results(item.get("raw_response"))) for item in raw_records
    ):
        raise SearchExpansionError("Organic results were dropped while building queue")
    eligible = queue.loc[queue["eligible_for_page_fetch"].map(
        lambda value: _as_bool(value, "eligible_for_page_fetch")
    )].copy()
    eligible = eligible.sort_values(
        ["candidate_year_id", "source_score", "exact_year_signal", "result_rank", "result_claim_id"],
        ascending=[True, False, False, True, True],
        kind="stable",
    )
    eligible["_page_rank"] = eligible.groupby("candidate_year_id").cumcount() + 1
    for item in eligible.to_dict("records"):
        result_id = item["result_claim_id"]
        rank = int(item["_page_rank"])
        selected = rank <= MAX_SOURCE_PAGES_PER_CANDIDATE
        mask = queue["result_claim_id"].eq(result_id)
        queue.loc[mask, "selected_for_page_fetch"] = selected
        queue.loc[mask, "page_fetch_rank"] = str(rank)
        queue.loc[mask, "page_selection_reason"] = (
            "top_three_identity_supported_election_source"
            if selected
            else "eligible_beyond_candidate_cap"
        )
    selected_counts = queue.loc[
        queue["selected_for_page_fetch"].map(
            lambda value: _as_bool(value, "selected_for_page_fetch")
        )
    ].groupby("candidate_year_id").size()
    if len(selected_counts) and int(selected_counts.max()) > MAX_SOURCE_PAGES_PER_CANDIDATE:
        raise SearchExpansionError("Page selection exceeded the three-page candidate cap")
    return queue.sort_values(
        ["candidate_year_id", "result_rank", "result_claim_id"], kind="stable"
    ).reset_index(drop=True).reindex(columns=RANKED_QUEUE_COLUMNS)


def _write_ranked_queue(
    query_manifest: pd.DataFrame,
    raw_records: list[dict[str, Any]],
    roster: pd.DataFrame,
    output_dir: Path,
) -> pd.DataFrame:
    queue = build_ranked_result_page_queue(query_manifest, raw_records, roster)
    _atomic_csv(queue, output_dir / "ranked_result_page_queue.csv", RANKED_QUEUE_COLUMNS)
    page_links = _read_csv_contract(
        output_dir / "page_links.csv", PAGE_LINK_COLUMNS, "page links"
    )
    leads = build_url_leads(
        queue,
        page_links,
        roster,
        set(query_manifest["candidate_year_id"].map(_text)),
    )
    _atomic_csv(leads, output_dir / "url_leads.csv", URL_LEAD_COLUMNS)
    return queue


def search_wave(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    screen_path: Path = DEFAULT_SCREEN,
    sampling_manifest_path: Path = DEFAULT_SAMPLING_MANIFEST,
    roster_path: Path = DEFAULT_ROSTER,
    request_budget: int = MAX_SERPER_REQUESTS,
    timeout: int = 30,
    delay_seconds: float = 0.2,
    environ: Mapping[str, str] | None = None,
    env_path: Path | None = None,
    session: Any = requests,
) -> dict[str, object]:
    """Issue at most one paid request per candidate and persist every response."""
    if not 0 <= request_budget <= MAX_SERPER_REQUESTS:
        raise SearchExpansionError(
            f"request_budget must be between 0 and {MAX_SERPER_REQUESTS}"
        )
    api_key = load_serper_api_key(environ=environ, env_path=env_path)
    screen, roster, input_audit = load_frozen_inputs(
        screen_path, sampling_manifest_path, roster_path
    )
    expected_manifest = build_query_manifest(
        screen,
        screen_sha256=input_audit["screen_sha256"],
        roster_sha256=input_audit["roster_sha256"],
    )
    manifest_path = output_dir / "query_manifest.csv"
    manifest = _validate_query_manifest(
        pd.read_csv(manifest_path, keep_default_na=False, low_memory=False),
        expected_manifest,
    )
    ledger_path = output_dir / "request_ledger.csv"
    raw_path = output_dir / "raw_responses.jsonl"
    ledger = _read_csv_contract(ledger_path, REQUEST_LEDGER_COLUMNS, "request ledger")
    if ledger["query_attempt_id"].map(_text).duplicated().any():
        raise SearchExpansionError("Request ledger has duplicate query rows")
    manifest_keys = set(manifest["query_attempt_id"].map(_text))
    unexpected = set(ledger["query_attempt_id"].map(_text)) - manifest_keys
    if unexpected:
        raise SearchExpansionError(f"Request ledger has unexpected keys: {sorted(unexpected)}")
    raw_records = read_raw_responses(raw_path)
    raw_keys = {_text(item["query_attempt_id"]) for item in raw_records}
    if not raw_keys.issubset(manifest_keys):
        raise SearchExpansionError("Raw responses contain unexpected query keys")
    ledger = _reconcile_ledger_with_raw(ledger, raw_records)
    _atomic_csv(ledger, ledger_path, REQUEST_LEDGER_COLUMNS)
    already_sent = set(ledger["query_attempt_id"].map(_text))
    sent_count = int(len(already_sent))
    if sent_count > request_budget:
        raise SearchExpansionError(
            f"Existing {sent_count} requests exceed approved budget {request_budget}"
        )

    provider_abort = ""
    for query_row in manifest.sort_values("sample_order", kind="stable").to_dict("records"):
        attempt_id = _text(query_row["query_attempt_id"])
        if attempt_id in already_sent:
            continue
        if sent_count >= request_budget:
            break
        sequence = sent_count + 1
        provisional = _provisional_ledger_row(query_row, sequence)
        ledger = pd.concat(
            [ledger, pd.DataFrame([provisional])], ignore_index=True
        ).reindex(columns=REQUEST_LEDGER_COLUMNS)
        _atomic_csv(ledger, ledger_path, REQUEST_LEDGER_COLUMNS)
        status, http_status, payload, raw_body, response_hash, error = request_serper_once(
            _text(query_row["query"]),
            api_key,
            results_requested=int(query_row["results_requested"]),
            timeout=timeout,
            session=session,
        )
        retrieved_at = _utc_now()
        raw_record = {
            "contract_version": CONTRACT_VERSION,
            "query_attempt_id": attempt_id,
            "candidate_year_id": query_row["candidate_year_id"],
            "query_sha256": query_row["query_sha256"],
            "provider": "serper",
            "request_status": status,
            "http_status": http_status,
            "retrieved_at": retrieved_at,
            "raw_response_sha256": response_hash,
            "error": error,
            "raw_response_body": raw_body,
            "raw_response": payload,
        }
        raw_records.append(raw_record)
        _atomic_jsonl_records(raw_path, raw_records)
        organic_count = len(_organic_results(payload))
        index = ledger.index[ledger["query_attempt_id"].map(_text).eq(attempt_id)]
        ledger.loc[index, [
            "request_status",
            "http_status",
            "results_returned",
            "raw_response_sha256",
            "error",
            "completed_at",
            "retryable",
            "complete_for_wave",
        ]] = [
            status,
            http_status,
            organic_count,
            response_hash,
            error,
            retrieved_at,
            status == "request_error",
            status in {"found", "no_result"},
        ]
        _atomic_csv(ledger, ledger_path, REQUEST_LEDGER_COLUMNS)
        already_sent.add(attempt_id)
        sent_count += 1
        if status == "provider_error":
            provider_abort = error or f"HTTP {http_status}"
            break
        if delay_seconds > 0:
            time.sleep(delay_seconds)

    queue = _write_ranked_queue(manifest, raw_records, roster, output_dir)
    audit = update_audit(
        output_dir=output_dir,
        input_audit=input_audit,
        query_manifest=manifest,
        request_ledger=ledger,
        raw_records=raw_records,
        ranked_queue=queue,
    )
    if provider_abort:
        raise SearchExpansionError(
            f"Serper provider rejected the wave after {sent_count} request(s): {provider_abort}"
        )
    return audit


def _content_extension(content_type: str, url: str) -> str:
    lowered = content_type.casefold()
    suffix = Path(urlsplit(url).path).suffix.casefold()
    if "pdf" in lowered or suffix == ".pdf":
        return ".pdf"
    if "html" in lowered or suffix in {".html", ".htm", ""}:
        return ".html"
    if "text" in lowered or suffix in {".txt", ".csv"}:
        return ".txt"
    return ".bin"


def _decode_text(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "windows-1252", "latin-1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("utf-8", errors="replace")


def _page_link_record(
    *,
    page_id: str,
    page_url: str,
    locator: str,
    method: str,
    anchor_text: str,
    context: str,
    raw_url: str,
) -> dict[str, object] | None:
    raw_url = _text(raw_url).rstrip(".,;:!?)]}>'\"")
    absolute = urljoin(page_url, raw_url)
    exact = normalize_exact_url(absolute)
    if not exact:
        return None
    page_host = (urlsplit(page_url).hostname or "").casefold().removeprefix("www.")
    link_host = (urlsplit(exact).hostname or "").casefold().removeprefix("www.")
    link_id = stable_id(
        "source_page_link", CONTRACT_VERSION, page_id, locator, method, raw_url
    )
    return {
        "contract_version": CONTRACT_VERSION,
        "page_link_id": link_id,
        "page_id": page_id,
        "page_url": page_url,
        "source_locator": locator,
        "extraction_method": method,
        "anchor_text": _text(anchor_text)[:1000],
        "surrounding_context": _text(context)[:4000],
        "raw_url": raw_url,
        "exact_normalized_url": exact,
        "site_root_url": site_root_url(exact),
        "registrable_domain": registrable_domain(exact),
        "url_type": classify_search_lead_url(exact),
        "is_external_to_source": bool(page_host and link_host and page_host != link_host),
        "parser_version": SOURCE_PAGE_PARSER_VERSION,
    }


def parse_html_page(
    content: bytes, page_id: str, page_url: str
) -> tuple[str, list[dict[str, object]]]:
    soup = BeautifulSoup(content, "html.parser")
    links: list[dict[str, object]] = []
    for number, anchor in enumerate(soup.find_all("a", href=True), 1):
        container = anchor.find_parent(["tr", "li", "p", "article", "section", "div"])
        context = (container or anchor.parent or anchor).get_text(" ", strip=True)
        item = _page_link_record(
            page_id=page_id,
            page_url=page_url,
            locator=f"anchor:{number}",
            method="html_hyperlink",
            anchor_text=anchor.get_text(" ", strip=True),
            context=context,
            raw_url=str(anchor.get("href", "")),
        )
        if item:
            links.append(item)
    for number, meta in enumerate(soup.find_all("meta"), 1):
        if _text(meta.get("http-equiv", "")).casefold() != "refresh":
            continue
        content_value = str(meta.get("content", ""))
        match = re.search(r"(?:^|;)\s*url\s*=\s*(.+)$", content_value, re.I)
        if not match:
            continue
        item = _page_link_record(
            page_id=page_id,
            page_url=page_url,
            locator=f"meta_refresh:{number}",
            method="html_meta_refresh",
            anchor_text="",
            context=content_value,
            raw_url=match.group(1).strip(" '\""),
        )
        if item:
            links.append(item)
    containers = soup.find_all(["p", "li", "tr"])
    if not containers:
        containers = [soup.body or soup]
    for number, container in enumerate(containers, 1):
        context = container.get_text(" ", strip=True)
        for url_number, raw_url in enumerate(extract_urls(context), 1):
            item = _page_link_record(
                page_id=page_id,
                page_url=page_url,
                locator=f"visible_text:{number}:{url_number}",
                method="html_visible_text",
                anchor_text="",
                context=context,
                raw_url=raw_url,
            )
            if item:
                links.append(item)
    for node in soup(["script", "style", "noscript", "svg"]):
        node.decompose()
    return soup.get_text(" ", strip=True), links


def parse_pdf_page(
    content: bytes, page_id: str, page_url: str
) -> tuple[str, list[dict[str, object]]]:
    try:
        from pypdf import PdfReader
    except ImportError as error:
        raise SearchExpansionError("PDF page parsing requires pypdf") from error
    try:
        reader = PdfReader(io.BytesIO(content))
        page_texts: list[str] = []
        links: list[dict[str, object]] = []
        for page_number, page in enumerate(reader.pages, 1):
            page_text = _text(page.extract_text() or "")
            page_texts.append(page_text)
            for url_number, raw_url in enumerate(extract_urls(page_text), 1):
                item = _page_link_record(
                    page_id=page_id,
                    page_url=page_url,
                    locator=f"pdf_page:{page_number}:visible:{url_number}",
                    method="pdf_visible_text",
                    anchor_text="",
                    context=page_text,
                    raw_url=raw_url,
                )
                if item:
                    links.append(item)
            for annotation_number, reference in enumerate(page.get("/Annots") or [], 1):
                annotation = reference.get_object() if hasattr(reference, "get_object") else reference
                if not isinstance(annotation, Mapping) or str(annotation.get("/Subtype", "")) != "/Link":
                    continue
                action = annotation.get("/A", {})
                action = action.get_object() if hasattr(action, "get_object") else action
                raw_url = action.get("/URI", "") if isinstance(action, Mapping) else ""
                if not raw_url:
                    continue
                item = _page_link_record(
                    page_id=page_id,
                    page_url=page_url,
                    locator=f"pdf_page:{page_number}:annotation:{annotation_number}",
                    method="pdf_hyperlink_annotation",
                    anchor_text=_text(annotation.get("/Contents", "")),
                    context=_text(annotation.get("/Contents", "")) or page_text,
                    raw_url=str(raw_url),
                )
                if item:
                    links.append(item)
        return "\n\n".join(page_texts), links
    except SearchExpansionError:
        raise
    except Exception as error:
        raise SearchExpansionError(f"Invalid PDF source page: {error}") from error


def parse_source_page(
    content: bytes,
    content_type: str,
    page_id: str,
    page_url: str,
) -> tuple[str, list[dict[str, object]]]:
    extension = _content_extension(content_type, page_url)
    if extension == ".html":
        return parse_html_page(content, page_id, page_url)
    if extension == ".pdf":
        return parse_pdf_page(content, page_id, page_url)
    if extension == ".txt":
        text = _decode_text(content)
        links: list[dict[str, object]] = []
        for number, raw_url in enumerate(extract_urls(text), 1):
            item = _page_link_record(
                page_id=page_id,
                page_url=page_url,
                locator=f"text_url:{number}",
                method="plain_text_url",
                anchor_text="",
                context=text,
                raw_url=raw_url,
            )
            if item:
                links.append(item)
        return text, links
    raise SearchExpansionError(f"Unsupported source-page content type: {content_type}")


def _write_bytes_atomic(content: bytes, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(mode="wb", dir=path.parent, delete=False) as handle:
        temporary = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _write_text_atomic(content: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    os.replace(temporary, path)


def _selected_page_map(queue: pd.DataFrame) -> dict[str, list[str]]:
    selected = queue.loc[
        queue["selected_for_page_fetch"].map(
            lambda value: _as_bool(value, "selected_for_page_fetch")
        )
    ]
    mapping: dict[str, list[str]] = {}
    for item in selected.to_dict("records"):
        url = _text(item["exact_normalized_result_url"])
        if not url:
            raise SearchExpansionError("Selected source page lacks a normalized URL")
        mapping.setdefault(url, []).append(_text(item["result_claim_id"]))
    return {url: sorted(set(ids)) for url, ids in sorted(mapping.items())}


def _read_response_bytes(response: Any, maximum_bytes: int) -> bytes:
    if hasattr(response, "iter_content"):
        chunks: list[bytes] = []
        total = 0
        for chunk in response.iter_content(chunk_size=64 * 1024):
            if not chunk:
                continue
            total += len(chunk)
            if total > maximum_bytes:
                raise SearchExpansionError(
                    f"Source page exceeds maximum {maximum_bytes} bytes"
                )
            chunks.append(bytes(chunk))
        return b"".join(chunks)
    content = bytes(getattr(response, "content", b""))
    if len(content) > maximum_bytes:
        raise SearchExpansionError(
            f"Source page exceeds maximum {maximum_bytes} bytes"
        )
    return content


def _download_one_page(
    page_url: str,
    result_ids: list[str],
    output_dir: Path,
    *,
    timeout: int,
    maximum_bytes: int,
    session: Any,
) -> tuple[dict[str, object], list[dict[str, object]]]:
    page_id = stable_id("source_page", CONTRACT_VERSION, page_url)
    base: dict[str, object] = {
        "contract_version": CONTRACT_VERSION,
        "page_id": page_id,
        "page_url": page_url,
        "result_claim_ids_json": json.dumps(result_ids, separators=(",", ":")),
        "fetch_status": "request_error",
        "http_status": "",
        "final_url": page_url,
        "content_type": "",
        "content_bytes": 0,
        "content_sha256": "",
        "local_file_path": "",
        "extracted_text_path": "",
        "extracted_text_sha256": "",
        "text_characters": 0,
        "link_count": 0,
        "fetched_at": _utc_now(),
        "retryable": True,
        "error": "",
        "parser_version": SOURCE_PAGE_PARSER_VERSION,
    }
    try:
        response = session.get(
            page_url,
            headers={
                "User-Agent": "CandidateWebsiteExtension/1.0 (academic research; source-page discovery)"
            },
            timeout=timeout,
            allow_redirects=True,
            stream=True,
        )
    except requests.RequestException as error:
        base["error"] = f"{type(error).__name__}: {error}"[:1000]
        return base, []
    status = int(response.status_code)
    base["http_status"] = status
    base["final_url"] = _text(getattr(response, "url", page_url)) or page_url
    base["content_type"] = _text((getattr(response, "headers", {}) or {}).get("content-type", ""))
    if status >= 400:
        base.update(
            fetch_status="http_error",
            retryable=status in {408, 425, 429} or status >= 500,
            error=f"HTTP {status}",
        )
        return base, []
    try:
        content = _read_response_bytes(response, maximum_bytes)
    except SearchExpansionError as error:
        base.update(fetch_status="too_large", retryable=False, error=str(error)[:1000])
        return base, []
    content_hash = hashlib.sha256(content).hexdigest()
    extension = _content_extension(_text(base["content_type"]), _text(base["final_url"]))
    file_path = output_dir / "files" / f"{content_hash}{extension}"
    if not file_path.exists():
        _write_bytes_atomic(content, file_path)
    base.update(
        content_bytes=len(content),
        content_sha256=content_hash,
        local_file_path=str(file_path.relative_to(output_dir)),
    )
    if extension == ".bin":
        base.update(fetch_status="unsupported_content", retryable=False)
        return base, []
    try:
        text, links = parse_source_page(
            content, _text(base["content_type"]), page_id, _text(base["final_url"])
        )
    except SearchExpansionError as error:
        base.update(fetch_status="parse_error", retryable=False, error=str(error)[:1000])
        return base, []
    text_hash = hashlib.sha256(text.encode("utf-8")).hexdigest()
    text_path = output_dir / "text" / f"{page_id}.txt"
    _write_text_atomic(text, text_path)
    base.update(
        fetch_status="fetched",
        retryable=False,
        extracted_text_path=str(text_path.relative_to(output_dir)),
        extracted_text_sha256=text_hash,
        text_characters=len(text),
        link_count=len(links),
        error="",
    )
    return base, links


ASSET_EXTENSIONS = {
    ".css",
    ".gif",
    ".ico",
    ".jpeg",
    ".jpg",
    ".js",
    ".json",
    ".png",
    ".svg",
    ".webp",
    ".woff",
    ".woff2",
    ".xml",
}
LEAD_CONTEXT_TERMS = {
    "campaign",
    "official site",
    "official website",
    "visit",
    "website",
}


def _candidate_shaped_roster_matches(
    exact_url: str, roster: pd.DataFrame
) -> list[str]:
    return sorted(
        _text(row.candidate_year_id)
        for row in roster.itertuples(index=False)
        if is_candidate_shaped_domain(exact_url, row.full_name, row.geo_name)
    )


def build_url_leads(
    queue: pd.DataFrame,
    page_links: pd.DataFrame,
    full_roster: pd.DataFrame,
    candidate_screen_ids: set[str],
) -> pd.DataFrame:
    """Attribute external page links with the full contest roster; approve none."""
    selected = queue.loc[
        queue["selected_for_page_fetch"].map(
            lambda value: _as_bool(value, "selected_for_page_fetch")
        )
    ]
    selected_by_page: dict[str, list[dict[str, object]]] = {}
    for row in selected.to_dict("records"):
        page_id = stable_id(
            "source_page", CONTRACT_VERSION, _text(row["exact_normalized_result_url"])
        )
        selected_by_page.setdefault(page_id, []).append(row)
    roster_by_contest = {
        _text(contest): group.reset_index(drop=True)
        for contest, group in full_roster.groupby("contest", sort=False)
    }
    roster_lookup = full_roster.set_index("candidate_year_id", drop=False)
    rows: list[dict[str, object]] = []

    # Direct organic hits do not consume a source-page fetch slot. Each result
    # remains an independent grade-D claim even when two results name one URL.
    if "result_classification" in queue.columns:
        direct_results = queue.loc[
            queue["result_classification"].map(_text).eq("direct_candidate_site")
        ]
    else:
        direct_results = queue.iloc[0:0]
    for result in direct_results.to_dict("records"):
        assigned_id = _text(result["candidate_year_id"])
        if assigned_id not in roster_lookup.index:
            raise SearchExpansionError(
                f"Direct result candidate is absent from full roster: {assigned_id}"
            )
        candidate = roster_lookup.loc[assigned_id]
        raw_url = _text(result["result_url"])
        exact_url = _text(result["exact_normalized_result_url"])
        if not exact_url:
            raise SearchExpansionError(
                f"Direct result lacks exact normalized URL: {result['result_claim_id']}"
            )
        lead_hash = hashlib.sha256(
            "\x1f".join(
                [
                    CONTRACT_VERSION,
                    _text(result["result_claim_id"]),
                    "serper_direct_candidate_site_result",
                    assigned_id,
                    raw_url,
                ]
            ).encode("utf-8")
        ).hexdigest()
        rows.append(
            {
                "contract_version": CONTRACT_VERSION,
                "url_claim_id": f"search_url_claim_{lead_hash[:24]}",
                "url_entity_id": stable_id("url_entity", assigned_id, exact_url),
                "result_claim_id": result["result_claim_id"],
                "query_attempt_id": result["query_attempt_id"],
                "page_link_id": "",
                "page_id": "",
                "source_page_url": "",
                "discovery_candidate_year_id": assigned_id,
                "discovery_candidate_contest_id": candidate["candidate_contest_id"],
                "candidate_year_id": assigned_id,
                "candidate_contest_id": candidate["candidate_contest_id"],
                "contest": candidate["contest"],
                "full_name": candidate["full_name"],
                "geo_name": candidate["geo_name"],
                "state_abb": candidate["state_abb"],
                "year": candidate["year"],
                "month": candidate["month"],
                "in_candidate_screen": assigned_id in candidate_screen_ids,
                "raw_url": raw_url,
                "exact_normalized_url": exact_url,
                "site_root_url": site_root_url(exact_url),
                "registrable_domain": registrable_domain(exact_url),
                "url_type": classify_search_lead_url(exact_url),
                "candidate_attribution_status": "identity_supported",
                "candidate_attribution_reason": (
                    "serper_result_unique_full_name_alias_and_candidate_site_shape"
                ),
                "matched_candidate_ids_json": json.dumps(
                    [assigned_id], separators=(",", ":")
                ),
                "source_locator": f"serper_organic_rank:{result['result_rank']}",
                "extraction_method": "serper_direct_candidate_site_result",
                "evidence_text": _text(
                    " ".join(
                        [
                            _text(result.get("result_title")),
                            _text(result.get("result_snippet")),
                        ]
                    )
                )[:4000],
                "evidence_grade": "D",
                "strict_coverage_eligible": False,
                "review_status": "pending",
                "acceptance": "",
                "parser_version": RESULT_CLASSIFIER_VERSION,
            }
        )

    for link in page_links.to_dict("records"):
        if not _as_bool(link["is_external_to_source"], "is_external_to_source"):
            continue
        exact_url = _text(link["exact_normalized_url"])
        if Path(urlsplit(exact_url).path).suffix.casefold() in ASSET_EXTENSIONS:
            continue
        context = " ".join(
            [_text(link["anchor_text"]), _text(link["surrounding_context"])]
        )
        normalized_context = _normalize_search_text(context)
        for result in selected_by_page.get(_text(link["page_id"]), []):
            contest_roster = roster_by_contest[_text(result["contest"])]
            target_id = _text(result["candidate_year_id"])
            _, context_reason, context_matches = _attribution_from_context(
                context, target_id, contest_roster
            )
            url_matches = _candidate_shaped_roster_matches(exact_url, contest_roster)
            matched_ids = sorted(set(context_matches) | set(url_matches))
            assigned_id = ""
            status = "match_unresolved"
            reason = ""
            if len(context_matches) == 1:
                assigned_id = context_matches[0]
                status = "identity_supported"
                reason = "link_context_full_name_alias_unique_in_contest"
            elif len(context_matches) > 1:
                assigned_id = target_id
                reason = "multiple_contest_candidates_named_in_link_context"
            elif len(url_matches) == 1:
                assigned_id = url_matches[0]
                reason = "candidate_shaped_url_is_lead_not_identity_proof"
            elif len(url_matches) > 1:
                assigned_id = target_id
                reason = "candidate_shaped_url_matches_multiple_contest_candidates"
            elif (
                _text(result["candidate_attribution_status"]) == "identity_supported"
                and any(term in normalized_context for term in LEAD_CONTEXT_TERMS)
            ):
                assigned_id = target_id
                reason = "source_page_identity_not_candidate_local_link_identity"
            else:
                continue
            candidate = roster_lookup.loc[assigned_id]
            lead_hash = hashlib.sha256(
                "\x1f".join(
                    [
                        CONTRACT_VERSION,
                        _text(result["result_claim_id"]),
                        _text(link["page_link_id"]),
                        assigned_id,
                        _text(link["raw_url"]),
                    ]
                ).encode("utf-8")
            ).hexdigest()
            entity_id = stable_id("url_entity", assigned_id, exact_url)
            rows.append(
                {
                    "contract_version": CONTRACT_VERSION,
                    "url_claim_id": f"search_url_claim_{lead_hash[:24]}",
                    "url_entity_id": entity_id,
                    "result_claim_id": result["result_claim_id"],
                    "query_attempt_id": result["query_attempt_id"],
                    "page_link_id": link["page_link_id"],
                    "page_id": link["page_id"],
                    "source_page_url": link["page_url"],
                    # The organic result belongs to the queried candidate.  A
                    # contest-wide source page may independently identify a
                    # different candidate as the owner of this URL.  Preserve
                    # both facts instead of overwriting discovery provenance.
                    "discovery_candidate_year_id": target_id,
                    "discovery_candidate_contest_id": roster_lookup.loc[target_id][
                        "candidate_contest_id"
                    ],
                    "candidate_year_id": assigned_id,
                    "candidate_contest_id": candidate["candidate_contest_id"],
                    "contest": candidate["contest"],
                    "full_name": candidate["full_name"],
                    "geo_name": candidate["geo_name"],
                    "state_abb": candidate["state_abb"],
                    "year": candidate["year"],
                    "month": candidate["month"],
                    "in_candidate_screen": assigned_id in candidate_screen_ids,
                    "raw_url": link["raw_url"],
                    "exact_normalized_url": exact_url,
                    "site_root_url": link["site_root_url"],
                    "registrable_domain": link["registrable_domain"],
                    "url_type": classify_search_lead_url(exact_url),
                    "candidate_attribution_status": status,
                    "candidate_attribution_reason": reason or context_reason,
                    "matched_candidate_ids_json": json.dumps(
                        sorted(set(matched_ids)), separators=(",", ":")
                    ),
                    "source_locator": link["source_locator"],
                    "extraction_method": link["extraction_method"],
                    "evidence_text": _text(context)[:4000],
                    "evidence_grade": "D",
                    "strict_coverage_eligible": False,
                    "review_status": "pending",
                    "acceptance": "",
                    "parser_version": SOURCE_PAGE_PARSER_VERSION,
                }
            )
    leads = pd.DataFrame(rows).reindex(columns=URL_LEAD_COLUMNS)
    if not leads.empty and leads["url_claim_id"].duplicated().any():
        raise SearchExpansionError("URL lead claims have duplicate claim IDs")
    if not leads.empty:
        if set(leads["evidence_grade"]) != {"D"}:
            raise SearchExpansionError("Search expansion may emit only grade-D leads")
        if leads["strict_coverage_eligible"].map(
            lambda value: _as_bool(value, "strict_coverage_eligible")
        ).any():
            raise SearchExpansionError("Search expansion cannot create strict coverage")
        if not leads["review_status"].eq("pending").all() or leads[
            "acceptance"
        ].map(_text).ne("").any():
            raise SearchExpansionError("Search expansion cannot approve URL leads")
    return leads.sort_values(
        ["candidate_year_id", "url_claim_id"], kind="stable"
    ).reset_index(drop=True)


def _reparse_downloaded_pages(
    downloaded: pd.DataFrame, output_dir: Path
) -> tuple[pd.DataFrame, pd.DataFrame]:
    all_links: list[dict[str, object]] = []
    rows: list[dict[str, object]] = []
    for row in downloaded.to_dict("records"):
        if _text(row["fetch_status"]) != "fetched":
            rows.append(row)
            continue
        file_path = output_dir / _text(row["local_file_path"])
        if not file_path.is_file():
            raise SearchExpansionError(f"Downloaded source file is missing: {file_path}")
        content = file_path.read_bytes()
        if hashlib.sha256(content).hexdigest() != _text(row["content_sha256"]):
            raise SearchExpansionError(f"Downloaded source hash drift for {row['page_id']}")
        text, links = parse_source_page(
            content,
            _text(row["content_type"]),
            _text(row["page_id"]),
            _text(row["final_url"]),
        )
        text_path = output_dir / _text(row["extracted_text_path"])
        if not text_path.is_file():
            _write_text_atomic(text, text_path)
        if hashlib.sha256(text.encode("utf-8")).hexdigest() != _text(
            row["extracted_text_sha256"]
        ):
            raise SearchExpansionError(f"Extracted text hash drift for {row['page_id']}")
        row["link_count"] = len(links)
        rows.append(row)
        all_links.extend(links)
    page_links = pd.DataFrame(all_links).reindex(columns=PAGE_LINK_COLUMNS)
    if not page_links.empty and page_links["page_link_id"].duplicated().any():
        raise SearchExpansionError("Page parser emitted duplicate link IDs")
    return (
        pd.DataFrame(rows).reindex(columns=DOWNLOADED_PAGE_COLUMNS),
        page_links.sort_values(["page_id", "source_locator", "page_link_id"], kind="stable")
        .reset_index(drop=True)
        .reindex(columns=PAGE_LINK_COLUMNS),
    )


def fetch_selected_pages(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    screen_path: Path = DEFAULT_SCREEN,
    sampling_manifest_path: Path = DEFAULT_SAMPLING_MANIFEST,
    roster_path: Path = DEFAULT_ROSTER,
    page_request_budget: int = EXPECTED_CANDIDATES * MAX_SOURCE_PAGES_PER_CANDIDATE,
    timeout: int = 30,
    delay_seconds: float = 0.2,
    maximum_bytes: int = 10 * 1024 * 1024,
    workers: int = 6,
    session: Any | None = None,
    session_factory: Callable[[], Any] | None = None,
) -> dict[str, object]:
    """Fetch each selected physical page once, then parse it for every candidate."""
    if not 0 <= page_request_budget <= EXPECTED_CANDIDATES * MAX_SOURCE_PAGES_PER_CANDIDATE:
        raise SearchExpansionError("page_request_budget exceeds the three-page candidate cap")
    if workers < 1:
        raise SearchExpansionError("workers must be at least 1")
    if session is not None and session_factory is not None:
        raise SearchExpansionError("provide session or session_factory, not both")
    if session is not None and workers != 1:
        raise SearchExpansionError(
            "an injected session requires workers=1; use session_factory for parallel fetches"
        )
    screen, roster, input_audit = load_frozen_inputs(
        screen_path, sampling_manifest_path, roster_path
    )
    query_manifest = _read_csv_contract(
        output_dir / "query_manifest.csv", QUERY_MANIFEST_COLUMNS, "query manifest"
    )
    queue = _read_csv_contract(
        output_dir / "ranked_result_page_queue.csv",
        RANKED_QUEUE_COLUMNS,
        "ranked result-page queue",
    )
    page_map = _selected_page_map(queue)
    downloaded_path = output_dir / "downloaded_pages.csv"
    downloaded = _read_csv_contract(
        downloaded_path, DOWNLOADED_PAGE_COLUMNS, "downloaded page ledger"
    )
    if downloaded["page_id"].map(_text).duplicated().any():
        raise SearchExpansionError("Downloaded page ledger has duplicate page IDs")
    expected_page_ids = {
        stable_id("source_page", CONTRACT_VERSION, url): url for url in page_map
    }
    downloaded = downloaded.loc[
        downloaded["page_id"].map(_text).isin(expected_page_ids)
    ].copy()
    existing = {
        _text(row["page_id"]): row for row in downloaded.to_dict("records")
    }
    pending: list[tuple[str, list[str]]] = []
    for page_url, result_ids in page_map.items():
        page_id = stable_id("source_page", CONTRACT_VERSION, page_url)
        prior = existing.get(page_id)
        if prior is not None and _text(prior["fetch_status"]) != "request_error":
            continue
        if len(pending) >= page_request_budget:
            break
        pending.append((page_url, result_ids))

    # Only the parent thread updates the restart ledger. Workers may atomically
    # persist response bodies and parsed text, but never mutate shared tables.
    host_queues: dict[str, deque[tuple[str, list[str]]]] = {}
    for page_url, result_ids in pending:
        host = registrable_domain(page_url) or (
            urlsplit(page_url).hostname or ""
        ).casefold()
        host_queues.setdefault(host, deque()).append((page_url, result_ids))
    worker_local = threading.local()
    created_sessions: list[Any] = []
    created_sessions_lock = threading.Lock()
    make_session = session_factory or requests.Session

    def worker_fetch(page_url: str, result_ids: list[str]) -> dict[str, object]:
        if session is not None:
            worker_session = session
        else:
            worker_session = getattr(worker_local, "session", None)
            if worker_session is None:
                worker_session = make_session()
                worker_local.session = worker_session
                with created_sessions_lock:
                    created_sessions.append(worker_session)
        row, _ = _download_one_page(
            page_url,
            result_ids,
            output_dir,
            timeout=timeout,
            maximum_bytes=maximum_bytes,
            session=worker_session,
        )
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        return row

    try:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_hosts = {}
            for host, host_queue in host_queues.items():
                page_url, result_ids = host_queue.popleft()
                future_hosts[executor.submit(worker_fetch, page_url, result_ids)] = host
            while future_hosts:
                completed, _ = wait(
                    tuple(future_hosts), return_when=FIRST_COMPLETED
                )
                for future in sorted(completed, key=lambda item: future_hosts[item]):
                    host = future_hosts.pop(future)
                    row = future.result()
                    existing[_text(row["page_id"])] = row
                    downloaded = pd.DataFrame(existing.values()).reindex(
                        columns=DOWNLOADED_PAGE_COLUMNS
                    )
                    downloaded = downloaded.sort_values(
                        "page_id", kind="stable"
                    ).reset_index(drop=True)
                    _atomic_csv(downloaded, downloaded_path, DOWNLOADED_PAGE_COLUMNS)
                    if host_queues[host]:
                        page_url, result_ids = host_queues[host].popleft()
                        future_hosts[
                            executor.submit(worker_fetch, page_url, result_ids)
                        ] = host
    finally:
        for worker_session in created_sessions:
            close = getattr(worker_session, "close", None)
            if callable(close):
                close()
    downloaded = pd.DataFrame(existing.values()).reindex(columns=DOWNLOADED_PAGE_COLUMNS)
    downloaded = downloaded.sort_values("page_id", kind="stable").reset_index(drop=True)
    downloaded, page_links = _reparse_downloaded_pages(downloaded, output_dir)
    _atomic_csv(downloaded, downloaded_path, DOWNLOADED_PAGE_COLUMNS)
    _atomic_csv(page_links, output_dir / "page_links.csv", PAGE_LINK_COLUMNS)
    leads = build_url_leads(
        queue,
        page_links,
        roster,
        set(screen["candidate_year_id"].map(_text)),
    )
    _atomic_csv(leads, output_dir / "url_leads.csv", URL_LEAD_COLUMNS)
    ledger = _read_csv_contract(
        output_dir / "request_ledger.csv", REQUEST_LEDGER_COLUMNS, "request ledger"
    )
    raw_records = read_raw_responses(output_dir / "raw_responses.jsonl")
    return update_audit(
        output_dir=output_dir,
        input_audit=input_audit,
        query_manifest=query_manifest,
        request_ledger=ledger,
        raw_records=raw_records,
        ranked_queue=queue,
        downloaded_pages=downloaded,
        page_links=page_links,
        url_leads=leads,
    )


def _status_counts(frame: pd.DataFrame, column: str) -> dict[str, int]:
    if frame.empty:
        return {}
    return {
        str(key): int(value)
        for key, value in frame[column].map(_text).value_counts().sort_index().items()
    }


def _output_file_record(path: Path) -> dict[str, object]:
    return {
        "path": _display_path(path),
        "bytes": path.stat().st_size,
        "sha256": sha256_file(path),
    }


def page_link_disposition_audit(
    ranked_queue: pd.DataFrame,
    page_links: pd.DataFrame,
    url_leads: pd.DataFrame,
) -> dict[str, object]:
    """Account for every raw page-link x selected-result evaluation."""
    selected = ranked_queue.loc[
        ranked_queue["selected_for_page_fetch"].map(
            lambda value: _as_bool(value, "selected_for_page_fetch")
        )
    ]
    results_by_page: dict[str, list[str]] = {}
    for result in selected.to_dict("records"):
        page_id = stable_id(
            "source_page",
            CONTRACT_VERSION,
            _text(result["exact_normalized_result_url"]),
        )
        results_by_page.setdefault(page_id, []).append(
            _text(result["result_claim_id"])
        )
    page_claims = url_leads.loc[
        ~url_leads["extraction_method"]
        .map(_text)
        .eq("serper_direct_candidate_site_result")
    ]
    claim_by_pair: dict[tuple[str, str], dict[str, object]] = {}
    for lead in page_claims.to_dict("records"):
        key = (_text(lead["page_link_id"]), _text(lead["result_claim_id"]))
        if key in claim_by_pair:
            raise SearchExpansionError(
                "Page-link disposition has duplicate candidate-source claims"
            )
        claim_by_pair[key] = lead

    counts: dict[str, int] = {
        "internal_source_link": 0,
        "asset_extension": 0,
        "ineligible_url_type": 0,
        "no_candidate_match": 0,
        "emitted_identity_supported_claim": 0,
        "emitted_domain_shape_unresolved_claim": 0,
        "emitted_ambiguous_identity_claim": 0,
        "emitted_identity_unresolved_claim": 0,
    }
    evaluated_keys: set[tuple[str, str]] = set()
    unmapped_page_links = 0

    def increment(reason: str) -> None:
        counts[reason] = counts.get(reason, 0) + 1

    for link in page_links.to_dict("records"):
        result_ids = results_by_page.get(_text(link["page_id"]), [])
        if not result_ids:
            unmapped_page_links += 1
            continue
        exact_url = _text(link["exact_normalized_url"])
        is_asset = Path(urlsplit(exact_url).path).suffix.casefold() in ASSET_EXTENSIONS
        is_external = _as_bool(link["is_external_to_source"], "is_external_to_source")
        for result_id in result_ids:
            key = (_text(link["page_link_id"]), result_id)
            evaluated_keys.add(key)
            if not is_external:
                increment("internal_source_link")
                continue
            if is_asset:
                increment("asset_extension")
                continue
            lead = claim_by_pair.get(key)
            if lead is None:
                increment("no_candidate_match")
                continue
            status = _text(lead["candidate_attribution_status"])
            reason = _text(lead["candidate_attribution_reason"])
            if status == "identity_supported":
                increment("emitted_identity_supported_claim")
            elif reason == "candidate_shaped_url_is_lead_not_identity_proof":
                increment("emitted_domain_shape_unresolved_claim")
            elif reason in {
                "multiple_contest_candidates_named_in_link_context",
                "candidate_shaped_url_matches_multiple_contest_candidates",
            }:
                increment("emitted_ambiguous_identity_claim")
            else:
                increment("emitted_identity_unresolved_claim")

    unexpected_claims = sorted(set(claim_by_pair) - evaluated_keys)
    if unexpected_claims:
        raise SearchExpansionError(
            "URL claims reference page-link/result pairs outside the selected queue"
        )
    evaluated = len(evaluated_keys)
    if sum(counts.values()) != evaluated:
        raise SearchExpansionError("Page-link disposition counts do not sum exactly")
    emitted = sum(
        count for reason, count in counts.items() if reason.startswith("emitted_")
    )
    if emitted != len(page_claims):
        raise SearchExpansionError(
            "Emitted page-link disposition count differs from URL-claim rows"
        )
    not_emitted = evaluated - emitted
    drop_fraction = not_emitted / evaluated if evaluated else 0.0
    return {
        "evaluated_page_link_result_pairs": evaluated,
        "disposition_counts": dict(sorted(counts.items())),
        "exact_disposition_sum_agreement": sum(counts.values()) == evaluated,
        "emitted_page_url_claims": emitted,
        "not_emitted_pairs": not_emitted,
        "not_emitted_fraction": drop_fraction,
        "greater_than_20_percent_not_emitted": bool(
            evaluated and drop_fraction > 0.20
        ),
        "unmapped_raw_page_link_rows": unmapped_page_links,
        "rule": (
            "Every parsed page link is retained in page_links.csv. Every link x selected-"
            "result evaluation receives one explicit disposition; non-emission is reported, "
            "not silently filtered."
        ),
    }


def update_audit(
    *,
    output_dir: Path,
    input_audit: Mapping[str, object],
    query_manifest: pd.DataFrame,
    request_ledger: pd.DataFrame,
    raw_records: list[dict[str, Any]],
    ranked_queue: pd.DataFrame,
    downloaded_pages: pd.DataFrame | None = None,
    page_links: pd.DataFrame | None = None,
    url_leads: pd.DataFrame | None = None,
) -> dict[str, object]:
    """Audit every transition, including large selections that retain source rows."""
    downloaded_pages = (
        _read_csv_contract(
            output_dir / "downloaded_pages.csv",
            DOWNLOADED_PAGE_COLUMNS,
            "downloaded page ledger",
        )
        if downloaded_pages is None
        else downloaded_pages
    )
    page_links = (
        _read_csv_contract(output_dir / "page_links.csv", PAGE_LINK_COLUMNS, "page links")
        if page_links is None
        else page_links
    )
    url_leads = (
        _read_csv_contract(output_dir / "url_leads.csv", URL_LEAD_COLUMNS, "URL leads")
        if url_leads is None
        else url_leads
    )
    _require_exact_columns(query_manifest, QUERY_MANIFEST_COLUMNS, "query manifest")
    _require_exact_columns(request_ledger, REQUEST_LEDGER_COLUMNS, "request ledger")
    _require_exact_columns(ranked_queue, RANKED_QUEUE_COLUMNS, "ranked queue")
    _require_exact_columns(downloaded_pages, DOWNLOADED_PAGE_COLUMNS, "downloaded pages")
    _require_exact_columns(page_links, PAGE_LINK_COLUMNS, "page links")
    _require_exact_columns(url_leads, URL_LEAD_COLUMNS, "URL leads")
    if len(query_manifest) != EXPECTED_CANDIDATES or query_manifest[
        "candidate_year_id"
    ].nunique() != EXPECTED_CANDIDATES:
        raise SearchExpansionError("Audit requires exactly 480 one-query candidate rows")
    if request_ledger["query_attempt_id"].duplicated().any():
        raise SearchExpansionError("Audit found duplicate request-ledger keys")
    allowed_statuses = {"found", "no_result", "request_error", "provider_error"}
    invalid_statuses = set(request_ledger["request_status"].map(_text)) - allowed_statuses
    if invalid_statuses:
        raise SearchExpansionError(f"Audit found invalid request statuses: {invalid_statuses}")
    if len(request_ledger) > MAX_SERPER_REQUESTS:
        raise SearchExpansionError("Serper request ledger exceeds 480")
    query_keys = set(query_manifest["query_attempt_id"].map(_text))
    ledger_keys = set(request_ledger["query_attempt_id"].map(_text))
    raw_keys = {_text(item.get("query_attempt_id")) for item in raw_records}
    if not ledger_keys.issubset(query_keys) or not raw_keys.issubset(query_keys):
        raise SearchExpansionError("Search outputs contain unexpected query keys")
    organic_count = sum(
        len(_organic_results(item.get("raw_response"))) for item in raw_records
    )
    if len(ranked_queue) != organic_count:
        raise SearchExpansionError(
            f"Ranked queue dropped organic results: expected {organic_count}, found {len(ranked_queue)}"
        )
    if ranked_queue["result_claim_id"].duplicated().any():
        raise SearchExpansionError("Ranked queue has duplicate result claims")
    selected = ranked_queue["selected_for_page_fetch"].map(
        lambda value: _as_bool(value, "selected_for_page_fetch")
    ) if len(ranked_queue) else pd.Series(dtype=bool)
    eligible = ranked_queue["eligible_for_page_fetch"].map(
        lambda value: _as_bool(value, "eligible_for_page_fetch")
    ) if len(ranked_queue) else pd.Series(dtype=bool)
    if len(ranked_queue) and (selected & ~eligible).any():
        raise SearchExpansionError("Ineligible result was selected for page fetch")
    selected_counts = ranked_queue.loc[selected].groupby("candidate_year_id").size() if len(ranked_queue) else pd.Series(dtype=int)
    if len(selected_counts) and int(selected_counts.max()) > MAX_SOURCE_PAGES_PER_CANDIDATE:
        raise SearchExpansionError("Audit found more than three selected pages for a candidate")
    direct_rows = ranked_queue.loc[
        ranked_queue["result_classification"].map(_text).eq("direct_candidate_site")
    ]
    direct_count = int(len(direct_rows))
    direct_leads = url_leads.loc[
        url_leads["extraction_method"]
        .map(_text)
        .eq("serper_direct_candidate_site_result")
    ]
    direct_result_keys = set(direct_rows["result_claim_id"].map(_text))
    direct_lead_keys = set(direct_leads["result_claim_id"].map(_text))
    if direct_leads["result_claim_id"].map(_text).duplicated().any():
        raise SearchExpansionError("Direct candidate-site result has duplicate URL claims")
    if direct_lead_keys != direct_result_keys or len(direct_leads) != len(direct_rows):
        raise SearchExpansionError(
            "Direct candidate-site result claims do not exactly match direct URL claims"
        )
    if direct_count:
        queue_direct_lookup = direct_rows.set_index("result_claim_id")
        for lead in direct_leads.to_dict("records"):
            source = queue_direct_lookup.loc[_text(lead["result_claim_id"])]
            if (
                _text(lead["raw_url"]) != _text(source["result_url"])
                or _text(lead["exact_normalized_url"])
                != _text(source["exact_normalized_result_url"])
            ):
                raise SearchExpansionError(
                    "Direct candidate-site URL claim changed the raw or exact result URL"
                )
    if not url_leads.empty:
        if url_leads["review_status"].map(_text).ne("pending").any():
            raise SearchExpansionError("URL leads contain a non-pending review status")
        if url_leads["acceptance"].map(_text).ne("").any():
            raise SearchExpansionError("URL leads contain an approval")
        if url_leads["evidence_grade"].map(_text).ne("D").any():
            raise SearchExpansionError("URL leads contain a non-D grade")

    page_link_audit = page_link_disposition_audit(
        ranked_queue, page_links, url_leads
    )

    before = len(ranked_queue)
    eligible_n = int(eligible.sum()) if before else 0
    selected_n = int(selected.sum()) if before else 0
    selection_drop = before - selected_n
    large_selection = bool(before and selection_drop / before > 0.20)
    exact_request_keys = bool(
        len(request_ledger) == EXPECTED_CANDIDATES and ledger_keys == query_keys
    )
    exact_raw_keys = bool(raw_keys == ledger_keys and len(raw_records) == len(ledger_keys))
    selected_page_ids = {
        stable_id("source_page", CONTRACT_VERSION, _text(url))
        for url in ranked_queue.loc[selected, "exact_normalized_result_url"]
    } if before else set()
    download_keys = set(downloaded_pages["page_id"].map(_text))
    pages_complete = bool(download_keys == selected_page_ids)
    retryable_requests = int(
        request_ledger["request_status"].map(_text).eq("request_error").sum()
    )
    if exact_request_keys:
        stage = "page_expansion_complete" if pages_complete else "search_complete_pages_pending"
        verdict = "pass_with_retryable_errors" if retryable_requests else "pass"
    else:
        stage = "search_in_progress"
        verdict = "partial"
    output_paths = [
        output_dir / "query_manifest.csv",
        output_dir / "request_ledger.csv",
        output_dir / "raw_responses.jsonl",
        output_dir / "ranked_result_page_queue.csv",
        output_dir / "downloaded_pages.csv",
        output_dir / "page_links.csv",
        output_dir / "url_leads.csv",
    ]
    audit: dict[str, object] = {
        "audit_version": "mayoral-search-source-expansion-audit-v1",
        "contract_version": CONTRACT_VERSION,
        "stage": stage,
        "verdict": verdict,
        **input_audit,
        "query_rows": int(len(query_manifest)),
        "unique_query_candidate_years": int(query_manifest["candidate_year_id"].nunique()),
        "one_query_per_candidate": True,
        "expected_serper_requests": EXPECTED_CANDIDATES,
        "maximum_serper_requests": MAX_SERPER_REQUESTS,
        "serper_requests_sent": int(len(request_ledger)),
        "exact_manifest_request_key_agreement": exact_request_keys,
        "exact_ledger_raw_response_key_agreement": exact_raw_keys,
        "request_status_counts": _status_counts(request_ledger, "request_status"),
        "retryable_request_rows": retryable_requests,
        "organic_result_rows": int(organic_count),
        "ranked_queue_rows": int(len(ranked_queue)),
        "all_organic_results_retained": int(len(ranked_queue)) == organic_count,
        "direct_candidate_site_rows_retained": direct_count,
        "direct_candidate_site_url_claim_rows": int(len(direct_leads)),
        "exact_direct_result_to_url_claim_agreement": bool(
            direct_lead_keys == direct_result_keys
            and len(direct_leads) == len(direct_rows)
        ),
        "credible_source_page_rows": eligible_n,
        "selected_source_page_result_rows": selected_n,
        "maximum_selected_pages_for_one_candidate": int(selected_counts.max()) if len(selected_counts) else 0,
        "unique_selected_physical_pages": int(len(selected_page_ids)),
        "downloaded_page_rows": int(len(downloaded_pages)),
        "exact_selected_download_key_agreement": pages_complete,
        "download_status_counts": _status_counts(downloaded_pages, "fetch_status"),
        "page_link_rows": int(len(page_links)),
        "url_lead_rows": int(len(url_leads)),
        "page_link_claim_disposition_audit": page_link_audit,
        "identity_supported_url_leads": int(
            url_leads["candidate_attribution_status"].map(_text).eq("identity_supported").sum()
        ) if len(url_leads) else 0,
        "unresolved_url_leads": int(
            url_leads["candidate_attribution_status"].map(_text).eq("match_unresolved").sum()
        ) if len(url_leads) else 0,
        "claims_validated_or_approved": 0,
        "large_filter_audit": {
            "triggered": large_selection,
            "before_organic_results": int(before),
            "eligible_source_pages": eligible_n,
            "selected_source_pages": selected_n,
            "selection_drop": int(selection_drop),
            "selection_drop_fraction": selection_drop / before if before else 0.0,
            "rule": (
                "The top-three source-page cap is a fetch prioritization, not a row filter. "
                "Every organic result, every direct candidate-site result, and every credible "
                "beyond-cap result remains in ranked_result_page_queue.csv."
            ),
        },
        "outputs": {
            path.name: _output_file_record(path) for path in output_paths if path.exists()
        },
    }
    _atomic_json(audit, output_dir / "audit.json")
    return audit


def rebuild_ranked_queue(
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    screen_path: Path = DEFAULT_SCREEN,
    sampling_manifest_path: Path = DEFAULT_SAMPLING_MANIFEST,
    roster_path: Path = DEFAULT_ROSTER,
) -> dict[str, object]:
    screen, roster, input_audit = load_frozen_inputs(
        screen_path, sampling_manifest_path, roster_path
    )
    expected = build_query_manifest(
        screen,
        screen_sha256=input_audit["screen_sha256"],
        roster_sha256=input_audit["roster_sha256"],
    )
    manifest = _validate_query_manifest(
        pd.read_csv(output_dir / "query_manifest.csv", keep_default_na=False), expected
    )
    ledger = _read_csv_contract(
        output_dir / "request_ledger.csv", REQUEST_LEDGER_COLUMNS, "request ledger"
    )
    raw_records = read_raw_responses(output_dir / "raw_responses.jsonl")
    queue = _write_ranked_queue(manifest, raw_records, roster, output_dir)
    return update_audit(
        output_dir=output_dir,
        input_audit=input_audit,
        query_manifest=manifest,
        request_ledger=ledger,
        raw_records=raw_records,
        ranked_queue=queue,
    )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare = subparsers.add_parser("prepare")
    prepare.add_argument("--screen", type=Path, default=DEFAULT_SCREEN)
    prepare.add_argument("--sampling-manifest", type=Path, default=DEFAULT_SAMPLING_MANIFEST)
    prepare.add_argument("--roster", type=Path, default=DEFAULT_ROSTER)
    prepare.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    subparsers.add_parser("check-key")
    search = subparsers.add_parser("search")
    search.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    search.add_argument("--request-budget", type=int, default=MAX_SERPER_REQUESTS)
    search.add_argument("--timeout", type=int, default=30)
    search.add_argument("--delay-seconds", type=float, default=0.2)
    rank = subparsers.add_parser("rank")
    rank.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    fetch = subparsers.add_parser("fetch")
    fetch.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    fetch.add_argument(
        "--page-request-budget",
        type=int,
        default=EXPECTED_CANDIDATES * MAX_SOURCE_PAGES_PER_CANDIDATE,
    )
    fetch.add_argument("--timeout", type=int, default=30)
    fetch.add_argument("--delay-seconds", type=float, default=0.2)
    fetch.add_argument("--maximum-bytes", type=int, default=10 * 1024 * 1024)
    fetch.add_argument("--workers", type=int, default=6)
    audit = subparsers.add_parser("audit")
    audit.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    run = subparsers.add_parser("run")
    run.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    run.add_argument("--request-budget", type=int, default=MAX_SERPER_REQUESTS)
    run.add_argument(
        "--page-request-budget",
        type=int,
        default=EXPECTED_CANDIDATES * MAX_SOURCE_PAGES_PER_CANDIDATE,
    )
    run.add_argument("--timeout", type=int, default=30)
    run.add_argument("--delay-seconds", type=float, default=0.2)
    run.add_argument("--workers", type=int, default=6)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if args.command == "prepare":
        result = prepare_wave(
            screen_path=args.screen,
            sampling_manifest_path=args.sampling_manifest,
            roster_path=args.roster,
            output_dir=args.output_dir,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "check-key":
        load_serper_api_key()
        print(json.dumps({"serper_api_key_available": True}))
    elif args.command == "search":
        result = search_wave(
            output_dir=args.output_dir,
            request_budget=args.request_budget,
            timeout=args.timeout,
            delay_seconds=args.delay_seconds,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command in {"rank", "audit"}:
        result = rebuild_ranked_queue(output_dir=args.output_dir)
        print(json.dumps(result, indent=2, sort_keys=True))
    elif args.command == "fetch":
        result = fetch_selected_pages(
            output_dir=args.output_dir,
            page_request_budget=args.page_request_budget,
            timeout=args.timeout,
            delay_seconds=args.delay_seconds,
            maximum_bytes=args.maximum_bytes,
            workers=args.workers,
        )
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        prepare_wave(output_dir=args.output_dir)
        search_wave(
            output_dir=args.output_dir,
            request_budget=args.request_budget,
            timeout=args.timeout,
            delay_seconds=args.delay_seconds,
            workers=args.workers,
        )
        result = fetch_selected_pages(
            output_dir=args.output_dir,
            page_request_budget=args.page_request_budget,
            timeout=args.timeout,
            delay_seconds=args.delay_seconds,
        )
        print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
