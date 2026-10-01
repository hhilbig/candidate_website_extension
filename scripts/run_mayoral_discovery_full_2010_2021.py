#!/usr/bin/env python3
"""Run the approved full 2010--2021 mayoral URL discovery wave.

The production universe excludes the earlier 480-case screen and candidates
that already have an active coverage-eligible URL claim. Serper receives one
query for at most 2,020 candidates. OpenAI web search is reserved for every
candidate without a direct Serper campaign-site result and is hard-capped at
the approved $36 estimated cost.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import requests

from scripts import run_openai_search_residual_pilot_2010_2021 as openai_search
from scripts.run_mayoral_search_source_expansion_2010_2021 import (
    PAGE_LINK_COLUMNS,
    QUERY_MANIFEST_COLUMNS,
    RANKED_QUEUE_COLUMNS,
    REQUEST_LEDGER_COLUMNS,
    SERPER_RESULTS_PER_QUERY,
    URL_LEAD_COLUMNS,
    build_ranked_result_page_queue,
    build_url_leads,
    canonical_json_hash,
    load_serper_api_key,
    primary_query,
    repair_query_name,
    request_serper_once,
    stable_hash,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCREEN_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_UNRESOLVED = SCREEN_DIR / "unresolved_2010_2021.csv"
DEFAULT_PRIOR_SCREEN = SCREEN_DIR / "candidate_screening_sample_2010_2021.csv"
DEFAULT_CLAIMS = REPO_ROOT / "data/municipal_discovery/final/mayoral_url_claims.csv"
DEFAULT_ROSTER = SCREEN_DIR / "current_coverage_roster.csv"
DEFAULT_OUTPUT_DIR = SCREEN_DIR / "full_discovery_2010_2021"

CONTRACT_VERSION = "mayoral-full-discovery-2010-2021-v1"
QUERY_VERSION = "mayoral-serper-full-2010-2021-v1"
OPENAI_CONTRACT_VERSION = "openai-web-search-full-residual-2010-2021-v1"
SERPER_REQUEST_CEILING = 2020
OPENAI_COST_CEILING_USD = 36.0
OPENAI_RETRY_CONTRACT_VERSION = "openai-web-search-full-residual-retry-v1"
OPENAI_RETRY_COST_CEILING_USD = 0.25
ACTIVE_URL_TYPES = {"custom_domain", "candidate_subdomain", "shared_host_path"}


class FullDiscoveryError(ValueError):
    """A frozen input or production checkpoint violates the wave contract."""


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path, columns: list[str] | None = None) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    output = frame if columns is None else frame.reindex(columns=columns)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        output.to_csv(handle, index=False, lineterminator="\n")
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


def _atomic_jsonl(records: list[dict[str, object]], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="\n", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(temporary, path)


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise FullDiscoveryError(f"Raw Serper line {number} is not an object")
        rows.append(value)
    keys = [_text(row.get("query_attempt_id")) for row in rows]
    if any(not key for key in keys) or len(keys) != len(set(keys)):
        raise FullDiscoveryError("Raw Serper responses have blank or duplicate keys")
    return rows


def build_production_universe(
    unresolved: pd.DataFrame, prior_screen: pd.DataFrame, claims: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, object]]:
    required = {
        "candidate_year_id", "candidate_contest_id", "contest", "ledb_candid",
        "full_name", "geo_name", "state_abb", "year", "month", "period", "covered",
    }
    if missing := sorted(required - set(unresolved.columns)):
        raise FullDiscoveryError(f"Unresolved frame is missing columns: {missing}")
    if unresolved["candidate_year_id"].astype(str).duplicated().any():
        raise FullDiscoveryError("Unresolved frame has duplicate candidate-years")
    if not pd.to_numeric(unresolved["year"], errors="raise").between(2010, 2021).all():
        raise FullDiscoveryError("Unresolved frame is outside 2010--2021")
    covered = unresolved["covered"].astype(str).str.casefold().isin({"true", "1"})
    if covered.any():
        raise FullDiscoveryError("Unresolved frame contains covered candidates")
    prior_ids = set(prior_screen["candidate_year_id"].map(_text))
    active = claims.copy()
    for column in ("candidate_year_id", "url_type", "exact_normalized_url", "review_status"):
        if column not in active.columns:
            raise FullDiscoveryError(f"Claims table lacks {column}")
    active = active.loc[
        active["url_type"].map(_text).isin(ACTIVE_URL_TYPES)
        & active["exact_normalized_url"].map(_text).ne("")
        & ~active["review_status"].map(_text).str.casefold().isin({"rejected", "exclude"})
    ]
    active_ids = set(active["candidate_year_id"].map(_text))
    working = unresolved.loc[
        ~unresolved["candidate_year_id"].map(_text).isin(prior_ids | active_ids)
    ].copy()
    working["_selection_hash"] = working["candidate_year_id"].map(
        lambda value: hashlib.sha256(
            f"{CONTRACT_VERSION}\x1f{_text(value)}".encode("utf-8")
        ).hexdigest()
    )
    working = working.sort_values(["_selection_hash", "candidate_year_id"], kind="stable")
    working = working.reset_index(drop=True)
    working.insert(0, "production_order", range(1, len(working) + 1))
    audit = {
        "contract_version": CONTRACT_VERSION,
        "unresolved_rows": int(len(unresolved)),
        "prior_screen_overlap_rows": int(
            unresolved["candidate_year_id"].map(_text).isin(prior_ids).sum()
        ),
        "active_claim_overlap_rows": int(
            unresolved["candidate_year_id"].map(_text).isin(active_ids - prior_ids).sum()
        ),
        "production_rows": int(len(working)),
        "reduction_rate": float(1 - len(working) / len(unresolved)),
        "production_by_period": {
            str(key): int(value) for key, value in working.groupby("period").size().items()
        },
        "missing_election_month_rows": int(working["month"].isna().sum()),
    }
    return working.drop(columns="_selection_hash"), audit


def build_serper_manifest(universe: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for item in universe.head(SERPER_REQUEST_CEILING).to_dict("records"):
        query_name, repaired, method = repair_query_name(item["full_name"])
        query = primary_query({**item, "query_name": query_name})
        query_hash = hashlib.sha256(query.encode("utf-8")).hexdigest()
        attempt_hash = stable_hash(
            CONTRACT_VERSION, item["candidate_year_id"], item["candidate_contest_id"],
            query, QUERY_VERSION, "serper", "us", "en", SERPER_RESULTS_PER_QUERY,
        )
        rows.append({
            "contract_version": CONTRACT_VERSION,
            "query_version": QUERY_VERSION,
            "query_attempt_id": f"serper_query_{attempt_hash[:24]}",
            "query_attempt_hash": attempt_hash,
            "candidate_year_id": item["candidate_year_id"],
            "candidate_contest_id": item["candidate_contest_id"],
            "contest": item["contest"], "ledb_candid": item["ledb_candid"],
            "full_name": item["full_name"], "query_name": query_name,
            "query_name_repaired": repaired, "query_name_repair_method": method,
            "geo_name": item["geo_name"], "state_abb": item["state_abb"],
            "year": int(item["year"]), "month": item["month"],
            "sample_order": item["production_order"], "period": item["period"],
            "query_variant": "primary", "query": query, "query_sha256": query_hash,
            "provider": "serper", "country": "us", "language": "en",
            "results_requested": SERPER_RESULTS_PER_QUERY,
            "screen_sha256": "", "roster_sha256": "",
        })
    result = pd.DataFrame(rows).reindex(columns=QUERY_MANIFEST_COLUMNS)
    if result["query_attempt_id"].duplicated().any():
        raise FullDiscoveryError("Serper manifest has duplicate attempt IDs")
    return result


def prepare(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    unresolved_path: Path = DEFAULT_UNRESOLVED,
    prior_screen_path: Path = DEFAULT_PRIOR_SCREEN,
    claims_path: Path = DEFAULT_CLAIMS,
    roster_path: Path = DEFAULT_ROSTER,
) -> dict[str, object]:
    unresolved = pd.read_csv(unresolved_path, low_memory=False)
    prior = pd.read_csv(prior_screen_path, low_memory=False)
    claims = pd.read_csv(claims_path, low_memory=False)
    universe, audit = build_production_universe(unresolved, prior, claims)
    manifest = build_serper_manifest(universe)
    output_dir.mkdir(parents=True, exist_ok=True)
    _atomic_csv(universe, output_dir / "production_universe.csv")
    _atomic_csv(manifest, output_dir / "serper/query_manifest.csv", QUERY_MANIFEST_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=REQUEST_LEDGER_COLUMNS), output_dir / "serper/request_ledger.csv", REQUEST_LEDGER_COLUMNS)
    _atomic_jsonl([], output_dir / "serper/raw_responses.jsonl")
    _atomic_csv(pd.DataFrame(columns=RANKED_QUEUE_COLUMNS), output_dir / "serper/ranked_result_page_queue.csv", RANKED_QUEUE_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=URL_LEAD_COLUMNS), output_dir / "serper/direct_url_leads.csv", URL_LEAD_COLUMNS)
    audit.update({
        "unresolved_input_sha256": _sha256_file(unresolved_path),
        "prior_screen_sha256": _sha256_file(prior_screen_path),
        "claims_input_sha256": _sha256_file(claims_path),
        "roster_input_sha256": _sha256_file(roster_path),
        "production_universe_sha256": _sha256_file(output_dir / "production_universe.csv"),
        "serper_query_manifest_rows": len(manifest),
        "serper_query_manifest_sha256": _sha256_file(output_dir / "serper/query_manifest.csv"),
        "serper_request_ceiling": SERPER_REQUEST_CEILING,
        "openai_cost_ceiling_usd": OPENAI_COST_CEILING_USD,
        "stage": "prepared_no_paid_requests",
    })
    _atomic_json(audit, output_dir / "audit.json")
    return audit


def _load_roster(path: Path) -> pd.DataFrame:
    roster = pd.read_csv(path, low_memory=False)
    if roster["candidate_year_id"].astype(str).duplicated().any():
        raise FullDiscoveryError("Roster has duplicate candidate-years")
    return roster


def run_serper(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    roster_path: Path = DEFAULT_ROSTER,
    request_budget: int = SERPER_REQUEST_CEILING,
    timeout: int = 30,
    workers: int = 1,
    api_key: str | None = None,
    session: Any = requests,
) -> dict[str, object]:
    if workers < 1 or workers > 12:
        raise FullDiscoveryError("Serper workers must be between 1 and 12")
    manifest = pd.read_csv(output_dir / "serper/query_manifest.csv", keep_default_na=False)
    ledger_path = output_dir / "serper/request_ledger.csv"
    raw_path = output_dir / "serper/raw_responses.jsonl"
    ledger = pd.read_csv(ledger_path, keep_default_na=False)
    raw = _read_jsonl(raw_path)
    completed = set(ledger["query_attempt_id"].map(_text))
    key = api_key or load_serper_api_key()
    pending = [
        row for row in manifest.to_dict("records")
        if row["query_attempt_id"] not in completed
    ][:request_budget]
    requests_made = 0
    stop = False

    def issue(row: Mapping[str, object]) -> tuple[str, tuple[object, ...]]:
        request_session = session if session is not requests else requests.Session()
        result = request_serper_once(
            row["query"], key, results_requested=SERPER_RESULTS_PER_QUERY,
            timeout=timeout, session=request_session,
        )
        return datetime.now(timezone.utc).isoformat(), result

    for batch_start in range(0, len(pending), workers):
        batch = pending[batch_start:batch_start + workers]
        if workers == 1:
            results = [issue(batch[0])]
        else:
            with ThreadPoolExecutor(max_workers=workers) as executor:
                results = list(executor.map(issue, batch))
        for row, (attempted, result) in zip(batch, results, strict=True):
            status, http_status, payload, raw_body, response_hash, error = result
            organic = payload.get("organic", []) if isinstance(payload, Mapping) else []
            raw.append({
                "contract_version": CONTRACT_VERSION,
                "query_attempt_id": row["query_attempt_id"],
                "candidate_year_id": row["candidate_year_id"],
                "raw_response_sha256": response_hash,
                "raw_response_text": raw_body,
                "raw_response": payload,
            })
            ledger.loc[len(ledger)] = {
                "contract_version": CONTRACT_VERSION,
                "query_attempt_id": row["query_attempt_id"],
                "candidate_year_id": row["candidate_year_id"],
                "request_sequence": len(ledger) + 1,
                "query_sha256": row["query_sha256"], "provider": "serper",
                "endpoint": "https://google.serper.dev/search", "request_status": status,
                "request_sent": True, "terminal_for_run": True, "http_status": http_status,
                "results_returned": len(organic) if isinstance(organic, list) else 0,
                "raw_response_sha256": response_hash, "error": error,
                "attempted_at": attempted, "completed_at": datetime.now(timezone.utc).isoformat(),
                "retryable": status == "request_error", "complete_for_wave": status in {"found", "no_result"},
            }
            completed.add(row["query_attempt_id"])
            requests_made += 1
            stop = stop or status == "provider_error"
        _atomic_jsonl(raw, raw_path)
        _atomic_csv(ledger, ledger_path, REQUEST_LEDGER_COLUMNS)
        if stop:
            break
    roster = _load_roster(roster_path)
    queue = build_ranked_result_page_queue(manifest, raw, roster)
    empty_links = pd.DataFrame(columns=PAGE_LINK_COLUMNS)
    leads = build_url_leads(queue, empty_links, roster, set(manifest["candidate_year_id"].map(_text)))
    direct_ids = set(queue.loc[queue["result_classification"].eq("direct_candidate_site"), "result_claim_id"])
    if set(leads["result_claim_id"]) != direct_ids:
        raise FullDiscoveryError("Direct Serper result-to-lead agreement failed")
    _atomic_csv(queue, output_dir / "serper/ranked_result_page_queue.csv", RANKED_QUEUE_COLUMNS)
    _atomic_csv(leads, output_dir / "serper/direct_url_leads.csv", URL_LEAD_COLUMNS)
    audit = {
        "contract_version": CONTRACT_VERSION,
        "manifest_rows": len(manifest), "attempted_rows": len(ledger),
        "remaining_rows": len(manifest) - len(ledger),
        "request_status_counts": ledger["request_status"].value_counts().sort_index().astype(int).to_dict(),
        "organic_result_rows": len(queue), "direct_candidate_site_claims": len(leads),
        "direct_candidate_owners": int(leads["candidate_year_id"].nunique()) if len(leads) else 0,
        "stage": "complete" if len(ledger) == len(manifest) else "partial",
    }
    _atomic_json(audit, output_dir / "serper/audit.json")
    return audit


def prepare_openai(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    universe = pd.read_csv(output_dir / "production_universe.csv", low_memory=False)
    serper_leads = pd.read_csv(output_dir / "serper/direct_url_leads.csv", keep_default_na=False)
    direct_owners = set(serper_leads["candidate_year_id"].map(_text))
    residual = universe.loc[~universe["candidate_year_id"].map(_text).isin(direct_owners)].copy()
    residual = residual.sort_values("production_order", kind="stable").reset_index(drop=True)
    residual["parent_sample_order"] = residual["production_order"]
    residual.insert(0, "sample_order", range(1, len(residual) + 1))
    query_fields = residual["full_name"].map(repair_query_name)
    residual["query_full_name"] = query_fields.map(lambda value: value[0])
    residual["query_name_repaired"] = query_fields.map(lambda value: value[1])
    residual["query_name_repair_reason"] = query_fields.map(lambda value: value[2])
    residual["residual_period_population"] = residual.groupby("period")["candidate_year_id"].transform("size")
    residual["residual_inclusion_probability"] = 1.0
    residual["residual_population_weight"] = 1.0
    residual["prompt"] = residual.apply(openai_search._prompt, axis=1)
    residual["model"] = openai_search.MODEL
    residual["model_snapshot"] = openai_search.MODEL_SNAPSHOT
    residual["max_tool_calls"] = openai_search.MAX_TOOL_CALLS
    residual["max_output_tokens"] = openai_search.MAX_OUTPUT_TOKENS
    residual["search_context_size"] = "low"
    residual["sample_seed"] = ""
    residual["contract_version"] = OPENAI_CONTRACT_VERSION
    residual["attempt_id"] = residual.apply(
        lambda row: openai_search._stable_id(
            "openai_search_attempt", OPENAI_CONTRACT_VERSION, row["candidate_year_id"],
            row["prompt"], openai_search.MODEL_SNAPSHOT, openai_search.MAX_TOOL_CALLS,
            openai_search.MAX_OUTPUT_TOKENS, "low",
        ), axis=1,
    )
    manifest = residual.reindex(columns=openai_search.MANIFEST_COLUMNS)
    if manifest["candidate_year_id"].duplicated().any() or manifest["attempt_id"].duplicated().any():
        raise FullDiscoveryError("OpenAI residual manifest has duplicate keys")
    openai_dir = output_dir / "openai"
    _atomic_csv(manifest, openai_dir / "candidate_manifest.csv", openai_search.MANIFEST_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=openai_search.LEDGER_COLUMNS), openai_dir / "request_ledger.csv", openai_search.LEDGER_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=openai_search.SOURCE_COLUMNS), openai_dir / "source_urls.csv", openai_search.SOURCE_COLUMNS)
    (openai_dir / "raw_responses.jsonl").write_text("", encoding="utf-8")
    audit = {
        "contract_version": OPENAI_CONTRACT_VERSION,
        "production_universe_rows": len(universe),
        "serper_direct_candidate_owners": len(direct_owners),
        "openai_residual_rows": len(manifest),
        "cost_ceiling_usd": OPENAI_COST_CEILING_USD,
        "candidate_manifest_sha256": _sha256_file(openai_dir / "candidate_manifest.csv"),
        "stage": "prepared_no_paid_requests",
    }
    _atomic_json(audit, openai_dir / "audit.json")
    return audit


def run_openai(output_dir: Path = DEFAULT_OUTPUT_DIR, request_budget: int | None = None, timeout: int = 60) -> dict[str, object]:
    openai_dir = output_dir / "openai"
    manifest = pd.read_csv(openai_dir / "candidate_manifest.csv", keep_default_na=False)
    budget = len(manifest) if request_budget is None else request_budget
    return openai_search.run(
        openai_dir, request_budget=budget, timeout=timeout, max_requests=len(manifest),
        expected_rows=len(manifest), cost_ceiling_usd=OPENAI_COST_CEILING_USD,
        contract_version=OPENAI_CONTRACT_VERSION,
    )


def run_openai_parallel(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    request_budget: int | None = None,
    timeout: int = 60,
    workers: int = 6,
    api_key: str | None = None,
    session: Any = requests,
) -> dict[str, object]:
    """Run bounded OpenAI requests in batches with parent-only checkpoints."""
    if workers < 1 or workers > 24:
        raise FullDiscoveryError("OpenAI workers must be between 1 and 24")
    openai_dir = output_dir / "openai"
    manifest = pd.read_csv(openai_dir / "candidate_manifest.csv", keep_default_na=False)
    ledger_path = openai_dir / "request_ledger.csv"
    source_path = openai_dir / "source_urls.csv"
    raw_path = openai_dir / "raw_responses.jsonl"
    ledger = pd.read_csv(ledger_path, keep_default_na=False)
    sources = pd.read_csv(source_path, keep_default_na=False)
    raw = [
        json.loads(line)
        for line in raw_path.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    raw_keys = [_text(row.get("attempt_id")) for row in raw]
    if any(not key for key in raw_keys) or len(raw_keys) != len(set(raw_keys)):
        raise FullDiscoveryError("Raw OpenAI responses have blank or duplicate keys")
    completed = set(ledger["attempt_id"].map(_text))
    limit = len(manifest) if request_budget is None else request_budget
    if limit < 1 or limit > len(manifest):
        raise FullDiscoveryError("OpenAI request budget is outside the manifest")
    pending = [
        row for row in manifest.to_dict("records") if row["attempt_id"] not in completed
    ][:limit]
    key = api_key or openai_search.load_openai_api_key()
    if not key:
        raise FullDiscoveryError("OPENAI_API_KEY is not available")
    spent = float(pd.to_numeric(ledger["estimated_cost_usd"], errors="coerce").fillna(0).sum())

    def issue(row: Mapping[str, object]) -> dict[str, object]:
        client = session if session is not requests else requests.Session()
        attempted = datetime.now(timezone.utc).isoformat()
        body: dict[str, object] = {}
        raw_bytes = b""
        http_status: object = ""
        error = ""
        try:
            response = client.post(
                openai_search.API_URL,
                headers={"Authorization": f"Bearer {key}", "Content-Type": "application/json"},
                json=openai_search.request_payload(row, contract_version=OPENAI_CONTRACT_VERSION),
                timeout=timeout,
            )
            http_status = response.status_code
            raw_bytes = response.content
            if response.status_code != 200:
                status = "request_error"
                error = f"http_{response.status_code}: {_text(response.text)[:500]}"
            else:
                parsed = response.json()
                if not isinstance(parsed, dict):
                    raise FullDiscoveryError("OpenAI response is not a JSON object")
                body = parsed
                status = "found" if len(openai_search.extract_sources(body, row)) else "no_result"
        except (requests.RequestException, ValueError, FullDiscoveryError) as exc:
            status = "request_error"
            error = f"{type(exc).__name__}: {exc}"
        cost, web_calls, completed_calls = openai_search.estimated_cost(body)
        input_tokens, output_tokens, total_tokens = openai_search._usage(body)
        return {
            "row": dict(row), "attempted": attempted, "body": body,
            "raw_bytes": raw_bytes, "http_status": http_status, "error": error,
            "status": status, "cost": cost, "web_calls": web_calls,
            "completed_calls": completed_calls, "input_tokens": input_tokens,
            "output_tokens": output_tokens, "total_tokens": total_tokens,
        }

    cursor = 0
    while cursor < len(pending):
        affordable = int(
            max(0.0, OPENAI_COST_CEILING_USD - spent)
            // openai_search.MAX_NEXT_RESPONSE_COST
        )
        batch_size = min(workers, affordable, len(pending) - cursor)
        if batch_size < 1:
            break
        batch = pending[cursor:cursor + batch_size]
        if workers == 1:
            results = [issue(batch[0])]
        else:
            with ThreadPoolExecutor(max_workers=batch_size) as executor:
                results = list(executor.map(issue, batch))
        for result in results:
            row = result["row"]
            raw_bytes = result["raw_bytes"]
            raw_hash = hashlib.sha256(raw_bytes).hexdigest() if raw_bytes else ""
            raw.append({
                "attempt_id": row["attempt_id"],
                "candidate_year_id": row["candidate_year_id"],
                "raw_response_sha256": raw_hash,
                "raw_response_text": raw_bytes.decode("utf-8", errors="replace"),
            })
            new_sources = openai_search.extract_sources(result["body"], row)
            if len(new_sources):
                sources = pd.concat([sources, new_sources], ignore_index=True)
            ledger.loc[len(ledger)] = {
                "attempt_id": row["attempt_id"], "candidate_year_id": row["candidate_year_id"],
                "request_status": result["status"], "http_status": result["http_status"],
                "response_id": _text(result["body"].get("id", "")),
                "web_search_calls": result["web_calls"],
                "web_search_calls_completed": result["completed_calls"],
                "input_tokens": result["input_tokens"], "output_tokens": result["output_tokens"],
                "total_tokens": result["total_tokens"],
                "estimated_cost_usd": round(float(result["cost"]), 9),
                "raw_response_sha256": raw_hash,
                "retryable": result["status"] == "request_error", "error": result["error"],
                "attempted_at_utc": result["attempted"],
            }
            spent += float(result["cost"])
        if ledger["attempt_id"].duplicated().any() or sources["source_claim_id"].duplicated().any():
            raise FullDiscoveryError("OpenAI checkpoints contain duplicate keys")
        _atomic_jsonl(raw, raw_path)
        _atomic_csv(ledger, ledger_path, openai_search.LEDGER_COLUMNS)
        _atomic_csv(sources, source_path, openai_search.SOURCE_COLUMNS)
        if spent > OPENAI_COST_CEILING_USD + 1e-12:
            raise FullDiscoveryError("Realized OpenAI cost exceeded the approved ceiling")
        cursor += batch_size
    audit = {
        "contract_version": OPENAI_CONTRACT_VERSION,
        "candidate_manifest_rows": len(manifest), "attempted_rows": len(ledger),
        "remaining_rows": len(manifest) - len(ledger),
        "request_status_counts": ledger["request_status"].value_counts().sort_index().astype(int).to_dict(),
        "web_search_calls": int(pd.to_numeric(ledger["web_search_calls"], errors="coerce").fillna(0).sum()),
        "source_claim_rows": len(sources),
        "source_unique_urls": int(sources["raw_url"].nunique()) if len(sources) else 0,
        "estimated_cost_usd": round(spent, 9), "cost_ceiling_usd": OPENAI_COST_CEILING_USD,
        "cost_ceiling_respected": spent <= OPENAI_COST_CEILING_USD + 1e-12,
        "stage": "complete" if len(ledger) == len(manifest) else "partial",
    }
    _atomic_json(audit, openai_dir / "audit.json")
    return audit


def prepare_openai_parse_retries(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    openai_dir = output_dir / "openai"
    outputs = pd.read_csv(openai_dir / "extracted/assistant_outputs.csv", keep_default_na=False)
    manifest = pd.read_csv(openai_dir / "candidate_manifest.csv", keep_default_na=False)
    error_ids = set(outputs.loc[outputs["parse_status"].eq("parse_error"), "candidate_year_id"].map(_text))
    retry = manifest.loc[manifest["candidate_year_id"].map(_text).isin(error_ids)].copy()
    retry = retry.sort_values("sample_order", kind="stable").reset_index(drop=True)
    retry["sample_order"] = range(1, len(retry) + 1)
    retry["contract_version"] = OPENAI_RETRY_CONTRACT_VERSION
    retry["attempt_id"] = retry.apply(
        lambda row: openai_search._stable_id(
            "openai_search_retry", OPENAI_RETRY_CONTRACT_VERSION,
            row["candidate_year_id"], row["prompt"],
        ), axis=1,
    )
    if set(retry["candidate_year_id"].map(_text)) != error_ids:
        raise FullDiscoveryError("Parse-error retry manifest key agreement failed")
    retry_dir = openai_dir / "retry_parse_errors"
    _atomic_csv(retry, retry_dir / "candidate_manifest.csv", openai_search.MANIFEST_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=openai_search.LEDGER_COLUMNS), retry_dir / "request_ledger.csv", openai_search.LEDGER_COLUMNS)
    _atomic_csv(pd.DataFrame(columns=openai_search.SOURCE_COLUMNS), retry_dir / "source_urls.csv", openai_search.SOURCE_COLUMNS)
    (retry_dir / "raw_responses.jsonl").write_text("", encoding="utf-8")
    audit = {
        "contract_version": OPENAI_RETRY_CONTRACT_VERSION,
        "retry_rows": len(retry), "retry_candidate_year_ids": sorted(error_ids),
        "cost_ceiling_usd": OPENAI_RETRY_COST_CEILING_USD,
        "stage": "prepared_no_paid_requests",
    }
    _atomic_json(audit, retry_dir / "audit.json")
    return audit


def run_openai_parse_retries(output_dir: Path = DEFAULT_OUTPUT_DIR, timeout: int = 60) -> dict[str, object]:
    retry_dir = output_dir / "openai/retry_parse_errors"
    manifest = pd.read_csv(retry_dir / "candidate_manifest.csv", keep_default_na=False)
    if manifest.empty:
        return {"retry_rows": 0, "stage": "complete"}
    return openai_search.run(
        retry_dir, request_budget=len(manifest), timeout=timeout,
        max_requests=len(manifest), expected_rows=len(manifest),
        cost_ceiling_usd=OPENAI_RETRY_COST_CEILING_USD,
        contract_version=OPENAI_RETRY_CONTRACT_VERSION,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "command",
        choices=[
            "prepare", "check-keys", "serper", "prepare-openai", "openai",
            "prepare-openai-retry", "openai-retry",
        ],
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--request-budget", type=int)
    parser.add_argument("--timeout", type=int, default=60)
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args()
    if args.command == "prepare":
        result = prepare(args.output_dir)
    elif args.command == "check-keys":
        result = {
            "serper_api_key_available": bool(load_serper_api_key()),
            "openai_api_key_available": bool(openai_search.load_openai_api_key()),
        }
    elif args.command == "serper":
        result = run_serper(
            args.output_dir,
            request_budget=args.request_budget or SERPER_REQUEST_CEILING,
            timeout=args.timeout,
            workers=args.workers,
        )
    elif args.command == "prepare-openai":
        result = prepare_openai(args.output_dir)
    elif args.command == "prepare-openai-retry":
        result = prepare_openai_parse_retries(args.output_dir)
    elif args.command == "openai-retry":
        result = run_openai_parse_retries(args.output_dir, timeout=args.timeout)
    else:
        result = run_openai_parallel(
            args.output_dir,
            request_budget=args.request_budget,
            timeout=args.timeout,
            workers=args.workers,
        )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
