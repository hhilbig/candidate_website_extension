#!/usr/bin/env python3
"""Run the approved OpenAI web-search pilot on residual mayoral candidates.

The pilot draws 100 candidates from the fixed 480-case screen after excluding
candidate owners that already have a high-precision Serper temporal-validation
lead.  Each candidate receives at most one Responses API request and one web
search call.  Full provider responses, source URLs, token usage, and estimated
costs are checkpointed without exposing the API key.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

import pandas as pd
import requests

from scripts.run_mayoral_search_source_expansion_2010_2021 import (
    candidate_identity_clause,
    repair_query_name,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
SCREEN_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_SCREEN = SCREEN_DIR / "candidate_screening_sample_2010_2021.csv"
DEFAULT_SERPER_ELIGIBLE = (
    SCREEN_DIR
    / "search_source_expansion/temporal_validation/frozen/eligible_leads.csv"
)
DEFAULT_OUTPUT_DIR = SCREEN_DIR / "openai_search_residual_pilot" / "production"
API_URL = "https://api.openai.com/v1/responses"
CONTRACT_VERSION = "openai-web-search-residual-pilot-v1"
MODEL = "gpt-5.4-nano"
MODEL_SNAPSHOT = "gpt-5.4-nano-2026-03-17"
SAMPLE_SEED = 20260825
SAMPLE_SIZE = 100
PERIOD_QUOTAS = {"2010_2013": 34, "2014_2017": 33, "2018_2021": 33}
MAX_TOOL_CALLS = 1
MAX_OUTPUT_TOKENS = 800
MAX_REQUESTS = 100
COST_CEILING_USD = 2.0
WEB_SEARCH_COST = 0.01
INPUT_COST_PER_TOKEN = 0.20 / 1_000_000
OUTPUT_COST_PER_TOKEN = 1.25 / 1_000_000
MAX_MODEL_INPUT_TOKENS = 400_000
MAX_BILLABLE_SEARCH_CALLS_PER_RESPONSE_GUARD = 2
MAX_NEXT_RESPONSE_COST = (
    MAX_BILLABLE_SEARCH_CALLS_PER_RESPONSE_GUARD * WEB_SEARCH_COST
    + MAX_MODEL_INPUT_TOKENS * INPUT_COST_PER_TOKEN
    + MAX_OUTPUT_TOKENS * OUTPUT_COST_PER_TOKEN
)

MANIFEST_COLUMNS = [
    "sample_order",
    "parent_sample_order",
    "attempt_id",
    "candidate_year_id",
    "candidate_contest_id",
    "full_name",
    "query_full_name",
    "query_name_repaired",
    "query_name_repair_reason",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "period",
    "residual_period_population",
    "residual_inclusion_probability",
    "residual_population_weight",
    "prompt",
    "model",
    "model_snapshot",
    "max_tool_calls",
    "max_output_tokens",
    "search_context_size",
    "sample_seed",
    "contract_version",
]
LEDGER_COLUMNS = [
    "attempt_id",
    "candidate_year_id",
    "request_status",
    "http_status",
    "response_id",
    "web_search_calls",
    "web_search_calls_completed",
    "input_tokens",
    "output_tokens",
    "total_tokens",
    "estimated_cost_usd",
    "raw_response_sha256",
    "retryable",
    "error",
    "attempted_at_utc",
]
SOURCE_COLUMNS = [
    "source_claim_id",
    "attempt_id",
    "candidate_year_id",
    "source_origin",
    "source_position",
    "raw_url",
    "title",
    "source_type",
    "response_id",
]


class OpenAISearchPilotError(ValueError):
    """The residual pilot contract or checkpoint is invalid."""


def _text(value: object) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip()


def _sha256_bytes(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    return _sha256_bytes(path.read_bytes())


def _stable_id(prefix: str, *values: object) -> str:
    payload = "\x1f".join(_text(value) for value in values)
    return f"{prefix}_{hashlib.sha256(payload.encode('utf-8')).hexdigest()[:24]}"


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", newline="", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False, lineterminator="\n")
    temporary.replace(path)


def _atomic_text(text: str, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", encoding="utf-8", dir=path.parent, delete=False
    ) as handle:
        temporary = Path(handle.name)
        handle.write(text)
    temporary.replace(path)


def _prompt(row: Mapping[str, object]) -> str:
    identity = candidate_identity_clause(row["full_name"])
    city = _text(row["geo_name"])
    state = _text(row["state_abb"])
    year = int(row["year"])
    return (
        "Invoke the web search tool exactly once; do not issue a second search. "
        f"Search for the historical mayoral campaign website for {identity}, "
        f"a candidate for mayor of \"{city}\", {state}, in {year}. "
        "Find exact candidate-owned campaign URLs and contemporaneous pages that "
        "explicitly identify the website. Exclude social media, fundraising pages, "
        "government profiles, and unrelated candidates. Return compact JSON with "
        "keys candidate, campaign_urls, evidence_urls, and notes. Preserve exact URL "
        "case and paths. Do not invent a URL."
    )


def build_candidate_manifest(
    screen: pd.DataFrame,
    serper_eligible: pd.DataFrame,
    *,
    seed: int = SAMPLE_SEED,
) -> tuple[pd.DataFrame, dict[str, object]]:
    screen = screen.copy()
    if "sample_order" in screen.columns:
        if "parent_sample_order" in screen.columns:
            raise OpenAISearchPilotError(
                "screen contains both sample_order and parent_sample_order"
            )
        screen = screen.rename(columns={"sample_order": "parent_sample_order"})
    required_screen = {
        "candidate_year_id",
        "candidate_contest_id",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "period",
    }
    missing = sorted(required_screen - set(screen.columns))
    if missing:
        raise OpenAISearchPilotError(f"screen is missing columns: {missing}")
    if "candidate_year_id" not in serper_eligible.columns:
        raise OpenAISearchPilotError("Serper eligible leads lack candidate_year_id")
    if len(screen) != 480 or screen["candidate_year_id"].astype(str).duplicated().any():
        raise OpenAISearchPilotError("screen must contain 480 unique candidate-years")

    previous_owner_ids = set(serper_eligible["candidate_year_id"].map(_text))
    working = screen.loc[
        ~screen["candidate_year_id"].map(_text).isin(previous_owner_ids)
    ].copy()
    records: list[pd.DataFrame] = []
    population_by_period: dict[str, int] = {}
    for period, quota in PERIOD_QUOTAS.items():
        pool = working.loc[working["period"].map(_text).eq(period)].copy()
        population_by_period[period] = len(pool)
        if len(pool) < quota:
            raise OpenAISearchPilotError(
                f"residual period {period} has {len(pool)} candidates for quota {quota}"
            )
        pool["_selection_hash"] = pool["candidate_year_id"].map(
            lambda value: hashlib.sha256(
                f"{seed}\x1f{_text(value)}".encode("utf-8")
            ).hexdigest()
        )
        selected = pool.sort_values(["_selection_hash", "candidate_year_id"]).head(quota)
        selected = selected.copy()
        selected["residual_period_population"] = len(pool)
        selected["residual_inclusion_probability"] = quota / len(pool)
        selected["residual_population_weight"] = len(pool) / quota
        records.append(selected)
    selected = pd.concat(records, ignore_index=True)
    selected = selected.sort_values(["period", "_selection_hash", "candidate_year_id"])
    selected = selected.reset_index(drop=True)
    selected.insert(0, "sample_order", range(1, len(selected) + 1))

    query_fields = selected["full_name"].map(repair_query_name)
    selected["query_full_name"] = query_fields.map(lambda value: value[0])
    selected["query_name_repaired"] = query_fields.map(lambda value: value[1])
    selected["query_name_repair_reason"] = query_fields.map(lambda value: value[2])
    selected["prompt"] = selected.apply(_prompt, axis=1)
    selected["model"] = MODEL
    selected["model_snapshot"] = MODEL_SNAPSHOT
    selected["max_tool_calls"] = MAX_TOOL_CALLS
    selected["max_output_tokens"] = MAX_OUTPUT_TOKENS
    selected["search_context_size"] = "low"
    selected["sample_seed"] = seed
    selected["contract_version"] = CONTRACT_VERSION
    selected["attempt_id"] = selected.apply(
        lambda row: _stable_id(
            "openai_search_attempt",
            CONTRACT_VERSION,
            row["candidate_year_id"],
            row["prompt"],
            MODEL_SNAPSHOT,
            MAX_TOOL_CALLS,
            MAX_OUTPUT_TOKENS,
            "low",
        ),
        axis=1,
    )
    result = selected.reindex(columns=MANIFEST_COLUMNS)
    if len(result) != SAMPLE_SIZE or result["attempt_id"].duplicated().any():
        raise AssertionError("residual manifest does not have 100 unique attempts")
    audit = {
        "contract_version": CONTRACT_VERSION,
        "screen_rows": int(len(screen)),
        "serper_eligible_owner_ids": int(
            screen["candidate_year_id"].map(_text).isin(previous_owner_ids).sum()
        ),
        "residual_rows": int(len(working)),
        "residual_by_period": population_by_period,
        "sample_rows": int(len(result)),
        "residual_rows_not_sampled": int(len(working) - len(result)),
        "residual_row_reduction_rate": float(1 - len(result) / len(working)),
        "residual_row_reduction_explanation": (
            "Expected budget-driven reduction under the approved 100-request pilot cap; "
            "not a cleaning or eligibility drop."
        ),
        "sample_by_period": {
            key: int(value) for key, value in result.groupby("period").size().items()
        },
        "sample_seed": seed,
        "no_serper_eligible_owner_overlap": not result["candidate_year_id"]
        .map(_text)
        .isin(previous_owner_ids)
        .any(),
    }
    return result, audit


def prepare(output_dir: Path = DEFAULT_OUTPUT_DIR) -> dict[str, object]:
    screen = pd.read_csv(DEFAULT_SCREEN, low_memory=False)
    eligible = pd.read_csv(DEFAULT_SERPER_ELIGIBLE, low_memory=False)
    manifest, audit = build_candidate_manifest(screen, eligible)
    output_dir.mkdir(parents=True, exist_ok=True)
    manifest_path = output_dir / "candidate_manifest.csv"
    _atomic_csv(manifest, manifest_path)
    for path, columns in (
        (output_dir / "request_ledger.csv", LEDGER_COLUMNS),
        (output_dir / "source_urls.csv", SOURCE_COLUMNS),
    ):
        if not path.exists():
            _atomic_csv(pd.DataFrame(columns=columns), path)
    raw_path = output_dir / "raw_responses.jsonl"
    if not raw_path.exists():
        _atomic_text("", raw_path)
    audit.update(
        {
            "candidate_manifest_sha256": _sha256_file(manifest_path),
            "candidate_manifest_rows": len(manifest),
            "model": MODEL,
            "model_snapshot": MODEL_SNAPSHOT,
            "max_requests": MAX_REQUESTS,
            "max_tool_calls_per_response": MAX_TOOL_CALLS,
            "cost_ceiling_usd": COST_CEILING_USD,
            "maximum_next_response_cost_usd": MAX_NEXT_RESPONSE_COST,
            "stage": "prepared_no_paid_requests",
        }
    )
    _atomic_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", output_dir / "audit.json")
    return audit


def load_openai_api_key(
    *, environ: Mapping[str, str] | None = None, env_path: Path | None = None
) -> str:
    environ = os.environ if environ is None else environ
    value = _text(environ.get("OPENAI_API_KEY", ""))
    if value:
        return value
    path = REPO_ROOT / ".env" if env_path is None else Path(env_path)
    if not path.is_file():
        return ""
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, raw = stripped.split("=", 1)
        if key.strip() == "OPENAI_API_KEY":
            return raw.strip().strip("'\"")
    return ""


def request_payload(
    row: Mapping[str, object],
    *,
    contract_version: str = CONTRACT_VERSION,
) -> dict[str, object]:
    return {
        "model": MODEL_SNAPSHOT,
        "tools": [{"type": "web_search", "search_context_size": "low"}],
        "tool_choice": "required",
        "parallel_tool_calls": False,
        "max_tool_calls": MAX_TOOL_CALLS,
        "max_output_tokens": MAX_OUTPUT_TOKENS,
        "reasoning": {"effort": "none"},
        "include": ["web_search_call.action.sources"],
        "store": False,
        "input": _text(row["prompt"]),
        "metadata": {
            "contract": contract_version,
            "attempt_id": _text(row["attempt_id"]),
        },
    }


def _usage(response: Mapping[str, object]) -> tuple[int, int, int]:
    usage = response.get("usage", {})
    usage = usage if isinstance(usage, Mapping) else {}
    input_tokens = int(usage.get("input_tokens", 0) or 0)
    output_tokens = int(usage.get("output_tokens", 0) or 0)
    total_tokens = int(usage.get("total_tokens", input_tokens + output_tokens) or 0)
    return input_tokens, output_tokens, total_tokens


def estimated_cost(response: Mapping[str, object]) -> tuple[float, int, int]:
    input_tokens, output_tokens, _ = _usage(response)
    calls = sum(
        1
        for item in response.get("output", [])
        if isinstance(item, Mapping) and item.get("type") == "web_search_call"
    )
    completed_calls = sum(
        1
        for item in response.get("output", [])
        if isinstance(item, Mapping)
        and item.get("type") == "web_search_call"
        and item.get("status") == "completed"
    )
    return (
        calls * WEB_SEARCH_COST
        + input_tokens * INPUT_COST_PER_TOKEN
        + output_tokens * OUTPUT_COST_PER_TOKEN,
        calls,
        completed_calls,
    )


def extract_sources(response: Mapping[str, object], row: Mapping[str, object]) -> pd.DataFrame:
    records: list[dict[str, object]] = []
    response_id = _text(response.get("id", ""))
    position = 0
    for item in response.get("output", []):
        if not isinstance(item, Mapping):
            continue
        if item.get("type") == "web_search_call":
            action = item.get("action", {})
            action = action if isinstance(action, Mapping) else {}
            for source in action.get("sources", []) or []:
                if not isinstance(source, Mapping) or not _text(source.get("url")):
                    continue
                position += 1
                records.append(
                    {
                        "attempt_id": row["attempt_id"],
                        "candidate_year_id": row["candidate_year_id"],
                        "source_origin": "web_search_action_source",
                        "source_position": position,
                        "raw_url": _text(source.get("url")),
                        "title": _text(source.get("title")),
                        "source_type": _text(source.get("type")),
                        "response_id": response_id,
                    }
                )
        if item.get("type") == "message":
            for content in item.get("content", []) or []:
                if not isinstance(content, Mapping):
                    continue
                for annotation in content.get("annotations", []) or []:
                    if not isinstance(annotation, Mapping):
                        continue
                    url = _text(annotation.get("url"))
                    if not url:
                        continue
                    position += 1
                    records.append(
                        {
                            "attempt_id": row["attempt_id"],
                            "candidate_year_id": row["candidate_year_id"],
                            "source_origin": "message_url_citation",
                            "source_position": position,
                            "raw_url": url,
                            "title": _text(annotation.get("title")),
                            "source_type": _text(annotation.get("type")),
                            "response_id": response_id,
                        }
                    )
    for record in records:
        record["source_claim_id"] = _stable_id(
            "openai_source_claim",
            record["attempt_id"],
            record["source_origin"],
            record["source_position"],
            record["raw_url"],
        )
    return pd.DataFrame(records, columns=SOURCE_COLUMNS)


def _append_raw(path: Path, wrapper: dict[str, object]) -> None:
    existing = path.read_text(encoding="utf-8") if path.exists() else ""
    line = json.dumps(wrapper, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    _atomic_text(existing + line + "\n", path)


def run(
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    *,
    request_budget: int = MAX_REQUESTS,
    timeout: int = 60,
    session: requests.Session | None = None,
    api_key: str | None = None,
    max_requests: int = MAX_REQUESTS,
    expected_rows: int = SAMPLE_SIZE,
    cost_ceiling_usd: float = COST_CEILING_USD,
    contract_version: str = CONTRACT_VERSION,
) -> dict[str, object]:
    if request_budget < 1 or request_budget > max_requests:
        raise OpenAISearchPilotError(
            f"request budget must be between 1 and {max_requests}"
        )
    manifest = pd.read_csv(output_dir / "candidate_manifest.csv", keep_default_na=False)
    if manifest.columns.tolist() != MANIFEST_COLUMNS or len(manifest) != expected_rows:
        raise OpenAISearchPilotError("candidate manifest differs from the frozen contract")
    ledger_path = output_dir / "request_ledger.csv"
    source_path = output_dir / "source_urls.csv"
    raw_path = output_dir / "raw_responses.jsonl"
    ledger = pd.read_csv(ledger_path, keep_default_na=False)
    if ledger.columns.tolist() != LEDGER_COLUMNS or ledger["attempt_id"].duplicated().any():
        raise OpenAISearchPilotError("request ledger schema or keys differ")
    sources = pd.read_csv(source_path, keep_default_na=False)
    if sources.columns.tolist() != SOURCE_COLUMNS or sources["source_claim_id"].duplicated().any():
        raise OpenAISearchPilotError("source URL schema or keys differ")
    key = api_key if api_key is not None else load_openai_api_key()
    if not key:
        raise OpenAISearchPilotError("OPENAI_API_KEY is not available")
    client = requests.Session() if session is None else session
    completed = set(ledger["attempt_id"].map(_text))
    spent = float(pd.to_numeric(ledger["estimated_cost_usd"], errors="coerce").fillna(0).sum())
    requests_made = 0
    for row in manifest.to_dict("records"):
        attempt_id = _text(row["attempt_id"])
        if attempt_id in completed or requests_made >= request_budget:
            continue
        if spent + MAX_NEXT_RESPONSE_COST > cost_ceiling_usd + 1e-12:
            break
        attempted_at = datetime.now(timezone.utc).isoformat()
        response_body: dict[str, object] = {}
        raw_bytes = b""
        http_status: object = ""
        error = ""
        try:
            response = client.post(
                API_URL,
                headers={
                    "Authorization": f"Bearer {key}",
                    "Content-Type": "application/json",
                },
                json=request_payload(row, contract_version=contract_version),
                timeout=timeout,
            )
            http_status = response.status_code
            raw_bytes = response.content
            if response.status_code != 200:
                error = f"http_{response.status_code}: {_text(response.text)[:500]}"
                status = "request_error"
            else:
                parsed = response.json()
                if not isinstance(parsed, dict):
                    raise OpenAISearchPilotError("OpenAI response is not a JSON object")
                response_body = parsed
                status = "found" if len(extract_sources(parsed, row)) else "no_result"
        except (requests.RequestException, ValueError, OpenAISearchPilotError) as exc:
            error = f"{type(exc).__name__}: {exc}"
            status = "request_error"
        input_tokens, output_tokens, total_tokens = _usage(response_body)
        cost, web_calls, completed_web_calls = estimated_cost(response_body)
        spent += cost
        raw_hash = _sha256_bytes(raw_bytes) if raw_bytes else ""
        wrapper = {
            "attempt_id": attempt_id,
            "candidate_year_id": _text(row["candidate_year_id"]),
            "raw_response_sha256": raw_hash,
            "raw_response_text": raw_bytes.decode("utf-8", errors="replace"),
        }
        _append_raw(raw_path, wrapper)
        new_sources = extract_sources(response_body, row)
        if len(new_sources):
            sources = pd.concat([sources, new_sources], ignore_index=True)
            if sources["source_claim_id"].duplicated().any():
                raise OpenAISearchPilotError("source claim IDs are not unique")
            _atomic_csv(sources.reindex(columns=SOURCE_COLUMNS), source_path)
        ledger.loc[len(ledger)] = {
            "attempt_id": attempt_id,
            "candidate_year_id": row["candidate_year_id"],
            "request_status": status,
            "http_status": http_status,
            "response_id": _text(response_body.get("id", "")),
            "web_search_calls": web_calls,
            "web_search_calls_completed": completed_web_calls,
            "input_tokens": input_tokens,
            "output_tokens": output_tokens,
            "total_tokens": total_tokens,
            "estimated_cost_usd": round(cost, 9),
            "raw_response_sha256": raw_hash,
            "retryable": status == "request_error",
            "error": error,
            "attempted_at_utc": attempted_at,
        }
        _atomic_csv(ledger.reindex(columns=LEDGER_COLUMNS), ledger_path)
        completed.add(attempt_id)
        requests_made += 1
        if spent > cost_ceiling_usd + 1e-12:
            raise OpenAISearchPilotError("realized pilot cost exceeded the approved ceiling")
    audit = {
        "contract_version": contract_version,
        "candidate_manifest_rows": len(manifest),
        "attempted_rows": len(ledger),
        "remaining_rows": len(manifest) - len(ledger),
        "request_status_counts": {
            str(key): int(value)
            for key, value in ledger["request_status"].value_counts().sort_index().items()
        },
        "web_search_calls": int(pd.to_numeric(ledger["web_search_calls"], errors="coerce").fillna(0).sum()),
        "web_search_calls_completed": int(
            pd.to_numeric(ledger["web_search_calls_completed"], errors="coerce")
            .fillna(0)
            .sum()
        ),
        "source_claim_rows": len(sources),
        "source_unique_urls": int(sources["raw_url"].nunique()) if len(sources) else 0,
        "estimated_cost_usd": round(float(spent), 9),
        "cost_ceiling_usd": cost_ceiling_usd,
        "cost_ceiling_respected": spent <= cost_ceiling_usd + 1e-12,
        "max_tool_calls_per_response": MAX_TOOL_CALLS,
        "stage": "complete" if len(ledger) == len(manifest) else "partial",
    }
    _atomic_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", output_dir / "audit.json")
    return audit


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prepare_parser = subparsers.add_parser("prepare")
    prepare_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    key_parser = subparsers.add_parser("check-key")
    key_parser.add_argument("--env-path", type=Path, default=REPO_ROOT / ".env")
    run_parser = subparsers.add_parser("run")
    run_parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    run_parser.add_argument("--request-budget", type=int, default=MAX_REQUESTS)
    run_parser.add_argument("--timeout", type=int, default=60)
    args = parser.parse_args()
    if args.command == "prepare":
        print(json.dumps(prepare(args.output_dir), indent=2, sort_keys=True))
    elif args.command == "check-key":
        print(json.dumps({"openai_api_key_available": bool(load_openai_api_key(env_path=args.env_path))}))
    else:
        print(
            json.dumps(
                run(args.output_dir, request_budget=args.request_budget, timeout=args.timeout),
                indent=2,
                sort_keys=True,
            )
        )


if __name__ == "__main__":
    main()
