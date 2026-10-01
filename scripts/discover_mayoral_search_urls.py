#!/usr/bin/env python3
"""Prepare and retrieve search-engine leads for unresolved mayoral candidates.

Search results are leads, not validated campaign URLs. Paid retrieval requires an
explicit cost ceiling at least as large as the script's worst-case estimate.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd
import requests

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mayoral_discovery import canonical_http_url

PROVIDER_COST_PER_1000 = {"brave": 5.0, "serper": 1.0}
PERIODS = [(2000, 2007, "2000_2007"), (2008, 2014, "2008_2014"), (2015, 2021, "2015_2021")]
EXCLUDED_LEAD_HOSTS = {
    "ballotpedia.org", "facebook.com", "instagram.com", "linkedin.com",
    "soft112.com", "twitter.com", "wikipedia.org", "x.com", "youtube.com",
}
URL_IN_TEXT_RE = re.compile(r"(?:https?://|www\.)[^\s<>()\[\]{},;]+", re.I)
CAMPAIGN_TERMS = ("campaign website", "campaign site", "for mayor", "vote ")
WEBSITE_EVIDENCE_TERMS = ("campaign website", "campaign site", "website", "visit ", "learn more")
OTHER_OFFICE_TERMS = ("congress", "senate", "governor", "secretary of state", "president")


class SearchConfigurationError(RuntimeError):
    """Non-retryable provider error such as missing credits or an invalid key."""


def stable_rank(value: str) -> str:
    return hashlib.sha256(str(value).encode()).hexdigest()


def query_variants(row: pd.Series) -> list[dict[str, str]]:
    name = str(row["full_name"]).translate(str.maketrans({
        '"': "", "“": "", "”": "", "‘": "'", "’": "'",
    })).strip()
    city = str(row["geo_name"]).strip()
    state = str(row["state_abb"]).strip()
    year = int(row["year"])
    return [
        {
            "query_variant": "campaign_website_year",
            "query": f'"{name}" mayor "{city}" {state} {year} campaign website',
        },
        {
            "query_variant": "for_mayor_phrase",
            "query": f'"{name} for mayor" "{city}" {year}',
        },
    ]


def prepare_manifest(summary: pd.DataFrame, sample_per_stratum: int, variants: int) -> pd.DataFrame:
    context_columns = ["candidate_contest_id", "contest", "month"]
    missing = [column for column in context_columns if column not in summary.columns]
    if missing:
        raise ValueError(
            "Candidate summary is missing election context columns: "
            + ", ".join(missing)
        )
    unresolved = summary.loc[~summary["has_validated_campaign_url"].fillna(False)].copy()
    unresolved["profile_stratum"] = unresolved["ballotpedia_url"].notna().map(
        {True: "profile", False: "no_profile"}
    )
    unresolved["period_stratum"] = "outside_period"
    for start, end, label in PERIODS:
        unresolved.loc[unresolved["year"].between(start, end), "period_stratum"] = label
    unresolved = unresolved[unresolved["period_stratum"].ne("outside_period")].copy()
    unresolved["sample_rank"] = unresolved["candidate_year_id"].map(stable_rank)
    selected = (
        unresolved.sort_values("sample_rank")
        .groupby(["period_stratum", "profile_stratum"], group_keys=False)
        .head(sample_per_stratum)
    )
    rows = []
    for _, candidate in selected.sort_values(["period_stratum", "profile_stratum", "sample_rank"]).iterrows():
        for item in query_variants(candidate)[:variants]:
            rows.append({
                "candidate_year_id": candidate["candidate_year_id"],
                "candidate_contest_id": candidate["candidate_contest_id"],
                "contest": candidate["contest"],
                "full_name": candidate["full_name"],
                "geo_name": candidate["geo_name"],
                "state_abb": candidate["state_abb"],
                "year": int(candidate["year"]),
                "month": int(candidate["month"]) if pd.notna(candidate["month"]) else pd.NA,
                "period_stratum": candidate["period_stratum"],
                "profile_stratum": candidate["profile_stratum"],
                **item,
            })
    return pd.DataFrame(rows)


def normalized_host(url: str) -> str:
    value = str(url).strip()
    if value.casefold().startswith("www."):
        value = f"http://{value}"
    value = canonical_http_url(value)
    host = urlparse(value).netloc.casefold().split(":")[0]
    return host[4:] if host.startswith("www.") else host


def is_candidate_lead(url: str) -> bool:
    host = normalized_host(url)
    return bool(host) and not any(host == x or host.endswith(f".{x}") for x in EXCLUDED_LEAD_HOSTS)


def is_candidate_shaped_domain(url: str, full_name: str, city: str) -> bool:
    """Require candidate identity plus a campaign/place marker in the hostname."""
    name_parts = re.findall(r"[a-z0-9]+", str(full_name).casefold())
    if not name_parts:
        return False
    first_name, surname = name_parts[0], name_parts[-1]
    host = normalized_host(url)
    if any(term.replace(" ", "") in host for term in OTHER_OFFICE_TERMS):
        return False
    city_token = re.sub(r"[^a-z0-9]", "", str(city).casefold())
    name_match = surname in host or (len(first_name) >= 4 and first_name in host)
    campaign_match = any(term and term in host for term in (
        "mayor", "vote", "elect", "campaign", "for", city_token,
    ))
    return is_candidate_lead(url) and name_match and campaign_match


def result_evidence(item: dict, row: dict) -> dict:
    """Classify a search hit without mistaking an evidence page for the campaign site."""
    url = str(item.get("url", ""))
    host = normalized_host(url)
    raw_text = " ".join(str(item.get(x, "")) for x in ("title", "snippet", "url"))
    text = raw_text.casefold()
    name_parts = re.findall(r"[a-z0-9]+", str(row.get("full_name", "")).casefold())
    surname = name_parts[-1] if name_parts else ""
    first_name = name_parts[0] if name_parts else ""
    city = str(row.get("geo_name", "")).casefold()
    year = str(int(row["year"])) if pd.notna(row.get("year")) else ""
    direct = is_candidate_lead(url) and bool(surname) and surname in host and (
        "mayor" in host or "vote" in host or "elect" in host
    )
    embedded_urls = [x.rstrip(".!?:;\"'") for x in URL_IN_TEXT_RE.findall(raw_text)]
    candidate_embedded_urls = [
        x for x in embedded_urls
        if is_candidate_shaped_domain(x, row.get("full_name", ""), city)
    ]
    candidate_embedded_urls = list(dict.fromkeys(candidate_embedded_urls))
    campaign_language = any(term in text for term in CAMPAIGN_TERMS)
    identity_context = surname in text and (city in text or "mayor" in text)
    exact_year_context = bool(year and year in text)
    mentioned_years = sorted(set(re.findall(r"\b(?:19|20)\d{2}\b", text)))
    temporal_conflict = bool(mentioned_years and year not in mentioned_years)
    office_mismatch_signal = "mayor" not in text and any(term in text for term in OTHER_OFFICE_TERMS)
    embedded_campaign_context = campaign_language or "campaign" in text or "visit " in text
    explicit_website_language = any(term in text for term in WEBSITE_EVIDENCE_TERMS)
    if direct:
        evidence_type = "candidate_domain"
    elif candidate_embedded_urls and embedded_campaign_context and identity_context:
        evidence_type = "embedded_domain_evidence"
    elif explicit_website_language and identity_context:
        evidence_type = "source_evidence"
    elif identity_context:
        evidence_type = "generic_identity_result"
    else:
        evidence_type = "noise"
    return {
        "candidate_lead": direct,
        "evidence_type": evidence_type,
        "embedded_urls": candidate_embedded_urls,
        "exact_year_context": exact_year_context,
        "mentioned_years": mentioned_years,
        "temporal_conflict": temporal_conflict,
        "office_mismatch_signal": office_mismatch_signal,
    }


def results_warrant_fallback(results: list[dict]) -> bool:
    decisive = {"candidate_domain", "embedded_domain_evidence"}
    return not any(
        item.get("evidence_type") in decisive
        and not item.get("temporal_conflict", False)
        and not item.get("office_mismatch_signal", False)
        for item in results
    )


LEAD_QUEUE_COLUMNS = [
    "candidate_year_id", "candidate_contest_id", "contest", "full_name",
    "geo_name", "state_abb", "year", "month",
    "query_variant", "result_rank", "proposed_campaign_url", "evidence_source_url",
    "evidence_type", "exact_year_context", "result_title", "result_snippet",
    "mentioned_years", "temporal_conflict", "office_mismatch_signal",
    "review_status", "review_note",
]


def build_lead_queue(records: list[dict]) -> pd.DataFrame:
    """Flatten raw search results into a conservative, auditable review queue."""
    rows = []
    for record in records:
        for item in record.get("results", []):
            rescored = {**item, **result_evidence(item, record)}
            evidence_type = rescored.get("evidence_type", "noise")
            proposed = []
            if evidence_type == "candidate_domain":
                direct_url = rescored.get("url", "")
                proposed = [canonical_http_url(direct_url) or direct_url]
            elif evidence_type == "embedded_domain_evidence":
                proposed = rescored.get("embedded_urls", [])
            elif evidence_type == "source_evidence":
                proposed = [""]
            for url in proposed:
                rows.append({
                    "candidate_year_id": record["candidate_year_id"],
                    "candidate_contest_id": record.get("candidate_contest_id", ""),
                    "contest": record.get("contest", ""),
                    "full_name": record["full_name"], "geo_name": record["geo_name"],
                    "state_abb": record["state_abb"], "year": int(record["year"]),
                    "month": int(record["month"]) if pd.notna(record.get("month")) else pd.NA,
                    "query_variant": record["query_variant"], "result_rank": rescored.get("rank"),
                    "proposed_campaign_url": url,
                    "evidence_source_url": rescored.get("url", ""),
                    "evidence_type": evidence_type,
                    "exact_year_context": bool(rescored.get("exact_year_context", False)),
                    "mentioned_years": " | ".join(rescored.get("mentioned_years", [])),
                    "temporal_conflict": bool(rescored.get("temporal_conflict", False)),
                    "office_mismatch_signal": bool(rescored.get("office_mismatch_signal", False)),
                    "result_title": rescored.get("title", ""),
                    "result_snippet": rescored.get("snippet", ""),
                    "review_status": "pending",
                    "review_note": "",
                })
    if not rows:
        return pd.DataFrame(columns=LEAD_QUEUE_COLUMNS)
    queue = pd.DataFrame(rows).drop_duplicates([
        "candidate_year_id", "candidate_contest_id", "proposed_campaign_url",
        "evidence_source_url",
    ])
    priority = {"candidate_domain": 0, "embedded_domain_evidence": 1, "source_evidence": 2}
    queue["_priority"] = queue["evidence_type"].map(priority).fillna(9)
    return queue.sort_values(
        ["_priority", "exact_year_context", "candidate_year_id", "result_rank"],
        ascending=[True, False, True, True],
    ).drop(columns="_priority").reset_index(drop=True).reindex(columns=LEAD_QUEUE_COLUMNS)


def read_jsonl(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def request_search(provider: str, query: str, api_key: str, count: int, timeout: int) -> list[dict]:
    if provider == "brave":
        response = requests.get(
            "https://api.search.brave.com/res/v1/web/search",
            headers={"Accept": "application/json", "X-Subscription-Token": api_key},
            params={"q": query, "count": count, "country": "US", "search_lang": "en"},
            timeout=timeout,
        )
        if response.status_code in {400, 401, 403}:
            raise SearchConfigurationError(
                f"{provider} rejected the request ({response.status_code}): {response.text[:200]}"
            )
        response.raise_for_status()
        raw = response.json().get("web", {}).get("results", [])
        return [{"rank": i, "title": x.get("title", ""), "url": x.get("url", ""),
                 "snippet": x.get("description", "")} for i, x in enumerate(raw, 1)]
    response = requests.post(
        "https://google.serper.dev/search",
        headers={"X-API-KEY": api_key, "Content-Type": "application/json"},
        json={"q": query, "gl": "us", "hl": "en", "num": count},
        timeout=timeout,
    )
    if response.status_code in {400, 401, 403}:
        raise SearchConfigurationError(
            f"{provider} rejected the request ({response.status_code}): {response.text[:200]}"
        )
    response.raise_for_status()
    raw = response.json().get("organic", [])
    return [{"rank": i, "title": x.get("title", ""), "url": x.get("link", ""),
             "snippet": x.get("snippet", "")} for i, x in enumerate(raw, 1)]


def retrieve(
    manifest: pd.DataFrame, output_path: Path, provider: str, approved_cost_usd: float,
    count: int, timeout: int, delay: float,
) -> None:
    estimated = len(manifest) * PROVIDER_COST_PER_1000[provider] / 1000
    if approved_cost_usd + 1e-9 < estimated:
        raise ValueError(
            f"Approved ceiling ${approved_cost_usd:.2f} is below worst-case search cost "
            f"${estimated:.2f} for {len(manifest)} requests."
        )
    env_name = "BRAVE_SEARCH_API_KEY" if provider == "brave" else "SERPER_API_KEY"
    api_key = os.environ.get(env_name, "")
    if not api_key:
        raise ValueError(f"{env_name} is not set")
    completed = set()
    decisive_candidates = set()
    if output_path.exists():
        for line in output_path.read_text().splitlines():
            if line.strip():
                item = json.loads(line)
                if item.get("retrieval_status", "ok") == "ok":
                    completed.add((item["candidate_year_id"], item["query_variant"]))
                rescored = [
                    {**result, **result_evidence(result, item)}
                    for result in item.get("results", [])
                ]
                if item["query_variant"] == "campaign_website_year" and not results_warrant_fallback(rescored):
                    decisive_candidates.add(item["candidate_year_id"])
    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("a", encoding="utf-8") as handle:
        for row in manifest.to_dict("records"):
            key = (row["candidate_year_id"], row["query_variant"])
            if key in completed:
                continue
            if row["query_variant"] != "campaign_website_year" and row["candidate_year_id"] in decisive_candidates:
                continue
            record = {**row, "provider": provider, "retrieval_status": "ok", "error": ""}
            try:
                results = request_search(provider, row["query"], api_key, count, timeout)
                record["results"] = [{**item, **result_evidence(item, row)} for item in results]
                record["fallback_warranted"] = results_warrant_fallback(record["results"])
                if not record["fallback_warranted"]:
                    decisive_candidates.add(row["candidate_year_id"])
            except SearchConfigurationError:
                raise
            except requests.RequestException as error:
                record.update({
                    "retrieval_status": "request_error",
                    "error": str(error)[:500],
                    "results": [],
                })
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            handle.flush()
            time.sleep(delay)


def main() -> None:
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    prepare = sub.add_parser("prepare")
    prepare.add_argument("--summary", default="data/municipal_discovery/final/mayoral_candidate_url_summary.csv")
    prepare.add_argument("--output", default="data/municipal_discovery/search_pilot/query_manifest.csv")
    prepare.add_argument("--sample-per-stratum", type=int, default=20)
    prepare.add_argument("--variants", type=int, choices=(1, 2), default=1)
    search = sub.add_parser("search")
    search.add_argument("--manifest", default="data/municipal_discovery/search_pilot/query_manifest.csv")
    search.add_argument("--output", default="data/municipal_discovery/search_pilot/search_results.jsonl")
    search.add_argument("--provider", choices=sorted(PROVIDER_COST_PER_1000), required=True)
    search.add_argument("--approved-cost-usd", type=float, required=True)
    search.add_argument("--count", type=int, default=10)
    search.add_argument("--timeout", type=int, default=30)
    search.add_argument("--delay", type=float, default=0.2)
    export = sub.add_parser("export")
    export.add_argument("--input", default="data/municipal_discovery/search_pilot/search_results.jsonl")
    export.add_argument("--output", default="data/municipal_discovery/search_pilot/search_lead_review.csv")
    args = parser.parse_args()
    if args.command == "prepare":
        manifest = prepare_manifest(pd.read_csv(args.summary, low_memory=False), args.sample_per_stratum, args.variants)
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        manifest.to_csv(path, index=False)
        print(json.dumps({"queries": len(manifest), "candidate_years": manifest.candidate_year_id.nunique(),
                          "output": str(path)}, sort_keys=True))
    elif args.command == "search":
        retrieve(pd.read_csv(args.manifest), Path(args.output), args.provider,
                 args.approved_cost_usd, args.count, args.timeout, args.delay)
    else:
        queue = build_lead_queue(read_jsonl(Path(args.input)))
        path = Path(args.output)
        path.parent.mkdir(parents=True, exist_ok=True)
        queue.to_csv(path, index=False)
        print(json.dumps({"review_rows": len(queue), "candidate_years": queue.candidate_year_id.nunique(),
                          "output": str(path)}, sort_keys=True))


if __name__ == "__main__":
    main()
