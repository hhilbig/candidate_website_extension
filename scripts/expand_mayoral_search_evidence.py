#!/usr/bin/env python3
"""Extract candidate-domain links from search-discovered evidence pages."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from urllib.parse import urljoin

import pandas as pd
import requests
from bs4 import BeautifulSoup

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from scripts.discover_mayoral_search_urls import LEAD_QUEUE_COLUMNS, is_candidate_shaped_domain
from scripts.build_mayoral_url_discovery import fetch_text_with_curl
from scripts.validate_mayoral_url_archives import nearest_capture, query_campaign_url_captures
from src.mayoral_discovery import canonical_http_url


OUTPUT_COLUMNS = [
    "candidate_year_id", "candidate_contest_id", "contest", "full_name",
    "geo_name", "state_abb", "year", "month",
    "evidence_source_url", "fetch_status", "http_status", "discovered_campaign_url",
    "extraction_method", "source_snapshot_timestamp", "source_fetch_url", "error",
    "query_variant", "result_rank", "exact_year_context", "mentioned_years",
    "temporal_conflict", "office_mismatch_signal", "result_title", "result_snippet",
]

UNIFIED_EXPANSION_COLUMNS = [
    "source_fetch_url", "source_snapshot_timestamp", "extraction_method",
    "expansion_fetch_status", "expansion_error",
]
UNIFIED_EVIDENCE_COLUMNS = LEAD_QUEUE_COLUMNS + UNIFIED_EXPANSION_COLUMNS


def extract_candidate_links(html: str, source_url: str, full_name: str, city: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    found = {}
    for anchor in soup.find_all("a", href=True):
        url = urljoin(source_url, anchor["href"].strip())
        if is_candidate_shaped_domain(url, full_name, city):
            found[canonical_http_url(url) or url] = "candidate_shaped_anchor"
    for meta in soup.find_all("meta", attrs={"http-equiv": lambda value: value and value.casefold() == "refresh"}):
        content = meta.get("content", "")
        if "url=" not in content.casefold():
            continue
        target = content.split("=", 1)[1].strip(" '\"")
        url = urljoin(source_url, target)
        if is_candidate_shaped_domain(url, full_name, city):
            found[canonical_http_url(url) or url] = "candidate_shaped_meta_refresh"
    # Some archived or text-rendered pages print domains without making them links.
    for text in soup.stripped_strings:
        for token in str(text).split():
            candidate = token.strip("()[]{}<>,.;:!?\"'")
            if candidate.startswith(("http://", "https://", "www.")) and is_candidate_shaped_domain(
                candidate, full_name, city
            ):
                found.setdefault(canonical_http_url(candidate) or candidate, "candidate_shaped_text")
    return [
        {"discovered_campaign_url": url, "extraction_method": method}
        for url, method in sorted(found.items())
    ]


def expand_queue(queue: pd.DataFrame, timeout: int = 20, use_wayback: bool = False) -> pd.DataFrame:
    eligible = (
        queue["evidence_type"].eq("source_evidence")
        & ~queue["office_mismatch_signal"].fillna(False)
    )
    source_key = [
        column for column in [
            "candidate_year_id", "candidate_contest_id", "evidence_source_url"
        ] if column in queue.columns
    ]
    sources = queue.loc[eligible].drop_duplicates(source_key)
    output = []
    session = requests.Session()
    session.headers["User-Agent"] = "candidate-website-research/1.0"
    for row in sources.to_dict("records"):
        base = {
            column: row.get(column, "")
            for column in OUTPUT_COLUMNS
            if column not in {
                "fetch_status", "http_status", "discovered_campaign_url",
                "extraction_method", "source_snapshot_timestamp", "source_fetch_url",
                "error",
            }
        }
        try:
            response = session.get(row["evidence_source_url"], timeout=timeout)
            response.raise_for_status()
            links = extract_candidate_links(
                response.text, response.url, row["full_name"], row["geo_name"]
            )
            if links:
                output.extend({**base, "fetch_status": "checked_with_candidate_link",
                               "http_status": response.status_code, **link,
                               "source_snapshot_timestamp": "", "source_fetch_url": response.url,
                               "error": ""}
                              for link in links)
            else:
                output.append({**base, "fetch_status": "checked_no_candidate_link",
                               "http_status": response.status_code, "discovered_campaign_url": "",
                               "extraction_method": "", "source_snapshot_timestamp": "",
                               "source_fetch_url": response.url, "error": ""})
        except requests.RequestException as error:
            status = getattr(error.response, "status_code", "") if error.response is not None else ""
            output.append({**base, "fetch_status": "error", "http_status": status,
                           "discovered_campaign_url": "", "extraction_method": "",
                           "source_snapshot_timestamp": "", "source_fetch_url": row["evidence_source_url"],
                           "error": f"{type(error).__name__}: {error}"[:500]})
    if use_wayback:
        unresolved = [item for item in output if not item["discovered_campaign_url"]]
        for item in unresolved:
            try:
                timestamps = query_campaign_url_captures(
                    item["evidence_source_url"], int(item["year"]), timeout, 2
                )
                if pd.isna(item.get("month")):
                    raise ValueError("Election month is required for archive expansion")
                timestamp, _ = nearest_capture(
                    timestamps, int(item["year"]), int(item["month"])
                )
                if not timestamp:
                    item["fetch_status"] = "no_source_archive_in_year_window"
                    continue
                archive_url = (
                    f"https://web.archive.org/web/{timestamp}id_/"
                    f"{item['evidence_source_url']}"
                )
                html = fetch_text_with_curl(archive_url, timeout, 2)
                links = extract_candidate_links(
                    html, item["evidence_source_url"], item["full_name"], item["geo_name"]
                )
                item["source_snapshot_timestamp"] = timestamp
                item["source_fetch_url"] = archive_url
                item["error"] = ""
                if links:
                    item.update(links[0])
                    item["fetch_status"] = "archive_checked_with_candidate_link"
                    for extra in links[1:]:
                        output.append({**item, **extra})
                else:
                    item["fetch_status"] = "archive_checked_no_candidate_link"
            except Exception as error:
                item["fetch_status"] = "archive_error"
                item["error"] = f"{type(error).__name__}: {error}"[:500]
    return pd.DataFrame(output).reindex(columns=OUTPUT_COLUMNS)


def merge_evidence_queue(queue: pd.DataFrame, expanded: pd.DataFrame) -> pd.DataFrame:
    """Merge direct, embedded, and expanded search leads into one review queue."""
    direct = queue.copy()
    for column in UNIFIED_EXPANSION_COLUMNS:
        if column not in direct:
            direct[column] = ""

    additions = []
    for row in expanded.to_dict("records"):
        discovered_url = row.get("discovered_campaign_url", "")
        if not isinstance(discovered_url, str) or not discovered_url.strip():
            continue
        additions.append({
            "candidate_year_id": row.get("candidate_year_id", ""),
            "candidate_contest_id": row.get("candidate_contest_id", ""),
            "contest": row.get("contest", ""),
            "full_name": row.get("full_name", ""),
            "geo_name": row.get("geo_name", ""),
            "state_abb": row.get("state_abb", ""),
            "year": row.get("year", pd.NA),
            "month": row.get("month", pd.NA),
            "query_variant": row.get("query_variant", ""),
            "result_rank": row.get("result_rank", pd.NA),
            "proposed_campaign_url": canonical_http_url(discovered_url) or discovered_url,
            "evidence_source_url": row.get("evidence_source_url", ""),
            "evidence_type": "expanded_domain_evidence",
            "exact_year_context": row.get("exact_year_context", False),
            "result_title": row.get("result_title", ""),
            "result_snippet": row.get("result_snippet", ""),
            "mentioned_years": row.get("mentioned_years", ""),
            "temporal_conflict": row.get("temporal_conflict", False),
            "office_mismatch_signal": row.get("office_mismatch_signal", False),
            "review_status": "pending",
            "review_note": "",
            "source_fetch_url": row.get("source_fetch_url", ""),
            "source_snapshot_timestamp": row.get("source_snapshot_timestamp", ""),
            "extraction_method": row.get("extraction_method", ""),
            "expansion_fetch_status": row.get("fetch_status", ""),
            "expansion_error": row.get("error", ""),
        })
    combined = pd.concat(
        [direct.reindex(columns=UNIFIED_EVIDENCE_COLUMNS), pd.DataFrame(additions)],
        ignore_index=True,
    ).reindex(columns=UNIFIED_EVIDENCE_COLUMNS)
    return combined.drop_duplicates([
        "candidate_year_id", "candidate_contest_id", "proposed_campaign_url",
        "evidence_source_url", "evidence_type", "extraction_method",
    ]).reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="data/municipal_discovery/search_pilot/search_lead_review.csv")
    parser.add_argument("--output", default="data/municipal_discovery/search_pilot/source_page_expansion.csv")
    parser.add_argument(
        "--unified-output",
        default="data/municipal_discovery/search_pilot/search_evidence_review.csv",
    )
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--wayback", action="store_true")
    args = parser.parse_args()
    queue = pd.read_csv(args.queue)
    result = expand_queue(queue, args.timeout, args.wayback)
    path = Path(args.output)
    path.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(path, index=False)
    unified_path = Path(args.unified_output)
    unified_path.parent.mkdir(parents=True, exist_ok=True)
    merge_evidence_queue(queue, result).to_csv(unified_path, index=False)
    print(json.dumps({"source_pages": result[["candidate_year_id", "evidence_source_url"]].drop_duplicates().shape[0],
                      "candidate_links": int(result["discovered_campaign_url"].fillna("").ne("").sum()),
                      "output": str(path), "unified_output": str(unified_path)}, sort_keys=True))


if __name__ == "__main__":
    main()
