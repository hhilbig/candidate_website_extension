#!/usr/bin/env python3
"""
Feasibility probe: are German Bundestag member websites archived on the Wayback Machine?

Queries Wikidata for MdBs with official websites (P856), then checks CDX API
coverage across three election windows (2017, 2021, 2025).

Usage:
    python -m src.probe_german_feasibility
    python -m src.probe_german_feasibility --sample-size 100
"""

import argparse
import csv
import logging
import os
import random
import time

import requests

from .scrape_wayback import WaybackConnectionRefused, query_cdx
from .utils import RateLimiter

logger = logging.getLogger(__name__)

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_API = "https://www.wikidata.org/w/api.php"
USER_AGENT = "GermanFeasibilityProbe/1.0 (Academic Research)"

# Bundestag election windows (YYYYMMDD)
ELECTION_WINDOWS = [
    {"year": 2017, "start": "20170101", "end": "20171231"},
    {"year": 2021, "start": "20210101", "end": "20211231"},
    {"year": 2025, "start": "20240701", "end": "20250630"},
]

WAYBACK_CONFIG = {
    "max_retries": 3,
    "timeout_connect": 30,
    "timeout_read": 120,
    "user_agent": USER_AGENT,
}


# ── Wikidata queries ────────────────────────────────────────────────

def fetch_mdb_total_count() -> int:
    """Count total distinct persons who held P39 = Q1939555 (MdB)."""
    query = """
    SELECT (COUNT(DISTINCT ?person) AS ?total) WHERE {
      ?person wdt:P39 wd:Q1939555 .
    }
    """
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers={"User-Agent": USER_AGENT,
                     "Accept": "application/sparql-results+json"},
            timeout=120,
        )
        resp.raise_for_status()
        bindings = resp.json()["results"]["bindings"]
        return int(bindings[0]["total"]["value"])
    except Exception as e:
        logger.error(f"Failed to count MdBs: {e}")
        return 0


def fetch_mdb_websites() -> list[dict]:
    """Fetch MdBs with official websites from Wikidata (two-step approach).

    Returns list of {"qid", "name", "website"} dicts.
    """
    # Step 1: SPARQL for entity IDs + websites
    query = """
    SELECT DISTINCT ?person ?website WHERE {
      ?person wdt:P39 wd:Q1939555 .
      ?person wdt:P856 ?website .
    }
    """
    try:
        resp = requests.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            headers={"User-Agent": USER_AGENT,
                     "Accept": "application/sparql-results+json"},
            timeout=120,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as e:
        logger.error(f"SPARQL query failed: {e}")
        return []

    # Collect entity ID -> first website
    entity_websites: dict[str, str] = {}
    for binding in data.get("results", {}).get("bindings", []):
        qid = binding.get("person", {}).get("value", "").split("/")[-1]
        website = binding.get("website", {}).get("value", "")
        if qid and website and qid not in entity_websites:
            entity_websites[qid] = website

    if not entity_websites:
        return []

    logger.info(f"SPARQL returned {len(entity_websites)} MdBs with websites")

    # Step 2: Resolve names via Wikidata API (50 per batch)
    entity_names: dict[str, str] = {}
    qids = list(entity_websites.keys())
    batch_size = 50
    for i in range(0, len(qids), batch_size):
        batch = qids[i : i + batch_size]
        try:
            resp = requests.get(
                WIKIDATA_API,
                params={
                    "action": "wbgetentities",
                    "ids": "|".join(batch),
                    "props": "labels",
                    "languages": "de|en",
                    "format": "json",
                },
                headers={"User-Agent": USER_AGENT},
                timeout=30,
            )
            resp.raise_for_status()
            for qid, entity in resp.json().get("entities", {}).items():
                labels = entity.get("labels", {})
                # Prefer German name, fall back to English
                label = (labels.get("de", {}).get("value")
                         or labels.get("en", {}).get("value", ""))
                if label:
                    entity_names[qid] = label
        except Exception as e:
            logger.warning(f"Label batch failed: {e}")

    logger.info(f"Resolved {len(entity_names)} entity names")

    results = []
    for qid, website in entity_websites.items():
        name = entity_names.get(qid, qid)
        results.append({"qid": qid, "name": name, "website": website})

    return results


# ── CDX probe ────────────────────────────────────────────────────────

def probe_cdx(url: str, rate_limiter: RateLimiter) -> list[dict]:
    """Query CDX for one URL across all election windows."""
    results = []
    for window in ELECTION_WINDOWS:
        rate_limiter.wait()
        try:
            snapshots = query_cdx(url, window["start"], window["end"],
                                  WAYBACK_CONFIG)
        except WaybackConnectionRefused:
            logger.warning(f"Wayback down for {url} ({window['year']})")
            snapshots = []

        n = len(snapshots)
        results.append({
            "election_year": window["year"],
            "n_snapshots": n,
            "has_snapshots": n > 0,
            "earliest_snapshot": snapshots[0]["timestamp"] if snapshots else "",
            "latest_snapshot": snapshots[-1]["timestamp"] if snapshots else "",
        })

    return results


# ── Reporting ────────────────────────────────────────────────────────

def print_summary(total_mdb: int, mdb_with_websites: int,
                  sample_size: int, results: list[dict]):
    """Print formatted summary to stdout."""
    pct_with_site = (mdb_with_websites / total_mdb * 100) if total_mdb else 0

    print()
    print("=" * 55)
    print("  German Bundestag Feasibility Probe")
    print("=" * 55)
    print()
    print("Wikidata coverage:")
    print(f"  Total MdBs on Wikidata (Q1939555):  {total_mdb:>5}")
    print(f"  MdBs with website (P856):           {mdb_with_websites:>5} ({pct_with_site:.1f}%)")
    print()
    print(f"Sample: N={sample_size} (seed=42)")
    print()
    print("Wayback Machine coverage (by election year):")

    for window in ELECTION_WINDOWS:
        year = window["year"]
        year_rows = [r for r in results if r["election_year"] == year]
        n_with = sum(1 for r in year_rows if r["has_snapshots"])
        total = len(year_rows)
        pct = n_with / total * 100 if total else 0
        avg_snaps = (sum(r["n_snapshots"] for r in year_rows) / total
                     if total else 0)
        print(f"  {year}: {n_with}/{total} URLs with snapshots "
              f"({pct:.1f}%), avg {avg_snaps:.1f} snapshots/URL")

    # Any year
    urls_any = set()
    for r in results:
        if r["has_snapshots"]:
            urls_any.add(r.get("name", r.get("website", "")))
    n_any = len(urls_any)
    pct_any = n_any / sample_size * 100 if sample_size else 0
    print()
    print(f"  ANY year: {n_any}/{sample_size} URLs with at least one "
          f"snapshot ({pct_any:.1f}%)")

    print()
    print("Note: This probes sitting/former MdBs only. Candidates who")
    print("ran but never won are largely absent from Wikidata. For those,")
    print("Bundeswahlleiter data or Abgeordnetenwatch would be needed.")
    print()


def save_csv(results: list[dict], output_path: str):
    """Save detailed results to CSV."""
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    fieldnames = ["qid", "name", "website", "election_year",
                  "window_start", "window_end",
                  "n_snapshots", "has_snapshots",
                  "earliest_snapshot", "latest_snapshot"]
    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)
    print(f"Detailed results saved to {output_path}")


# ── Main ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(
        description="Probe Wayback Machine coverage of German Bundestag member websites")
    parser.add_argument("--sample-size", type=int, default=50,
                        help="Number of MdBs to sample (default: 50)")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    # Step 1: Wikidata queries
    print("Querying Wikidata for German Bundestag members...")
    total_mdb = fetch_mdb_total_count()
    mdb_list = fetch_mdb_websites()

    if not mdb_list:
        print("ERROR: No MdBs with websites found on Wikidata.")
        return

    print(f"  Found {total_mdb} total MdBs, {len(mdb_list)} with websites")

    # Step 2: Sample
    sample_size = min(args.sample_size, len(mdb_list))
    random.seed(42)
    sample = random.sample(mdb_list, sample_size)
    print(f"  Sampled {sample_size} for CDX probe")

    # Step 3: CDX probe
    rate_limiter = RateLimiter(min_delay=1.0)
    all_results = []
    n_elections = len(ELECTION_WINDOWS)
    total_queries = sample_size * n_elections

    print(f"\nProbing CDX API ({total_queries} queries, ~{total_queries}s)...")
    for i, mdb in enumerate(sample, 1):
        print(f"  [{i}/{sample_size}] {mdb['name']} — {mdb['website']}")
        cdx_results = probe_cdx(mdb["website"], rate_limiter)

        for window, cdx in zip(ELECTION_WINDOWS, cdx_results):
            all_results.append({
                "qid": mdb["qid"],
                "name": mdb["name"],
                "website": mdb["website"],
                "election_year": cdx["election_year"],
                "window_start": window["start"],
                "window_end": window["end"],
                "n_snapshots": cdx["n_snapshots"],
                "has_snapshots": cdx["has_snapshots"],
                "earliest_snapshot": cdx["earliest_snapshot"],
                "latest_snapshot": cdx["latest_snapshot"],
            })

    # Step 4: Report
    # Add name to flat results for ANY-year dedup
    for r in all_results:
        r.setdefault("name", "")

    print_summary(total_mdb, len(mdb_list), sample_size, all_results)

    output_path = "data/probe_german_feasibility.csv"
    save_csv(all_results, output_path)


if __name__ == "__main__":
    main()
