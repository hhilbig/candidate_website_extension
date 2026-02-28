#!/usr/bin/env python3
"""
Live validation of page-type classification on real Wayback Machine data.

Runs classify_page_type() and prioritize_subpage_urls() against subpage URLs
from 5 fresh candidates (all Senate 2018), then compares the page-type
distribution to ICPSR 226001 benchmarks.

Hits real Wayback Machine endpoints — run sparingly.

Already-tested candidates (excluded):
  Stevens, Warnock, Pelosi, Rubio, Hochul, Doug Jones, Jeff Miller, Chris Dodd,
  John McCain, AOC, Tim Scott, Ted Cruz.
"""

import sys
import os
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup

from src.extract_text import (
    classify_page_type,
    get_subpage_urls,
    is_wayback_page,
    prioritize_subpage_urls,
    strip_wayback_toolbar,
    PAGE_TYPE_PRIORITY,
)
from src.scrape_wayback import query_cdx


CDX_CONFIG = {
    "max_retries": 2,
    "timeout_connect": 30,
    "timeout_read": 120,
    "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; validation test)",
}

FETCH_HEADERS = {"User-Agent": CDX_CONFIG["user_agent"]}

# ICPSR 226001 reference distribution (Di Tella et al. 2025, House 2002-2016)
ICPSR_DISTRIBUTION = {
    "homepage": 0.27,
    "issues": 0.05,
    "news": 0.07,
    "action": 0.04,
    "other": 0.52,
    # biography, endorsements, constituent_services not broken out in ICPSR
}


def header(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


def fetch_snapshot(url: str) -> BeautifulSoup | None:
    """Fetch a Wayback snapshot and return parsed soup, or None on failure."""
    try:
        resp = requests.get(url, timeout=(30, 90), headers=FETCH_HEADERS)
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code} for {url[:80]}")
            return None
        if not is_wayback_page(resp.text):
            print(f"    Not a Wayback page: {url[:80]}")
            return None
        clean_html = strip_wayback_toolbar(resp.text)
        return BeautifulSoup(clean_html, "html.parser")
    except Exception as e:
        print(f"    Fetch error: {e}")
        return None


def classify_all_subpages(urls: list[str]) -> Counter:
    """Classify each URL and return a Counter of page types."""
    types = Counter()
    for url in urls:
        page_type = classify_page_type(url)
        types[page_type] += 1
    return types


def check_priority_ordering(urls: list[str]) -> bool:
    """
    Verify that prioritize_subpage_urls() produces correct ordering:
    no high-priority type (issues/biography) appears after a low-priority
    type (action/other) in the sorted list.
    """
    sorted_urls = prioritize_subpage_urls(urls)
    priority_map = {pt: i for i, pt in enumerate(PAGE_TYPE_PRIORITY)}

    max_priority_seen = -1
    for url in sorted_urls:
        page_type = classify_page_type(url)
        priority = priority_map.get(page_type, len(PAGE_TYPE_PRIORITY))
        if priority < max_priority_seen:
            print(f"    FAIL: {page_type} (priority {priority}) appears after "
                  f"priority {max_priority_seen}")
            return False
        max_priority_seen = max(max_priority_seen, priority)
    return True


def run_candidate_test(label: str, domain: str, year: int = 2018,
                       fallback_domain: str | None = None) -> dict:
    """
    Run page-type classification on a single candidate's Wayback data.

    Returns dict with candidate name, type counts, and skip status.
    """
    header(label)

    start = f"{year}0101"
    end = f"{year}1231"
    snapshots = query_cdx(domain, start, end, CDX_CONFIG)
    url_used = f"{domain} ({year})"

    if not snapshots and fallback_domain:
        print(f"  No snapshots for {domain} in {year}, trying {fallback_domain}...")
        time.sleep(1)
        snapshots = query_cdx(fallback_domain, start, end, CDX_CONFIG)
        url_used = f"{fallback_domain} ({year})"

    if not snapshots:
        # Try adjacent year
        for alt_year in [year - 1, year + 1]:
            print(f"  No snapshots for {year}, trying {alt_year}...")
            time.sleep(1)
            snapshots = query_cdx(domain, f"{alt_year}0101", f"{alt_year}1231", CDX_CONFIG)
            url_used = f"{domain} ({alt_year})"
            if snapshots:
                break

    print(f"  CDX returned {len(snapshots)} snapshots for {url_used}")

    if not snapshots:
        print(f"  SKIP: No snapshots found for {label}")
        return {"candidate": label, "types": Counter(), "skipped": True}

    # Pick a mid-list snapshot
    idx = min(len(snapshots) // 2, len(snapshots) - 1)
    snap = snapshots[idx]
    wb_url = snap["wayback_url"]
    print(f"  Fetching snapshot: {wb_url[:90]}")

    time.sleep(1)
    soup = fetch_snapshot(wb_url)

    # Fallback: try other snapshots
    if soup is None:
        for fallback_idx in [0, len(snapshots) - 1]:
            snap = snapshots[fallback_idx]
            wb_url = snap["wayback_url"]
            print(f"  Retrying with: {wb_url[:90]}")
            time.sleep(1)
            soup = fetch_snapshot(wb_url)
            if soup is not None:
                break

    if soup is None:
        print(f"  SKIP: Could not fetch any snapshot for {label}")
        return {"candidate": label, "types": Counter(), "skipped": True}

    # Discover subpages
    subpages = get_subpage_urls(soup, wb_url)
    print(f"  Subpages discovered: {len(subpages)}")

    # Classify homepage URL + all subpage URLs
    all_urls = [wb_url] + subpages
    types = classify_all_subpages(all_urls)

    # Validate: all types must be valid PAGE_TYPE_PRIORITY members
    valid_types = set(PAGE_TYPE_PRIORITY)
    for t in types:
        assert t in valid_types, f"FAIL: Unknown page type '{t}' not in PAGE_TYPE_PRIORITY"

    # Print distribution
    total = sum(types.values())
    print(f"  Page-type distribution ({total} URLs):")
    for pt in PAGE_TYPE_PRIORITY:
        count = types.get(pt, 0)
        pct = 100 * count / total if total else 0
        bar = "#" * int(pct / 2)
        print(f"    {pt:25s} {count:4d} ({pct:5.1f}%) {bar}")

    # Soft check: at least 1 non-"other" type (WARNING, not FAIL)
    non_other = sum(v for k, v in types.items() if k != "other")
    if non_other == 0:
        print(f"  WARNING: All {total} URLs classified as 'other' — sparse site?")
    else:
        print(f"  {non_other} non-'other' URLs found")

    # Priority ordering check (only if enough subpages)
    if len(subpages) >= 3:
        ordering_ok = check_priority_ordering(subpages)
        if ordering_ok:
            print(f"  Priority ordering: PASS")
        else:
            print(f"  Priority ordering: FAIL")
            assert False, "prioritize_subpage_urls() produced incorrect ordering"
    else:
        print(f"  Priority ordering: SKIP (only {len(subpages)} subpages)")

    print(f"  PASS: {label}")
    return {"candidate": label, "types": types, "skipped": False}


def aggregate_comparison(results: list[dict]):
    """Pool type counts and compare to ICPSR 226001 distribution."""
    header("AGGREGATE: ICPSR 226001 Comparison")

    # Filter to non-skipped candidates
    active = [r for r in results if not r["skipped"]]
    if not active:
        print("  SKIP: No candidates produced data")
        return

    print(f"  Pooling data from {len(active)} candidates "
          f"(skipped {len(results) - len(active)})")

    pooled = Counter()
    for r in active:
        pooled.update(r["types"])

    total = sum(pooled.values())
    if total == 0:
        print("  SKIP: No URLs classified across all candidates")
        return

    # Side-by-side comparison table
    print(f"\n  {'Type':25s} {'Ours':>8s} {'Ours %':>8s} {'ICPSR %':>8s}")
    print(f"  {'-'*25} {'-'*8} {'-'*8} {'-'*8}")
    for pt in PAGE_TYPE_PRIORITY:
        count = pooled.get(pt, 0)
        our_pct = 100 * count / total
        icpsr_pct = ICPSR_DISTRIBUTION.get(pt, None)
        icpsr_str = f"{100*icpsr_pct:5.1f}%" if icpsr_pct is not None else "    n/a"
        print(f"  {pt:25s} {count:8d} {our_pct:7.1f}% {icpsr_str:>8s}")
    print(f"  {'TOTAL':25s} {total:8d}")

    # Hard assertion: at least 2 distinct non-"other" types across all candidates
    non_other_types = {t for t in pooled if t != "other" and pooled[t] > 0}
    n_non_other = len(non_other_types)
    print(f"\n  Distinct non-'other' types found: {n_non_other} {sorted(non_other_types)}")
    assert n_non_other >= 2, (
        f"FAIL: Only {n_non_other} non-'other' types across {len(active)} candidates — "
        f"classifier may not be working on real data"
    )
    print(f"  Hard assertion PASS: >= 2 distinct non-'other' types")

    # Soft assertions (WARNING only)
    homepage_pct = 100 * pooled.get("homepage", 0) / total
    other_pct = 100 * pooled.get("other", 0) / total
    if homepage_pct < 10:
        print(f"  WARNING: Homepage share ({homepage_pct:.1f}%) < 10% — "
              f"expected ~27% from ICPSR")
    else:
        print(f"  Homepage share ({homepage_pct:.1f}%) >= 10%: OK")

    if other_pct < 20:
        print(f"  WARNING: Other share ({other_pct:.1f}%) < 20% — "
              f"expected ~52% from ICPSR")
    else:
        print(f"  Other share ({other_pct:.1f}%) >= 20%: OK")


# ── Candidate definitions ────────────────────────────────────────────

def test_sherrod_brown():
    return run_candidate_test(
        "CANDIDATE 1: Sherrod Brown (Senate 2018, sherrodbrown.com)",
        "sherrodbrown.com",
        year=2018,
    )

def test_claire_mccaskill():
    return run_candidate_test(
        "CANDIDATE 2: Claire McCaskill (Senate 2018, clairemccaskill.com)",
        "clairemccaskill.com",
        year=2018,
    )

def test_tammy_baldwin():
    return run_candidate_test(
        "CANDIDATE 3: Tammy Baldwin (Senate 2018, tammybaldwin.com)",
        "tammybaldwin.com",
        year=2018,
    )

def test_richard_blumenthal():
    return run_candidate_test(
        "CANDIDATE 4: Richard Blumenthal (Senate 2018, blumenthal.senate.gov)",
        "blumenthal.senate.gov",
        year=2018,
    )

def test_bob_corker():
    return run_candidate_test(
        "CANDIDATE 5: Bob Corker (Senate 2018, corker.senate.gov)",
        "corker.senate.gov",
        year=2018,
    )


# ── Runner ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_sherrod_brown,
        test_claire_mccaskill,
        test_tammy_baldwin,
        test_richard_blumenthal,
        test_bob_corker,
    ]

    results = []
    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            result = test_fn()
            results.append(result)
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            results.append({"candidate": test_fn.__name__, "types": Counter(), "skipped": True})
            failed += 1
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            results.append({"candidate": test_fn.__name__, "types": Counter(), "skipped": True})
            failed += 1
        time.sleep(2)  # courtesy delay between candidates

    # Aggregate comparison (runs even if some candidates failed)
    try:
        aggregate_comparison(results)
    except AssertionError as e:
        print(f"  AGGREGATE FAIL: {e}")
        failed += 1

    header("SUMMARY")
    print(f"  {passed} passed, {failed} failed out of {len(tests)} candidates")
    non_skipped = sum(1 for r in results if not r["skipped"])
    print(f"  {non_skipped} candidates produced page-type data")
    if failed == 0:
        print("  All live page-type validation tests passed.")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
