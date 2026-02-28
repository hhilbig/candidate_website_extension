#!/usr/bin/env python3
"""
Validation tests on fresh candidates not used in prior test suites.

Confirms Fix A (stratified sampling) and Fix B (subpage URL resolution)
work on real Wayback data across different eras and site types.

Hits real Wayback Machine endpoints — run sparingly.

Already-tested candidates (excluded):
  Stevens, Warnock, Pelosi, Rubio, Hochul, Doug Jones, Jeff Miller, Chris Dodd.
"""

import sys
import os
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup

from src.extract_text import (
    extract_visible_text,
    get_subpage_urls,
    strip_wayback_toolbar,
    is_wayback_page,
)
from src.scrape_wayback import (
    query_cdx,
    _normalize_url,
    _sample_snapshots_stratified,
)


CDX_CONFIG = {
    "max_retries": 2,
    "timeout_connect": 30,
    "timeout_read": 120,
    "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; validation test)",
}

FETCH_HEADERS = {"User-Agent": CDX_CONFIG["user_agent"]}


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


def check_dedup(snapshots: list[dict], bucket_months: int = 3) -> bool:
    """Verify no (normalized URL, bucket) pair appears twice."""
    seen = set()
    for snap in snapshots:
        norm = _normalize_url(snap["original_url"])
        year = snap["timestamp"][:4]
        month = int(snap["timestamp"][4:6])
        bucket = f"{year}Q{(month - 1) // bucket_months}"
        key = (norm, bucket)
        if key in seen:
            print(f"    FAIL: Duplicate (url, bucket): {key}")
            return False
        seen.add(key)
    label = {1: "monthly", 3: "quarterly", 12: "yearly"}.get(
        bucket_months, f"{bucket_months}-month"
    )
    print(f"    {label.capitalize()} dedup OK — {len(seen)} unique (url, bucket) pairs")
    return True


def check_stratified_sampling(snapshots: list[dict], max_snapshots: int = 200):
    """If >max_snapshots, run stratified sampling and verify month spread."""
    if len(snapshots) <= max_snapshots:
        print(f"    {len(snapshots)} snapshots <= {max_snapshots} cap — sampling not needed")
        return True

    sampled = _sample_snapshots_stratified(snapshots, max_snapshots)
    assert len(sampled) == max_snapshots, (
        f"FAIL: Expected {max_snapshots} sampled, got {len(sampled)}"
    )

    month_counts = Counter(s["timestamp"][:6] for s in sampled)
    n_months = len(month_counts)
    print(f"    Sampled {len(sampled)} from {len(snapshots)} — {n_months} months represented")
    print(f"    Month distribution: {dict(sorted(month_counts.items()))}")

    assert n_months > 1, "FAIL: Stratified sample covers only 1 month"

    # Check no single month hogs >60% of sample
    max_month_share = max(month_counts.values()) / len(sampled)
    assert max_month_share < 0.6, (
        f"FAIL: Largest month has {max_month_share:.0%} of sample"
    )
    print(f"    Largest month share: {max_month_share:.1%} (< 60%)")

    # Output should be sorted by timestamp
    timestamps = [s["timestamp"] for s in sampled]
    assert timestamps == sorted(timestamps), "FAIL: Sampled output not sorted"
    print("    Sampled output sorted by timestamp")
    return True


def check_subpage_discovery(snapshots: list[dict], label: str):
    """Fetch one snapshot, extract text, and verify subpage discovery."""
    # Pick a snapshot from mid-list (avoid edge cases at start/end)
    idx = min(len(snapshots) // 2, len(snapshots) - 1)
    snap = snapshots[idx]
    wb_url = snap["wayback_url"]
    print(f"    Fetching snapshot: {wb_url[:90]}")

    time.sleep(1)  # rate-limit courtesy
    soup = fetch_snapshot(wb_url)
    if soup is None:
        # Try a few more snapshots before giving up
        for fallback_idx in [0, len(snapshots) - 1]:
            snap = snapshots[fallback_idx]
            wb_url = snap["wayback_url"]
            print(f"    Retrying with: {wb_url[:90]}")
            time.sleep(1)
            soup = fetch_snapshot(wb_url)
            if soup is not None:
                break

    if soup is None:
        print(f"    SKIP: Could not fetch any snapshot for {label}")
        return None

    text = extract_visible_text(soup, "#+#")
    word_count = len(text.split()) if text else 0
    print(f"    Extracted text: {word_count} words")

    subpages = get_subpage_urls(soup, wb_url)
    print(f"    Subpages found: {len(subpages)}")

    # All subpage URLs should be valid Wayback format
    for u in subpages:
        assert "web.archive.org" in u, f"FAIL: Non-Wayback subpage URL: {u}"

    if subpages:
        for u in subpages[:3]:
            print(f"      {u[:100]}")
        if len(subpages) > 3:
            print(f"      ... and {len(subpages) - 3} more")

    return len(subpages)


# ── Candidate 1: John McCain (Senate 2008, .gov) ────────────────────

def test_mccain():
    """
    John McCain, Senate 2008 — mccain.senate.gov.
    Large .gov site: tests Fix A (stratified sampling over many CDX records).
    """
    header("CANDIDATE 1: John McCain (Senate 2008, mccain.senate.gov)")

    snapshots = query_cdx("mccain.senate.gov", "20080101", "20081231", CDX_CONFIG)
    print(f"  CDX returned {len(snapshots)} snapshots (after monthly dedup)")
    assert len(snapshots) > 0, "FAIL: No snapshots for mccain.senate.gov in 2008"

    # Monthly dedup check
    assert check_dedup(snapshots), "FAIL: Dedup broken"

    # Fix A: stratified sampling (McCain should have many records)
    check_stratified_sampling(snapshots, max_snapshots=200)

    # Fix B: subpage discovery on .gov site
    n_subpages = check_subpage_discovery(snapshots, "McCain")
    if n_subpages is not None:
        assert n_subpages > 0, (
            "FAIL: get_subpage_urls found 0 subpages on mccain.senate.gov — "
            "Fix B (relative URL resolution) may not be working"
        )
        print(f"  PASS: {n_subpages} subpages found on .gov site")
    else:
        print("  SKIP: Could not fetch snapshot to test subpage discovery")

    print("  PASS: John McCain")


# ── Candidate 2: AOC (House 2018, modern site) ──────────────────────

def test_aoc():
    """
    Alexandria Ocasio-Cortez, House 2018 — ocasio-cortez.house.gov.
    Modern site: tests Fix B (subpage discovery on relative links).
    """
    header("CANDIDATE 2: Alexandria Ocasio-Cortez (House 2018)")

    # Try .house.gov first (took office Jan 2019, so 2019 captures)
    snapshots = query_cdx("ocasio-cortez.house.gov", "20190101", "20191231", CDX_CONFIG)
    url_used = "ocasio-cortez.house.gov (2019)"

    if not snapshots:
        # Try campaign site
        print("  No .house.gov snapshots in 2019, trying ocasiocortez.com for 2018...")
        time.sleep(1)
        snapshots = query_cdx("ocasiocortez.com", "20180101", "20181231", CDX_CONFIG)
        url_used = "ocasiocortez.com (2018)"

    print(f"  CDX returned {len(snapshots)} snapshots for {url_used}")
    assert len(snapshots) > 0, f"FAIL: No snapshots for AOC at {url_used}"

    assert check_dedup(snapshots), "FAIL: Dedup broken"
    check_stratified_sampling(snapshots, max_snapshots=200)

    n_subpages = check_subpage_discovery(snapshots, "AOC")
    if n_subpages is not None:
        # Modern sites typically have many internal links
        assert n_subpages > 0, (
            "FAIL: get_subpage_urls found 0 subpages on modern site — "
            "Fix B may not be resolving relative links"
        )
        print(f"  PASS: {n_subpages} subpages found on modern site")
    else:
        print("  SKIP: Could not fetch snapshot to test subpage discovery")

    print(f"  PASS: AOC ({url_used})")


# ── Candidate 3: Tim Scott (Senate 2016, campaign domain) ───────────

def test_tim_scott():
    """
    Tim Scott, Senate 2016 — timscottforsenate.com.
    Mid-era campaign domain: moderate CDX volume.
    """
    header("CANDIDATE 3: Tim Scott (Senate 2016, timscottforsenate.com)")

    snapshots = query_cdx("timscottforsenate.com", "20160101", "20161231", CDX_CONFIG)
    url_used = "timscottforsenate.com"

    if not snapshots:
        # Fallback: try his .senate.gov
        print("  No snapshots for campaign domain, trying scott.senate.gov...")
        time.sleep(1)
        snapshots = query_cdx("scott.senate.gov", "20160101", "20161231", CDX_CONFIG)
        url_used = "scott.senate.gov"

    print(f"  CDX returned {len(snapshots)} snapshots for {url_used}")
    assert len(snapshots) > 0, f"FAIL: No snapshots for Tim Scott at {url_used}"

    assert check_dedup(snapshots), "FAIL: Dedup broken"
    check_stratified_sampling(snapshots, max_snapshots=200)

    n_subpages = check_subpage_discovery(snapshots, "Tim Scott")
    if n_subpages is not None and n_subpages > 0:
        print(f"  PASS: {n_subpages} subpages found")
    elif n_subpages == 0:
        print("  NOTE: 0 subpages — campaign sites sometimes have minimal internal links")
    else:
        print("  SKIP: Could not fetch snapshot to test subpage discovery")

    print(f"  PASS: Tim Scott ({url_used})")


# ── Runner ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [test_mccain, test_aoc, test_tim_scott]
    passed = 0
    failed = 0

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {type(e).__name__}: {e}")
            failed += 1
        time.sleep(2)  # courtesy delay between candidates

    header("SUMMARY")
    print(f"  {passed} passed, {failed} failed out of {len(tests)} candidates")
    if failed == 0:
        print("  All uncorrelated validation tests passed.")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
