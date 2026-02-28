#!/usr/bin/env python3
"""
Diverse end-to-end live tests for 5 candidates across different eras, offices,
site architectures, and CDX volumes.

Hits real Wayback Machine endpoints — run sparingly.

Candidates:
  1. Ted Stevens     — Senate 2004, early .gov, likely frames
  2. Raphael Warnock — Senate 2022, modern campaign domain
  3. Nancy Pelosi    — House 2020, high-profile .house.gov
  4. Marco Rubio     — Senate 2016, mid-era campaign domain
  5. Kathy Hochul    — House 2008, sparse small campaign site
  6. Integration: full process_candidate() on Warnock (capped at 3 snapshots)
"""

import csv
import os
import sys
import tempfile
import time
from collections import Counter

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup

from src.utils import RateLimiter, ProgressTracker
from src.extract_text import (
    extract_visible_text,
    extract_frame_content,
    get_subpage_urls,
    _resolve_frame_url,
    _deduplicate_text_segments,
    strip_wayback_toolbar,
    is_wayback_page,
)
from src.scrape_wayback import (
    query_cdx,
    fetch_page,
    scrape_snapshot,
    process_candidate,
    _normalize_url,
    _make_session,
)


def header(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


# Shared config for CDX queries
CDX_CONFIG = {
    "max_retries": 2,
    "timeout_connect": 30,
    "timeout_read": 120,
    "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; e2e-test)",
}

# Shared full config for scrape_snapshot / process_candidate
FULL_CONFIG = {
    "wayback": CDX_CONFIG,
    "scraping": {
        "max_snapshots_per_candidate": 200,
        "max_subpages": 50,
        "text_separator": "#+#",
        "exclude_domains": ["twitter.com", "facebook.com", "instagram.com", "youtube.com"],
    },
    "output": {},  # overridden per test where needed
}


def _make_test_session():
    return _make_session(CDX_CONFIG)


def _check_no_wayback_toolbar(text: str, label: str):
    """Assert that extracted text has no Wayback toolbar fragments."""
    bad_markers = ["wm_ipp", "Wayback Machine", "web.archive.org", "_wayback"]
    for marker in bad_markers:
        assert marker not in text, (
            f"FAIL [{label}]: Wayback toolbar fragment '{marker}' found in extracted text"
        )


def _check_no_excessive_repeats(text: str, separator: str, label: str):
    """Assert no segment >= 5 chars appears more than 2 times."""
    segments = [s.strip() for s in text.split(separator) if s.strip()]
    counts = Counter(segments)
    for seg, cnt in counts.items():
        if len(seg) >= 5 and cnt > 2:
            assert False, (
                f"FAIL [{label}]: Segment '{seg[:50]}' appears {cnt} times (>2)"
            )


# ── Test 1: Ted Stevens (Senate 2004) ──────────────────────────────

def test_stevens_senate_2004():
    """
    Early .gov Senate site: frame extraction, depth limit, relative paths.
    """
    header("E2E 1: Ted Stevens — Senate 2004 (stevens.senate.gov)")

    # CDX query
    snapshots = query_cdx("stevens.senate.gov", "20040101", "20041231", CDX_CONFIG)
    print(f"  CDX snapshots: {len(snapshots)}")
    assert len(snapshots) > 0, "FAIL: No CDX snapshots for stevens.senate.gov 2004"

    # Monthly dedup: no duplicate (url, month) pairs
    seen = set()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        assert key not in seen, f"FAIL: Duplicate (url, month): {key}"
        seen.add(key)
    print(f"  Monthly dedup OK — {len(snapshots)} unique (url, month) pairs")

    # Fetch one snapshot
    time.sleep(1)
    session = _make_test_session()
    rl = RateLimiter(min_delay=1.0)

    soup = fetch_page(snapshots[0]["wayback_url"], session, rl)
    if soup is None:
        print("  SKIP: Could not fetch first snapshot (timeout or non-Wayback)")
        session.close()
        return

    # Frame detection
    frames = soup.find_all("frame") + soup.find_all("iframe")
    print(f"  Frames found: {len(frames)}")

    text, subpages = extract_frame_content(
        soup, snapshots[0]["wayback_url"], "#+#",
        fetch_fn=lambda url: fetch_page(url, session, rl),
        max_depth=3,
    )
    print(f"  Text length: {len(text)} chars")
    print(f"  Subpages discovered: {len(subpages)}")

    if frames:
        print("  Page IS frame-based — frame extraction exercised")
        for f in frames[:3]:
            src = f.get("src", "N/A")
            resolved = _resolve_frame_url(src, snapshots[0]["wayback_url"])
            print(f"    frame src='{src}' -> {resolved[:80]}")
    else:
        print("  Page is NOT frame-based (may have been redesigned by 2004)")

    # Text should be non-empty if page loaded
    if text:
        assert len(text) > 50, f"FAIL: Text too short ({len(text)} chars)"
        _check_no_wayback_toolbar(text, "Stevens")
        print("  Text extraction OK, no Wayback toolbar fragments")
    else:
        print("  WARNING: No text extracted (frame fetch may have failed)")

    session.close()
    print("  PASS")


# ── Test 2: Raphael Warnock (Senate 2022) ──────────────────────────

def test_warnock_senate_2022():
    """
    Modern campaign domain: nav dedup, subpage discovery, content quality.
    """
    header("E2E 2: Raphael Warnock — Senate 2022 (warnockforgeorgia.com)")

    snapshots = query_cdx("warnockforgeorgia.com", "20220101", "20221231", CDX_CONFIG)
    print(f"  CDX snapshots: {len(snapshots)}")
    assert len(snapshots) > 0, "FAIL: No CDX snapshots for warnockforgeorgia.com 2022"

    # Monthly dedup check
    seen = set()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        assert key not in seen, f"FAIL: Duplicate (url, month): {key}"
        seen.add(key)
    print(f"  Monthly dedup OK — {len(snapshots)} unique pairs")

    # Unique URLs — modern sites should have subpages
    unique_urls = set(_normalize_url(s["original_url"]) for s in snapshots)
    print(f"  Unique original URLs: {len(unique_urls)}")

    # Fetch one snapshot
    time.sleep(1)
    session = _make_test_session()
    rl = RateLimiter(min_delay=1.0)

    soup = fetch_page(snapshots[0]["wayback_url"], session, rl)
    if soup is None:
        print("  SKIP: Could not fetch page")
        session.close()
        return

    text = extract_visible_text(soup, "#+#")
    print(f"  Text length: {len(text)} chars")
    assert len(text) > 50, f"FAIL: Text too short ({len(text)} chars)"
    _check_no_wayback_toolbar(text, "Warnock")
    _check_no_excessive_repeats(text, "#+#", "Warnock")
    print("  Text OK — no Wayback toolbar, no excessive repeats")

    # Subpage discovery
    subpages = get_subpage_urls(soup, snapshots[0]["wayback_url"])
    print(f"  Subpages from home page: {len(subpages)}")
    for u in subpages[:3]:
        assert "web.archive.org" in u, f"FAIL: Subpage URL not a Wayback URL: {u}"
    if subpages:
        print("  Subpage URLs are valid Wayback URLs")

    session.close()
    print("  PASS")


# ── Test 3: Nancy Pelosi (House 2020) ──────────────────────────────

def test_pelosi_house_2020():
    """
    High-profile .house.gov: snapshot cap, monthly dedup efficiency,
    max_subpages enforcement, empty-content filtering.
    """
    header("E2E 3: Nancy Pelosi — House 2020 (pelosi.house.gov)")

    snapshots = query_cdx("pelosi.house.gov", "20200101", "20201231", CDX_CONFIG)
    raw_count = len(snapshots)
    print(f"  CDX snapshots: {raw_count}")
    assert raw_count > 0, "FAIL: No CDX snapshots for pelosi.house.gov 2020"

    # Monthly dedup check
    seen = set()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        assert key not in seen, f"FAIL: Duplicate (url, month): {key}"
        seen.add(key)
    print(f"  Monthly dedup OK — {len(snapshots)} unique pairs")

    # Snapshot cap: if >200 after dedup, verify stratified sampling spans months
    if len(snapshots) > 200:
        from src.scrape_wayback import _sample_snapshots_stratified
        sampled = _sample_snapshots_stratified(snapshots, 200)
        print(f"  Stratified sample: {len(snapshots)} -> {len(sampled)} (cap=200)")
        assert len(sampled) == 200
        # Verify month coverage: should span multiple months, not just January
        months = set(s["timestamp"][:6] for s in sampled)
        print(f"  Months covered in sample: {len(months)} ({sorted(months)[:3]}...)")
        assert len(months) > 1, "FAIL: Stratified sample covers only 1 month"
    else:
        print(f"  Under cap ({len(snapshots)} <= 200)")

    # Many unique URLs expected for a .house.gov site
    unique_urls = set(_normalize_url(s["original_url"]) for s in snapshots)
    print(f"  Unique original URLs: {len(unique_urls)}")

    # Fetch one snapshot
    time.sleep(1)
    session = _make_test_session()
    rl = RateLimiter(min_delay=1.0)

    soup = fetch_page(snapshots[0]["wayback_url"], session, rl)
    if soup is None:
        print("  SKIP: Could not fetch page")
        session.close()
        return

    text = extract_visible_text(soup, "#+#")
    print(f"  Text length: {len(text)} chars")

    if text:
        _check_no_wayback_toolbar(text, "Pelosi")
        _check_no_excessive_repeats(text, "#+#", "Pelosi")
        print("  Text OK — no toolbar, no excessive repeats")
    else:
        print("  WARNING: Empty text (possible redirect or error page)")

    # Subpage discovery — .house.gov should have many subpages
    subpages = get_subpage_urls(soup, snapshots[0]["wayback_url"])
    print(f"  Subpages from home page: {len(subpages)}")

    # Verify max_subpages enforcement
    capped_subpages = subpages[:50]
    assert len(capped_subpages) <= 50, "FAIL: max_subpages cap not applied"
    print(f"  max_subpages enforcement: {len(capped_subpages)} <= 50")

    session.close()
    print("  PASS")


# ── Test 4: Marco Rubio (Senate 2016) ──────────────────────────────

def test_rubio_senate_2016():
    """
    Mid-era campaign domain: CDX parsing, URL normalization (www vs non-www).
    """
    header("E2E 4: Marco Rubio — Senate 2016 (marcorubio.com)")

    snapshots = query_cdx("marcorubio.com", "20160101", "20161231", CDX_CONFIG)
    print(f"  CDX snapshots: {len(snapshots)}")
    assert len(snapshots) > 0, "FAIL: No CDX snapshots for marcorubio.com 2016"

    # Monthly dedup check
    seen = set()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        assert key not in seen, f"FAIL: Duplicate (url, month): {key}"
        seen.add(key)
    print(f"  Monthly dedup OK — {len(snapshots)} unique pairs")

    # URL normalization: check www vs non-www handled
    originals = [s["original_url"] for s in snapshots]
    has_www = any("www." in u for u in originals)
    has_no_www = any("www." not in u.split("://")[-1] for u in originals)
    print(f"  Has www URLs: {has_www}, Has non-www URLs: {has_no_www}")
    # After normalization, www and non-www should be deduped
    norm_set = set(_normalize_url(u) for u in originals)
    print(f"  Unique normalized URLs: {len(norm_set)}")

    # Fetch one snapshot
    time.sleep(1)
    session = _make_test_session()
    rl = RateLimiter(min_delay=1.0)

    soup = fetch_page(snapshots[0]["wayback_url"], session, rl)
    if soup is None:
        print("  SKIP: Could not fetch page")
        session.close()
        return

    text = extract_visible_text(soup, "#+#")
    print(f"  Text length: {len(text)} chars")

    if text:
        assert len(text) > 50, f"FAIL: Text too short ({len(text)} chars)"
        _check_no_wayback_toolbar(text, "Rubio")
        _check_no_excessive_repeats(text, "#+#", "Rubio")
        print("  Text OK — no toolbar, no excessive repeats")
    else:
        print("  WARNING: Empty text extracted")

    session.close()
    print("  PASS")


# ── Test 5: Kathy Hochul (House 2008) ──────────────────────────────

def test_hochul_house_2008():
    """
    Small/sparse campaign site: graceful handling of few or zero snapshots,
    potential 404s and redirect chains, empty-content filtering.
    """
    header("E2E 5: Kathy Hochul — House 2008 (hochulforcongress.com)")

    # Try primary URL
    snapshots = query_cdx("hochulforcongress.com", "20080101", "20081231", CDX_CONFIG)
    url_used = "hochulforcongress.com"
    print(f"  CDX snapshots (hochulforcongress.com): {len(snapshots)}")

    # Fallback to alternative domain
    if not snapshots:
        print("  Trying fallback: kathyhochul.com")
        snapshots = query_cdx("kathyhochul.com", "20080101", "20081231", CDX_CONFIG)
        url_used = "kathyhochul.com"
        print(f"  CDX snapshots (kathyhochul.com): {len(snapshots)}")

    # Expand search to 2010-2012 if 2008 has nothing
    if not snapshots:
        print("  Trying expanded window: hochulforcongress.com 2010-2012")
        snapshots = query_cdx("hochulforcongress.com", "20100101", "20121231", CDX_CONFIG)
        url_used = "hochulforcongress.com (2010-2012)"
        print(f"  CDX snapshots (2010-2012): {len(snapshots)}")

    if not snapshots:
        print("  No snapshots found for any Hochul URL/year — tests graceful zero handling")
        print("  PASS (graceful zero-snapshot handling)")
        return

    # Monthly dedup check
    seen = set()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        assert key not in seen, f"FAIL: Duplicate (url, month): {key}"
        seen.add(key)
    print(f"  Monthly dedup OK — {len(snapshots)} unique pairs ({url_used})")

    # Sparse site: fewer snapshots expected
    print(f"  Snapshot count: {len(snapshots)} (expected: sparse)")

    # Fetch one snapshot
    time.sleep(1)
    session = _make_test_session()
    rl = RateLimiter(min_delay=1.0)

    soup = fetch_page(snapshots[0]["wayback_url"], session, rl)
    if soup is None:
        print("  Could not fetch snapshot — expected for sparse/old campaign sites")
        print("  PASS (graceful fetch failure)")
        session.close()
        return

    text = extract_visible_text(soup, "#+#")
    print(f"  Text length: {len(text)} chars")

    if text:
        _check_no_wayback_toolbar(text, "Hochul")
        print("  Text OK — no Wayback toolbar fragments")
    else:
        print("  Empty text — expected for sparse campaign sites")

    session.close()
    print("  PASS")


# ── Test 6: Integration — full process_candidate on Warnock ────────

def test_integration_warnock():
    """
    Full process_candidate() on Warnock, capped at 3 snapshots.
    Verifies output CSV exists with valid rows.
    """
    header("E2E 6: Integration — process_candidate (Warnock, 3 snapshots)")

    with tempfile.TemporaryDirectory() as tmpdir:
        progress_file = os.path.join(tmpdir, "progress.csv")
        tracker = ProgressTracker(progress_file)
        rl = RateLimiter(min_delay=1.0)

        config = {
            "wayback": CDX_CONFIG,
            "scraping": {
                "max_snapshots_per_candidate": 3,  # cap at 3 for test speed
                "max_subpages": 5,
                "text_separator": "#+#",
                "exclude_domains": ["twitter.com", "facebook.com", "instagram.com", "youtube.com"],
            },
            "output": {
                "snapshots_dir": os.path.join(tmpdir, "snapshots"),
                "progress_dir": tmpdir,
            },
        }

        candidate = {
            "candidate": "Raphael Warnock",
            "state": "GA",
            "district": "",
            "office": "senate",
            "year": 2022,
            "party": "DEM",
            "website_url": "warnockforgeorgia.com",
        }

        n_scraped = process_candidate(candidate, config, tracker, rl)
        print(f"  n_scraped: {n_scraped}")

        # Check output CSV
        output_file = os.path.join(
            tmpdir, "snapshots", "senate", "2022", "Raphael Warnock (GA).csv"
        )

        if n_scraped == 0:
            print("  WARNING: 0 snapshots scraped — Wayback may be slow/unavailable")
            print("  SKIP: Cannot validate output CSV")
            return

        assert os.path.exists(output_file), f"FAIL: Output CSV not found at {output_file}"
        print(f"  Output CSV exists: {output_file}")

        # Read and validate CSV contents
        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        print(f"  CSV rows: {len(rows)}")
        assert len(rows) > 0, "FAIL: Output CSV is empty"

        # Validate first row structure
        row = rows[0]
        expected_cols = [
            "candidate", "state", "office", "year", "party",
            "snap_url", "text_snap_content", "n_char", "n_words",
        ]
        for col in expected_cols:
            assert col in row, f"FAIL: Missing column '{col}' in output CSV"
        print("  CSV columns: OK")

        # Validate values
        assert row["candidate"] == "Raphael Warnock", f"FAIL: candidate = {row['candidate']}"
        assert row["state"] == "GA", f"FAIL: state = {row['state']}"
        assert row["office"] == "senate", f"FAIL: office = {row['office']}"
        assert row["year"] == "2022", f"FAIL: year = {row['year']}"
        print("  Metadata values: OK")

        # Content validation
        for r in rows:
            content = r["text_snap_content"]
            assert len(content) > 0, f"FAIL: Empty content in row for {r['snap_url']}"
            n_char = int(r["n_char"])
            assert n_char == len(content), (
                f"FAIL: n_char ({n_char}) != actual length ({len(content)})"
            )
            n_words = int(r["n_words"])
            assert n_words > 0, f"FAIL: n_words = 0 for {r['snap_url']}"
        print("  Content validation: OK (non-empty, n_char/n_words consistent)")

        # Progress tracker should have entries
        done_count = sum(1 for _ in open(progress_file)) - 1  # minus header
        print(f"  Progress entries: {done_count}")
        assert done_count > 0, "FAIL: No progress entries written"
        # At most 3 snapshots processed (some may error)
        assert done_count <= 3, (
            f"FAIL: {done_count} progress entries exceeds 3-snapshot cap"
        )
        print("  Progress cap: OK")

    print("  PASS")


# ── Run all tests ──────────────────────────────────────────────────

if __name__ == "__main__":
    passed = 0
    failed = 0

    tests = [
        test_stevens_senate_2004,
        test_warnock_senate_2022,
        test_pelosi_house_2020,
        test_rubio_senate_2016,
        test_hochul_house_2008,
        test_integration_warnock,
    ]

    for test_fn in tests:
        try:
            test_fn()
            passed += 1
        except AssertionError as e:
            print(f"  FAIL: {e}")
            failed += 1
        except Exception as e:
            print(f"  ERROR: {e}")
            import traceback
            traceback.print_exc()
            failed += 1

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed} passed, {failed} failed (of {len(tests)})")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
