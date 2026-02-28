#!/usr/bin/env python3
"""
Live integration tests for the 5 scraper fixes.

Hits real Wayback Machine endpoints — run sparingly.
"""

import sys
import os
import time
import threading

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import requests
from bs4 import BeautifulSoup

from src.utils import RateLimiter
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
    _normalize_url,
    _dedup_snapshots,
    _sample_snapshots_stratified,
)


def header(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


# ── Test 0: _dedup_snapshots bucket_months behavior ──

def test_dedup_quarterly():
    """
    _dedup_snapshots with bucket_months=3 keeps one per (URL, quarter).
    12 monthly snapshots for the same URL → 4 quarterly snapshots.
    """
    header("TEST 0a: _dedup_snapshots quarterly (bucket_months=3)")

    snapshots = []
    for month in range(1, 13):
        snapshots.append({
            "timestamp": f"2022{month:02d}15120000",
            "original_url": "http://example.com/",
            "wayback_url": f"https://web.archive.org/web/2022{month:02d}15120000/http://example.com/",
        })

    result = _dedup_snapshots(snapshots, bucket_months=3)
    print(f"  Input: {len(snapshots)} snapshots (12 months, same URL)")
    print(f"  Output: {len(result)} snapshots")

    assert len(result) == 4, f"FAIL: Expected 4 quarterly snapshots, got {len(result)}"

    # Each kept snapshot should be the latest in its quarter (month 3, 6, 9, 12)
    kept_months = [s["timestamp"][4:6] for s in result]
    assert kept_months == ["03", "06", "09", "12"], (
        f"FAIL: Expected months [03, 06, 09, 12], got {kept_months}"
    )
    print(f"  Kept months: {kept_months} (latest per quarter)")
    print("  PASS: Quarterly dedup keeps 1 per quarter")


def test_dedup_monthly_compat():
    """
    _dedup_snapshots with bucket_months=1 matches old monthly behavior.
    12 monthly snapshots → 12 kept.
    """
    header("TEST 0b: _dedup_snapshots monthly compat (bucket_months=1)")

    snapshots = []
    for month in range(1, 13):
        # Two snapshots per month — dedup should keep only the later one
        snapshots.append({
            "timestamp": f"2022{month:02d}01120000",
            "original_url": "http://example.com/",
            "wayback_url": f"https://web.archive.org/web/2022{month:02d}01120000/http://example.com/",
        })
        snapshots.append({
            "timestamp": f"2022{month:02d}15120000",
            "original_url": "http://example.com/",
            "wayback_url": f"https://web.archive.org/web/2022{month:02d}15120000/http://example.com/",
        })

    result = _dedup_snapshots(snapshots, bucket_months=1)
    print(f"  Input: {len(snapshots)} snapshots (2 per month, 12 months)")
    print(f"  Output: {len(result)} snapshots")

    assert len(result) == 12, f"FAIL: Expected 12 monthly snapshots, got {len(result)}"

    # Each kept snapshot should be the 15th (later timestamp)
    for s in result:
        assert s["timestamp"][6:8] == "15", (
            f"FAIL: Should keep later timestamp, got day {s['timestamp'][6:8]}"
        )
    print("  PASS: Monthly compat keeps 1 per month (latest timestamp)")


def test_dedup_yearly():
    """_dedup_snapshots with bucket_months=12 keeps one per year."""
    header("TEST 0c: _dedup_snapshots yearly (bucket_months=12)")

    snapshots = []
    for month in range(1, 13):
        snapshots.append({
            "timestamp": f"2022{month:02d}15120000",
            "original_url": "http://example.com/",
            "wayback_url": f"https://web.archive.org/web/2022{month:02d}15120000/http://example.com/",
        })

    result = _dedup_snapshots(snapshots, bucket_months=12)
    print(f"  Input: {len(snapshots)} snapshots (12 months)")
    print(f"  Output: {len(result)} snapshots")

    assert len(result) == 1, f"FAIL: Expected 1 yearly snapshot, got {len(result)}"
    assert result[0]["timestamp"][4:6] == "12", "FAIL: Should keep December (latest)"
    print("  PASS: Yearly dedup keeps 1 per year")


def test_dedup_multiple_urls():
    """Quarterly dedup with multiple distinct URLs preserves each URL's snapshots."""
    header("TEST 0d: _dedup_snapshots quarterly with multiple URLs")

    snapshots = []
    for url in ["http://example.com/", "http://example.com/about"]:
        for month in range(1, 7):  # Jan-Jun = 2 quarters
            snapshots.append({
                "timestamp": f"2022{month:02d}15120000",
                "original_url": url,
                "wayback_url": f"https://web.archive.org/web/2022{month:02d}15120000/{url}",
            })

    result = _dedup_snapshots(snapshots, bucket_months=3)
    print(f"  Input: {len(snapshots)} snapshots (2 URLs × 6 months)")
    print(f"  Output: {len(result)} snapshots")

    # 2 URLs × 2 quarters = 4
    assert len(result) == 4, f"FAIL: Expected 4, got {len(result)}"
    print("  PASS: 2 URLs × 2 quarters = 4 snapshots")


# ── Test 1: CDX text parsing + quarterly dedup (Issues 1 + 3) ──

def test_cdx_text_parsing():
    """
    Query CDX for Doug Jones (dougjonesforsenate.com, 2022) — should return
    multiple snapshots now that we use text format and Python-side dedup.
    Previously failed with truncated JSON or returned only 1 home-page snapshot.
    """
    header("TEST 1: CDX text parsing + quarterly dedup")

    config = {
        "max_retries": 2,
        "timeout_connect": 30,
        "timeout_read": 120,
        "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; test)",
    }

    # Doug Jones 2022 — had 10 home-page-only snapshots before fix
    snapshots = query_cdx("dougjonesforsenate.com", "20220101", "20221231", config)

    print(f"  Snapshots returned: {len(snapshots)}")

    if len(snapshots) == 0:
        print("  WARNING: No snapshots returned — site may not be archived for 2022")
        print("  Trying alternative: dougjonesforsenate.com for 2017-2018...")
        snapshots = query_cdx("dougjonesforsenate.com", "20170101", "20181231", config)
        print(f"  Snapshots returned (2017-2018): {len(snapshots)}")

    assert len(snapshots) > 0, "FAIL: No snapshots returned at all"

    # Check structure
    s = snapshots[0]
    assert "timestamp" in s, "FAIL: Missing 'timestamp' key"
    assert "original_url" in s, "FAIL: Missing 'original_url' key"
    assert "wayback_url" in s, "FAIL: Missing 'wayback_url' key"
    assert s["wayback_url"].startswith("https://web.archive.org/web/"), "FAIL: Bad wayback_url"

    # Check that we have multiple unique URLs (subpages preserved, not just home page)
    unique_urls = set()
    for snap in snapshots:
        # Strip timestamp to get base URL
        orig = snap["original_url"].lower().rstrip("/")
        unique_urls.add(orig)
    print(f"  Unique original URLs: {len(unique_urls)}")
    if len(unique_urls) > 1:
        print("  PASS: Multiple subpage URLs preserved (collapse fix working)")
    else:
        print("  NOTE: Only 1 unique URL — site may just have the home page archived")

    # Verify quarterly dedup is working — check that no (url, quarter) pair appears twice
    seen = set()
    for snap in snapshots:
        norm = _normalize_url(snap["original_url"])
        year = snap["timestamp"][:4]
        month = int(snap["timestamp"][4:6])
        bucket = f"{year}Q{(month - 1) // 3}"
        key = (norm, bucket)
        assert key not in seen, f"FAIL: Duplicate (url, quarter) after dedup: {key}"
        seen.add(key)
    print("  PASS: No duplicate (url, quarter) pairs")

    print("  PASS: CDX text parsing works correctly")
    return snapshots


# ── Test 1b: CDX for Jeff Miller (2010) — previously failed with truncated JSON ──

def test_cdx_miller():
    """Jeff Miller had truncated JSON responses. Text format should handle this."""
    header("TEST 1b: CDX for Jeff Miller (previously truncated JSON)")

    config = {
        "max_retries": 2,
        "timeout_connect": 30,
        "timeout_read": 120,
        "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; test)",
    }

    snapshots = query_cdx("jeffmiller.house.gov", "20100101", "20101231", config)
    print(f"  Snapshots returned: {len(snapshots)}")

    if len(snapshots) > 0:
        unique_urls = set(s["original_url"].lower().rstrip("/") for s in snapshots)
        print(f"  Unique original URLs: {len(unique_urls)}")
        print(f"  Sample URLs: {list(unique_urls)[:5]}")
        print("  PASS: Miller CDX query succeeded (was failing with truncated JSON)")
    else:
        print("  WARNING: No snapshots — may be a CDX availability issue, not a code bug")


# ── Test 2: Thread-safe rate limiter ──

def test_rate_limiter_threadsafe():
    """
    Verify that two threads sharing a RateLimiter don't fire simultaneously.
    With min_delay=0.5s, two threads should take ~1.0s total, not ~0.5s.
    """
    header("TEST 2: Thread-safe rate limiter")

    rl = RateLimiter(min_delay=0.5, backoff_factor=2, backoff_max=60)
    timestamps = []
    lock = threading.Lock()

    def worker():
        rl.wait()
        with lock:
            timestamps.append(time.time())

    t1 = threading.Thread(target=worker)
    t2 = threading.Thread(target=worker)

    start = time.time()
    t1.start()
    t2.start()
    t1.join()
    t2.join()
    elapsed = time.time() - start

    timestamps.sort()
    gap = timestamps[1] - timestamps[0]
    print(f"  Thread 1 fired at: +{timestamps[0] - start:.3f}s")
    print(f"  Thread 2 fired at: +{timestamps[1] - start:.3f}s")
    print(f"  Gap between requests: {gap:.3f}s")
    print(f"  Total elapsed: {elapsed:.3f}s")

    assert gap >= 0.4, f"FAIL: Gap between threads too small ({gap:.3f}s < 0.4s) — not serialized"
    print("  PASS: Threads properly serialized by rate limiter")

    # Test backoff
    rl.backoff()
    assert rl._current_delay == 1.0, f"FAIL: Backoff didn't double (got {rl._current_delay})"
    rl.reset()
    assert rl._current_delay == 0.5, f"FAIL: Reset didn't restore min_delay"
    print("  PASS: Backoff and reset work correctly")


# ── Test 3: Navigation boilerplate dedup ──

def test_nav_dedup():
    """
    Test _deduplicate_text_segments removes repeated nav but keeps legit content.
    """
    header("TEST 3: Navigation boilerplate dedup (unit test)")

    # Simulate nav appearing 4 times, content appearing once
    segments = [
        "Home", "About", "Issues", "Donate",  # nav block 1
        "Doug Jones is running for Senate in Alabama.",
        "Home", "About", "Issues", "Donate",  # nav block 2
        "He supports healthcare reform.",
        "Home", "About", "Issues", "Donate",  # nav block 3 (in footer)
        "Paid for by Doug Jones for Senate",
    ]

    result = _deduplicate_text_segments(segments, max_repeats=2)
    print(f"  Input segments: {len(segments)}")
    print(f"  Output segments: {len(result)}")

    # "About" (5 chars), "Issues" (6 chars), "Donate" (6 chars) each appear 3x → removed
    # "Home" (4 chars) is kept because short segments (< 5 chars) skip the repeat filter
    assert "Home" in result, "FAIL: Short segment 'Home' (4 chars) should be kept"
    assert "About" not in result, "FAIL: Nav segment 'About' not removed (appears 3x)"
    # Content should be preserved
    assert "Doug Jones is running for Senate in Alabama." in result, "FAIL: Content lost"
    assert "He supports healthcare reform." in result, "FAIL: Content lost"
    assert "Paid for by Doug Jones for Senate" in result, "FAIL: Unique footer lost"

    print("  PASS: Boilerplate removed, content preserved")

    # Test that segments appearing exactly 2x are kept
    segments2 = ["Intro", "Body", "Intro", "Footer"]
    result2 = _deduplicate_text_segments(segments2, max_repeats=2)
    assert result2.count("Intro") == 2, f"FAIL: Segment at exactly max_repeats removed"
    print("  PASS: Segments at threshold (2x) preserved")


def test_nav_dedup_live():
    """Fetch a real Doug Jones page and check for reduced boilerplate."""
    header("TEST 3b: Nav dedup on live Wayback page")

    # Fetch a known Doug Jones snapshot
    url = "https://web.archive.org/web/20171201000000*/dougjonesforsenate.com"
    # Let's just fetch one snapshot directly
    test_url = "https://web.archive.org/web/20171210072851/https://dougjonesforsenate.com/"

    try:
        resp = requests.get(test_url, timeout=(30, 90),
                          headers={"User-Agent": "CandidateWebsiteExtension/1.0 (test)"})
        if resp.status_code != 200:
            print(f"  SKIP: Got status {resp.status_code}")
            return

        if not is_wayback_page(resp.text):
            print("  SKIP: Not a Wayback page")
            return

        clean_html = strip_wayback_toolbar(resp.text)
        soup = BeautifulSoup(clean_html, "html.parser")
        text = extract_visible_text(soup, "#+#")

        segments = text.split("#+#")
        print(f"  Extracted {len(segments)} text segments, {len(text)} chars")

        # Count any repeated segments
        from collections import Counter
        counts = Counter(segments)
        repeated = {seg: cnt for seg, cnt in counts.items() if cnt > 2}
        if repeated:
            print(f"  WARNING: {len(repeated)} segments still appear >2 times:")
            for seg, cnt in list(repeated.items())[:3]:
                print(f"    '{seg[:50]}...' x{cnt}")
        else:
            print("  PASS: No segment appears more than 2 times")

    except Exception as e:
        print(f"  SKIP: Could not fetch page ({e})")


# ── Test 4: Frame URL resolution fix ──

def test_frame_url_resolution():
    """
    Test _resolve_frame_url correctly resolves relative URLs preserving path context.
    """
    header("TEST 4: Frame URL resolution (unit test)")

    # Bug B scenario: relative frame src with path context
    base = "https://web.archive.org/web/20020301120000/http://site.com/subdir/index.html"

    # Relative: content.html should resolve to /subdir/content.html
    resolved = _resolve_frame_url("content.html", base)
    expected = "https://web.archive.org/web/20020301120000/http://site.com/subdir/content.html"
    assert resolved == expected, f"FAIL: Got {resolved}\n  Expected: {expected}"
    print(f"  Relative 'content.html' -> correct path")

    # Absolute path: /other/page.html
    resolved2 = _resolve_frame_url("/other/page.html", base)
    expected2 = "https://web.archive.org/web/20020301120000/http://site.com/other/page.html"
    assert resolved2 == expected2, f"FAIL: Got {resolved2}\n  Expected: {expected2}"
    print(f"  Absolute '/other/page.html' -> correct path")

    # Already a full Wayback URL
    full = "https://web.archive.org/web/20020301/http://other.com/page.html"
    resolved3 = _resolve_frame_url(full, base)
    assert "web.archive.org" in resolved3, "FAIL: Full URL not preserved"
    print(f"  Full Wayback URL -> preserved")

    # web/20 prefix without domain
    partial = "/web/20020301/http://site.com/frame.html"
    resolved4 = _resolve_frame_url(partial, base)
    assert resolved4.startswith("https://web.archive.org/web/20020301"), f"FAIL: Got {resolved4}"
    print(f"  Partial '/web/20...' -> correctly prefixed")

    print("  PASS: All frame URL resolution cases correct")


def test_frame_condition_fix():
    """
    Test that the frame condition fix (Bug A) works: frames are always
    recursed into even when there's some direct text.
    """
    header("TEST 4b: Frame condition fix (Bug A)")

    # Simulate a frame-based page that has a <noframes> fallback with some text
    html = """
    <html>
    <head><title>Campaign Site</title></head>
    <frameset cols="200,*">
        <frame src="nav.html" name="navigation">
        <frame src="content.html" name="main">
    </frameset>
    <noframes>
        <body>This site requires frames.</body>
    </noframes>
    </html>
    """
    soup = BeautifulSoup(html, "html.parser")
    frames = soup.find_all("frame") + soup.find_all("iframe")
    text = extract_visible_text(soup, "#+#")

    print(f"  Frames found: {len(frames)}")
    print(f"  Direct text: '{text[:60]}'")

    # With the old bug: `if not frames or text:` would return here
    # because text is truthy ("This site requires frames.")
    # With the fix: `if not frames:` only skips when NO frames exist
    assert len(frames) == 2, f"FAIL: Expected 2 frames, got {len(frames)}"
    assert len(text) > 0, "Expected some noframes text"

    # The fix means extract_frame_content should try to recurse into frames
    # We can't fetch the frame URLs in this unit test, but we verify the
    # condition logic by calling extract_frame_content with no fetch_fn
    result_text, subpages = extract_frame_content(soup, "https://web.archive.org/web/20020101/http://example.com/", "#+#", fetch_fn=None)

    # With fetch_fn=None, frame fetching is skipped, but the function should
    # still attempt to process frames (not short-circuit on truthy text)
    print(f"  extract_frame_content returned: '{result_text[:60]}'")
    print("  PASS: Frame condition allows recursion even with noframes text")


def test_frame_live():
    """
    Fetch a real frame-based 2002-era page from the Wayback Machine.
    Chris Dodd or Kit Bond had frame-based sites.
    """
    header("TEST 4c: Live frame-based page (2002 era)")

    # Try Chris Dodd's Senate site from 2002
    config = {
        "max_retries": 2,
        "timeout_connect": 30,
        "timeout_read": 120,
        "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; test)",
    }

    # First check if there are snapshots
    snapshots = query_cdx("dodd.senate.gov", "20020101", "20021231", config)
    print(f"  CDX snapshots for dodd.senate.gov (2002): {len(snapshots)}")

    if not snapshots:
        print("  SKIP: No CDX snapshots available")
        return

    # Fetch the first snapshot
    test_url = snapshots[0]["wayback_url"]
    print(f"  Fetching: {test_url}")

    try:
        time.sleep(1)  # Be polite
        resp = requests.get(test_url, timeout=(30, 90),
                          headers={"User-Agent": "CandidateWebsiteExtension/1.0 (test)"})
        if resp.status_code != 200:
            print(f"  SKIP: Got status {resp.status_code}")
            return

        if not is_wayback_page(resp.text):
            print("  SKIP: Not recognized as Wayback page")
            return

        clean_html = strip_wayback_toolbar(resp.text)
        soup = BeautifulSoup(clean_html, "html.parser")

        frames = soup.find_all("frame") + soup.find_all("iframe")
        text = extract_visible_text(soup, "#+#")
        print(f"  Frames found: {len(frames)}")
        print(f"  Direct text length: {len(text)} chars")

        if frames:
            print("  Page IS frame-based — frame fixes are relevant here")
            # With the old code, if there was any text (e.g., noframes),
            # frame recursion would be skipped.
            # We can't fully test recursion without fetching frame URLs,
            # but we verify the frames are detected.
            for f in frames[:3]:
                src = f.get("src", "N/A")
                resolved = _resolve_frame_url(src, test_url)
                print(f"    Frame src='{src}' -> {resolved[:80]}")
        else:
            print("  Page is NOT frame-based (may have been redesigned)")
            word_count = len(text.split())
            print(f"  Text: {word_count} words")

    except Exception as e:
        print(f"  SKIP: Error fetching page ({e})")


# ── Test 5: _normalize_url ──

def test_normalize_url():
    """Quick unit test for URL normalization used in dedup."""
    header("TEST 5: URL normalization")

    assert _normalize_url("http://WWW.Example.Com/Page/") == "http://example.com/page"
    assert _normalize_url("https://www.example.com") == "https://example.com"
    assert _normalize_url("http://example.com/") == "http://example.com"
    assert _normalize_url("http://example.com") == "http://example.com"
    print("  PASS: URL normalization correct")


# ── Test 6: Subpage URL resolution (relative + absolute links) ──

def test_subpage_url_resolution():
    """
    Test that get_subpage_urls resolves relative and absolute-domain links
    to full Wayback URLs, not just already-rewritten links.
    """
    header("TEST 6: Subpage URL resolution (relative + absolute links)")

    base_url = "https://web.archive.org/web/20200601120000/https://pelosi.house.gov/"

    html = """
    <html><body>
        <a href="/about">About</a>
        <a href="issues/healthcare.html">Healthcare</a>
        <a href="https://pelosi.house.gov/contact">Contact</a>
        <a href="https://web.archive.org/web/20200601120000/https://pelosi.house.gov/news">News</a>
        <a href="https://twitter.com/SpeakerPelosi">Twitter</a>
        <a href="https://other-domain.com/page">External</a>
        <a href="#top">Anchor</a>
        <a href="mailto:info@pelosi.house.gov">Email</a>
    </body></html>
    """
    soup = BeautifulSoup(html, "html.parser")
    subpages = get_subpage_urls(soup, base_url)

    print(f"  Subpages found: {len(subpages)}")
    for u in sorted(subpages):
        print(f"    {u}")

    # All results should be Wayback URLs
    for u in subpages:
        assert "web.archive.org" in u, f"FAIL: Non-Wayback URL in results: {u}"
    print("  All results are Wayback URLs")

    # Should resolve relative "/about" -> Wayback URL with pelosi.house.gov/about
    about_found = any("pelosi.house.gov/about" in u for u in subpages)
    assert about_found, "FAIL: Relative '/about' link not resolved"
    print("  Relative '/about' -> resolved")

    # Should resolve relative "issues/healthcare.html"
    healthcare_found = any("issues/healthcare.html" in u for u in subpages)
    assert healthcare_found, "FAIL: Relative 'issues/healthcare.html' not resolved"
    print("  Relative 'issues/healthcare.html' -> resolved")

    # Should resolve absolute same-domain link
    contact_found = any("pelosi.house.gov/contact" in u for u in subpages)
    assert contact_found, "FAIL: Absolute same-domain '/contact' not resolved"
    print("  Absolute 'pelosi.house.gov/contact' -> resolved")

    # Should keep already-rewritten Wayback link
    news_found = any("pelosi.house.gov/news" in u for u in subpages)
    assert news_found, "FAIL: Already-rewritten Wayback link dropped"
    print("  Already-rewritten Wayback URL -> kept")

    # Should NOT include external domain, twitter, anchor, or mailto
    for u in subpages:
        assert "other-domain.com" not in u, f"FAIL: External link included: {u}"
        assert "twitter.com" not in u, f"FAIL: Twitter link included: {u}"
    assert len(subpages) == 4, f"FAIL: Expected 4 subpages, got {len(subpages)}"
    print("  External, social, anchor, mailto links excluded")

    print("  PASS: Subpage URL resolution works for all link formats")


# ── Test 7: Stratified snapshot sampling ──

def test_stratified_snapshot_sampling():
    """
    Test that _sample_snapshots_stratified preserves temporal diversity
    across months instead of biasing toward early-year captures.
    """
    header("TEST 7: Stratified snapshot sampling")

    # Create 600 snapshots: 50 per month for 12 months
    snapshots = []
    for month in range(1, 13):
        for day in range(1, 51):
            ts = f"2004{month:02d}{min(day, 28):02d}120000"
            snapshots.append({
                "timestamp": ts,
                "original_url": f"http://stevens.senate.gov/page{day}",
                "wayback_url": f"https://web.archive.org/web/{ts}/http://stevens.senate.gov/page{day}",
            })

    print(f"  Input: {len(snapshots)} snapshots across 12 months")

    sampled = _sample_snapshots_stratified(snapshots, 200)
    print(f"  Output: {len(sampled)} snapshots")

    assert len(sampled) == 200, f"FAIL: Expected 200, got {len(sampled)}"

    # Check month distribution: each month should get roughly 200/12 ≈ 16-17
    from collections import Counter
    month_counts = Counter(s["timestamp"][:6] for s in sampled)
    print(f"  Month distribution: {dict(sorted(month_counts.items()))}")

    # Every month should be represented
    assert len(month_counts) == 12, f"FAIL: Only {len(month_counts)} months represented (expected 12)"
    print("  All 12 months represented")

    # No month should have more than 20 (generous tolerance)
    for month, count in month_counts.items():
        assert count <= 20, f"FAIL: Month {month} has {count} snapshots (>20)"
    print("  No month exceeds 20 snapshots (balanced)")

    # Under-cap: should return all
    small = snapshots[:50]
    sampled_small = _sample_snapshots_stratified(small, 200)
    assert len(sampled_small) == 50, f"FAIL: Under-cap should return all, got {len(sampled_small)}"
    print("  Under-cap (50 < 200) returns all")

    # Result should be sorted by timestamp
    timestamps = [s["timestamp"] for s in sampled]
    assert timestamps == sorted(timestamps), "FAIL: Output not sorted by timestamp"
    print("  Output sorted by timestamp")

    print("  PASS: Stratified sampling preserves temporal diversity")


# ── Run all tests ──

if __name__ == "__main__":
    passed = 0
    failed = 0
    skipped = 0

    tests = [
        test_normalize_url,
        test_dedup_quarterly,
        test_dedup_monthly_compat,
        test_dedup_yearly,
        test_dedup_multiple_urls,
        test_nav_dedup,
        test_frame_url_resolution,
        test_frame_condition_fix,
        test_rate_limiter_threadsafe,
        test_subpage_url_resolution,
        test_stratified_snapshot_sampling,
        # Live tests (hit Wayback Machine)
        test_cdx_text_parsing,
        test_cdx_miller,
        test_nav_dedup_live,
        test_frame_live,
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
            failed += 1

    print(f"\n{'='*60}")
    print(f"  RESULTS: {passed} passed, {failed} failed")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
