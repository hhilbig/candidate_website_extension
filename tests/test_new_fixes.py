#!/usr/bin/env python3
"""
Unit tests for Issues A-F: ProgressTracker thread safety, snapshot cap,
frame depth limit, empty-content filtering, short-segment dedup, stale comment.
"""

import os
import sys
import tempfile
import threading

# Add project root to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import MagicMock, patch
from bs4 import BeautifulSoup

from src.utils import ProgressTracker
from src.extract_text import (
    _deduplicate_text_segments,
    extract_frame_content,
)


def header(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


# ── Issue A: ProgressTracker thread safety ──

def test_progress_tracker_thread_safety():
    """Two threads calling mark_done + is_done concurrently must not race."""
    header("Issue A: ProgressTracker thread safety")

    with tempfile.TemporaryDirectory() as tmpdir:
        progress_file = os.path.join(tmpdir, "progress.csv")
        tracker = ProgressTracker(progress_file)

        errors = []
        n_per_thread = 50

        def worker(thread_id):
            for i in range(n_per_thread):
                url = f"http://example.com/{thread_id}/{i}"
                if not tracker.is_done(url):
                    tracker.mark_done({
                        "url": url,
                        "candidate": f"test_{thread_id}",
                        "state": "XX",
                        "office": "test",
                        "year": 2024,
                        "scrape_complete": 1,
                        "scrape_error": 0,
                    })

        threads = [threading.Thread(target=worker, args=(t,)) for t in range(4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Verify all URLs are marked done
        total_expected = 4 * n_per_thread
        done_count = sum(
            1 for t in range(4) for i in range(n_per_thread)
            if tracker.is_done(f"http://example.com/{t}/{i}")
        )
        assert done_count == total_expected, (
            f"FAIL: {done_count}/{total_expected} URLs marked done"
        )
        print(f"  All {total_expected} URLs correctly marked done")

        # Verify checkpoint file has correct number of rows
        with open(progress_file, "r") as f:
            lines = f.readlines()
        # Header + data rows
        data_rows = len(lines) - 1
        assert data_rows == total_expected, (
            f"FAIL: Expected {total_expected} data rows, got {data_rows}"
        )
        print(f"  Checkpoint file has {data_rows} rows (correct)")

    print("  PASS: ProgressTracker is thread-safe")


# ── Issue B: Snapshot cap ──

def test_snapshot_cap():
    """process_candidate should cap snapshots at max_snapshots_per_candidate."""
    header("Issue B: Per-candidate snapshot cap")

    # Create 500 fake snapshots spread across 12 months
    month_day_pairs = [(m, d) for m in range(1, 13) for d in range(1, 29)][:500]
    fake_snapshots = [
        {
            "timestamp": f"2022{m:02d}{d:02d}120000",
            "original_url": f"http://example.com/page{i}",
            "wayback_url": f"https://web.archive.org/web/2022{m:02d}{d:02d}120000/http://example.com/page{i}",
        }
        for i, (m, d) in enumerate(month_day_pairs)
    ]
    # Sort by timestamp to match what _dedup_snapshots_monthly returns
    fake_snapshots.sort(key=lambda s: s["timestamp"])

    with tempfile.TemporaryDirectory() as tmpdir:
        progress_file = os.path.join(tmpdir, "progress.csv")
        tracker = ProgressTracker(progress_file)

        config = {
            "wayback": {
                "max_retries": 1,
                "timeout_connect": 5,
                "timeout_read": 10,
                "user_agent": "test",
            },
            "scraping": {
                "max_snapshots_per_candidate": 200,
                "max_subpages": 5,
                "text_separator": "#+#",
                "exclude_domains": [],
            },
            "output": {
                "snapshots_dir": os.path.join(tmpdir, "snapshots"),
                "progress_dir": tmpdir,
            },
        }

        # Mock query_cdx to return 500 snapshots, and scrape_snapshot to return
        # minimal content. We only need to verify the cap logic.
        with patch("src.scrape_wayback.query_cdx", return_value=fake_snapshots) as mock_cdx, \
             patch("src.scrape_wayback.scrape_snapshot", return_value=[{"snap_url": "u", "snap_content": "text"}]) as mock_scrape, \
             patch("src.scrape_wayback._make_session"):

            from src.scrape_wayback import process_candidate
            from src.utils import RateLimiter

            candidate = {
                "candidate": "Test Person",
                "state": "XX",
                "district": "01",
                "office": "house",
                "year": 2022,
                "party": "DEM",
                "website_url": "http://example.com",
            }

            rate_limiter = RateLimiter(min_delay=0.0)
            n_scraped = process_candidate(candidate, config, tracker, rate_limiter)

            # scrape_snapshot should have been called at most 200 times
            assert mock_scrape.call_count <= 200, (
                f"FAIL: scrape_snapshot called {mock_scrape.call_count} times, expected <= 200"
            )
            print(f"  scrape_snapshot called {mock_scrape.call_count} times (capped from 500)")

    print("  PASS: Snapshot cap works correctly")


# ── Issue C: Frame recursion depth limit ──

def test_frame_depth_limit():
    """Nested frames beyond max_depth should be skipped."""
    header("Issue C: Frame recursion depth limit")

    # Create HTML with a frame at each level
    def make_frame_html(level):
        if level == 0:
            return "<html><body>Leaf content at level 0</body></html>"
        return f"""
        <html>
        <frameset>
            <frame src="level{level - 1}.html">
        </frameset>
        <body>Text at level {level}</body>
        </html>
        """

    fetch_calls = []

    def mock_fetch(url):
        fetch_calls.append(url)
        # Extract level from URL
        for i in range(10):
            if f"level{i}" in url:
                html = make_frame_html(i)
                return BeautifulSoup(html, "html.parser")
        return None

    # Start at depth 5 with max_depth=3
    top_html = make_frame_html(5)
    top_soup = BeautifulSoup(top_html, "html.parser")

    text, subpages = extract_frame_content(
        top_soup,
        "https://web.archive.org/web/20020101/http://example.com/level5.html",
        "#+#",
        fetch_fn=mock_fetch,
        max_depth=3,
    )

    print(f"  Fetch calls made: {len(fetch_calls)}")
    print(f"  URLs fetched: {fetch_calls}")

    # max_depth=3: fetch level4, fetch level3, fetch level2 (at depth 1, gets level2
    # which has a frame to level1 but max_depth is now 0 so it stops).
    # So we should see at most 3 fetch calls (levels 4, 3, 2)
    assert len(fetch_calls) <= 3, (
        f"FAIL: Made {len(fetch_calls)} fetch calls, expected <= 3 with max_depth=3"
    )
    print(f"  PASS: Recursion stopped at depth limit (max 3 fetches, got {len(fetch_calls)})")

    # Also test max_depth=0 returns immediately
    text_zero, sub_zero = extract_frame_content(
        top_soup, "http://example.com", "#+#", mock_fetch, max_depth=0
    )
    assert text_zero == "" and sub_zero == [], (
        f"FAIL: max_depth=0 should return ('', []), got ('{text_zero}', {sub_zero})"
    )
    print("  PASS: max_depth=0 returns empty immediately")


# ── Issue D: Empty-content filtering ──

def test_empty_content_filtering():
    """Empty snap_content rows should not be written to CSV."""
    header("Issue D: Empty-content snapshot filtering")

    with tempfile.TemporaryDirectory() as tmpdir:
        progress_file = os.path.join(tmpdir, "progress.csv")
        tracker = ProgressTracker(progress_file)

        config = {
            "wayback": {
                "max_retries": 1,
                "timeout_connect": 5,
                "timeout_read": 10,
                "user_agent": "test",
            },
            "scraping": {
                "max_snapshots_per_candidate": 200,
                "max_subpages": 5,
                "text_separator": "#+#",
                "exclude_domains": [],
            },
            "output": {
                "snapshots_dir": os.path.join(tmpdir, "snapshots"),
                "progress_dir": tmpdir,
            },
        }

        # Mock: scrape_snapshot returns one empty page and one with content
        fake_snapshots = [
            {
                "timestamp": "20220601120000",
                "original_url": "http://example.com",
                "wayback_url": "https://web.archive.org/web/20220601120000/http://example.com",
            },
            {
                "timestamp": "20220701120000",
                "original_url": "http://example.com",
                "wayback_url": "https://web.archive.org/web/20220701120000/http://example.com",
            },
        ]

        call_count = [0]

        def mock_scrape(url, session, rl, cfg):
            call_count[0] += 1
            if call_count[0] == 1:
                return [{"snap_url": url, "snap_content": ""}]  # Empty
            return [{"snap_url": url, "snap_content": "Real content here"}]

        with patch("src.scrape_wayback.query_cdx", return_value=fake_snapshots), \
             patch("src.scrape_wayback.scrape_snapshot", side_effect=mock_scrape), \
             patch("src.scrape_wayback._make_session"):

            from src.scrape_wayback import process_candidate
            from src.utils import RateLimiter

            candidate = {
                "candidate": "Test Person",
                "state": "XX",
                "district": "01",
                "office": "house",
                "year": 2022,
                "party": "DEM",
                "website_url": "http://example.com",
            }

            rate_limiter = RateLimiter(min_delay=0.0)
            n_scraped = process_candidate(candidate, config, tracker, rate_limiter)

            # Only 1 snapshot should have been written (the one with content)
            assert n_scraped == 1, f"FAIL: Expected 1 scraped, got {n_scraped}"
            print(f"  n_scraped = {n_scraped} (only non-empty snapshot counted)")

            # Both should be marked done in progress (so we don't re-scrape)
            assert tracker.is_done(fake_snapshots[0]["wayback_url"]), (
                "FAIL: Empty snapshot not marked done in progress"
            )
            assert tracker.is_done(fake_snapshots[1]["wayback_url"]), (
                "FAIL: Non-empty snapshot not marked done in progress"
            )
            print("  Both snapshots marked done in progress tracker")

    print("  PASS: Empty-content rows filtered, progress still tracked")


# ── Issue E: Short-segment dedup edge case ──

def test_short_segment_dedup():
    """Short segments (< 5 chars) should not be removed by repeat filter."""
    header("Issue E: Short-segment dedup edge case")

    segments = [
        "and", "and", "and",   # 3 chars, appears 3x — should be KEPT
        "Home", "Home", "Home",  # 4 chars, appears 3x — should be KEPT
        "About Us", "About Us", "About Us",  # 8 chars, appears 3x — should be REMOVED
        "Real content here",
    ]

    result = _deduplicate_text_segments(segments, max_repeats=2)

    # "and" (3 chars < 5) should survive despite 3 repeats
    assert "and" in result, "FAIL: 'and' (3 chars) was incorrectly removed"
    print("  'and' (3 chars, 3x) -> kept")

    # "Home" (4 chars < 5) should survive despite 3 repeats
    assert "Home" in result, "FAIL: 'Home' (4 chars) was incorrectly removed"
    print("  'Home' (4 chars, 3x) -> kept")

    # "About Us" (8 chars >= 5) should be removed (3 > max_repeats=2)
    assert "About Us" not in result, "FAIL: 'About Us' (8 chars, 3x) was not removed"
    print("  'About Us' (8 chars, 3x) -> removed")

    # Real content always kept
    assert "Real content here" in result, "FAIL: Unique content was removed"
    print("  'Real content here' (unique) -> kept")

    print("  PASS: Short-segment dedup works correctly")


# ── Run all tests ──

if __name__ == "__main__":
    passed = 0
    failed = 0

    tests = [
        test_progress_tracker_thread_safety,
        test_snapshot_cap,
        test_frame_depth_limit,
        test_empty_content_filtering,
        test_short_segment_dedup,
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
    print(f"  RESULTS: {passed} passed, {failed} failed")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
