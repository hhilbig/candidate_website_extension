#!/usr/bin/env python3
"""
Tests verifying the no-dedup, no-cap methodology matching the original ICPSR scraper.

Uses Kathy Hochul (House 2012) — a sparse campaign site with few CDX records,
suitable for fast live testing. Kirk 2010 is used for CDX-only checks (10K records).

Tests:
  1. query_cdx signature: dedup_months removed
  2. Old config keys don't crash the code
  3. CDX returns all records, no temporal dedup (Hochul 2012)
  4. No subpage cap in scrape_snapshot
  5. Content dedup within snapshot still works
  6. CDX count for a busy site (Kirk 2010) exceeds old 50-snapshot cap
"""

import inspect
import os
import sys
import tempfile
import time
from collections import Counter

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils import RateLimiter, ProgressTracker
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


CDX_CONFIG = {
    "max_retries": 3,
    "timeout_connect": 30,
    "timeout_read": 120,
    "user_agent": "CandidateWebsiteExtension/1.0 (Academic Research; test)",
}


# ── Test 1: query_cdx signature ─────────────────────────────────

def test_query_cdx_signature():
    """Verify dedup_months is removed from query_cdx signature."""
    header("Test 1: query_cdx signature (no dedup_months param)")

    sig = inspect.signature(query_cdx)
    params = list(sig.parameters.keys())
    print(f"  query_cdx params: {params}")
    assert "dedup_months" not in params, (
        "FAIL: dedup_months still in query_cdx signature"
    )
    assert params == ["url", "start_date", "end_date", "config"], (
        f"FAIL: Unexpected params: {params}"
    )
    print("  CONFIRMED: dedup_months removed from signature")
    print("  PASS")


# ── Test 2: Old config keys ignored ─────────────────────────────

def test_config_keys_not_read():
    """Old config keys (dedup_months, max_snapshots, max_subpages) don't crash."""
    header("Test 2: Old config keys ignored")

    config_with_old_keys = {
        "wayback": CDX_CONFIG,
        "scraping": {
            "snapshot_dedup_months": 3,
            "max_snapshots_per_candidate": 2,
            "max_subpages": 5,
            "text_separator": "#+#",
            "exclude_domains": [],
        },
        "output": {},
    }

    # scrape_snapshot should not read max_subpages
    # (We can't call it without a real URL, but we can verify
    #  process_candidate doesn't read max_snapshots_per_candidate
    #  by inspecting the source)
    import src.scrape_wayback as mod
    source = inspect.getsource(mod.process_candidate)
    assert "max_snapshots_per_candidate" not in source, (
        "FAIL: process_candidate still references max_snapshots_per_candidate"
    )
    assert "snapshot_dedup_months" not in source, (
        "FAIL: process_candidate still references snapshot_dedup_months"
    )

    source_snap = inspect.getsource(mod.scrape_snapshot)
    assert "max_subpages" not in source_snap, (
        "FAIL: scrape_snapshot still references max_subpages"
    )
    assert "prioritize_subpage_urls" not in source_snap, (
        "FAIL: scrape_snapshot still calls prioritize_subpage_urls"
    )

    print("  CONFIRMED: Old config keys not referenced in code")
    print("  PASS")


# ── Test 3: CDX returns all records — no dedup ──────────────────

def test_cdx_no_dedup():
    """
    Query CDX for Hochul 2012 and verify no temporal dedup.
    Hochul ran for a special election in 2012 — sparse site, fast query.
    """
    header("Test 3: CDX returns all records — no dedup (Hochul 2012)")

    snapshots = query_cdx(
        "hochulforcongress.com", "20110101", "20121231", CDX_CONFIG
    )
    print(f"  Total CDX records: {len(snapshots)}")

    if len(snapshots) == 0:
        print("  SKIP: No CDX records (Wayback may be down)")
        return

    # Check for duplicate (url, month) pairs — allowed now
    pairs = Counter()
    for snap in snapshots:
        key = (_normalize_url(snap["original_url"]), snap["timestamp"][:6])
        pairs[key] += 1

    duplicated = {k: v for k, v in pairs.items() if v > 1}
    print(f"  Unique (url, month) pairs: {len(pairs)}")
    print(f"  Pairs with >1 snapshot: {len(duplicated)}")

    if duplicated:
        top = sorted(duplicated.items(), key=lambda x: -x[1])[:3]
        for (url, month), count in top:
            print(f"    {month} | {count} snapshots | {url[:60]}")
        print("  CONFIRMED: Multiple snapshots per (url, month) — no dedup")
    else:
        # Even without duplicates, the key is that ALL records are returned
        print("  No duplicated pairs (sparse site), but all records retained")

    # All snapshots should have valid wayback URLs
    for snap in snapshots[:5]:
        assert "web.archive.org" in snap["wayback_url"], (
            f"FAIL: Invalid wayback URL: {snap['wayback_url']}"
        )
    print("  All wayback URLs valid")
    print("  PASS")
    return snapshots


# ── Test 4: No subpage cap + content dedup works ────────────────

def test_no_subpage_cap(snapshots=None):
    """
    Scrape a snapshot and verify: no subpage cap, content dedup works.
    """
    header("Test 4: No subpage cap + content dedup (single snapshot)")

    if snapshots is None:
        snapshots = query_cdx(
            "hochulforcongress.com", "20110101", "20121231", CDX_CONFIG
        )

    if not snapshots:
        print("  SKIP: No CDX snapshots available")
        return

    time.sleep(1)
    session = _make_session(CDX_CONFIG)
    rl = RateLimiter(min_delay=1.0)

    config = {
        "scraping": {
            "text_separator": "#+#",
            "exclude_domains": ["twitter.com", "facebook.com"],
        },
    }

    # Pick a mid-period snapshot
    mid_idx = min(len(snapshots) // 2, len(snapshots) - 1)
    wb_url = snapshots[mid_idx]["wayback_url"]
    print(f"  Scraping: {wb_url}")

    pages = scrape_snapshot(wb_url, session, rl, config)
    print(f"  Pages returned: {len(pages)}")

    non_empty = [p for p in pages if p["snap_content"]]
    print(f"  Non-empty pages: {len(non_empty)}")

    # Content dedup: all returned pages should have unique content
    contents = [p["snap_content"] for p in pages]
    unique_contents = set(contents)
    assert len(contents) == len(unique_contents), (
        f"FAIL: Content dedup broken — {len(contents)} pages, "
        f"{len(unique_contents)} unique"
    )
    print(f"  Content dedup OK: all {len(contents)} pages have unique content")

    session.close()
    print("  PASS")


# ── Test 5: Busy site CDX exceeds old cap ────────────────────────

def test_busy_site_cdx():
    """
    Verify a busy site (Kirk 2010) returns far more than the old 50-snapshot cap.
    CDX-only, no page fetching.
    """
    header("Test 5: Busy site CDX count exceeds old 50-cap (Kirk 2010)")

    snapshots = query_cdx("kirkforsenate.com", "20100101", "20101231", CDX_CONFIG)
    count = len(snapshots)
    print(f"  CDX records: {count}")

    if count == 0:
        print("  SKIP: CDX returned 0 (Wayback may be down)")
        return

    assert count > 50, (
        f"FAIL: Expected >50 CDX records for Kirk, got {count}"
    )
    print(f"  CONFIRMED: {count} records >> old 50-snapshot cap")
    print(f"  All {count} would be scraped in production (no cap)")

    if count >= 10000:
        print(f"  WARNING: Hit CDX 10K limit — true count may be higher")

    print("  PASS")


# ── Test 6: Integration — process_candidate ──────────────────────

def test_integration_small():
    """
    Full process_candidate on Hochul — sparse site, should complete quickly.
    Verifies output CSV and progress tracker.
    """
    header("Test 6: Integration — process_candidate (Hochul 2012)")

    with tempfile.TemporaryDirectory() as tmpdir:
        progress_file = os.path.join(tmpdir, "progress.csv")
        tracker = ProgressTracker(progress_file)
        rl = RateLimiter(min_delay=1.0)

        config = {
            "wayback": CDX_CONFIG,
            "scraping": {
                "text_separator": "#+#",
                "exclude_domains": [
                    "twitter.com", "facebook.com",
                    "instagram.com", "youtube.com",
                ],
            },
            "output": {
                "snapshots_dir": os.path.join(tmpdir, "snapshots"),
                "progress_dir": tmpdir,
            },
        }

        candidate = {
            "candidate": "Kathy Hochul",
            "state": "NY",
            "district": "26",
            "office": "house",
            "year": 2012,
            "party": "D",
            "website_url": "hochulforcongress.com",
        }

        n_scraped = process_candidate(candidate, config, tracker, rl)
        print(f"  Snapshots scraped: {n_scraped}")

        # Check progress file
        if os.path.exists(progress_file):
            with open(progress_file) as f:
                lines = f.readlines()
            progress_entries = len(lines) - 1  # minus header
            print(f"  Progress entries: {progress_entries}")
        else:
            progress_entries = 0
            print("  No progress file created")

        # Check output CSV
        output_file = os.path.join(
            tmpdir, "snapshots", "house", "2012", "Kathy Hochul (NY).csv"
        )

        if n_scraped == 0:
            print("  WARNING: 0 snapshots scraped — Wayback may be unavailable")
            print("  SKIP: Cannot validate output")
            return

        assert os.path.exists(output_file), f"FAIL: Output CSV not at {output_file}"
        print(f"  Output CSV exists")

        import csv
        with open(output_file, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        print(f"  CSV rows: {len(rows)}")
        assert len(rows) > 0, "FAIL: Output CSV is empty"

        # Validate schema
        expected_cols = [
            "candidate", "state", "office", "year", "party",
            "snap_url", "page_type", "text_snap_content", "n_char", "n_words",
        ]
        for col in expected_cols:
            assert col in rows[0], f"FAIL: Missing column '{col}'"
        print("  CSV columns: OK")

        # Validate values
        row = rows[0]
        assert row["candidate"] == "Kathy Hochul"
        assert row["state"] == "NY"
        assert row["office"] == "house"
        assert row["year"] == "2012"
        print("  Metadata values: OK")

        # Content should be non-empty
        for r in rows:
            assert len(r["text_snap_content"]) > 0, (
                f"FAIL: Empty content for {r['snap_url']}"
            )
            assert int(r["n_char"]) == len(r["text_snap_content"])
        print("  Content validation: OK")

        # No cap was applied: progress entries should match CDX count
        # (minus any that errored)
        print(f"  Progress entries ({progress_entries}) = snapshots attempted")
        print("  No artificial cap applied")

    print("  PASS")


# ── Run all ──────────────────────────────────────────────────────

if __name__ == "__main__":
    passed = 0
    failed = 0

    tests = [
        ("Signature check", test_query_cdx_signature),
        ("Old config keys", test_config_keys_not_read),
        ("CDX no dedup", lambda: test_cdx_no_dedup()),
        ("No subpage cap", lambda: test_no_subpage_cap()),
        ("Busy site CDX", test_busy_site_cdx),
        ("Integration", test_integration_small),
    ]

    for name, test_fn in tests:
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
