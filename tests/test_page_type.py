#!/usr/bin/env python3
"""
Unit tests for page-type classification and subpage prioritization.

Tests classify_page_type() and prioritize_subpage_urls() from extract_text.py.
No network access required — all tests use synthetic URLs.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.extract_text import classify_page_type, prioritize_subpage_urls


def header(msg):
    print(f"\n{'='*60}")
    print(f"  {msg}")
    print(f"{'='*60}")


# ── Test 1: Homepage detection ───────────────────────────────────────

def test_homepage():
    """Homepage URLs: empty path, /, /home, /index.html."""
    header("TEST 1: Homepage detection")

    cases = [
        ("https://web.archive.org/web/20200601/https://example.com/", "homepage"),
        ("https://web.archive.org/web/20200601/https://example.com", "homepage"),
        ("https://web.archive.org/web/20200601/http://www.example.com/home", "homepage"),
        ("https://web.archive.org/web/20200601/http://example.com/index.html", "homepage"),
        ("https://web.archive.org/web/20200601/http://example.com/index.php", "homepage"),
    ]

    for url, expected in cases:
        result = classify_page_type(url)
        assert result == expected, f"FAIL: {url} -> '{result}', expected '{expected}'"
        print(f"  {url.split('/')[-1] or '(root)'} -> {result}")

    print("  PASS: All homepage cases correct")


# ── Test 2: Issues pages ────────────────────────────────────────────

def test_issues():
    """Issues-related URL patterns."""
    header("TEST 2: Issues page detection")

    base = "https://web.archive.org/web/20200601/https://candidate.com"
    patterns = ["issues", "issue", "the-issues", "on-the-issues", "platform",
                "priorities", "agenda", "positions", "plan", "legislation",
                "proven-leader"]

    for p in patterns:
        url = f"{base}/{p}"
        result = classify_page_type(url)
        assert result == "issues", f"FAIL: /{p} -> '{result}', expected 'issues'"
        print(f"  /{p} -> {result}")

    # Subpath: /issues/healthcare should still classify as issues
    url = f"{base}/issues/healthcare"
    result = classify_page_type(url)
    assert result == "issues", f"FAIL: /issues/healthcare -> '{result}'"
    print(f"  /issues/healthcare -> {result}")

    print("  PASS: All issues patterns correct")


# ── Test 3: Biography pages ─────────────────────────────────────────

def test_biography():
    """Biography URL patterns including meet-* prefix."""
    header("TEST 3: Biography page detection")

    base = "https://web.archive.org/web/20200601/https://candidate.com"
    exact = ["about", "bio", "biography", "story", "our-story", "background"]

    for p in exact:
        result = classify_page_type(f"{base}/{p}")
        assert result == "biography", f"FAIL: /{p} -> '{result}'"
        print(f"  /{p} -> biography")

    # Prefix: meet-ted, meet_jane
    prefix_cases = ["meet-ted", "meet-jane-doe", "meet_the_candidate"]
    for p in prefix_cases:
        result = classify_page_type(f"{base}/{p}")
        assert result == "biography", f"FAIL: /{p} -> '{result}'"
        print(f"  /{p} -> biography (prefix match)")

    print("  PASS: All biography patterns correct")


# ── Test 4: News pages ──────────────────────────────────────────────

def test_news():
    """News/media URL patterns."""
    header("TEST 4: News page detection")

    base = "https://web.archive.org/web/20200601/https://candidate.com"
    patterns = ["news", "press", "press-releases", "press-release", "newsroom",
                "media", "media-center", "blog", "category", "articles",
                "updates", "in-the-news"]

    for p in patterns:
        result = classify_page_type(f"{base}/{p}")
        assert result == "news", f"FAIL: /{p} -> '{result}'"
        print(f"  /{p} -> news")

    print("  PASS: All news patterns correct")


# ── Test 5: Other category types ────────────────────────────────────

def test_other_categories():
    """Endorsements, constituent_services, action types."""
    header("TEST 5: Endorsements, constituent_services, action")

    base = "https://web.archive.org/web/20200601/https://candidate.com"

    endorsement_patterns = ["endorsements", "supporters", "campaign-supporters", "endorsement"]
    for p in endorsement_patterns:
        result = classify_page_type(f"{base}/{p}")
        assert result == "endorsements", f"FAIL: /{p} -> '{result}'"
    print("  Endorsements patterns: PASS")

    cs_patterns = ["constituentservices", "services", "district", "offices",
                   "casework", "resources", "help"]
    for p in cs_patterns:
        result = classify_page_type(f"{base}/{p}")
        assert result == "constituent_services", f"FAIL: /{p} -> '{result}'"
    print("  Constituent services patterns: PASS")

    action_patterns = ["donate", "contribute", "volunteer", "get-involved",
                       "take-action", "join", "support", "events", "event", "calendar"]
    for p in action_patterns:
        result = classify_page_type(f"{base}/{p}")
        assert result == "action", f"FAIL: /{p} -> '{result}'"
    print("  Action patterns: PASS")


# ── Test 6: Other / unknown paths ───────────────────────────────────

def test_other_fallback():
    """Unknown paths fall through to 'other'."""
    header("TEST 6: Unknown paths -> other")

    base = "https://web.archive.org/web/20200601/https://candidate.com"
    other_paths = ["privacy-policy", "photos", "gallery", "sitemap", "terms",
                   "wp-content", "feed", "some-random-page"]

    for p in other_paths:
        result = classify_page_type(f"{base}/{p}")
        assert result == "other", f"FAIL: /{p} -> '{result}', expected 'other'"
        print(f"  /{p} -> other")

    print("  PASS: All unknown paths classified as 'other'")


# ── Test 7: Edge cases ──────────────────────────────────────────────

def test_edge_cases():
    """Query params, fragments, double slashes, case insensitivity."""
    header("TEST 7: Edge cases")

    base = "https://web.archive.org/web/20200601/https://candidate.com"

    # Query params should not affect classification
    result = classify_page_type(f"{base}/issues?topic=healthcare")
    assert result == "issues", f"FAIL: /issues?topic=... -> '{result}'"
    print("  /issues?topic=healthcare -> issues (query params ignored)")

    # Fragment should not affect classification
    result = classify_page_type(f"{base}/about#section2")
    assert result == "biography", f"FAIL: /about#section2 -> '{result}'"
    print("  /about#section2 -> biography (fragment ignored)")

    # Case should not matter (first segment is lowercased)
    result = classify_page_type(f"{base}/Issues")
    assert result == "issues", f"FAIL: /Issues -> '{result}'"
    print("  /Issues -> issues (case insensitive)")

    result = classify_page_type(f"{base}/ABOUT")
    assert result == "biography", f"FAIL: /ABOUT -> '{result}'"
    print("  /ABOUT -> biography (case insensitive)")

    # Wayback timestamp with modifier flags (e.g., "id_" suffix)
    result = classify_page_type(
        "https://web.archive.org/web/20200601120000id_/https://candidate.com/issues"
    )
    assert result == "issues", f"FAIL: timestamp with id_ modifier -> '{result}'"
    print("  Timestamp with id_ modifier -> issues")

    # Non-Wayback URL should still work (fallback parsing)
    result = classify_page_type("https://candidate.com/donate")
    assert result == "action", f"FAIL: non-Wayback URL -> '{result}'"
    print("  Non-Wayback URL -> action (fallback parsing)")

    print("  PASS: All edge cases correct")


# ── Test 8: prioritize_subpage_urls() ────────────────────────────────

def test_prioritize_subpage_urls():
    """Subpage URLs sorted by page-type priority."""
    header("TEST 8: prioritize_subpage_urls()")

    base = "https://web.archive.org/web/20200601/https://candidate.com"
    urls = [
        f"{base}/donate",           # action (priority 6)
        f"{base}/privacy-policy",   # other (priority 7)
        f"{base}/issues",           # issues (priority 1)
        f"{base}/news",             # news (priority 3)
        f"{base}/about",            # biography (priority 2)
        f"{base}/endorsements",     # endorsements (priority 4)
        f"{base}/district",         # constituent_services (priority 5)
    ]

    sorted_urls = prioritize_subpage_urls(urls)

    # Extract page types in sorted order
    types = [classify_page_type(u) for u in sorted_urls]
    print(f"  Sorted order: {types}")

    expected_order = ["issues", "biography", "news", "endorsements",
                      "constituent_services", "action", "other"]
    assert types == expected_order, f"FAIL: Got {types}, expected {expected_order}"
    print("  PASS: Priority ordering correct")

    # Homepage should sort first
    urls_with_home = [f"{base}/donate", f"{base}/"]
    sorted_home = prioritize_subpage_urls(urls_with_home)
    assert classify_page_type(sorted_home[0]) == "homepage", "FAIL: Homepage not first"
    print("  PASS: Homepage sorts first")

    # Same-priority URLs preserve original relative order
    urls_same = [
        f"{base}/press",        # news
        f"{base}/blog",         # news
        f"{base}/media-center", # news
    ]
    sorted_same = prioritize_subpage_urls(urls_same)
    types_same = [classify_page_type(u) for u in sorted_same]
    assert all(t == "news" for t in types_same), "FAIL: Same-tier items changed type"
    print("  PASS: Same-priority URLs maintain stable sort")

    # Empty list
    assert prioritize_subpage_urls([]) == [], "FAIL: Empty list not handled"
    print("  PASS: Empty list handled")


# ── Test 9: CMS router patterns ────────────────────────────────────

def test_cms_router_patterns():
    """CMS-routed URLs: .senate.gov ColdFusion, PHP, ASP.NET, generic prefixes."""
    header("TEST 9: CMS router patterns")

    wb = "https://web.archive.org/web/20180601"

    cases = [
        # ColdFusion (.senate.gov style)
        (f"{wb}/https://corker.senate.gov/public/index.cfm/press-releases", "news"),
        (f"{wb}/https://corker.senate.gov/public/index.cfm/biography", "biography"),
        (f"{wb}/https://corker.senate.gov/public/index.cfm/issues-and-legislation", "issues"),
        (f"{wb}/https://senator.senate.gov/public/index.cfm/services", "constituent_services"),
        # PHP router
        (f"{wb}/https://site.com/index.php/about", "biography"),
        (f"{wb}/https://site.com/index.php/news", "news"),
        # Generic prefix: pages/
        (f"{wb}/https://site.com/pages/donate", "action"),
        (f"{wb}/https://site.com/pages/issues", "issues"),
        # Empty after noise removal -> homepage
        (f"{wb}/https://corker.senate.gov/public/index.cfm/", "homepage"),
        (f"{wb}/https://site.com/public/", "homepage"),
        (f"{wb}/https://site.com/index.php", "homepage"),
        # New pattern additions
        (f"{wb}/https://site.com/pressreleases", "news"),
        (f"{wb}/https://site.com/press-room", "news"),
        (f"{wb}/https://site.com/issues-and-legislation", "issues"),
    ]

    for url, expected in cases:
        result = classify_page_type(url)
        # Extract the path portion for display
        path = url.split(wb + "/")[-1].split("/", 2)[-1] if "://" in url.split(wb + "/")[-1] else ""
        assert result == expected, f"FAIL: {url} -> '{result}', expected '{expected}'"
        print(f"  /{path} -> {result}")

    # Verify no regressions: non-CMS URLs still work (noise filter is a no-op)
    base = f"{wb}/https://candidate.com"
    regression_cases = [
        (f"{base}/issues", "issues"),
        (f"{base}/about", "biography"),
        (f"{base}/donate", "action"),
        (f"{base}/", "homepage"),
        (f"{base}/privacy-policy", "other"),
    ]
    for url, expected in regression_cases:
        result = classify_page_type(url)
        assert result == expected, f"REGRESSION: {url} -> '{result}', expected '{expected}'"

    print("  No regressions on standard URLs")
    print("  PASS: All CMS router patterns correct")


# ── Test 10: Ted Cruz spot-check ──────────────────────────────────────

def test_ted_cruz_urls():
    """Spot-check classify_page_type on realistic Ted Cruz URL patterns."""
    header("TEST 9: Ted Cruz URL spot-check")

    base = "https://web.archive.org/web/20180601/https://tedcruz.org"

    cases = [
        (f"{base}/", "homepage"),
        (f"{base}/issues", "issues"),
        (f"{base}/issues/border-security", "issues"),
        (f"{base}/about", "biography"),
        (f"{base}/meet-ted", "biography"),
        (f"{base}/news", "news"),
        (f"{base}/press-releases", "news"),
        (f"{base}/endorsements", "endorsements"),
        (f"{base}/donate", "action"),
        (f"{base}/volunteer", "action"),
        (f"{base}/get-involved", "action"),
        (f"{base}/privacy-policy", "other"),
    ]

    for url, expected in cases:
        result = classify_page_type(url)
        assert result == expected, f"FAIL: {url} -> '{result}', expected '{expected}'"
        path = url.split("tedcruz.org")[-1] or "/"
        print(f"  {path} -> {result}")

    print("  PASS: Ted Cruz URL patterns classified correctly")


# ── Runner ──────────────────────────────────────────────────────────

if __name__ == "__main__":
    tests = [
        test_homepage,
        test_issues,
        test_biography,
        test_news,
        test_other_categories,
        test_other_fallback,
        test_edge_cases,
        test_prioritize_subpage_urls,
        test_cms_router_patterns,
        test_ted_cruz_urls,
    ]

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

    header("SUMMARY")
    print(f"  {passed} passed, {failed} failed out of {len(tests)} tests")
    if failed == 0:
        print("  All page-type classification tests passed.")
    print(f"{'='*60}")

    sys.exit(1 if failed > 0 else 0)
