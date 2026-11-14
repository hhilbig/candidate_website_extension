"""Offline regression tests for the current production snapshot policy.

The historical release combines collections made under different rules. These
tests cover only the current scraper: quarterly URL-level deduplication followed
by a stratified cap of 200 snapshots per candidate.
"""
from src.scrape_wayback import _dedup_snapshots, _sample_snapshots_stratified


def _snap(month: int, day: int = 15, url: str = "https://example.com/") -> dict:
    timestamp = f"2022{month:02d}{day:02d}120000"
    return {
        "timestamp": timestamp,
        "original_url": url,
        "wayback_url": f"https://web.archive.org/web/{timestamp}/{url}",
    }


def test_current_policy_deduplicates_each_url_quarterly():
    snapshots = [_snap(month) for month in range(1, 13)]
    selected = _dedup_snapshots(snapshots, bucket_months=3)
    assert [s["timestamp"][4:6] for s in selected] == ["03", "06", "09", "12"]


def test_quarterly_dedup_is_url_specific():
    snapshots = [
        _snap(1, url="https://example.com/"),
        _snap(2, url="https://example.com/"),
        _snap(1, url="https://example.com/issues"),
        _snap(2, url="https://example.com/issues"),
    ]
    selected = _dedup_snapshots(snapshots, bucket_months=3)
    assert len(selected) == 2
    assert {s["original_url"] for s in selected} == {
        "https://example.com/", "https://example.com/issues"
    }


def test_current_policy_caps_with_temporal_coverage():
    snapshots = []
    for month in range(1, 13):
        for day in range(1, 26):
            snapshots.append(_snap(month, day, f"https://example.com/p{day}"))
    selected = _sample_snapshots_stratified(snapshots, 200)
    assert len(selected) == 200
    assert {s["timestamp"][:6] for s in selected} == {
        f"2022{month:02d}" for month in range(1, 13)
    }


def test_under_cap_input_is_unchanged():
    snapshots = [_snap(month) for month in range(1, 13)]
    assert _sample_snapshots_stratified(snapshots, 200) == snapshots
