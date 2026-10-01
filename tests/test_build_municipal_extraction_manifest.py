import pandas as pd

from scripts.build_municipal_extraction_manifest import build_manifest


def test_builder_expands_downloaded_source_to_target_years_only():
    inventory = pd.DataFrame([{
        "priority_rank": 1, "source_id": "city_records", "city": "example city",
        "state": "EX", "official_url": "https://example.gov/elections",
    }])
    downloads = pd.DataFrame([
        {
            "source_id": "city_records", "source_target_id": "city_records__example_city__ex",
            "city": "example city", "state": "EX", "download_status": "downloaded",
            "local_file": "data/record.pdf", "sha256": "a" * 64,
            "final_url": "https://example.gov/record.pdf",
        },
        {
            "source_id": "city_records", "source_target_id": "city_records__example_city__ex",
            "city": "example city", "state": "EX", "download_status": "request_error",
            "local_file": "", "sha256": "", "final_url": "https://example.gov/error",
        },
    ])
    universe = pd.DataFrame([
        {"geo_name": "example city", "state_abb": "EX", "year": year, "in_discovery_scope": True}
        for year in [2001, 2005, 2009]
    ])
    result = build_manifest(inventory, downloads, universe, "data/universe.csv")
    assert [source["year"] for source in result["sources"]] == [2001, 2005]
    assert all(len(source["local_files"]) == 1 for source in result["sources"])
    assert result["sources"][0]["local_files"][0] == {
        "path": "data/record.pdf", "type": "pdf", "file_id": f"sha256:{'a' * 64}",
    }


def test_builder_retains_sources_without_supported_downloads_as_terminal_attempts():
    inventory = pd.DataFrame([{
        "priority_rank": 1, "source_id": "missing", "city": "example city",
        "state": "EX", "official_url": "https://example.gov/missing",
    }])
    downloads = pd.DataFrame([{
        "source_id": "missing", "source_target_id": "missing__example_city__ex",
        "city": "example city", "state": "EX", "download_status": "source_unavailable",
        "local_file": "", "sha256": "", "final_url": "https://example.gov/missing",
    }])
    universe = pd.DataFrame([{
        "geo_name": "example city", "state_abb": "EX", "year": 2005,
        "in_discovery_scope": True,
    }])
    sources = build_manifest(inventory, downloads, universe, "universe.csv")["sources"]
    assert len(sources) == 1
    assert sources[0]["local_files"] == []
    assert sources[0]["upstream_status"] == "source_unavailable"
    assert "No supported local file" in sources[0]["upstream_error"]


def test_builder_preserves_request_error_when_no_file_was_recovered():
    inventory = pd.DataFrame([{
        "priority_rank": 1, "source_id": "timed_out", "city": "example city",
        "state": "EX", "official_url": "https://example.gov/timeout",
    }])
    downloads = pd.DataFrame([{
        "source_id": "timed_out", "source_target_id": "timed_out__example_city__ex",
        "city": "example city", "state": "EX", "download_status": "request_error",
        "local_file": "", "sha256": "", "final_url": "",
        "error": "HTTP request timed out",
    }])
    universe = pd.DataFrame([{
        "geo_name": "example city", "state_abb": "EX", "year": 2005,
        "in_discovery_scope": True,
    }])
    source = build_manifest(inventory, downloads, universe, "universe.csv")["sources"][0]
    assert source["upstream_status"] == "request_error"
    assert source["upstream_error"] == "HTTP request timed out"


def test_builder_uses_explicit_2010_plus_target_years():
    inventory = pd.DataFrame([{
        "priority_rank": 1, "source_id": "recent", "city": "example city",
        "state": "EX", "official_url": "https://example.gov/recent",
        "target_years": "2013|2017",
    }])
    downloads = pd.DataFrame([{
        "source_id": "recent", "source_target_id": "recent__example_city__ex",
        "city": "example city", "state": "EX", "download_status": "downloaded",
        "local_file": "data/recent.csv", "sha256": "b" * 64,
        "final_url": "https://example.gov/recent.csv",
    }])
    universe = pd.DataFrame([
        {"geo_name": "example city", "state_abb": "EX", "year": year, "in_discovery_scope": True}
        for year in [2005, 2013, 2017, 2021]
    ])
    result = build_manifest(
        inventory, downloads, universe, "universe.csv", start_year=2010, end_year=2021,
    )
    assert [source["year"] for source in result["sources"]] == [2013, 2017]
