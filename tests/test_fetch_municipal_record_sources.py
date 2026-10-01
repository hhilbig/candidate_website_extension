from pathlib import Path
from urllib.error import HTTPError

import pandas as pd

import scripts.fetch_municipal_record_sources as fetch_module
from scripts.fetch_municipal_record_sources import (
    Response, _parse_target_years, crawl_inventory, target_link,
)


def _universe():
    return pd.DataFrame([{
        "candidate_year_id": "jane__2005", "full_name": "Jane Doe",
        "geo_name": "example city", "state_abb": "EX", "year": 2005,
        "in_discovery_scope": True,
    }])


def test_target_link_requires_same_host_and_target_context():
    years, candidates = {"2005"}, {"jane doe"}
    assert target_link(
        "https://example.gov/elections/2005/candidates.pdf", "Candidate list",
        "example.gov", years, candidates,
    )
    assert target_link(
        "https://example.gov/files/doe-filing.pdf", "Jane Doe filing",
        "example.gov", years, candidates,
    )
    assert not target_link(
        "https://other.gov/elections/2005/candidates.pdf", "Candidate list",
        "example.gov", years, candidates,
    )
    assert not target_link(
        "https://example.gov/elections/current", "Current election",
        "example.gov", years, candidates,
    )
    assert not target_link(
        "https://example.gov/mayor/doe", "Mayor John Doe",
        "example.gov", years, candidates,
    )
    assert not target_link(
        "https://example.gov/elections/Special%20Election%20of%2005-01-2022.pdf",
        "Special election report", "example.gov", years, candidates,
    )


def test_crawler_saves_explicit_root_and_target_file_once(tmp_path: Path):
    root = "https://example.gov/elections"
    target = "https://example.gov/elections/2005/candidates.pdf"
    bodies = {
        root: Response(
            root, 200, "text/html",
            b'<a href="/elections/current">Current</a><a href="/elections/2005/candidates.pdf">2005 candidate list</a>',
        ),
        target: Response(target, 200, "application/pdf", b"%PDF fixture"),
    }
    calls = []

    def fetcher(url):
        calls.append(url)
        return bodies[url]

    inventory = pd.DataFrame([{
        "source_id": "official_example", "city": "example city",
        "state": "EX", "official_url": root,
    }])
    downloads, attempts = crawl_inventory(
        inventory, _universe(), tmp_path / "files", fetcher=fetcher,
    )
    assert calls == [root, target]
    assert len(downloads) == 2
    assert downloads["sha256"].str.len().eq(64).all()
    assert all(Path(path).is_file() for path in downloads["local_file"])
    assert attempts.iloc[0]["source_status"] == "found"
    assert attempts.iloc[0]["files_downloaded"] == 2


def test_crawler_records_terminal_unavailable_status(tmp_path: Path):
    def fetcher(url):
        raise HTTPError(url, 404, "Not Found", None, None)

    inventory = pd.DataFrame([{
        "source_id": "missing", "city": "example city", "state": "EX",
        "official_url": "https://example.gov/missing",
    }])
    downloads, attempts = crawl_inventory(
        inventory, _universe(), tmp_path / "files", fetcher=fetcher,
    )
    assert downloads.iloc[0]["download_status"] == "source_unavailable"
    assert attempts.iloc[0]["source_status"] == "source_unavailable"
    assert attempts.iloc[0]["files_downloaded"] == 0


def test_same_source_can_target_two_cities(tmp_path: Path):
    root = "https://example.gov/elections"
    inventory = pd.DataFrame([
        {"source_id": "state_history", "city": city, "state": "EX", "official_url": root}
        for city in ["example city", "second city"]
    ])
    universe = pd.concat([
        _universe(),
        pd.DataFrame([{
            "candidate_year_id": "john__2005", "full_name": "John Roe",
            "geo_name": "second city", "state_abb": "EX", "year": 2005,
            "in_discovery_scope": True,
        }]),
    ], ignore_index=True)
    response = Response(root, 200, "text/html", b"<p>Election history</p>")
    downloads, attempts = crawl_inventory(
        inventory, universe, tmp_path / "files", fetcher=lambda _url: response,
    )
    assert len(downloads) == len(attempts) == 2
    assert downloads["source_target_id"].nunique() == 2
    assert attempts["source_target_id"].nunique() == 2


def test_resilient_fetcher_uses_fallback_after_client_block(monkeypatch):
    url = "https://example.gov/elections"
    expected = Response(url, 200, "text/html", b"official page")

    def blocked(*_args, **_kwargs):
        raise HTTPError(url, 403, "Forbidden", None, None)

    monkeypatch.setattr(fetch_module, "default_fetcher", blocked)
    monkeypatch.setattr(fetch_module, "curl_fetcher", lambda *_args, **_kwargs: expected)
    assert fetch_module.resilient_fetcher(url) == expected


def test_crawler_respects_explicit_2010_plus_target_years(tmp_path: Path):
    root = "https://example.gov/elections"
    universe = pd.DataFrame([
        {**_universe().iloc[0].to_dict(), "candidate_year_id": "old", "year": 2005},
        {**_universe().iloc[0].to_dict(), "candidate_year_id": "new", "full_name": "New Person", "year": 2017},
    ])
    inventory = pd.DataFrame([{
        "source_id": "recent", "city": "example city", "state": "EX",
        "official_url": root, "target_years": "2017",
    }])
    response = Response(root, 200, "text/html", b"<p>Election history</p>")
    downloads, attempts = crawl_inventory(
        inventory, universe, tmp_path / "files", start_year=2010, end_year=2021,
        fetcher=lambda _url: response,
    )
    assert downloads.iloc[0]["target_years"] == "2017"
    assert attempts.iloc[0]["target_years"] == "2017"


def test_target_year_parser_accepts_lists_and_ranges():
    assert _parse_target_years("2010|2012,2014-2015") == {2010, 2012, 2014, 2015}
