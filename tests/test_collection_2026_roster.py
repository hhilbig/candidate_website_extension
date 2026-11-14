import csv
import io
import json
import zipfile
from collections import Counter
from pathlib import Path

import pytest
import requests

from src import collection_2026_roster as module


def fec_row(cid="H6CA01000", name="DOE, JANE", party="DEM", year="2026", office="H", status="N"):
    return [cid, name, party, year, "CA", office, "01" if office == "H" else "00",
            "C", status, "C00000001", "1 MAIN ST", "", "CITY", "CA", "90000"]


def archive(rows):
    stream = io.BytesIO()
    text = io.StringIO()
    writer = csv.writer(text, delimiter="|", lineterminator="\n")
    writer.writerows(rows)
    with zipfile.ZipFile(stream, "w") as zipped:
        zipped.writestr("cn.txt", text.getvalue().encode("latin-1"))
    return stream.getvalue()


class Response:
    def __init__(self, data=None, content=b"", status=200):
        self.data, self.content, self.status_code = data, content, status
        self.headers = {"ETag": "unit-test"}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError("unit-test failure")

    def json(self):
        if isinstance(self.data, Exception):
            raise self.data
        return self.data


@pytest.fixture
def offline(monkeypatch):
    monkeypatch.setattr(module, "_legacy_leads", lambda: {})
    monkeypatch.setattr(module, "_api_key", lambda: "")
    monkeypatch.setattr(module.time, "sleep", lambda seconds: None)
    monkeypatch.setattr(module, "_wikidata", lambda session, rows: {
        row["candidate_id"]: {"source": "wikidata", "status": "no_result", "urls": []} for row in rows})


def test_exact_population_and_status_is_not_a_filter():
    rows, counts = module._parse_fec(archive([
        fec_row(status="N"), fec_row("H6CA01001", party="DFL"),
        fec_row("S6CA01000", office="S", party="REP"),
        fec_row("H6CA01002", year="2028"), fec_row("P60000000", office="P"),
        fec_row("H6CA01003", party="IND"),
    ]))
    assert len(rows) == 3
    assert rows[0]["candidate"] == "Jane Doe"
    assert rows[0]["cand_status"] == "N"
    assert rows[1]["party"] == "D"
    assert counts == {"raw_rows": 6, "other_election_year": 1, "other_office": 1,
                      "other_party": 1, "eligible_candidates": 3}


def test_bad_rows_and_conflicting_ids_are_not_silently_skipped():
    with pytest.raises(ValueError, match="expected 15"):
        module._parse_fec(archive([fec_row()[:-1]]))
    with pytest.raises(ValueError, match="conflicting duplicate"):
        module._parse_fec(archive([fec_row(), fec_row(name="DOE, JANET")]))
    rows, report = module._parse_fec(archive([fec_row(), fec_row()]))
    assert len(rows) == 1
    assert report["identical_duplicate_rows"] == 1


def test_refresh_versions_source_and_retains_missing_urls(tmp_path, monkeypatch, offline):
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: Response(content=archive([fec_row()])))
    rows = module.refresh_roster(tmp_path, lookup_limit=0)
    assert rows[0]["website_url"] == ""
    assert rows[0]["participation_status"] == "unknown"
    assert rows[0]["identity_status"] == "unresolved"
    assert rows[0]["url_lookup_status"] == "budget_deferred"
    assert len(list((tmp_path / "rosters/fec").glob("*.zip"))) == 1
    assert len(list((tmp_path / "rosters/versions").glob("*.csv"))) == 1
    assert module.load_roster(tmp_path) == rows
    report = json.loads((tmp_path / "rosters/latest_refresh.json").read_text())
    assert report["missing_website_urls"] == 1
    assert report["source_url"] == module.FEC_URL
    assert report["url_lookup"]["candidates_attempted"] == 0


def test_refresh_retains_disappeared_ids_and_old_urls(tmp_path, monkeypatch, offline):
    downloads = iter([archive([fec_row()]), archive([fec_row("H6CA01002")])])
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: Response(content=next(downloads)))
    monkeypatch.setattr(module, "_legacy_leads", lambda: {"H6CA01000": [
        {"url": "https://old.example", "source": "test.csv", "year": "2024"}]})
    original = module.refresh_roster(tmp_path, lookup_limit=0)
    updated = module.refresh_roster(tmp_path, lookup_limit=0)
    assert len(updated) == 2
    retained = next(row for row in updated if row["candidate_id"] == "H6CA01000")
    assert retained["website_url"] == "https://old.example"
    assert retained["roster_status"] == "retained_absent_from_current_fec"
    assert retained["last_seen_at"] == original[0]["last_seen_at"]
    assert len(list((tmp_path / "rosters/versions").glob("*.csv"))) == 2


def test_failed_download_or_parse_does_not_replace_current(tmp_path, monkeypatch, offline):
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: Response(content=archive([fec_row()])))
    original = module.refresh_roster(tmp_path, lookup_limit=0)
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: Response(content=archive([fec_row()[:-1]])))
    with pytest.raises(ValueError):
        module.refresh_roster(tmp_path, lookup_limit=0)
    assert module.load_roster(tmp_path) == original


def test_openfec_preserves_failures_and_hides_key():
    class Session:
        def get(self, *args, **kwargs):
            raise requests.ConnectionError("https://fake/?api_key=SECRET")
    result = module._openfec(Session(), {"committee_id": "C00000001"}, "SECRET")
    assert result["status"] == "request_error"
    assert "SECRET" not in json.dumps(result)
    assert module._openfec(Session(), {"committee_id": "C00000001"}, "")["status"] == "no_registered_key"


@pytest.mark.parametrize("status,expected", [(404, "http_error"), (429, "rate_limited"), (503, "http_error")])
def test_source_http_failures_are_not_empty_results(status, expected):
    assert module._source_result(Response(status=status))["status"] == expected


def test_openfec_checks_committee_identity_and_preserves_path_case():
    class Session:
        def get(self, *args, **kwargs):
            return Response(data={"results": [{"committee_id": "C00000001",
                "candidate_ids": ["H6CA01000"], "website": "Example.com/AboutJane"}]})
    row = {"candidate_id": "H6CA01000", "committee_id": "C00000001"}
    result = module._openfec(Session(), row, "unit-test-key")
    assert result["urls"] == ["https://example.com/AboutJane"]
    row["candidate_id"] = "H6CA01002"
    assert module._openfec(Session(), row, "unit-test-key")["status"] == "identity_conflict"


def test_wikidata_matches_fec_ids_and_rejects_entity_conflicts():
    class Session:
        def get(self, *args, **kwargs):
            assert "P1839" in kwargs["params"]["query"]
            assert "Jane Doe" not in kwargs["params"]["query"]
            return Response(data={"results": {"bindings": [
                {"fec": {"value": "H6CA01000"}, "person": {"value": entity},
                 "website": {"value": "https://example.com"}}
                for entity in ("https://www.wikidata.org/entity/Q1", "https://www.wikidata.org/entity/Q2")
            ]}})
    result = module._wikidata(Session(), [{"candidate_id": "H6CA01000", "candidate": "Jane Doe"}])
    assert result["H6CA01000"]["status"] == "identity_conflict"
    assert result["H6CA01000"]["urls"] == []


def balanced_rows(n=12):
    return [{"candidate_id": f"{office[0].upper()}6CA{party}{index:03}", "candidate": "Test Name",
             "office": office, "party": party, "state": "CA", "year": "2026",
             "website_url": f"https://example{index}.com" if index else "", "website_urls": "[]"}
            for office in ("house", "senate") for party in ("D", "R") for index in range(n)]


def test_pilot_is_balanced_deterministic_and_retains_missing_url_cases(tmp_path):
    roster = balanced_rows()
    path = tmp_path / "evidence.json"
    path.write_text(json.dumps({"candidates": [{"candidate_id": "H6CAD005", "office": "house", "party": "D", "state": "CA"}]}))
    selected = module.select_pilot(roster, path)
    assert selected == module.select_pilot(list(reversed(roster)), path)
    assert Counter((row["office"], row["party"]) for row in selected) == Counter({
        (office, party): 10 for office in ("house", "senate") for party in ("D", "R")})
    assert sum(not row["website_url"] for row in selected) == 4
    assert "H6CAD005" in {row["candidate_id"] for row in selected}
    assert all("pilot_reason" not in row for row in roster)


def test_pilot_frozen_ids_survive_url_changes(tmp_path):
    roster = balanced_rows()
    selected = module.select_pilot(roster, tmp_path / "absent.json")
    ids = {row["candidate_id"] for row in selected}
    for row in roster:
        row["pilot_member"] = "true" if row["candidate_id"] in ids else ""
        row["website_url"] = "" if row["website_url"] else "https://new.example"
    assert ids == {row["candidate_id"] for row in module.select_pilot(roster, tmp_path / "absent.json")}


def test_lookup_subset_preserves_roster_and_url_history(tmp_path, monkeypatch, offline):
    rows = balanced_rows()
    module._atomic(tmp_path / "rosters/current.csv", module._csv_bytes(rows))
    selected = module.select_pilot(rows)
    def found(session, row, key):
        return {"source": "openfec", "status": "found", "urls": ["https://new.example/About"]}
    monkeypatch.setattr(module, "_openfec", found)
    result = module.lookup_candidate_urls(tmp_path, selected, limit=40)
    assert len(result) == 48
    frozen = module.select_pilot(module.load_roster(tmp_path))
    assert {row["candidate_id"] for row in frozen} == {row["candidate_id"] for row in selected}
    assert all(row["website_url"] == "https://new.example/About" for row in frozen)
    for row in frozen:
        old = next(r for r in rows if r["candidate_id"] == row["candidate_id"])
        if old["website_url"]:
            assert old["website_url"] in json.loads(row["website_urls"])
    untouched = [row for row in result if row.get("pilot_member") != "true"]
    assert len(untouched) == 8
    assert all(row["url_checked_at"] == "" for row in untouched)


def test_default_refresh_resolves_frozen_pilot_on_saved_rows(tmp_path, monkeypatch, offline):
    source = [fec_row(f"{office}6CA{party_index * 100 + index:05}", party=party, office=office)
              for office in ("H", "S") for party_index, party in enumerate(("DEM", "REP")) for index in range(12)]
    monkeypatch.setattr(module.requests, "get", lambda *args, **kwargs: Response(content=archive(source)))
    monkeypatch.setattr(module, "_openfec", lambda *args: {
        "source": "openfec", "status": "found", "urls": ["https://new.example"]})
    result = module.refresh_roster(tmp_path)
    assert len(result) == 48
    assert sum(row["url_lookup_status"] == "found" for row in result) == 40
    assert sum(row["pilot_member"] == "true" for row in result) == 40
    selected = module.select_pilot(module.load_roster(tmp_path))
    assert all(row["website_url"] == "https://new.example" for row in selected)


def test_completed_source_attempt_survives_interrupted_lookup(tmp_path, monkeypatch, offline):
    rows = balanced_rows(1)[:2]
    calls = []
    def interrupted(session, row, key):
        calls.append(row["candidate_id"])
        if len(calls) == 2:
            raise KeyboardInterrupt
        return {"source": "openfec", "status": "no_result", "urls": []}
    monkeypatch.setattr(module, "_openfec", interrupted)
    with pytest.raises(KeyboardInterrupt):
        module._lookup_urls(rows, 2, tmp_path, "interrupted")
    records = [json.loads(line) for line in (tmp_path / "rosters/url_attempts/interrupted.jsonl").read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["candidate_id"] == calls[0]
    assert records[0]["status"] == "no_result"


def test_lookup_distinguishes_missing_key_errors_and_successful_absence(tmp_path, monkeypatch, offline):
    rows = [{**balanced_rows(1)[0], "committee_id": "C00000001"}]
    module._lookup_urls(rows, 1, tmp_path, "no_key")
    assert rows[0]["url_lookup_status"] == "incomplete_no_registered_key"
    monkeypatch.setattr(module, "_openfec", lambda *args: {"source": "openfec", "status": "no_result", "urls": []})
    module._lookup_urls(rows, 1, tmp_path, "absent")
    assert rows[0]["url_lookup_status"] == "no_result"
    monkeypatch.setattr(module, "_openfec", lambda *args: {"source": "openfec", "status": "request_error", "urls": []})
    module._lookup_urls(rows, 1, tmp_path, "error")
    assert rows[0]["url_lookup_status"] == "source_error"


@pytest.mark.parametrize("url", ["jane@example.com", "n/a", "not a site", "https://member.house.gov",
                                 "https://secure.actblue.com/donate/example", "https://facebook.com/candidate"])
def test_non_campaign_urls_rejected(url):
    assert module._normal_url(url) == ""


def test_evidence_file_has_valid_sources_and_distinct_ids():
    evidence = json.loads(module.PILOT_EVIDENCE.read_text())
    ids = [row["candidate_id"] for row in evidence["candidates"]]
    assert len(ids) == len(set(ids))
    assert all(row["source_url"].startswith("https://") for row in evidence["candidates"])
    assert any(row["outcome"] == "lost" for row in evidence["candidates"])
    assert any(row["election_round"] == "primary_runoff" for row in evidence["candidates"])
