import json
import zipfile
from pathlib import Path

import pandas as pd

import scripts.extract_municipal_mayoral_urls as adapter
from scripts.build_mayoral_evidence_claims import build_claim_table


def write_universe(path: Path) -> None:
    pd.DataFrame([
        {
            "candidate_year_id": "cand-jane__2005",
            "candidate_contest_id": "contest-2005__cand-jane",
            "contest": "contest-2005",
            "ledb_candid": "cand-jane",
            "full_name": "Jane A. Doe",
            "geo_name": "example city",
            "state_abb": "EX",
            "year": 2005,
            "month": 11,
            "in_discovery_scope": True,
            "election_stage_status": "verified_general_election",
            "source_dataset": "Fixture candidate universe",
            "source_doi": "fixture-doi",
        },
        {
            "candidate_year_id": "cand-john__2005",
            "candidate_contest_id": "contest-2005__cand-john",
            "contest": "contest-2005",
            "ledb_candid": "cand-john",
            "full_name": "John Smith",
            "geo_name": "example city",
            "state_abb": "EX",
            "year": 2005,
            "month": 11,
            "in_discovery_scope": True,
            "election_stage_status": "verified_general_election",
            "source_dataset": "Fixture candidate universe",
            "source_doi": "fixture-doi",
        },
    ]).to_csv(path, index=False)


def source(source_id: str, file_name: str, **updates) -> dict:
    value = {
        "source_id": source_id,
        "source_name": f"Official source {source_id}",
        "source_url": f"https://example.gov/{source_id}",
        "source_date": "2005-10-15",
        "geo_name": "example city",
        "state_abb": "EX",
        "year": 2005,
        "month": 11,
        "election_round": "general",
        "local_files": [file_name],
    }
    value.update(updates)
    return value


def run_sources(tmp_path: Path, sources: list[dict]):
    universe = tmp_path / "universe.csv"
    write_universe(universe)
    manifest = tmp_path / "manifest.json"
    manifest.write_text(json.dumps({
        "candidate_universe": "universe.csv",
        "sources": sources,
    }))
    return adapter.extract_manifest(manifest)


def write_minimal_xlsx(path: Path, rows: list[list[str]]) -> None:
    def cell(reference: str, value: str) -> str:
        escaped = (
            value.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
        )
        return f'<c r="{reference}" t="inlineStr"><is><t>{escaped}</t></is></c>'

    xml_rows = []
    for row_number, values in enumerate(rows, start=1):
        cells = "".join(
            cell(f"{chr(ord('A') + column)}{row_number}", value)
            for column, value in enumerate(values)
        )
        xml_rows.append(f'<row r="{row_number}">{cells}</row>')
    sheet = (
        '<?xml version="1.0" encoding="UTF-8"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        f'<sheetData>{"".join(xml_rows)}</sheetData></worksheet>'
    )
    with zipfile.ZipFile(path, "w") as workbook:
        workbook.writestr("xl/worksheets/sheet1.xml", sheet)


def test_html_preserves_shared_host_path_case_and_contest_fields(tmp_path):
    html = tmp_path / "guide.html"
    html.write_text(
        '<table><tr><td>Jane Doe</td><td><a href="https://Sites.Google.com/site/JaneDoe/Mayor?Ref=ABC">Website</a></td></tr></table>'
    )

    evidence, attempts = run_sources(tmp_path, [source("official_html", "guide.html")])

    assert len(evidence) == 1
    row = evidence.iloc[0]
    assert row["raw_url"] == "https://Sites.Google.com/site/JaneDoe/Mayor?Ref=ABC"
    assert row["exact_normalized_url"] == "https://sites.google.com/site/JaneDoe/Mayor?Ref=ABC"
    assert row["site_root_url"] == "https://sites.google.com/"
    assert row["url_type"] == "shared_host_path"
    assert row["contest_id"] == "contest-2005"
    assert row["candidate_contest_id"] == "contest-2005__cand-jane"
    assert row["month"] == 11
    assert row["election_round"] == "general"
    assert row["evidence_grade"] == "C"
    assert row["review_status"] == "pending"
    assert row["acceptance"] == ""
    assert attempts.iloc[0]["source_status"] == "found"

    claims, _ = build_claim_table(
        pd.read_csv(tmp_path / "universe.csv"),
        pd.DataFrame(columns=[
            "candidate_year_id", "evidence_url", "discovery_method"
        ]),
        [("municipal_fixture", evidence)],
    )
    assert len(claims) == 1
    assert claims.iloc[0]["review_status"] == "pending"
    assert not bool(claims.iloc[0]["strict_exact_url"])


def test_csv_tsv_json_and_xlsx_parse_all_candidate_rows(tmp_path):
    (tmp_path / "contacts.csv").write_text(
        "candidate,website\nJane Doe,https://JaneForMayor.example/CSVPath\n"
    )
    (tmp_path / "contacts.tsv").write_text(
        "candidate\twebsite\nJohn Smith\twww.JohnSmith.example/TSVPath\n"
    )
    (tmp_path / "contacts.json").write_text(json.dumps({
        "candidates": [{"name": "Jane Doe", "campaign_url": "https://JaneForMayor.example/JsonPath"}]
    }))
    write_minimal_xlsx(tmp_path / "contacts.xlsx", [
        ["candidate", "website"],
        ["John Smith", "https://JohnSmith.example/XlsxPath"],
    ])

    evidence, attempts = run_sources(tmp_path, [
        source("official_csv", "contacts.csv"),
        source("official_tsv", "contacts.tsv"),
        source("official_json", "contacts.json"),
        source("official_xlsx", "contacts.xlsx"),
    ])

    assert set(evidence["source_file_type"]) == {"csv", "tsv", "json", "xlsx"}
    assert set(evidence["candidate_year_id"]) == {"cand-jane__2005", "cand-john__2005"}
    assert evidence["exact_normalized_url"].str.contains("Path", regex=False).all()
    assert set(attempts["source_status"]) == {"found"}
    assert len(attempts) == 4


def test_source_aware_dedup_preserves_independent_claims(tmp_path):
    html = tmp_path / "guide.html"
    html.write_text(
        '<p>Jane Doe <a href="https://JaneForMayor.example/AboutMe">Website</a></p>'
    )

    sources = [
        source("official_guide", "guide.html"),
        source("official_contact_sheet", "guide.html"),
    ]
    evidence, attempts = run_sources(tmp_path, sources)
    repeated_evidence, repeated_attempts = run_sources(tmp_path, sources)

    assert len(evidence) == 2
    assert evidence["source_id"].nunique() == 2
    assert evidence["claim_id"].nunique() == 2
    assert evidence["exact_normalized_url"].nunique() == 1
    assert evidence["claim_id"].tolist() == repeated_evidence["claim_id"].tolist()
    assert attempts["attempt_id"].tolist() == repeated_attempts["attempt_id"].tolist()


def test_shared_local_file_is_parsed_once_across_source_targets(tmp_path, monkeypatch):
    html = tmp_path / "guide.html"
    html.write_text('<p>Jane Doe <a href="https://JaneForMayor.example/">Website</a></p>')
    calls = []
    original = adapter.parse_local_file

    def tracked_parse(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(adapter, "parse_local_file", tracked_parse)
    evidence, _ = run_sources(tmp_path, [
        source("official_guide", "guide.html"),
        source("official_contact_sheet", "guide.html"),
    ])
    assert len(calls) == 1
    assert len(evidence) == 2


def test_pdf_extracts_visible_urls_and_hyperlink_annotations(tmp_path, monkeypatch):
    pdf = tmp_path / "guide.pdf"
    pdf.write_bytes(b"%PDF-fixture")

    class Reference:
        def __init__(self, value):
            self.value = value

        def get_object(self):
            return self.value

    class Page(dict):
        def extract_text(self):
            return "Jane Doe website https://JaneForMayor.example/VisiblePath"

    page = Page({
        "/Annots": [Reference({
            "/Subtype": "/Link",
            "/A": Reference({"/URI": "https://JaneForMayor.example/AnnotationPath"}),
            "/Contents": "Jane Doe campaign website",
        })]
    })

    class Reader:
        def __init__(self, _path):
            self.pages = [page]

    monkeypatch.setattr(adapter, "PdfReader", Reader)
    evidence, attempts = run_sources(tmp_path, [source("official_pdf", "guide.pdf")])

    assert set(evidence["extraction_method"]) == {
        "pdf_visible_text", "pdf_hyperlink_annotation"
    }
    assert set(evidence["exact_normalized_url"]) == {
        "https://janeformayor.example/VisiblePath",
        "https://janeformayor.example/AnnotationPath",
    }
    assert attempts.iloc[0]["source_status"] == "found"


def test_attempt_ledger_has_one_allowed_terminal_status_per_target(tmp_path, monkeypatch):
    (tmp_path / "found.html").write_text(
        '<p>Jane Doe <a href="https://JaneForMayor.example/">Website</a></p>'
    )
    (tmp_path / "empty.html").write_text("<p>Jane Doe filed for mayor.</p>")
    (tmp_path / "ambiguous.html").write_text(
        '<p>Campaign website <a href="https://UnknownCandidate.example/">Website</a></p>'
    )
    (tmp_path / "broken.json").write_text("{not valid json")
    (tmp_path / "needs-pypdf.pdf").write_bytes(b"%PDF-fixture")
    monkeypatch.setattr(adapter, "PdfReader", None)

    evidence, attempts = run_sources(tmp_path, [
        source("status_found", "found.html"),
        source("status_no_result", "empty.html"),
        source("status_match_unresolved", "ambiguous.html"),
        source("status_request_error", "broken.json"),
        source("status_missing", "missing.csv"),
        source("status_pdf_unavailable", "needs-pypdf.pdf"),
    ])

    status = attempts.set_index("source_id")["source_status"].to_dict()
    assert status == {
        "status_found": "found",
        "status_match_unresolved": "match_unresolved",
        "status_missing": "source_unavailable",
        "status_no_result": "no_result",
        "status_pdf_unavailable": "source_unavailable",
        "status_request_error": "request_error",
    }
    assert len(attempts) == 6
    assert not attempts.duplicated(["source_id", "contest_id"]).any()
    assert set(attempts["source_status"]) <= adapter.TERMINAL_STATUSES
    assert len(evidence) == 1
    assert "pypdf" in attempts.loc[
        attempts["source_id"].eq("status_pdf_unavailable"), "error"
    ].iloc[0]


def test_empty_file_source_preserves_upstream_terminal_status(tmp_path):
    evidence, attempts = run_sources(tmp_path, [
        source(
            "upstream_timeout", "unused.html", local_files=[],
            upstream_status="request_error", upstream_error="fetch timed out",
        ),
        source(
            "upstream_missing", "unused.html", local_files=[],
            upstream_status="source_unavailable", upstream_error="HTTP 403",
        ),
    ])

    assert evidence.empty
    status = attempts.set_index("source_id")["source_status"].to_dict()
    assert status == {
        "upstream_missing": "source_unavailable",
        "upstream_timeout": "request_error",
    }
    assert attempts.set_index("source_id").loc["upstream_timeout", "error"] == "fetch timed out"


def test_source_is_parsed_once_and_gets_one_attempt_per_contest(tmp_path, monkeypatch):
    universe = tmp_path / "universe.csv"
    write_universe(universe)
    frame = pd.read_csv(universe)
    primary = frame.copy()
    primary["contest"] = "contest-primary-2005"
    primary["candidate_contest_id"] = primary["candidate_contest_id"].str.replace(
        "contest-2005", "contest-primary-2005", regex=False
    )
    primary["month"] = 6
    primary["election_stage_status"] = "verified_primary_election"
    frame.to_csv(universe, index=False)
    primary.to_csv(universe, mode="a", header=False, index=False)
    (tmp_path / "guide.html").write_text("<p>The official candidate list.</p>")
    manifest = tmp_path / "manifest.json"
    target_source = source("two_contests", "guide.html")
    target_source.pop("month")
    target_source.pop("election_round")
    manifest.write_text(json.dumps({
        "candidate_universe": "universe.csv", "sources": [target_source]
    }))

    calls = []
    original = adapter.parse_local_file

    def tracked_parse(*args, **kwargs):
        calls.append(args[0])
        return original(*args, **kwargs)

    monkeypatch.setattr(adapter, "parse_local_file", tracked_parse)
    evidence, attempts = adapter.extract_manifest(manifest)

    assert evidence.empty
    assert len(calls) == 1
    assert set(attempts["contest_id"]) == {"contest-2005", "contest-primary-2005"}
    assert set(attempts["source_status"]) == {"no_result"}
