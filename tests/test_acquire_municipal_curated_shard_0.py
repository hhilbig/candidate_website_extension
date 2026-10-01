from pathlib import Path

import pandas as pd

import scripts.acquire_municipal_curated_shard_0 as shard
from scripts.build_municipal_curated_acquisition_contract import (
    audit_post_shard_directories,
)
from scripts.extract_municipal_mayoral_urls import FileResult, SourceRecord


def test_shard_0_exact_contract_parse_once_and_retryable_status(tmp_path, monkeypatch):
    output = tmp_path / "shard_0"
    parse_calls: list[Path] = []
    real_parse = shard.parse_local_file

    def tracked_parse(path: Path, file_type: str):
        parse_calls.append(path)
        if file_type == "pdf":
            return FileResult(path, "pdf", (
                SourceRecord(
                    raw_url="https://CheyForIrvine.example/VisiblePath",
                    context="David Chey candidate website",
                    locator="page:1",
                    extraction_method="pdf_visible_text",
                    file_path=path,
                    file_type="pdf",
                ),
                SourceRecord(
                    raw_url="https://CheyForIrvine.example/AnnotationPath",
                    context="David Chey campaign website",
                    locator="page:1:annotation",
                    extraction_method="pdf_hyperlink_annotation",
                    file_path=path,
                    file_type="pdf",
                ),
            ))
        return real_parse(path, file_type)

    monkeypatch.setattr(shard, "parse_local_file", tracked_parse)

    def fake_fetch(spec: shard.RequestSpec) -> shard.FetchResult:
        timestamp = "2026-08-25T12:00:00Z"
        if "web.archive.org/web/20131030210007" in spec.url:
            return shard.FetchResult(
                spec, "request_error", None, "", "", b"", timestamp, 2,
                "ReadTimeout after three bounded attempts",
            )
        if spec.label == "orange_candidate_statement":
            return shard.FetchResult(
                spec, "downloaded", 200, spec.url, "application/pdf",
                b"%PDF-fixture-unique", timestamp, 0, "",
            )
        if spec.request_id == "oc_contest_municipal_target_7762aac7fc0d21bd5787":
            body = (
                "<html><p>DAVID CHEY</p>"
                "<span>/var/www/html/fileadmin/user_upload/elections/GEN2016/cs/5101-5.pdf</span>"
                "</html>"
            ).encode()
        else:
            body = f"<html><p>{spec.request_id}: no candidate URL</p></html>".encode()
        return shard.FetchResult(
            spec, "downloaded", 200, spec.url, "text/html", body,
            timestamp, 0, "",
        )

    attempts, files, evidence = shard.acquire(output_dir=output, fetcher=fake_fetch)

    assert set(path.name for path in output.iterdir()) == {
        "attempts.csv", "downloaded_files.csv", "evidence.csv", "files"
    }
    assert len(attempts) == 25
    assert attempts["attempt_key"].nunique() == 25
    expected = pd.read_csv(shard.DEFAULT_TARGETS, dtype=str, keep_default_na=False)
    assert set(attempts["attempt_key"]) == set(expected["attempt_key"])
    assert set(attempts["target_id"]) == set(expected["target_id"])
    candidate_contract = pd.read_csv(
        shard.DEFAULT_CANDIDATES, dtype=str, keep_default_na=False
    )
    assert len(candidate_contract) == 124
    parsed_unresolved_n = sum(
        int(dict(field.split("=") for field in note.split("; "))["full_contest_candidates"])
        for note in attempts["match_note"]
    )
    assert parsed_unresolved_n == 124

    retryable = attempts.loc[
        attempts["target_id"].eq("municipal_target_02ed683ac8c30484e8c9")
    ].iloc[0]
    assert retryable["terminal_status"] == "request_error"
    assert int(retryable["retry_count"]) == 2
    assert "ReadTimeout" in retryable["error"]

    assert len(parse_calls) == len(files)
    assert len({path.resolve() for path in parse_calls}) == len(files)
    assert set(evidence["exact_normalized_url"]) == {
        "https://cheyforirvine.example/VisiblePath",
        "https://cheyforirvine.example/AnnotationPath",
    }
    assert set(evidence["parser_id"]) == {"pdf_text_annotations_v1"}
    assert set(evidence["candidate_year_id"]).issubset(
        set(candidate_contract["candidate_year_id"])
    )

    parser_matrix = pd.read_csv(
        shard.AUDIT_DIR / "parser_support_matrix.csv", keep_default_na=False
    )
    summary = audit_post_shard_directories(
        expected,
        pd.read_csv(shard.DEFAULT_CANDIDATES, keep_default_na=False),
        parser_matrix,
        {0: output},
    )
    assert summary["verdict"] == "pass"
    assert summary["terminal_attempts"] == 25
