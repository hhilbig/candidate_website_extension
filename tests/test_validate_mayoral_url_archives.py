import json

import pandas as pd
import pytest
import scripts.validate_mayoral_url_archives as validator

from scripts.validate_mayoral_url_archives import (
    VALIDATION_COLUMNS, VALIDATION_METHOD_VERSION, campaign_archive_query_urls,
    nearest_capture,
)


def _merge_context(candidate_ids):
    evidence_rows = []
    universe_rows = []
    for number, candidate_id in enumerate(candidate_ids, start=1):
        evidence_rows.append({
            "candidate_year_id": candidate_id,
            "full_name": f"Candidate {number}",
            "geo_name": "city",
            "state_abb": "AA",
            "year": 2021,
            "validated_campaign_url": f"https://candidate{number}.example/Path",
            "campaign_host": f"candidate{number}.example",
            "validation_status": "valid_source_backed_campaign_url",
        })
        universe_rows.append({
            "candidate_contest_id": f"contest__{number}",
            "candidate_year_id": candidate_id,
            "contest": f"contest-{number}",
            "month": 11,
        })
    return pd.DataFrame(evidence_rows), pd.DataFrame(universe_rows)


def _validation_row(
    evidence_row,
    status,
    *,
    nearest="",
    capture_count=0,
    marker="",
):
    row = {column: "" for column in VALIDATION_COLUMNS}
    row.update({
        "candidate_year_id": evidence_row["candidate_year_id"],
        "validated_campaign_url": evidence_row["validated_campaign_url"],
        "archive_query_status": status,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "archive_capture_count": capture_count,
        "nearest_capture_timestamp": nearest,
        "error": marker,
    })
    return row


def test_archive_validation_queries_cited_path_site_root_and_wildcard():
    assert campaign_archive_query_urls("https://candidate.example/issues?id=1") == [
        "https://candidate.example/issues?id=1", "https://candidate.example/",
        "https://candidate.example/*",
    ]
    assert campaign_archive_query_urls("https://candidate.example/") == [
        "https://candidate.example/", "https://candidate.example/*",
    ]


def test_nearest_capture_reports_days_from_election_month():
    timestamp, days = nearest_capture(
        ["20200101000000", "20211116000000", "bad"], 2021, 11
    )
    assert timestamp == "20211116000000"
    assert days == 1


def test_nearest_capture_handles_no_valid_timestamps():
    assert nearest_capture(["bad"], 2021, 11) == ("", None)


def test_validate_urls_combines_exact_and_root_archive_results(tmp_path, monkeypatch):
    evidence = pd.DataFrame([{
        "candidate_year_id":"1__2021", "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021,
        "validated_campaign_url":"https://candidate.example/issues",
        "campaign_host":"candidate.example",
        "validation_status":"valid_source_backed_campaign_url",
    }])
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "month":11,
    }])
    seen = []
    monkeypatch.setattr(
        validator, "query_campaign_url_captures",
        lambda url, *_args: seen.append(url) or (
            ["20211116000000"] if url.endswith("/") else []
        ),
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    result = validator.validate_urls(
        evidence, universe, tmp_path / "validation.csv", delay_seconds=0
    ).iloc[0]
    assert seen == [
        "https://candidate.example/issues", "https://candidate.example/",
        "https://candidate.example/*",
    ]
    assert result["nearest_capture_timestamp"] == "20211116000000"
    assert result["validation_method_version"] == VALIDATION_METHOD_VERSION
    assert result["candidate_contest_id"] == "c1__1"
    assert result["contest"] == "c1"


def test_validation_prunes_stale_and_old_method_rows(tmp_path, monkeypatch):
    evidence = pd.DataFrame([{
        "candidate_year_id":"1__2021", "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021,
        "validated_campaign_url":"https://candidate.example/Issues",
        "campaign_host":"candidate.example",
        "validation_status":"valid_source_backed_campaign_url",
    }])
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "month":4,
    }])
    output = tmp_path / "validation.csv"
    pd.DataFrame([
        {"candidate_year_id":"stale", "validated_campaign_url":"https://stale.example/",
         "validation_method_version":VALIDATION_METHOD_VERSION, "archive_query_status":"checked"},
        {"candidate_year_id":"1__2021", "validated_campaign_url":"https://candidate.example/Issues",
         "validation_method_version":"old_v3", "archive_query_status":"checked"},
    ]).to_csv(output, index=False)
    monkeypatch.setattr(validator, "query_campaign_url_captures", lambda *_a: [])
    monkeypatch.setattr(validator.time, "sleep", lambda *_a: None)
    result = validator.validate_urls(evidence, universe, output, delay_seconds=0)
    assert list(zip(result["candidate_year_id"], result["validated_campaign_url"])) == [
        ("1__2021", "https://candidate.example/Issues")
    ]
    persisted = pd.read_csv(output)
    assert len(persisted) == 1
    assert persisted.iloc[0]["validation_method_version"] == VALIDATION_METHOD_VERSION


def test_partial_archive_query_error_is_retried(tmp_path, monkeypatch):
    evidence = pd.DataFrame([{
        "candidate_year_id":"1__2021", "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021,
        "validated_campaign_url":"https://candidate.example/Issues",
        "campaign_host":"candidate.example",
        "validation_status":"valid_source_backed_campaign_url",
    }])
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "month":4,
    }])
    output = tmp_path / "validation.csv"
    pd.DataFrame([{
        "candidate_year_id":"1__2021",
        "validated_campaign_url":"https://candidate.example/Issues",
        "validation_method_version":VALIDATION_METHOD_VERSION,
        "archive_query_status":"checked_with_partial_error",
    }]).to_csv(output, index=False)
    seen = []
    monkeypatch.setattr(
        validator, "query_campaign_url_captures",
        lambda url, *_a: seen.append(url) or [],
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_a: None)
    result = validator.validate_urls(evidence, universe, output, delay_seconds=0)
    assert len(seen) == 3
    assert result.iloc[0]["archive_query_status"] == "checked"


def test_validation_records_missing_month_instead_of_assuming_november(tmp_path):
    evidence = pd.DataFrame([{
        "candidate_year_id":"1__2021", "full_name":"Alpha", "geo_name":"city",
        "state_abb":"AA", "year":2021,
        "validated_campaign_url":"https://candidate.example/",
        "campaign_host":"candidate.example",
        "validation_status":"valid_source_backed_campaign_url",
    }])
    universe = pd.DataFrame([{
        "candidate_contest_id":"c1__1", "candidate_year_id":"1__2021",
        "contest":"c1", "month":pd.NA,
    }])
    result = validator.validate_urls(
        evidence, universe, tmp_path / "validation.csv", delay_seconds=0
    ).iloc[0]
    assert pd.isna(result["month"])
    assert result["archive_query_status"] == "not_attempted_missing_election_month"
    assert result["temporal_validation_status"] == "missing_election_month"
    assert "no month was imputed" in result["error"]


def test_parallel_validation_preserves_exact_target_keys(tmp_path, monkeypatch):
    evidence = pd.DataFrame([
        {
            "candidate_year_id": candidate_id,
            "full_name": candidate_id,
            "geo_name": "city",
            "state_abb": "AA",
            "year": 2021,
            "validated_campaign_url": url,
            "campaign_host": url.split("//", 1)[1].strip("/"),
            "validation_status": "valid_source_backed_campaign_url",
        }
        for candidate_id, url in [
            ("1__2021", "https://one.example/Path"),
            ("2__2021", "https://two.example/Other"),
        ]
    ])
    universe = pd.DataFrame([
        {
            "candidate_contest_id": f"c{number}__{number}",
            "candidate_year_id": f"{number}__2021",
            "contest": f"c{number}",
            "month": 11,
        }
        for number in (1, 2)
    ])
    monkeypatch.setattr(
        validator, "query_campaign_url_captures",
        lambda *_args: ["20211116000000"],
    )
    monkeypatch.setattr(validator.time, "sleep", lambda *_args: None)
    output = tmp_path / "validation.csv"
    result = validator.validate_urls(
        evidence, universe, output, delay_seconds=0, workers=2
    )
    expected = set(zip(evidence.candidate_year_id, evidence.validated_campaign_url))
    observed = set(zip(result.candidate_year_id, result.validated_campaign_url))
    assert observed == expected
    assert len(result) == 2
    assert set(result.validation_method_version) == {VALIDATION_METHOD_VERSION}


def test_offline_merge_ranks_status_capture_metadata_and_source_order(tmp_path):
    candidate_ids = [f"{number}__2021" for number in range(1, 7)]
    evidence, universe = _merge_context(candidate_ids)
    first_rows = [
        _validation_row(evidence.iloc[0], "error", marker="first-error"),
        _validation_row(
            evidence.iloc[1], "not_attempted_missing_election_month",
            capture_count=999, marker="first-missing-month",
        ),
        _validation_row(
            evidence.iloc[2], "checked_with_partial_error",
            nearest="20211115000000", capture_count=999, marker="first-partial",
        ),
        _validation_row(
            evidence.iloc[3], "checked", capture_count=100, marker="first-no-nearest",
        ),
        _validation_row(
            evidence.iloc[4], "checked", nearest="20211115000000",
            capture_count=10, marker="first-higher-count",
        ),
        _validation_row(
            evidence.iloc[5], "checked", nearest="20211115000000",
            capture_count=10, marker="first-tied",
        ),
    ]
    second_rows = [
        _validation_row(
            evidence.iloc[0], "not_attempted_missing_election_month",
            marker="second-missing-month",
        ),
        _validation_row(
            evidence.iloc[1], "checked_with_partial_error",
            marker="second-partial",
        ),
        _validation_row(
            evidence.iloc[2], "checked", marker="second-checked",
        ),
        _validation_row(
            evidence.iloc[3], "checked", nearest="20211115000000",
            capture_count=1, marker="second-has-nearest",
        ),
        _validation_row(
            evidence.iloc[4], "checked", nearest="20211115000000",
            capture_count=2, marker="second-lower-count",
        ),
        _validation_row(
            evidence.iloc[5], "checked", nearest="20211115000000",
            capture_count=10, marker="second-tied",
        ),
    ]
    first = tmp_path / "first.csv"
    second = tmp_path / "second.csv"
    output = tmp_path / "merged.csv"
    pd.DataFrame(first_rows).to_csv(first, index=False)
    pd.DataFrame(second_rows).to_csv(second, index=False)

    result = validator.merge_validation_csvs(
        evidence, universe, [first, second], output
    )

    assert result["error"].tolist() == [
        "second-missing-month",
        "second-partial",
        "second-checked",
        "second-has-nearest",
        "first-higher-count",
        "second-tied",
    ]
    assert result.columns.tolist() == VALIDATION_COLUMNS
    persisted = pd.read_csv(output)
    assert persisted.columns.tolist() == VALIDATION_COLUMNS
    assert len(persisted) == len(evidence)


def test_offline_merge_rejects_duplicate_keys_within_an_input(tmp_path):
    evidence, universe = _merge_context(["1__2021"])
    row = _validation_row(evidence.iloc[0], "checked")
    source = tmp_path / "duplicates.csv"
    pd.DataFrame([row, row]).to_csv(source, index=False)

    with pytest.raises(ValueError, match="contains duplicate keys"):
        validator.merge_validation_csvs(
            evidence, universe, [source], tmp_path / "merged.csv"
        )


def test_offline_merge_rejects_missing_evidence_keys(tmp_path):
    evidence, universe = _merge_context(["1__2021", "2__2021"])
    source = tmp_path / "incomplete.csv"
    pd.DataFrame([
        _validation_row(evidence.iloc[0], "checked")
    ]).to_csv(source, index=False)

    with pytest.raises(ValueError, match=r"missing=1, unexpected=0"):
        validator.merge_validation_csvs(
            evidence, universe, [source], tmp_path / "merged.csv"
        )


def test_offline_merge_rejects_unexpected_evidence_keys(tmp_path):
    evidence, universe = _merge_context(["1__2021"])
    expected = _validation_row(evidence.iloc[0], "checked")
    unexpected = dict(expected)
    unexpected["candidate_year_id"] = "unexpected__2021"
    source = tmp_path / "unexpected.csv"
    pd.DataFrame([expected, unexpected]).to_csv(source, index=False)

    with pytest.raises(ValueError, match=r"unexpected=1"):
        validator.merge_validation_csvs(
            evidence, universe, [source], tmp_path / "merged.csv"
        )


def test_validation_audit_reports_key_agreement_statuses_and_retryable_rows(tmp_path):
    evidence, universe = _merge_context(["1__2021", "2__2021", "3__2021"])
    rows = [
        _validation_row(evidence.iloc[0], "checked"),
        _validation_row(evidence.iloc[1], "checked_with_partial_error"),
        _validation_row(evidence.iloc[2], "error"),
    ]
    rows[0]["temporal_validation_status"] = "archive_capture_within_365_days"
    rows[1]["temporal_validation_status"] = "unresolved"
    rows[2]["temporal_validation_status"] = "unresolved"
    validation = pd.DataFrame(rows)
    validation_output = tmp_path / "validation.csv"

    audit = validator.validation_audit(
        validation, evidence, universe, validation_output
    )

    assert audit["validation_method_version"] == VALIDATION_METHOD_VERSION
    assert audit["row_count"] == audit["key_count"] == audit["evidence_key_count"] == 3
    assert audit["exact_evidence_key_agreement"] is True
    assert audit["missing_evidence_keys"] == audit["unexpected_validation_keys"] == 0
    assert audit["archive_query_status_counts"] == {
        "checked": 1,
        "checked_with_partial_error": 1,
        "error": 1,
    }
    assert audit["temporal_validation_status_counts"] == {
        "archive_capture_within_365_days": 1,
        "unresolved": 2,
    }
    assert audit["retryable_rows"] == 2
    assert audit["output_path"] == str(validation_output)

    audit_output = tmp_path / "audit.json"
    validator.write_json_atomic(audit, audit_output)
    assert json.loads(audit_output.read_text()) == audit
