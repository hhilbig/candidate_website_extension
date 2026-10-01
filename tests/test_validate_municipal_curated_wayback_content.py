from __future__ import annotations

import hashlib
import json
from datetime import date
from pathlib import Path
from urllib.parse import parse_qs, urlsplit

import pandas as pd
import pytest

import scripts.validate_municipal_curated_wayback_content as validator
from scripts.freeze_municipal_temporal_validation_claims import (
    ELIGIBLE_URL_TYPES,
    FREEZE_CONTRACT_VERSION,
    FROZEN_CLAIM_COLUMNS,
    IDENTITY_SUPPORTED_STATUS,
    VALIDATION_METHOD_VERSION,
    VALIDATION_SHARD_COUNT,
    validation_evidence_hash,
    validation_key,
)
from scripts.merge_municipal_temporal_validation import RESULT_COLUMNS


EXACT_URL = "https://example.org/Campaign/Mayor?Ref=CityVote&A=UPPER"


def _claim(
    *,
    evidence_id: str = "evidence-1",
    candidate_year_id: str = "candidate-2018",
    exact_url: str = EXACT_URL,
    full_name: str = "Jane Doe",
    city: str = "Metroville",
    year: int = 2018,
    month: object = 5,
    url_type: str = "shared_host_path",
    identity_match: str = "candidate_statement_owner_exact",
    identity_attribution_status: str = IDENTITY_SUPPORTED_STATUS,
) -> dict[str, object]:
    row: dict[str, object] = {column: "" for column in FROZEN_CLAIM_COLUMNS}
    key = validation_key(evidence_id, candidate_year_id, exact_url)
    evidence_hash = validation_evidence_hash(key)
    identity_pass = identity_attribution_status == IDENTITY_SUPPORTED_STATUS
    coverage_pass = url_type in ELIGIBLE_URL_TYPES
    row.update(
        {
            "freeze_contract_version": FREEZE_CONTRACT_VERSION,
            "validation_method_version": VALIDATION_METHOD_VERSION,
            "validation_key": key,
            "evidence_hash": evidence_hash,
            "evidence_record_hash": "a" * 64,
            "context_hash": "b" * 64,
            "identity_decision_hash": "d" * 64,
            "validation_shard_id": int(evidence_hash, 16)
            % VALIDATION_SHARD_COUNT,
            "validation_shard_count": VALIDATION_SHARD_COUNT,
            "coverage_url_type_eligible": coverage_pass,
            "identity_gate_pass": identity_pass,
            "identity_attribution_status": identity_attribution_status,
            "identity_attribution_reason": (
                "fixture source label identifies the assigned candidate"
                if identity_pass
                else "fixture source-to-roster identity remains unresolved"
            ),
            "eligible_for_temporal_validation": coverage_pass and identity_pass,
            "eligibility_reason": (
                "eligible_candidate_website_url_type_and_identity"
                if coverage_pass and identity_pass
                else (
                    f"ineligible_url_type:{url_type}"
                    if not coverage_pass
                    else f"identity_gate_failed:{identity_attribution_status}"
                )
            ),
            "acquisition_contract_version": "municipal-curated-acquisition-v1",
            "evidence_id": evidence_id,
            "attempt_key": f"attempt-{evidence_id}",
            "target_id": f"target-{candidate_year_id}",
            "source_id": "official-municipal-records",
            "parse_group_id": "fixture-group",
            "acquisition_shard_id": 0,
            "partition_hostname": "example.org",
            "candidate_year_id": candidate_year_id,
            "full_name": full_name,
            "geo_name": city,
            "state_abb": "XY",
            "year": year,
            "month": month,
            "contest": "Mayor",
            "in_candidate_screen": True,
            "in_complete_contest_screen": False,
            "raw_url": exact_url,
            "exact_normalized_url": exact_url,
            "site_root_url": "https://example.org/",
            "registrable_domain": "example.org",
            "url_type": url_type,
            "source_url": "https://city.example.gov/candidates",
            "source_date": "2018-04-01",
            "source_file_id": "fixture-file",
            "source_file_sha256": "c" * 64,
            "source_snapshot_timestamp": "20180401120000",
            "evidence_text": "Jane Doe campaign website",
            "expected_website_field": "Campaign Website",
            "parser_id": "fixture_parser",
            "parser_version": "fixture-v1",
            "identity_match": identity_match,
            "temporal_match": "source_election_period",
            "evidence_grade": "C",
            "review_status": "unreviewed",
        }
    )
    return row


def _frame(*rows: dict[str, object]) -> pd.DataFrame:
    return pd.DataFrame(list(rows), columns=FROZEN_CLAIM_COLUMNS)


def _sha256_file(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_freeze_bundle(
    directory: Path, frame: pd.DataFrame
) -> tuple[Path, Path]:
    directory.mkdir(parents=True, exist_ok=True)
    all_claims_path = directory / "all_claims.csv"
    claims_path = directory / "eligible_claims.csv"
    manifest_path = directory / "manifest.json"
    frame.to_csv(all_claims_path, index=False, lineterminator="\n")
    frame.to_csv(claims_path, index=False, lineterminator="\n")
    manifest = {
        "manifest_version": "municipal-curated-wayback-freeze-manifest-v1",
        "freeze_contract_version": FREEZE_CONTRACT_VERSION,
        "validation_method_version": VALIDATION_METHOD_VERSION,
        "validation_key_fields": [
            "evidence_id",
            "candidate_year_id",
            "exact_normalized_url",
        ],
        "validation_key_separator": "U+001F",
        "evidence_hash_definition": (
            "sha256(utf8(evidence_id + U+001F + candidate_year_id + U+001F + "
            "exact_normalized_url))"
        ),
        "evidence_record_hash_definition": (
            "sha256(canonical JSON of the exact acquisition evidence row)"
        ),
        "context_hash_definition": (
            "sha256(canonical JSON of the exact authoritative candidate-target row)"
        ),
        "identity_decision_hash_definition": (
            "sha256(canonical JSON of the exact independent merged "
            "identity-decision row)"
        ),
        "validation_shard_definition": (
            f"int(evidence_hash, 16) modulo {VALIDATION_SHARD_COUNT}"
        ),
        "validation_shard_count": VALIDATION_SHARD_COUNT,
        "eligible_url_types": sorted(ELIGIBLE_URL_TYPES),
        "identity_gate_pass_status": IDENTITY_SUPPORTED_STATUS,
        "frozen_claim_columns": FROZEN_CLAIM_COLUMNS,
        "acquisition_audit": {"verdict": "pass"},
        "merged_acquisition_audit": {"verdict": "pass"},
        "identity_decisions_source": {"path": "fixture", "rows": len(frame)},
        "merged_acquisition_manifest": {"path": "fixture"},
        "inputs": [],
        "summary": {
            "all_claims": len(frame),
            "coverage_url_type_claims": int(
                frame["coverage_url_type_eligible"].astype(bool).sum()
            ),
            "identity_supported_claims": int(
                frame["identity_gate_pass"].astype(bool).sum()
            ),
            "eligible_claims": len(frame),
            "validation_shard_counts": {
                str(shard_id): int(
                    frame["validation_shard_id"].astype(int).eq(shard_id).sum()
                )
                for shard_id in range(VALIDATION_SHARD_COUNT)
            },
        },
        "outputs": {
            "all_claims": {
                "path": str(all_claims_path),
                "bytes": all_claims_path.stat().st_size,
                "sha256": _sha256_file(all_claims_path),
                "rows": len(frame),
            },
            "eligible_claims": {
                "path": str(claims_path),
                "bytes": claims_path.stat().st_size,
                "sha256": _sha256_file(claims_path),
                "rows": len(frame),
            }
        },
    }
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return claims_path, manifest_path


def _capture(
    timestamp: str,
    *,
    original: str = EXACT_URL,
    digest: str = "digest-1",
) -> validator.Capture:
    return validator.Capture(
        timestamp=timestamp,
        original=original,
        mimetype="text/html",
        statuscode=200,
        digest=digest,
    )


def _validate_one(row: dict[str, object]) -> dict[str, object]:
    return validator.validate_one_claim(
        row,
        timeout=1,
        attempts=1,
        delay_seconds=0,
        query_cache={},
        capture_cache={},
    )


def test_frozen_manifest_hash_shard_and_identity_contract(tmp_path: Path) -> None:
    claim = _claim()
    claims_path, manifest_path = _write_freeze_bundle(tmp_path / "valid", _frame(claim))
    loaded = validator.load_frozen_targets(
        claims_path, manifest_path, int(claim["validation_shard_id"])
    )

    assert loaded.columns.tolist() == FROZEN_CLAIM_COLUMNS
    assert loaded.iloc[0]["validation_key"] == validation_key(
        claim["evidence_id"], claim["candidate_year_id"], claim["exact_normalized_url"]
    )
    assert int(loaded.iloc[0]["validation_shard_id"]) == (
        int(str(claim["evidence_hash"]), 16) % VALIDATION_SHARD_COUNT
    )

    bad_hash = claim.copy()
    bad_hash["evidence_hash"] = "0" * 64
    bad_path, bad_manifest = _write_freeze_bundle(
        tmp_path / "bad-hash", _frame(bad_hash)
    )
    with pytest.raises(validator.MunicipalWaybackValidationError, match="hash"):
        validator.load_frozen_targets(
            bad_path, bad_manifest, int(bad_hash["validation_shard_id"])
        )

    bad_shard = claim.copy()
    bad_shard["validation_shard_id"] = 1 - int(claim["validation_shard_id"])
    shard_path, shard_manifest = _write_freeze_bundle(
        tmp_path / "bad-shard", _frame(bad_shard)
    )
    with pytest.raises(validator.MunicipalWaybackValidationError, match="shard"):
        validator.load_frozen_targets(
            shard_path, shard_manifest, int(bad_shard["validation_shard_id"])
        )

    failed_identity = _claim(identity_attribution_status="match_unresolved")
    identity_path, identity_manifest = _write_freeze_bundle(
        tmp_path / "failed-identity", _frame(failed_identity)
    )
    with pytest.raises(validator.MunicipalWaybackValidationError, match="identity"):
        validator.load_frozen_targets(
            identity_path,
            identity_manifest,
            int(failed_identity["validation_shard_id"]),
        )

    ineligible_url = _claim(url_type="social")
    url_path, url_manifest = _write_freeze_bundle(
        tmp_path / "ineligible-url", _frame(ineligible_url)
    )
    with pytest.raises(
        validator.MunicipalWaybackValidationError,
        match="URL type|url_type|Eligible row count",
    ):
        validator.load_frozen_targets(
            url_path, url_manifest, int(ineligible_url["validation_shard_id"])
        )


def test_manifest_file_digest_drift_is_rejected(tmp_path: Path) -> None:
    claim = _claim()
    claims_path, manifest_path = _write_freeze_bundle(
        tmp_path, _frame(claim)
    )
    claims_path.write_text(claims_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")

    with pytest.raises(validator.MunicipalWaybackValidationError, match="sha256|bytes"):
        validator.load_frozen_targets(
            claims_path, manifest_path, int(claim["validation_shard_id"])
        )


def test_public_cdx_url_uses_exact_target_and_preserves_path_query_case() -> None:
    url = validator.public_cdx_url(
        EXACT_URL, date(2016, 5, 15), date(2020, 5, 15)
    )
    query = parse_qs(urlsplit(url).query)

    assert query["url"] == [EXACT_URL]
    assert query["matchType"] == ["exact"]
    assert query["from"] == ["20160515"]
    assert query["to"] == ["20200515"]
    assert query["output"] == ["json"]
    assert query["fl"] == ["timestamp,original,mimetype,statuscode,digest"]
    assert "*" not in query["url"][0]
    assert "/Campaign/Mayor?Ref=CityVote&A=UPPER" in query["url"][0]


def test_missing_month_never_queries_or_imputes_november(monkeypatch: pytest.MonkeyPatch) -> None:
    def fail_query(*args: object, **kwargs: object) -> list[validator.Capture]:
        pytest.fail("missing election month must not issue a Wayback request")

    monkeypatch.setattr(validator, "query_public_wayback", fail_query)
    result = _validate_one(_claim(month=""))

    assert result["archive_query_status"] == "not_attempted_missing_election_month"
    assert result["capture_fetch_status"] == "not_attempted_missing_election_month"
    assert result["domain_reuse_status"] == "not_inspected_missing_election_month"
    assert result["election_reference_date"] == ""
    assert result["validation_reason"] == "missing_election_month_no_imputation"
    assert result["temporal_validation_status"] == "missing_election_month"
    assert "november" not in json.dumps(result, default=str).casefold()


@pytest.mark.parametrize(
    "html,missing_signal,reason",
    [
        (
            "<html><title>Alex Roe for Mayor of Metroville</title></html>",
            "candidate_identity_match",
            "candidate_identity_not_confirmed",
        ),
        (
            "<html><title>Jane Doe for City Council in Metroville</title></html>",
            "mayoral_campaign_match",
            "mayoral_campaign_not_confirmed",
        ),
        (
            "<html><title>Jane Doe for Mayor of Elsewhere</title></html>",
            "city_match",
            "city_not_confirmed",
        ),
    ],
)
def test_grade_a_requires_identity_mayoral_campaign_and_city(
    monkeypatch: pytest.MonkeyPatch,
    html: str,
    missing_signal: str,
    reason: str,
) -> None:
    monkeypatch.setattr(
        validator,
        "query_public_wayback",
        lambda *args, **kwargs: [_capture("20180515120000")],
    )
    monkeypatch.setattr(
        validator,
        "fetch_public_capture",
        lambda *args, **kwargs: html,
    )
    result = _validate_one(_claim())

    assert result[missing_signal] is False
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible"] is False
    assert result["validation_reason"] == reason


def test_happy_exact_capture_emits_grade_a(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        validator,
        "query_public_wayback",
        lambda *args, **kwargs: [_capture("20180515120000")],
    )
    monkeypatch.setattr(
        validator,
        "fetch_public_capture",
        lambda *args, **kwargs: (
            "<html><head><title>Jane Doe for Mayor of Metroville</title></head>"
            "<body>Elect Jane Doe mayor of Metroville.</body></html>"
        ),
    )
    result = _validate_one(_claim())

    assert result["exact_normalized_url"] == EXACT_URL
    assert result["archive_query_target"] == EXACT_URL
    assert result["nearest_capture_original_url"] == EXACT_URL
    assert result["candidate_identity_match"] is True
    assert result["mayoral_campaign_match"] is True
    assert result["city_match"] is True
    assert result["within_headline_window"] is True
    assert result["within_sensitivity_window"] is True
    assert result["domain_reuse_status"] == "no_additional_capture"
    assert result["proposed_evidence_grade"] == "A"
    assert result["strict_coverage_eligible"] is True
    assert (
        result["temporal_validation_status"]
        == "grade_a_content_confirmed_pending_review"
    )


def test_sensitivity_only_capture_remains_grade_c(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(
        validator,
        "query_public_wayback",
        lambda *args, **kwargs: [_capture("20170301120000")],
    )
    monkeypatch.setattr(
        validator,
        "fetch_public_capture",
        lambda *args, **kwargs: (
            "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        ),
    )
    result = _validate_one(_claim())

    assert int(result["nearest_capture_days_absolute"]) > 365
    assert result["within_headline_window"] is False
    assert result["within_sensitivity_window"] is True
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible"] is False
    assert result["validation_reason"] == "outside_headline_365_within_sensitivity"
    assert (
        result["temporal_validation_status"]
        == "outside_headline_365_within_sensitivity"
    )


def test_two_year_reuse_conflict_blocks_grade_a(monkeypatch: pytest.MonkeyPatch) -> None:
    nearest = _capture("20180515120000", digest="nearest")
    earlier = _capture("20160615120000", digest="earlier")
    monkeypatch.setattr(
        validator,
        "query_public_wayback",
        lambda *args, **kwargs: [earlier, nearest],
    )

    def fetch(capture: validator.Capture, *args: object, **kwargs: object) -> str:
        if capture.timestamp == nearest.timestamp:
            return "<html><title>Jane Doe for Mayor of Metroville</title></html>"
        return "<html><title>Alex Roe for Mayor of Metroville</title></html>"

    monkeypatch.setattr(validator, "fetch_public_capture", fetch)
    result = _validate_one(_claim())

    assert result["reuse_window_start"] == "2016-05-15"
    assert result["reuse_window_end"] == "2020-05-15"
    assert result["reuse_probe_count"] == 1
    assert result["reuse_conflict_count"] == 1
    assert result["domain_reuse_status"] == "conflicting_content_detected"
    assert result["proposed_evidence_grade"] == "C"
    assert result["strict_coverage_eligible"] is False
    assert result["validation_reason"] == "domain_reuse_conflict"


@pytest.mark.parametrize(
    "stale_column", ["evidence_record_hash", "context_hash", "identity_decision_hash"]
)
def test_request_error_is_retried_and_stale_hash_checkpoint_is_pruned(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, stale_column: str
) -> None:
    claim = _claim()
    frozen = _frame(claim)
    output = tmp_path / "validation.csv"
    calls = 0

    def transient_error(*args: object, **kwargs: object) -> list[validator.Capture]:
        nonlocal calls
        calls += 1
        raise validator.PublicWaybackRequestError("temporary public Wayback failure")

    monkeypatch.setattr(validator, "query_public_wayback", transient_error)
    first = validator.validate_claims(
        frozen,
        output,
        shard_id=int(claim["validation_shard_id"]),
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert calls == 1
    assert first.iloc[0]["archive_query_status"] == "request_error"
    assert first.iloc[0]["temporal_validation_status"] == "request_error_retryable"

    monkeypatch.setattr(
        validator, "query_public_wayback", lambda *args, **kwargs: []
    )
    second = validator.validate_claims(
        frozen,
        output,
        shard_id=int(claim["validation_shard_id"]),
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert second.iloc[0]["archive_query_status"] == "checked"
    assert second.iloc[0]["validation_reason"] == "no_public_wayback_capture"

    checkpoint = pd.read_csv(output, keep_default_na=False)
    checkpoint.loc[0, stale_column] = "0" * 64
    checkpoint.to_csv(output, index=False, lineterminator="\n")
    stale_calls = 0

    def count_revalidation(*args: object, **kwargs: object) -> list[validator.Capture]:
        nonlocal stale_calls
        stale_calls += 1
        return []

    monkeypatch.setattr(validator, "query_public_wayback", count_revalidation)
    third = validator.validate_claims(
        frozen,
        output,
        shard_id=int(claim["validation_shard_id"]),
        timeout=1,
        attempts=1,
        delay_seconds=0,
    )
    assert stale_calls == 1
    assert len(third) == 1
    assert third.iloc[0][stale_column] == claim[stale_column]


def test_shard_output_has_exact_key_equality_and_accepted_schema(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    claims = [
        _claim(
            evidence_id=f"evidence-{index}",
            candidate_year_id=f"candidate-{index}",
            exact_url=f"https://example.org/Candidate/{index}?Ref=Vote{index}",
        )
        for index in range(1, 9)
    ]
    frame = _frame(*claims)
    monkeypatch.setattr(
        validator, "query_public_wayback", lambda *args, **kwargs: []
    )

    for shard_id in range(VALIDATION_SHARD_COUNT):
        expected = {
            str(row["validation_key"])
            for row in claims
            if int(row["validation_shard_id"]) == shard_id
        }
        output = tmp_path / f"validation_shard_{shard_id}.csv"
        result = validator.validate_claims(
            frame,
            output,
            shard_id=shard_id,
            timeout=1,
            attempts=1,
            delay_seconds=0,
        )
        assert validator.VALIDATION_COLUMNS == RESULT_COLUMNS
        assert result.columns.tolist() == RESULT_COLUMNS
        assert set(result["validation_key"].astype(str)) == expected
        assert not result["validation_key"].duplicated().any()
        assert set(result["validation_shard_id"].astype(int)) <= {shard_id}
        assert pd.read_csv(output, keep_default_na=False).columns.tolist() == RESULT_COLUMNS
