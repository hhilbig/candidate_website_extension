import pandas as pd
import pytest

from scripts.build_archiveit_mayoral_handcode_task import (
    metadata_urls,
    prepare_review_rows,
    select_collections,
    task_payload,
)


def _claims() -> pd.DataFrame:
    rows = []
    for index in range(12):
        rows.append({
            "claim_id": f"claim_{index}",
            "source_id": "archive_it_collection_5903",
            "candidate_year_id": f"{index}__2001",
            "full_name": f"Candidate {index}",
            "geo_name": "Los Angeles",
            "state_abb": "CA",
            "year": 2001,
            "month": 6,
            "collection_id": 5903,
            "raw_url": f"http://archive.test/{index}/",
            "exact_normalized_url": f"http://archive.test/{index}/",
            "site_root_url": "http://archive.test/",
            "url_type": "shared_host_path",
            "evidence_grade": "A" if index < 2 else "C",
            "review_status": "pending",
            "acceptance": pd.NA,
            "temporal_validation_status": (
                "grade_a_content_confirmed_pending_review"
                if index < 2 else "no_collection_capture"
            ),
            "source_metadata_json": (
                '{"Description":[{"value":"Captured from '
                f'http://candidate{index}.example/path."}}]}}'
            ),
            "archive_within_headline_window": index < 2,
            "archive_within_sensitivity_window": index < 2,
            "archive_candidate_identity_match": index < 2,
            "archive_mayoral_campaign_match": index < 2,
            "archive_city_match": index < 2,
        })
    return pd.DataFrame(rows)


def test_prepare_review_rows_includes_all_claims_and_deterministic_audit():
    claims = _claims()
    first = prepare_review_rows(claims)
    second = prepare_review_rows(claims.sample(frac=1, random_state=4))

    assert len(first) == len(claims) == 12
    assert first["item_id"].is_unique
    assert first["source_review_status"].eq("pending").all()
    assert first["source_acceptance"].isna().all()
    assert first["proposed_strict_positive"].sum() == 2
    assert first["deterministic_non_strict_audit"].sum() == 1
    assert set(first.loc[first["deterministic_non_strict_audit"], "claim_id"]) == set(
        second.loc[second["deterministic_non_strict_audit"], "claim_id"]
    )


def test_task_payload_uses_required_battery_without_prefilled_decisions():
    payload = task_payload(prepare_review_rows(_claims()))

    assert payload["task_id"] == "mayoral_url_archiveit_adjudication_20260824"
    assert len(payload["items"]) == 12
    assert [question["id"] for question in payload["questions"]] == [
        "identity_match",
        "temporal_match",
        "accept_url",
        "url_type",
        "evidence_grade",
        "corrected_url",
    ]
    assert all("answers" not in item for item in payload["items"])
    assert "Machine-proposed grade (not an acceptance decision)" in (
        payload["items"][0]["display"][2]["content"]
    )


def test_task_payload_accepts_a_distinct_batch_task_id():
    payload = task_payload(
        prepare_review_rows(_claims()),
        task_id="archiveit_batch20_audit_20260825",
        title="Archive-It batch audit",
    )
    assert payload["task_id"] == "archiveit_batch20_audit_20260825"
    assert payload["title"] == "Archive-It batch audit"


def test_metadata_urls_extracts_original_campaign_url():
    raw = (
        '{"Description":[{"value":"Captured from http://CaseSensitive.example/Path. "},'
        '{"value":"Mirror https://second.example/index.html)"}]}'
    )
    assert metadata_urls(raw) == [
        "http://CaseSensitive.example/Path",
        "https://second.example/index.html",
    ]


def test_prepare_review_rows_rejects_nonpending_input():
    claims = _claims()
    claims.loc[0, "review_status"] = "accepted"
    with pytest.raises(ValueError, match="only pending"):
        prepare_review_rows(claims)


def test_select_collections_builds_an_explicit_review_batch():
    claims = _claims()
    claims.loc[0, "collection_id"] = 17435
    selected = select_collections(claims, "17435")
    assert selected["claim_id"].tolist() == ["claim_0"]
    with pytest.raises(ValueError, match="No claims found"):
        select_collections(claims, "99999")


def test_prepare_review_rows_preserves_explicit_batch_audit_reason():
    claims = _claims().iloc[:1].copy()
    claims["audit_inclusion_reason"] = "sampled_marginal_grade_a"
    rows = prepare_review_rows(claims)
    assert rows.iloc[0].review_inclusion_reason == "sampled_marginal_grade_a"
