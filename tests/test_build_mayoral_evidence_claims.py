import pandas as pd
import pytest

from scripts.build_mayoral_evidence_claims import (
    ClaimSchemaError,
    build_claim_table,
    write_claim_table,
)


def universe():
    return pd.DataFrame([
        {
            "candidate_year_id": "10__2021",
            "candidate_contest_id": "contest_2021_mayor__10",
            "ledb_candid": 10,
            "full_name": "jane doe",
            "geo_name": "sample city",
            "state_abb": "CA",
            "year": 2021,
            "month": 11,
        },
        {
            "candidate_year_id": "11__2021",
            "candidate_contest_id": "contest_2021_mayor__11",
            "ledb_candid": 11,
            "full_name": "john roe",
            "geo_name": "sample city",
            "state_abb": "CA",
            "year": 2021,
            "month": 11,
        },
    ])


def legacy(**updates):
    row = {
        "candidate_year_id": "10__2021",
        "evidence_url": (
            "https://web.archive.org/web/20211001000000/"
            "https://VoteJane.COM/Issues/Plan.HTML?Ref=Mail"
        ),
        "validated_campaign_url": (
            "https://VoteJane.COM/Issues/Plan.HTML?Ref=Mail"
        ),
        "source_profile_url": "https://example.org/source",
        "source_snapshot_timestamp": "20211001000000",
        "url_source": "Ballotpedia archived profile",
        "evidence_text": "Campaign website",
        "discovery_method": "ballotpedia_campaign_section_citation",
        "validation_status": "valid_source_backed_campaign_url",
    }
    row.update(updates)
    return row


def adapter(**updates):
    row = {
        "claim_id": "adapter_claim_1",
        "source_id": "official_city_records",
        "parser_version": "official_records_v1",
        "candidate_year_id": "10__2021",
        "candidate_contest_id": "contest_2021_mayor__10",
        "contest_id": "contest_2021_mayor",
        "year": 2021,
        "month": 11,
        "raw_url": "https://JaneForMayor.org/Issues?Ref=Mail",
        "exact_normalized_url": "https://JaneForMayor.org/Issues?Ref=Mail",
        "url_type": "candidate_owned_custom_domain",
        "source_date": "2021-10-01",
        "source_profile_url": "https://city.gov/candidates",
        "source_snapshot_timestamp": "20211001000000",
        "url_source": "Official candidate filing",
        "evidence_text": "Website: JaneForMayor.org",
        "discovery_method": "official_record_hyperlink",
        "identity_match": True,
        "temporal_match": "exact_election_month",
        "evidence_grade": "A",
        "review_status": "accepted",
        "acceptance": True,
    }
    row.update(updates)
    return row


def test_legacy_rows_preserve_raw_and_exact_url_case_but_never_count_strict():
    claims, audit = build_claim_table(universe(), pd.DataFrame([legacy()]))
    row = claims.iloc[0]

    assert row.raw_url.startswith("https://web.archive.org/web/")
    assert row.exact_normalized_url == (
        "https://VoteJane.COM/Issues/Plan.HTML?Ref=Mail"
    )
    assert row.site_root_url == "https://VoteJane.COM/"
    assert row.registrable_domain == "votejane.com"
    assert row.source_date == "2021-10-01"
    assert row.source_id == "internet_archive_ballotpedia"
    assert row.evidence_grade == "legacy_ungraded"
    assert row.review_status == "legacy_not_adjudicated_current_rules"
    assert not row.human_accepted
    assert not row.strict_custom_domain
    assert not row.strict_exact_url
    assert row.legacy_validation_status == "valid_source_backed_campaign_url"
    assert audit["strict_exact_url_claims"] == 0


def test_human_accepted_grade_a_custom_domain_counts_for_both_measures():
    claims, _ = build_claim_table(
        universe(), pd.DataFrame([legacy()]), [("adapter.csv", pd.DataFrame([adapter()]))]
    )
    row = claims.loc[claims.claim_origin.eq("adapter")].iloc[0]

    assert row.human_accepted
    assert row.strict_custom_domain
    assert row.strict_exact_url
    assert row.strict_exclusion_reason == ""


@pytest.mark.parametrize(
    "updates,reason",
    [
        ({"review_status": "pending", "acceptance": None}, "human_acceptance_required"),
        ({"evidence_grade": "C"}, "evidence_grade_not_strict"),
        ({"identity_match": False}, "identity_mismatch"),
        ({"temporal_match": "mismatch"}, "temporal_mismatch"),
        ({"url_type": "excluded_social_lead"}, "url_type_not_strict"),
    ],
)
def test_strict_flags_require_review_grade_identity_timing_and_url_type(updates, reason):
    claims, _ = build_claim_table(
        universe(), pd.DataFrame([legacy()]),
        [("adapter.csv", pd.DataFrame([adapter(**updates)]))],
    )
    row = claims.loc[claims.claim_origin.eq("adapter")].iloc[0]
    assert not row.strict_custom_domain
    assert not row.strict_exact_url
    assert row.strict_exclusion_reason == reason


def test_exact_url_coverage_includes_accepted_shared_paths_but_primary_does_not():
    row = adapter(
        url_type="shared_host_path",
        raw_url="https://sites.google.com/site/Jane/Mayor?Ref=A",
        exact_normalized_url="https://sites.google.com/site/Jane/Mayor?Ref=A",
    )
    claims, _ = build_claim_table(
        universe(), pd.DataFrame([legacy()]), [("adapter.csv", pd.DataFrame([row]))]
    )
    claim = claims.loc[claims.claim_origin.eq("adapter")].iloc[0]
    assert not claim.strict_custom_domain
    assert claim.strict_exact_url


def test_independent_source_claims_for_the_same_url_are_preserved():
    first = adapter(claim_id="claim_a", source_id="official_city_records")
    second = adapter(claim_id="claim_b", source_id="archive_it_collection_1")
    claims, audit = build_claim_table(
        universe(), pd.DataFrame([legacy()]),
        [("one.csv", pd.DataFrame([first])), ("two.csv", pd.DataFrame([second]))],
    )

    same_url = claims.loc[claims.exact_normalized_url.eq(first["exact_normalized_url"])]
    assert len(same_url) == 2
    assert same_url.source_id.nunique() == 2
    assert audit["input_claim_rows"] == audit["output_claim_rows"] == 3
    assert audit["exact_claim_id_agreement"] is True


def test_duplicate_claim_ids_fail_instead_of_silently_deduplicating():
    first = adapter(claim_id="collision", source_id="source_one")
    second = adapter(claim_id="collision", source_id="source_two")
    with pytest.raises(ClaimSchemaError, match="Duplicate claim_id"):
        build_claim_table(
            universe(), pd.DataFrame([legacy()]),
            [("one.csv", pd.DataFrame([first])), ("two.csv", pd.DataFrame([second]))],
        )


def test_acceptance_and_review_status_must_agree():
    with pytest.raises(ClaimSchemaError, match="acceptance=true"):
        build_claim_table(
            universe(), pd.DataFrame([legacy()]),
            [("adapter.csv", pd.DataFrame([adapter(review_status="pending")]))],
        )


def test_candidate_context_mismatch_fails():
    with pytest.raises(ClaimSchemaError, match="month=10"):
        build_claim_table(
            universe(), pd.DataFrame([legacy()]),
            [("adapter.csv", pd.DataFrame([adapter(month=10)]))],
        )


def test_rebuild_reports_and_removes_stale_claims(tmp_path):
    claims, audit = build_claim_table(universe(), pd.DataFrame([legacy()]))
    stale = claims.iloc[0].copy()
    stale["claim_id"] = "stale_claim"
    output = tmp_path / "claims.csv"
    audit_output = tmp_path / "audit.json"
    pd.concat([claims, stale.to_frame().T], ignore_index=True).to_csv(output, index=False)

    final_audit = write_claim_table(claims, audit, output, audit_output)

    rebuilt = pd.read_csv(output)
    assert rebuilt.claim_id.tolist() == claims.claim_id.tolist()
    assert final_audit["stale_claims_removed"] == 1
    assert audit_output.exists() and audit_output.stat().st_size > 0


def test_stable_legacy_claim_id_is_independent_of_input_row_order():
    rows = pd.DataFrame([
        legacy(),
        legacy(
            candidate_year_id="11__2021",
            evidence_url="https://JohnForMayor.org/",
            validated_campaign_url="https://JohnForMayor.org/",
            evidence_text="John's campaign website",
        ),
    ])
    first, _ = build_claim_table(universe(), rows)
    second, _ = build_claim_table(universe(), rows.iloc[::-1].reset_index(drop=True))
    assert first.claim_id.tolist() == second.claim_id.tolist()
