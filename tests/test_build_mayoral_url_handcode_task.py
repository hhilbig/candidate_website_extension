import pandas as pd

from scripts.build_mayoral_url_handcode_task import build_review_rows, task_payload


def test_review_rows_include_all_leads_and_ten_percent_negatives():
    leads = pd.DataFrame([
        {
            "candidate_year_id": "1__2020", "full_name": "Alpha", "geo_name": "City",
            "state_abb": "AA", "year": 2020, "proposed_campaign_url": "https://alpha.test",
            "evidence_source_url": "https://source.test/a", "result_title": "Alpha",
            "result_snippet": "Campaign website", "query_variant": "primary",
            "evidence_type": "candidate_domain", "temporal_conflict": False,
            "office_mismatch_signal": False,
        },
        *[
            {
                "candidate_year_id": f"{index}__2020", "full_name": f"Name {index}",
                "geo_name": "City", "state_abb": "AA", "year": 2020,
                "proposed_campaign_url": "", "evidence_source_url": f"https://source.test/{index}",
                "result_title": "Source", "result_snippet": "No link", "query_variant": "primary",
                "evidence_type": "source_evidence", "temporal_conflict": False,
                "office_mismatch_signal": False,
            }
            for index in range(2, 12)
        ],
    ])
    expansion = pd.DataFrame([
        {
            "candidate_year_id": "2__2020", "full_name": "Beta", "geo_name": "City",
            "state_abb": "AA", "year": 2020, "evidence_source_url": "https://source.test/2",
            "fetch_status": "checked_with_candidate_link", "discovered_campaign_url": "https://beta.test",
            "extraction_method": "anchor",
        },
        *[
            {
                "candidate_year_id": f"{index}__2020", "full_name": f"Name {index}",
                "geo_name": "City", "state_abb": "AA", "year": 2020,
                "evidence_source_url": f"https://source.test/{index}",
                "fetch_status": "checked_no_candidate_link", "discovered_campaign_url": "",
                "extraction_method": "",
            }
            for index in range(3, 12)
        ],
    ])
    archive = pd.DataFrame([{
        "candidate_year_id": "1__2020", "proposed_campaign_url": "https://alpha.test",
        "archive_query_status": "checked", "archive_capture_count": 1,
        "nearest_capture_timestamp": "20201101000000", "nearest_capture_days": 2,
        "temporal_validation_status": "archive_capture_within_365_days",
    }])

    rows = build_review_rows(leads, expansion, archive)
    assert rows["candidate_url"].fillna("").ne("").sum() == 2
    assert rows["lead_origin"].eq("negative_audit").sum() == 1
    assert len(rows) == 3
    payload = task_payload(rows)
    assert payload["task_id"] == "mayoral_url_pilot_adjudication_20260824"
    assert len(payload["questions"]) == 6
