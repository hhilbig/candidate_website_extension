import pandas as pd
import pytest

from scripts.repair_mayoral_search_pilot_context import repair_pilot_tables


def frames():
    universe = pd.DataFrame([{
        "candidate_year_id": "1__2021",
        "candidate_contest_id": "contest-1__candidate-1",
        "contest": "contest-1",
        "month": 4,
    }])
    manifest = pd.DataFrame([{
        "candidate_year_id": "1__2021", "full_name": "Alpha", "year": 2021,
        "query_variant": "primary",
    }])
    queue = pd.DataFrame([{
        "candidate_year_id": "1__2021", "full_name": "Alpha", "geo_name": "city",
        "state_abb": "AA", "year": 2021, "query_variant": "primary", "result_rank": 1,
        "proposed_campaign_url": "https://Alpha.example/Issues",
        "evidence_source_url": "https://official.example/guide", "evidence_type": "direct_candidate_domain",
        "exact_year_context": False, "result_title": "Old guide", "result_snippet": "",
        "mentioned_years": "2019", "temporal_conflict": True,
        "office_mismatch_signal": False, "review_status": "pending", "review_note": "",
    }])
    expansion = pd.DataFrame([{
        "candidate_year_id": "1__2021", "full_name": "Alpha", "geo_name": "city",
        "state_abb": "AA", "year": 2021,
        "evidence_source_url": "https://official.example/guide", "fetch_status": "checked_with_candidate_link",
        "http_status": 200, "discovered_campaign_url": "https://shared.example/Alpha/Mayor",
        "extraction_method": "candidate_heading_link", "source_snapshot_timestamp": "",
        "source_fetch_url": "https://official.example/guide", "error": "",
    }])
    return manifest, queue, expansion, universe


def test_repairs_all_context_and_builds_one_unified_queue():
    outputs = repair_pilot_tables(*frames())
    for key in ("query_manifest", "lead_queue", "source_expansion"):
        assert outputs[key].iloc[0]["candidate_contest_id"] == "contest-1__candidate-1"
        assert outputs[key].iloc[0]["contest"] == "contest-1"
        assert outputs[key].iloc[0]["month"] == 4
    unified = outputs["unified_evidence"]
    assert len(unified) == 2
    assert set(unified["evidence_type"]) == {
        "direct_candidate_domain", "expanded_domain_evidence"
    }
    assert unified["temporal_conflict"].astype(bool).all()
    assert unified["proposed_campaign_url"].str.contains("/Issues|/Alpha/Mayor").all()


def test_rejects_unmatched_or_conflicting_context():
    manifest, queue, expansion, universe = frames()
    with pytest.raises(ValueError, match="unmatched candidates"):
        repair_pilot_tables(
            manifest.assign(candidate_year_id="missing"), queue, expansion, universe
        )
    with pytest.raises(ValueError, match="conflicting month"):
        repair_pilot_tables(
            manifest.assign(month=11), queue, expansion, universe
        )
