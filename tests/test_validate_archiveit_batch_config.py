import pandas as pd
import pytest

from scripts.validate_archiveit_batch_config import validate_config


def inputs():
    collections = [{
        "collection_id": i,
        "geo_name": f"city{i}",
        "state_abb": "AA",
        "start_year": 2020,
        "end_year": 2020,
        "uncovered_candidates_before": 1,
    } for i in range(1, 21)]
    universe = pd.DataFrame([{
        "candidate_year_id": f"c{i}", "geo_name": f"city{i}", "state_abb": "AA",
        "year": 2020, "contest": f"contest{i}", "in_discovery_scope": True,
    } for i in range(1, 21)])
    coverage = pd.DataFrame([{
        "candidate_year_id": "old", "covered": True, "coverage_basis": "legacy"
    }])
    config = {"batch_id": "b", "covered_candidate_years_before": 1, "collections": collections}
    return config, universe, coverage


def test_validates_exact_in_scope_uncovered_targets():
    result = validate_config(*inputs())
    assert result["collections"] == result["contest_targets"] == 20
    assert result["target_candidate_rows_with_collection_overlap"] == 20


def test_rejects_out_of_scope_target_instead_of_dropping_attempt():
    config, universe, coverage = inputs()
    universe.loc[universe.candidate_year_id.eq("c1"), "in_discovery_scope"] = False
    with pytest.raises(ValueError, match="no in-scope target"):
        validate_config(config, universe, coverage)
