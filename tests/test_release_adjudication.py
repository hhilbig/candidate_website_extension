import pandas as pd

from src.release_adjudication import (
    apply_capture_date_adjudication,
    apply_universe_adjudication,
)


def test_universe_exclusion_is_candidate_year_specific():
    rows = pd.DataFrame([
        {"year": 2022, "office": "house", "state": "TN", "cand_id": "A"},
        {"year": 2024, "office": "house", "state": "TN", "cand_id": "A"},
    ])
    decisions = pd.DataFrame([
        {"year": 2022, "office": "house", "state": "TN", "cand_id": "A",
         "action": "exclude"},
    ])
    out = apply_universe_adjudication(rows, decisions)
    assert list(zip(out.year, out.cand_id)) == [(2024, "A")]


def test_capture_date_range_is_inclusive_and_does_not_affect_other_ids():
    decisions = pd.DataFrame([
        {"year": 2014, "office": "senate", "state": "IA", "cand_id": "A",
         "action": "retain_date_range", "min_snap_day": 20140105,
         "max_snap_day": 20140105},
    ])
    rows = pd.DataFrame([
        {"year": 2014, "office": "senate", "state": "IA", "cand_id": "A",
         "date": "20140105010101"},
        {"year": 2014, "office": "senate", "state": "IA", "cand_id": "A",
         "date": "20140109010101"},
    ])
    out = apply_capture_date_adjudication(rows, decisions)
    assert out.date.tolist() == ["20140105010101"]

    other = rows.assign(cand_id="B")
    assert len(apply_capture_date_adjudication(other, decisions)) == 2
