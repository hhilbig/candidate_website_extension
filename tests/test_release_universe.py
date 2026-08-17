import pandas as pd

from src.release_universe import apply_capture_adjudication, select_captured_universe


def test_same_year_filter_does_not_filter_candidate_status():
    captures = pd.DataFrame([
        {"year": 2022, "office": "house", "state": "CA", "cand_id": "A"},
        {"year": 2022, "office": "house", "state": "CA", "cand_id": "B"},
    ])
    fec = pd.DataFrame([
        {"year": 2022, "cand_id": "A", "cand_election_yr": "2022", "cand_status": "N"},
        {"year": 2022, "cand_id": "B", "cand_election_yr": "2020", "cand_status": "C"},
    ])
    out = select_captured_universe(captures, fec, pd.DataFrame())
    assert out.cand_id.tolist() == ["A"]
    assert out.cand_status.tolist() == ["N"]


def test_verified_alias_is_remapped_and_false_match_is_dropped():
    captures = pd.DataFrame([
        {"year": 2018, "office": "house", "state": "IL", "cand_id": "OLD"},
        {"year": 2022, "office": "house", "state": "WA", "cand_id": "SARAH"},
    ])
    fec = pd.DataFrame(columns=["year", "cand_id", "cand_election_yr", "cand_status"])
    overrides = pd.DataFrame([
        {"year": 2018, "office": "house", "state": "IL", "source_cand_id": "OLD",
         "canonical_cand_id": "NEW", "action": "retain_ballot_override"},
        {"year": 2022, "office": "house", "state": "WA", "source_cand_id": "SARAH",
         "canonical_cand_id": "SARAH", "action": "exclude_false_ballot_match"},
    ])
    out = select_captured_universe(captures, fec, overrides)
    assert out.cand_id.tolist() == ["NEW"]
    assert out.universe_source.tolist() == ["ballot_override"]
    assert out.on_ballot.tolist() == [True]


def test_capture_adjudication_excludes_only_reviewed_candidate_year():
    rows = pd.DataFrame([
        {"year": 2018, "office": "house", "state": "WI", "cand_id": "BAD"},
        {"year": 2020, "office": "house", "state": "WI", "cand_id": "BAD"},
        {"year": 2018, "office": "house", "state": "LA", "cand_id": "ODD"},
    ])
    decisions = pd.DataFrame([
        {"year": 2018, "office": "house", "state": "WI", "cand_id": "BAD", "action": "exclude"},
        {"year": 2018, "office": "house", "state": "LA", "cand_id": "ODD", "action": "retain"},
    ])
    out = apply_capture_adjudication(rows, decisions)
    assert list(zip(out.year, out.cand_id)) == [(2020, "BAD"), (2018, "ODD")]
