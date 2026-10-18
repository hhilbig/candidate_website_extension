"""Tests for scripts/build_mayoral_v6_primaries.py: IDs, finalist links and the searched flag."""
import pytest

from scripts.build_mayoral_v6_primaries import build
from scripts.build_mayoral_v6_primary_search_args import candidate_id

ROW = {"city_id": "1", "city_name": "A city", "state_abb": "XX", "general_election_id": "1__2013-11-05__regular",
       "primary_party": "Democratic", "primary_date": "2013-09-10", "rank": "1", "votes": "10", "vote_share": "0.5",
       "unopposed": "False", "total_candidates_on_ballot": "4", "source_url": "u", "second_source_url": "",
       "finalist_role_in_general": "winner"}
COVERAGE = [{"election_id": "1__2013-11-05__regular", "candidate_name": "William de Blasio", "candidate_election_id": "f1"},
            {"election_id": "1__2013-11-05__regular", "candidate_name": "Joseph Lhota", "candidate_election_id": "f2"}]


def test_link_and_searched_flag():
    rows = [{**ROW, "candidate_name": "Bill de Blasio", "already_finalist": "yes"},
            {**ROW, "candidate_name": "Bill Thompson", "rank": "2", "already_finalist": "no"}]
    elections, candidates = build(rows, COVERAGE)
    assert [e["primary_election_id"] for e in elections] == ["1__2013-09-10__primary_democratic"]
    by_name = {c["candidate_name"]: c for c in candidates}
    assert by_name["Bill de Blasio"]["finalist_candidate_election_id"] == "f1"
    assert by_name["Bill de Blasio"]["searched"] == "False"
    assert by_name["Bill Thompson"]["searched"] == "True"
    assert by_name["Bill Thompson"]["candidate_election_id"] == candidate_id(rows[1])


def test_unopposed_is_not_searched_and_missing_link_fails():
    rows = [{**ROW, "candidate_name": "Lone Person", "unopposed": "True", "already_finalist": "no"}]
    assert build(rows, COVERAGE)[1][0]["searched"] == "False"
    with pytest.raises(ValueError):
        build([{**ROW, "candidate_name": "Nobody Here", "already_finalist": "yes"}], COVERAGE)
