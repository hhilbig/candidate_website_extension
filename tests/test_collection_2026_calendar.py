import copy
import json
from pathlib import Path

import pytest

from src.collection_2026_calendar import (
    events_for_candidate, load_calendar, load_events, validate_events,
)

ROOT = Path(__file__).resolve().parents[1]
CALENDAR = ROOT / "config/collection_2026_calendar.json"


def candidate_events(state, office="house", district="01", party="D", **extra):
    return events_for_candidate(
        {"state": state, "office": office, "district": district, "party": party,
         "year": 2026, **extra}, load_events(CALENDAR)
    )


def test_calendar_has_50_states_dc_and_territorial_house_elections():
    events = load_events(CALENDAR)
    states = {e["state"] for e in events if e["office"] == "house"}
    assert len(states) == 55
    assert {"DC", "GU", "VI", "MP", "AS"} <= states
    assert "PR" not in states
    assert len({e["state"] for e in events if e["office"] == "senate"}) == 35
    assert all(e["timezones"] and e["source_url"] for e in events)


def test_fec_filing_does_not_establish_participation():
    events = candidate_events("TX", cand_status="C", cand_ici="I")
    assert events
    assert all(e["participation"] == "unknown" for e in events)
    assert all(not e["participation_source_url"] for e in events)


def test_alabama_changed_primary_does_not_match_unchanged_districts():
    changed = candidate_events("AL", district="1")
    unchanged = candidate_events("AL", district="03")
    senate = candidate_events("AL", office="senate", district="")
    assert [(e["date"], e["event_type"]) for e in changed if "primary" in e["event_type"]] == [
        ("2026-08-11", "special_primary")
    ]
    assert {e["date"] for e in unchanged if "primary" in e["event_type"]} == {"2026-05-19", "2026-06-16"}
    assert {e["date"] for e in senate if "primary" in e["event_type"]} == {"2026-05-19", "2026-06-16"}


def test_louisiana_qualifying_deadline_is_not_a_primary_vote():
    house = candidate_events("LA")
    senate = candidate_events("LA", office="senate")
    assert [(e["date"], e["event_type"]) for e in house] == [
        ("2026-11-03", "nonpartisan_primary"), ("2026-12-12", "general_runoff")
    ]
    assert {e["date"] for e in senate} == {"2026-05-16", "2026-06-27", "2026-11-03"}
    assert not any(e["date"] == "2026-08-07" for e in load_events(CALENDAR))


@pytest.mark.parametrize("state,expected", [("AZ", "2026-07-21"), ("VA", "2026-08-04"), ("RI", "2026-09-09")])
def test_changed_state_primary_dates(state, expected):
    events = candidate_events(state)
    assert [e["date"] for e in events if e["event_type"] == "primary"] == [expected]


def test_south_carolina_replacement_rounds_are_separate_and_party_specific():
    republican = candidate_events("SC", office="senate", party="R")
    democrat = candidate_events("SC", office="senate", party="D")
    assert {e["date"] for e in republican} >= {"2026-06-09", "2026-08-11", "2026-08-25"}
    assert not {"2026-08-11", "2026-08-25"} & {e["date"] for e in democrat}
    assert len({e["event_id"] for e in republican}) == len(republican)


def test_ca_cancelled_runoff_is_auditable_but_not_collectable():
    events = candidate_events("CA", district="01")
    assert len([e for e in events if e["date"] == "2026-06-02"]) == 2
    assert not any(e["date"] == "2026-08-04" for e in events)
    cancelled = [e for e in load_calendar(CALENDAR)["events"] if e["event_status"] == "cancelled"]
    assert len(cancelled) == 1
    assert cancelled[0]["date"] == "2026-08-04"


def test_separate_special_and_regular_new_jersey_elections():
    events = candidate_events("NJ", district="11")
    assert {e["date"] for e in events} >= {"2026-02-05", "2026-04-16", "2026-06-02", "2026-11-03"}
    assert not any(e["is_special"] for e in candidate_events("NJ", district="10") if "is_special" in e)


def test_senate_specials_and_states_without_senate_elections():
    for state in ["FL", "OH"]:
        assert {e["event_type"] for e in candidate_events(state, office="senate")} == {"special_primary", "special_general"}
    assert candidate_events("CA", office="senate") == []


def test_sourced_participation_does_not_remove_losers_or_change_shared_events():
    events = load_events(CALENDAR)
    event = next(e for e in events if e["state"] == "TX" and e["event_type"] == "primary" and e["office"] == "house")
    evidence = {event["event_id"]: {"status": "primary_loser", "source_url": "https://www.sos.texas.gov/results"}}
    matched = candidate_events("TX", event_participation=evidence)
    row = next(e for e in matched if e["event_id"] == event["event_id"])
    assert row["participation"] == "primary_loser"
    assert "participation" not in event
    row["parties"].append("changed")
    assert event["parties"] == ["D", "R"]


def test_participation_without_source_is_rejected():
    event = candidate_events("TX")[0]
    with pytest.raises(ValueError, match="requires a source_url"):
        candidate_events("TX", event_participation={event["event_id"]: {"status": "primary_loser"}})


def test_fec_aliases_and_float_district_are_supported():
    candidate = {"cand_office_st": "AL", "cand_office": "H", "cand_office_district": "1.0", "cand_pty_affiliation": "DFL", "cand_election_yr": "2026"}
    events = events_for_candidate(candidate, load_events(CALENDAR))
    assert any(e["date"] == "2026-08-11" for e in events)
    assert not any(e["date"] == "2026-05-19" for e in events)


def test_missing_district_does_not_match_district_specific_primary():
    assert not any("primary" in e["event_type"] for e in candidate_events("AL", district=""))
    assert candidate_events("TX", year=2024) == []
    assert candidate_events("TX", party="LIB") == []
    assert candidate_events("PR") == []


def test_unresolved_dates_do_not_generate_fabricated_events():
    data = load_calendar(CALENDAR)
    assert any(u["state"] == "FL" and u["districts"] == ["20"] for u in data["unresolved_items"])
    assert any(u["state"] == "TX" and u["districts"] == ["23"] for u in data["unresolved_items"])
    assert not any(e.get("is_special") for e in candidate_events("FL", district="20"))
    assert not any(e.get("is_special") for e in candidate_events("TX", district="23"))


@pytest.mark.parametrize("field,value", [
    ("date", "2026-02-30"), ("date", "2025-01-01"),
    ("office", "governor"), ("parties", ["LIB"]),
    ("districts", ["*", "01"]), ("timezones", ["Invalid/Zone"]),
    ("source_url", "file:///tmp/source"),
    ("verification_status", "guess"),
])
def test_malformed_event_fails_validation(field, value):
    event = copy.deepcopy(load_events(CALENDAR)[0])
    event[field] = value
    with pytest.raises(ValueError):
        validate_events([event])


def test_duplicate_event_ids_and_definitions_fail_validation():
    event = load_events(CALENDAR)[0]
    with pytest.raises(ValueError, match="duplicate event_id"):
        validate_events([event, copy.deepcopy(event)])
    duplicate = copy.deepcopy(event)
    duplicate["event_id"] += "-other"
    with pytest.raises(ValueError, match="duplicate event definition"):
        validate_events([event, duplicate])


def test_list_format_is_supported(tmp_path):
    event = load_events(CALENDAR)[0]
    path = tmp_path / "calendar.json"
    path.write_text(json.dumps([event]))
    assert load_events(path) == [event]


def test_all_connecticut_convention_dates_have_state_verification():
    conventions = [e for e in load_events(CALENDAR) if e["state"] == "CT" and e["event_type"] == "convention"]
    assert len(conventions) == 10
    assert all(e["verification_status"] == "state_verified" for e in conventions)
    assert {e["date"] for e in conventions if e["parties"] == ["D"]} == {"2026-05-11"}
    assert {e["date"] for e in conventions if e["parties"] == ["R"]} == {"2026-05-15"}


def test_alabama_august_plurality_primary_has_no_runoff():
    events = candidate_events("AL", district="02")
    primary = next(e for e in events if e["date"] == "2026-08-11")
    assert primary["runoff_rule"] == "none_plurality_nomination"
    assert primary["prior_primary_status"] == "votes_void_for_nomination"
    assert not any("runoff" in e["event_type"] for e in events)


def test_maine_convention_date_uses_party_date_not_state_receipt_date():
    event = next(e for e in candidate_events("ME", office="senate") if e["event_type"] == "convention")
    assert event["date"] == "2026-07-25"
    assert event["verification_status"] == "party_verified"
    assert event["state_authority_source_url"].startswith("https://www1.maine.gov/sos/")
    assert event["participation"] == "unknown"


def test_georgia_april_runoff_has_verified_state_results():
    event = next(e for e in candidate_events("GA", district="14") if e["date"] == "2026-04-07")
    assert event["verification_status"] == "state_verified"
    assert event["event_status"] == "held"
    assert "results.sos.ga.gov" in event["verified_source_url"]


def test_sourced_pilot_participation_applies_only_to_the_exact_round():
    events=load_events()
    candidate={'candidate_id':'TEST','state':'TX','office':'senate','party':'R','year':2026,
               'pilot_evidence':{'candidate_id':'TEST','event_date':'2026-05-26','election_round':'primary_runoff',
                                 'participation_status':'confirmed','source_url':'https://example.org/results'}}
    matched=events_for_candidate(candidate,events)
    confirmed=[e for e in matched if e['participation']=='confirmed']
    assert len(confirmed)==1
    assert confirmed[0]['date']=='2026-05-26'
    assert all(e['participation']=='unknown' for e in matched if e['date']!='2026-05-26')
    candidate['pilot_evidence']['candidate_id']='OTHER'
    assert all(e['participation']=='unknown' for e in events_for_candidate(candidate,events))
