from scripts.build_mayoral_first_round_dates import build


def election(eid, date, city="c1", state="WA", stage="general"):
    return {"election_id": eid, "city_id": city, "city_name": "X city", "state_abb": state, "election_date": date,
            "stage": stage, "year": date[:4]}


def test_sources_in_order_and_ballotpedia_state_filter():
    elections = [election("p", "2021-11-02"), election("n", "2017-11-07"), election("b", "2013-11-05"),
                 election("s", "2011-11-08"), election("o", "2009-11-03")]
    primaries = [{"general_election_id": "p", "primary_date": "2021-06-22", "source_url": "u", "primary_party": "Democratic"}]
    calendars = {("c1", 2017): {"note": "The primary on August 1, 2017 and the general.", "source_url": "cal"}}
    pages = [{"city_id": "c1", "page": "Mayoral_election_in_X,_Washington_(2013)", "snapshot": "bp",
              "fields": [{"round": "primary", "date": "2013-08-06"}, {"round": "general", "date": "2013-11-05"}]},
             {"city_id": "c1", "page": "Mayoral_election_in_X,_Illinois_(2011)", "snapshot": "wrong state",
              "fields": [{"round": "primary", "date": "2011-02-22"}, {"round": "general", "date": "2011-11-08"}]}]
    search = {"s": {"rounds": "single_round", "first_round_date": "2011-11-08", "first_round_source": "agent_search",
                    "first_round_source_url": "w", "note": ""}}
    rows = {r["election_id"]: r for r in build(elections, primaries, calendars, pages, search)}
    assert (rows["p"]["first_round_date"], rows["p"]["first_round_source"]) == ("2021-06-22", "primary_file")
    assert (rows["n"]["first_round_date"], rows["n"]["first_round_source"]) == ("2017-08-01", "calendar_note")
    assert (rows["b"]["first_round_date"], rows["b"]["first_round_source"]) == ("2013-08-06", "ballotpedia")
    assert (rows["s"]["rounds"], rows["s"]["first_round_source"]) == ("single_round", "agent_search")
    assert (rows["o"]["rounds"], rows["o"]["first_round_date"]) == ("unknown", "")


def test_ballotpedia_single_round():
    pages = [{"city_id": "c1", "page": "Mayoral_election_in_X,_Washington_(2019)", "snapshot": "bp",
              "fields": [{"round": "general", "date": "2019-11-05"}]}]
    row = build([election("g", "2019-11-05")], [], {}, pages, {})[0]
    assert (row["rounds"], row["first_round_date"]) == ("single_round", "2019-11-05")
