import pandas as pd
import pytest

from scripts.build_municipal_records_request_packet import build_packet


def fixture_frames():
    priorities = []
    inventory = []
    universe = []
    for rank in range(1, 21):
        city = f"city {rank}"
        state = f"S{rank:02d}"
        priorities.append({
            "priority_rank": rank, "geo_name": city, "state_abb": state,
            "unresolved_candidate_years": 1,
        })
        inventory.append({
            "priority_rank": rank, "city": city, "state": state,
            "unresolved_2000_2007": 1, "source_id": f"source_{rank}",
            "source_type": "candidate_records", "official_url": f"https://city{rank}.gov/records",
            "access_status": "open_site",
        })
        universe.append({
            "candidate_year_id": f"candidate_{rank}", "contest": f"contest_{rank}",
            "geo_name": city, "state_abb": state, "year": 2003, "month": 5,
            "in_discovery_scope": True,
        })
    return pd.DataFrame(priorities), pd.DataFrame(inventory), pd.DataFrame(universe)


def test_builds_20_exact_scoped_drafts_and_keeps_approval_gate():
    rendered = build_packet(*fixture_frames())
    assert rendered.count("**Subject:**") == 20
    assert "City 1, S01" in rendered
    assert "2003 (month 05, 1 candidates)" in rendered
    assert "No request has been sent" in rendered
    assert "https://city20.gov/records" in rendered


def test_rejects_inventory_that_does_not_match_priority():
    priority, inventory, universe = fixture_frames()
    inventory.loc[0, "unresolved_2000_2007"] = 2
    with pytest.raises(ValueError, match="does not match"):
        build_packet(priority, inventory, universe)
