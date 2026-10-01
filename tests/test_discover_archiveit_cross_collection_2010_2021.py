from __future__ import annotations

import pandas as pd

from scripts import discover_archiveit_cross_collection_2010_2021 as cross


def universe() -> pd.DataFrame:
    return pd.DataFrame([
        {
            "candidate_year_id": "cat_2018", "candidate_contest_id": "oak_2018_cat",
            "contest": "oak_2018", "full_name": "Cat Brooks", "geo_name": "oakland",
            "state_abb": "CA", "year": 2018, "month": 11,
        },
        {
            "candidate_year_id": "leslie_2020", "candidate_contest_id": "lan_2020_leslie",
            "contest": "lan_2020", "full_name": "Leslie Underwood", "geo_name": "lancaster",
            "state_abb": "PA", "year": 2020, "month": 11,
        },
    ])


def test_exact_name_plus_city_yields_pending_lead() -> None:
    payload = [{
        "id": 1, "collection": 11325, "url": "https://catbrooksforoakland.com/",
        "metadata": {"Title": ["Cat Brooks"]},
    }]
    audit, leads = cross.scan_collection(11325, payload, universe())
    assert len(audit) == 1
    assert len(leads) == 1
    assert leads.iloc[0].candidate_year_id == "cat_2018"
    assert leads.iloc[0].evidence_grade == "D"
    assert leads.iloc[0].review_status == "pending"


def test_other_office_context_is_retained_but_excluded() -> None:
    payload = [{
        "id": 2, "collection": 11325, "url": "https://underwoodforcongress.com/",
        "metadata": {
            "Title": ["Leslie Underwood"],
            "Description": ["Candidate for Congress in Lancaster"],
        },
    }]
    audit, leads = cross.scan_collection(11325, payload, universe())
    assert leads.empty
    assert audit.iloc[0].disposition == "other_office_conflict"


def test_no_exact_name_cannot_be_inferred_from_surname() -> None:
    payload = [{
        "id": 3, "collection": 11325, "url": "https://brooksforoakland.com/",
        "metadata": {"Title": ["Brooks campaign"]},
    }]
    audit, leads = cross.scan_collection(11325, payload, universe())
    assert leads.empty
    assert audit.iloc[0].disposition == "no_exact_candidate_identity"
