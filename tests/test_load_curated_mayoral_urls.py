import pandas as pd
import pytest

from scripts.load_curated_mayoral_urls import load_curated_claims


def universe():
    return pd.DataFrame([{
        "candidate_year_id": "1__2021", "ledb_candid": 1,
        "full_name": "annissa essaibi george", "geo_name": "boston",
        "state_abb": "MA", "year": 2021, "in_discovery_scope": True,
    }])


def claim(**updates):
    value = {
        "full_name": "annissa essaibi george", "geo_name": "boston",
        "state_abb": "MA", "year": 2021,
        "evidence_url": "https://www.annissaforboston.com",
        "url_source": "Boston.com guide",
        "source_profile_url": "https://www.boston.com/example",
        "evidence_text": "Official campaign website",
        "discovery_method": "curated_explicit_label", "confidence": "high",
    }
    value.update(updates)
    return value


def test_curated_claim_matches_one_scoped_candidate_and_preserves_provenance():
    result = load_curated_claims(universe(), [claim()])
    assert result.iloc[0]["candidate_year_id"] == "1__2021"
    assert result.iloc[0]["evidence_url"] == "https://www.annissaforboston.com"
    assert result.iloc[0]["evidence_text"] == "Official campaign website"


def test_curated_claim_rejects_nonunique_or_missing_candidate_match():
    with pytest.raises(ValueError, match="exactly one candidate-year"):
        load_curated_claims(universe(), [claim(full_name="another person")])


def test_curated_claim_rejects_excluded_host():
    with pytest.raises(ValueError, match="Invalid or excluded"):
        load_curated_claims(universe(), [claim(evidence_url="https://facebook.com/example")])
