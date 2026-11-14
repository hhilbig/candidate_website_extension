import pandas as pd
import pytest

from src.release_geography import normalize_geography


def test_senate_district_is_missing_and_house_zero_is_retained():
    rows = pd.DataFrame({
        "office": ["senate", "house", "house"],
        "district": [3, 0, 53],
        "district_id": ["IA03", "AK00", "CA53"],
    })
    out = normalize_geography(rows)
    assert pd.isna(out.loc[0, "district"])
    assert pd.isna(out.loc[0, "district_id"])
    assert out.loc[1:, "district"].tolist() == [0.0, 53.0]


def test_impossible_house_district_requires_adjudication():
    rows = pd.DataFrame({
        "candidate_cycle_id": ["BAD-2024"],
        "office": ["house"],
        "district": [57],
    })
    with pytest.raises(ValueError, match="unadjudicated"):
        normalize_geography(rows)
