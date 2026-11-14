"""Geography rules shared by all public release products."""
from __future__ import annotations

import pandas as pd


def normalize_geography(rows: pd.DataFrame) -> pd.DataFrame:
    """Normalize districts and reject impossible federal House geography.

    Senate districts are not meaningful and are always missing. House district
    zero represents at-large and territorial seats; numbered districts must be
    between 1 and 53. Invalid source records must be adjudicated before calling
    this function rather than silently recoded.
    """
    out = rows.copy()
    if "office" not in out or "district" not in out:
        return out

    office = out["office"].astype("string").str.strip().str.lower()
    district = pd.to_numeric(out["district"], errors="coerce")
    house = office.eq("house")
    invalid = house & ~(district.eq(0) | district.between(1, 53))
    if invalid.any():
        keys = [c for c in ("candidate_cycle_id", "cand_id", "year") if c in out]
        details = out.loc[invalid, keys + ["office", "district"]].to_dict("records")
        raise ValueError(f"unadjudicated federal House district values: {details}")

    district = district.astype(float)
    district.loc[office.eq("senate")] = float("nan")
    out["office"] = office
    out["district"] = district
    if "district_id" in out:
        out.loc[office.eq("senate"), "district_id"] = pd.NA
    return out
