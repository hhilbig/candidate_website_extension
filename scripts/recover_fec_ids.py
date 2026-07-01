#!/usr/bin/env python3
"""
Recover FEC candidate ids for our captured candidates and build the id crosswalk.

`build_candidate_roster.py` computes cand_id / fec_raw_name / cand_pcc while
building rosters, then drops them (`build_roster` helper-col drop). Here we
reproduce the FEC->candidate mapping (same `build_fec_roster` + `clean_name`)
offline from data/fec_cache/, add incumbency (`cand_ici`), and join to the
candidate-year panel so every panel row gets its FEC id.

Output: data/deliverable/fec_id_crosswalk.csv + a match-rate report.
Run on the droplet (repo root, venv active).
"""
import os
import sys

import pandas as pd
import yaml

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.build_candidate_roster import build_fec_roster  # noqa: E402

HOUSE_YEARS = [2018, 2020, 2022, 2024]
SENATE_YEARS = list(range(2002, 2025, 2))
OUT_DIR = "data/deliverable"


def norm_district(v):
    """Normalize district to a nullable-int string for joining ('01'->1, '19.0'->19)."""
    s = str(v).strip()
    if s in ("", "nan", "None"):
        return ""
    try:
        return str(int(float(s)))
    except ValueError:
        return s


def main():
    os.makedirs(OUT_DIR, exist_ok=True)
    with open("config/config.yaml") as f:
        config = yaml.safe_load(f)

    rows = []
    for office, years in (("house", HOUSE_YEARS), ("senate", SENATE_YEARS)):
        for year in years:
            r = build_fec_roster(year, office, config)  # offline: reads fec_cache
            if r.empty:
                print(f"  WARN empty FEC roster {office} {year}")
                continue
            # incumbency: map cand_id -> cand_ici from that year's cn file
            cn = pd.read_csv(f"data/fec_cache/cn{year}.csv", dtype=str)
            ici = cn.drop_duplicates("cand_id").set_index("cand_id")["cand_ici"]
            r = r.copy()
            r["cand_ici"] = r["cand_id"].map(ici)
            rows.append(r[["candidate", "state", "district", "office", "year",
                           "party", "cand_id", "fec_raw_name", "cand_pcc", "cand_ici"]])
    xwalk = pd.concat(rows, ignore_index=True)
    xwalk["district_n"] = xwalk["district"].map(norm_district)
    xwalk.to_csv(f"{OUT_DIR}/fec_id_crosswalk.csv", index=False)
    print(f"FEC crosswalk: {len(xwalk)} rows -> {OUT_DIR}/fec_id_crosswalk.csv")

    # ---- join to the panel, measure match ----
    panel = pd.read_csv("data/panel/panel_candidate_year_meta.csv")
    panel["district_n"] = panel["district"].map(norm_district)

    # primary key (candidate, state, office, year); disambiguate with district_n
    key = ["candidate", "state", "office", "year"]
    xw_key = xwalk.drop_duplicates(key, keep=False)  # unambiguous on the 4-key
    ambig = xwalk[xwalk.duplicated(key, keep=False)]
    m = panel.merge(xw_key[key + ["cand_id"]], on=key, how="left")
    matched = m["cand_id"].notna().sum()
    print(f"\nPanel rows: {len(panel)}")
    print(f"Matched on (candidate,state,office,year): {matched} ({100*matched/len(panel):.1f}%)")
    print(f"Ambiguous xwalk keys (same name/state/office/year, >1 cand_id): "
          f"{ambig[key].drop_duplicates().shape[0]} keys / {len(ambig)} rows")
    miss = m[m["cand_id"].isna()]
    print(f"Unmatched panel rows: {len(miss)}")
    if len(miss):
        print(miss.groupby(["office", "year"]).size().to_string())
        print("\nsample unmatched:")
        print(miss[["candidate", "state", "district", "office", "year"]].head(10).to_string(index=False))


if __name__ == "__main__":
    main()
