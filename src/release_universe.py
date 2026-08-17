"""Candidate-universe and manual-adjudication rules for the public release."""
from __future__ import annotations

import pandas as pd

ID_KEY = ["year", "office", "state", "cand_id"]


def _normalized(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    out["year"] = pd.to_numeric(out["year"], errors="raise").astype(int)
    for col in ("office", "state", "cand_id"):
        if col in out:
            out[col] = out[col].astype("string").fillna("").str.strip()
    if "state" in out:
        out["state"] = out["state"].str.upper()
    if "office" in out:
        out["office"] = out["office"].str.lower()
    return out


def select_captured_universe(
    captures: pd.DataFrame,
    fec_candidates: pd.DataFrame,
    overrides: pd.DataFrame,
) -> pd.DataFrame:
    """Keep same-year FEC captures plus explicitly reviewed ballot exceptions."""
    c = _normalized(captures)
    f = _normalized(fec_candidates)
    f["cand_election_yr"] = f["cand_election_yr"].astype("string")
    f = f[f["cand_election_yr"] == f["year"].astype(str)].copy()
    meta = ["year", "cand_id", "cand_election_yr", "cand_status"]
    same = c.merge(f[meta].drop_duplicates(["year", "cand_id"]),
                   on=["year", "cand_id"], how="inner")
    same["source_cand_id"] = same["cand_id"]
    same["universe_source"] = "same_year_fec"
    if "on_ballot" not in same:
        same["on_ballot"] = False
    same["on_ballot"] = same["on_ballot"].fillna(False).astype(bool)

    if overrides.empty:
        out = same
    else:
        o = _normalized(overrides.rename(columns={"source_cand_id": "cand_id"}))
        o = o[o["action"] == "retain_ballot_override"].copy()
        extra = c.merge(o[["year", "office", "state", "cand_id", "canonical_cand_id"]],
                        on=ID_KEY, how="inner")
        extra["source_cand_id"] = extra["cand_id"]
        extra["cand_id"] = extra["canonical_cand_id"]
        extra["cand_election_yr"] = pd.NA
        extra["cand_status"] = pd.NA
        extra["universe_source"] = "ballot_override"
        extra["on_ballot"] = True
        extra = extra.drop(columns=["canonical_cand_id"])
        frames = [frame for frame in (same, extra) if not frame.empty]
        out = pd.concat(frames, ignore_index=True) if frames else same

    out["candidate_cycle_id"] = out["cand_id"] + "-" + out["year"].astype(str)
    return out.drop_duplicates("candidate_cycle_id", keep="first")


def apply_capture_adjudication(rows: pd.DataFrame, decisions: pd.DataFrame) -> pd.DataFrame:
    """Remove only candidate-years explicitly adjudicated as invalid captures."""
    r = _normalized(rows)
    if decisions.empty:
        return r
    d = _normalized(decisions)
    bad = d[d["action"] == "exclude"][ID_KEY].drop_duplicates()
    marked = r.merge(bad.assign(_exclude=True), on=ID_KEY, how="left")
    return marked[marked["_exclude"].isna()].drop(columns="_exclude")
