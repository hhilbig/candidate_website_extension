"""Reviewed universe and date-level adjudications for release construction."""
from __future__ import annotations

import pandas as pd

KEY = ["year", "office", "state", "cand_id"]


def _normalize_keys(rows: pd.DataFrame) -> pd.DataFrame:
    out = rows.copy()
    out["year"] = pd.to_numeric(out["year"], errors="raise").astype(int)
    out["office"] = out["office"].astype("string").str.strip().str.lower()
    out["state"] = out["state"].astype("string").str.strip().str.upper()
    out["cand_id"] = out["cand_id"].astype("string").str.strip()
    return out


def apply_universe_adjudication(
    rows: pd.DataFrame,
    decisions: pd.DataFrame,
) -> pd.DataFrame:
    """Remove candidate-years explicitly reviewed as outside the universe."""
    out = _normalize_keys(rows)
    if decisions.empty:
        return out
    dec = _normalize_keys(decisions)
    unknown = sorted(set(dec["action"].dropna()) - {"exclude", "retain"})
    if unknown:
        raise ValueError(f"unknown universe adjudication actions: {unknown}")
    if dec.duplicated(KEY).any():
        raise ValueError("duplicate candidate-universe adjudications")
    excluded = dec.loc[dec.action.eq("exclude"), KEY]
    marked = out.merge(excluded.assign(_universe_excluded=True), on=KEY, how="left")
    return marked.loc[marked._universe_excluded.isna()].drop(
        columns="_universe_excluded"
    )


def apply_capture_date_adjudication(
    rows: pd.DataFrame,
    decisions: pd.DataFrame,
    date_col: str = "date",
) -> pd.DataFrame:
    """Apply approved inclusive capture-day ranges without changing other rows."""
    out = _normalize_keys(rows)
    if decisions.empty:
        return out
    dec = _normalize_keys(decisions)
    unknown = sorted(set(dec["action"].dropna()) - {"retain_date_range"})
    if unknown:
        raise ValueError(f"unknown date adjudication actions: {unknown}")
    if dec.duplicated(KEY).any():
        raise ValueError("duplicate capture-date adjudications")

    dec = dec.set_index(KEY)
    key = tuple(out[c].iloc[0] for c in KEY) if len(out) else None
    if key not in dec.index:
        return out
    rule = dec.loc[key]
    day = out[date_col].astype("string").str.slice(0, 8)
    keep = pd.Series(True, index=out.index)
    if pd.notna(rule.get("min_snap_day")):
        keep &= day.ge(str(rule.min_snap_day).split(".")[0].zfill(8))
    if pd.notna(rule.get("max_snap_day")):
        keep &= day.le(str(rule.max_snap_day).split(".")[0].zfill(8))
    kept = out.loc[keep].copy()
    if kept.empty:
        raise ValueError(f"date adjudication removed every capture for {key}")
    return kept
