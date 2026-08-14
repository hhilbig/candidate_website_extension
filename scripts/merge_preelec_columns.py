#!/usr/bin/env python3
"""Fold the pre-election and snapshot-timing columns into the released panel.

`icpsr_replicate_coding.py --apply` was re-run without `--topics`, because the
topic columns do not depend on the pre-election split and recomputing them needs
an nltk install the machine does not have. This merges the new columns onto the
existing panel and leaves the 31 topic columns, on_ballot and general_votes in
place.

It also regression-checks the columns that should NOT have moved. The edit only
added a filtered aggregate, so every all-year coded value must come back
identical. If it does not, the edit changed something it should not have and the
merge stops.

Usage: python scripts/merge_preelec_columns.py [--dry-run]
"""
from __future__ import annotations

import argparse
import shutil
import sys
from pathlib import Path

import numpy as np
import pandas as pd

REPO = Path(__file__).resolve().parent.parent
OLD = REPO / "data/deliverable/panel_icpsr_compat.csv"
NEW = REPO / "data/deliverable/panel_coded_preelec.csv"
KEY = ["candidate_icpsr", "state", "office", "year"]


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true")
    a = ap.parse_args()

    if not NEW.exists():
        sys.exit(f"{NEW} not written yet")
    old = pd.read_csv(OLD, low_memory=False)
    new = pd.read_csv(NEW, low_memory=False)
    print(f"existing panel: {len(old):,} rows, {len(old.columns)} cols")
    print(f"recoded       : {len(new):,} rows, {len(new.columns)} cols")

    for d in (old, new):
        d["_k"] = d[KEY].astype(str).agg("|".join, axis=1)
    if set(old._k) != set(new._k):
        sys.exit(f"key mismatch: {len(set(old._k) - set(new._k))} only in old, "
                 f"{len(set(new._k) - set(old._k))} only in new")

    add = [c for c in new.columns
           if c.endswith("_preelec") or c in ("icpsr_first_snap_day",
                                              "icpsr_last_snap_day",
                                              "icpsr_snap_span_days")]
    print(f"\nnew columns to add ({len(add)}): {', '.join(add)}")

    # ---- regression check on everything that should be unchanged -----------
    shared = [c for c in new.columns
              if c in old.columns and c not in add and c != "_k"
              and c.startswith("icpsr_")]
    o = old.set_index("_k"); n = new.set_index("_k").reindex(o.index)
    bad = []
    for c in shared:
        a_, b_ = o[c], n[c]
        if pd.api.types.is_numeric_dtype(a_) and pd.api.types.is_numeric_dtype(b_):
            same = np.isclose(a_.astype(float), b_.astype(float),
                              rtol=1e-9, atol=1e-9, equal_nan=True)
        else:
            same = (a_.astype(str) == b_.astype(str)) | (a_.isna() & b_.isna())
        n_diff = int((~same).sum())
        if n_diff:
            bad.append((c, n_diff))
    print(f"\nregression check on {len(shared)} unchanged coded columns: "
          f"{len(bad)} differ")
    for c, k in bad[:10]:
        print(f"  {c}: {k:,} rows differ")
    if bad:
        sys.exit("unchanged columns moved; not merging")

    out = old.join(n[add], on="_k").drop(columns="_k")
    cov = out[add[0]].notna().mean() if add else float("nan")
    print(f"\npre-election variant present for {100*cov:.1f}% of candidate-years")
    if "icpsr_n_char" in out and "icpsr_n_char_preelec" in out:
        both = out[["icpsr_n_char", "icpsr_n_char_preelec"]].dropna()
        print(f"median n_char  all-year {both.icpsr_n_char.median():,.0f}  "
              f"pre-election {both.icpsr_n_char_preelec.median():,.0f}  "
              f"(n={len(both):,})")
    if "icpsr_snap_span_days" in out:
        print(f"median snapshot span: {out.icpsr_snap_span_days.median():.0f} days")

    if a.dry_run:
        print("\ndry run, nothing written")
        return 0
    shutil.copy2(OLD, OLD.with_suffix(".csv.pre_preelec"))
    out.to_csv(OLD, index=False)
    print(f"\nwrote {OLD}  ({len(out):,} rows, {len(out.columns)} cols)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
