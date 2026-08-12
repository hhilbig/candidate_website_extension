#!/usr/bin/env python3
"""Reproduce ICPSR 226001's candidate-level coding, and apply it to our corpus.

ICPSR 226001 (Di Tella, Kotti, Le Pennec, Pons) ships *coded variables*, not
text. To let our extension stack onto theirs with comparable columns we have to
compute the same variables the same way. This script does two things:

  --validate  reproduce ICPSR's own published values from their own text, so the
              coding is verified before we apply it to anything
  --apply     compute the verified columns for our corpus

Design rule: this script NEVER modifies or replaces our existing columns. It
writes a separate, join-able table of ICPSR-compatible columns, all prefixed
`icpsr_`. Where our own measure differs from theirs (e.g. our candidate-year
panel collapses to the longest-text snapshot, theirs averages over snapshots),
both are kept side by side and the reader picks.

Verified findings (see quality_reports/icpsr_variable_extension_2026-08-12.md):

  * Aggregation rule (EXACT, 13,020/13,020 rows, every year x stage):
        drop pages with n_char == 0
        -> mean over pages within a snapshot `date`
        -> mean over snapshot dates
    The grain is candidate x state x district x year x stage x data_source.
    A candidate-year-stage can appear twice (primary and general_wayback),
    distinguished in the raw data only by name casing.

  * n_char  == len(text_snap_content)                    (100.00% exact)
  * n_words == len(re.findall(r'\\w+', text.lower()))     (100.00% exact)
  * TTR   = types/tokens on the same tokenizer. NOT bit-exact (12.8%), but
    corr 0.9990 and median ratio 1.0015 against their published values.
  * MATTR = moving-average TTR, window 200 (searched 10..500; 200 is the clear
    optimum). NOT bit-exact (12.8%), corr 0.9982, median ratio 1.0000.
    TTR/MATTR evidently come from a different preprocessing stage of their
    pipeline than n_words, which is why the same tokenizer reproduces n_words
    exactly but these only near-exactly. They are shipped as `_approx`.
  * n_tags / n_clean_tags are NOT recoverable from stored text: they are
    HTML-structure counts fixed at scrape time, and the cleaned text has the
    `#+#` separators stripped.
  * `entropy` and `subordinates` are NOT reproducible. Four natural entropy
    definitions all fail (best |corr| ~0.32, and word-frequency entropy
    correlates *negatively* with theirs); `subordinates` needs their exact
    dependency-parsing pipeline. Both omitted rather than approximated.
  * The topic family is NOT reproducible from the shipped artifacts.

Usage:
    python scripts/icpsr_replicate_coding.py --validate
    python scripts/icpsr_replicate_coding.py --apply --corpus PATH --out PATH
"""
from __future__ import annotations

import argparse
import math
import re
import sys
from collections import Counter
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

ICPSR_DIR = Path.home() / (
    "Library/CloudStorage/Dropbox/Research/19_Great_Recession"
    "/data/candidate_websites/226001-V1")

# ICPSR's word tokenizer, verified at 100% against websites_clean.n_words
TOKEN_RE = re.compile(r"\w+")

# Variables carried directly on ICPSR's per-page rows
PAGE_VARS = ["n_char", "n_words", "n_tags", "n_clean_tags"]


# --------------------------------------------------------------------------
# core coding
# --------------------------------------------------------------------------
def tokenize(text: str) -> list[str]:
    """ICPSR's tokenizer. Verified exactly against their n_words."""
    return TOKEN_RE.findall(text.lower())


def _mattr(toks: list[str], win: int) -> float:
    """Moving-average type-token ratio, O(n) via a rolling counter."""
    n = len(toks)
    if n == 0:
        return float("nan")
    if n < win:
        return len(set(toks)) / n
    cnt = Counter(toks[:win])
    types = len(cnt)
    acc = types
    for i in range(win, n):
        out, inc = toks[i - win], toks[i]
        cnt[out] -= 1
        if cnt[out] == 0:
            del cnt[out]
            types -= 1
        if inc not in cnt:
            types += 1
        cnt[inc] += 1
        acc += types
    return acc / (n - win + 1) / win


def page_measures(text: str, mattr_window: int) -> dict:
    """Text-derived measures for a single page."""
    n_char = len(text)
    toks = tokenize(text)
    n = len(toks)
    if n == 0:
        return {"n_char": n_char, "n_words": 0, "ttr": np.nan, "mattr": np.nan}
    return {"n_char": n_char,
            "n_words": n,
            "ttr": len(set(toks)) / n,
            "mattr": _mattr(toks, mattr_window)}


def icpsr_aggregate(pages: pd.DataFrame, value_cols: list[str],
                    group_col: str = "ck",
                    date_col: str = "date") -> pd.DataFrame:
    """ICPSR's snapshot -> candidate aggregation.

    Drop empty pages, mean over pages within a snapshot date, mean over dates.
    Verified exact on all 13,020 published rows.
    """
    valid = pages[pages["n_char"] > 0]
    per_snap = valid.groupby([group_col, date_col])[value_cols].mean()
    return per_snap.groupby(group_col).mean()


def icpsr_key(df: pd.DataFrame) -> pd.Series:
    """ICPSR's candidate grain, including data_source.

    The same candidate-year-stage can appear once as `primary` and once as
    `general_wayback`; in the raw files they are distinguished only by the
    casing of the name, so data_source must be part of the key.
    """
    return (df["candidate"].str.upper().str.strip() + "|"
            + df["state"].str.lower().str.strip() + "|"
            + df["district"].astype(str) + "|"
            + df["year"].astype(str) + "|"
            + df["stage"].astype(str) + "|"
            + df["data_source"].str.strip())


# --------------------------------------------------------------------------
# validate
# --------------------------------------------------------------------------
def run_validate(mattr_window: int, sample: int) -> int:
    truth = pd.read_csv(ICPSR_DIR / "candidates_complexity.csv")
    w = pq.read_table(ICPSR_DIR / "websites_clean.parquet").to_pandas()
    w["ck"] = icpsr_key(w)
    truth["ck"] = icpsr_key(truth)

    print(f"websites_clean pages : {len(w):,}")
    print(f"complexity rows      : {len(truth):,}")
    print(f"key overlap          : {truth.ck.isin(set(w.ck)).sum():,}"
          f" / {len(truth):,}\n")

    print("=== aggregation rule, on ICPSR's own per-page values ===")
    rep = icpsr_aggregate(w, PAGE_VARS)
    m = truth.set_index("ck")[PAGE_VARS].join(rep, rsuffix="_rep")
    ok = True
    for v in PAGE_VARS:
        rate = float(((m[v] - m[v + "_rep"]).abs() < 1e-6).mean())
        ok &= rate >= 0.99
        print(f"  {v:<14} {100*rate:6.2f}% exact")

    print("\n=== per-page measures recomputed from text (exact reproduction) ===")
    s = w[w.n_char > 0]
    if sample and sample < len(s):
        s = s.sample(sample, random_state=20260812)
    meas = pd.DataFrame([page_measures(t, mattr_window)
                         for t in s.text_snap_content], index=s.index)
    for v in ("n_char", "n_words"):
        rate = float((meas[v] == s[v]).mean())
        ok &= rate >= 0.99
        print(f"  {v:<14} {100*rate:6.2f}% exact  (n={len(s):,})")

    # TTR/MATTR are candidate-level aggregates, so they must be validated on
    # COMPLETE candidate-years. Sampling pages would give each candidate-year a
    # random partial page set and understate agreement.
    print("\n=== TTR / MATTR (approximate; agreement with published values) ===")
    rng = np.random.default_rng(20260812)
    n_cand = max(1, min(250, truth.ck.nunique()))
    cks = rng.choice(truth.ck.unique(), size=n_cand, replace=False)
    full = w[w.ck.isin(set(cks)) & (w.n_char > 0)].copy()
    fm = pd.DataFrame([page_measures(t, mattr_window)
                       for t in full.text_snap_content], index=full.index)
    full["ttr"], full["mattr"] = fm["ttr"], fm["mattr"]
    agg = icpsr_aggregate(full, ["ttr", "mattr"])
    j = truth.set_index("ck").loc[cks][["TTR", "MATTR"]].join(agg).dropna()
    for tcol, rcol in (("TTR", "ttr"), ("MATTR", "mattr")):
        corr = float(np.corrcoef(j[tcol], j[rcol])[0, 1])
        ratio = float((j[tcol] / j[rcol]).median())
        good = corr >= 0.99 and abs(ratio - 1) <= 0.01
        ok &= good
        print(f"  {tcol:<14} corr {corr:6.4f}  median ratio {ratio:6.4f}  "
              f"({len(j):,} complete candidate-years, {len(full):,} pages)  "
              f"{'ok' if good else 'FAIL'}")

    print(f"\nVALIDATE {'PASS' if ok else 'FAIL'}")
    return 0 if ok else 1


# --------------------------------------------------------------------------
# apply
# --------------------------------------------------------------------------
def run_apply(corpus: Path, out: Path, mattr_window: int) -> int:
    pf = pq.ParquetFile(corpus)
    print(f"corpus: {corpus}  ({pf.metadata.num_rows:,} rows)")

    cols = ["candidate_icpsr", "name_key", "cand_id", "state", "district_id",
            "office", "year", "stage", "party", "data_source", "date",
            "n_char", "text_snap_content"]
    have = set(pf.schema_arrow.names)
    use = [c for c in cols if c in have]
    missing = [c for c in cols if c not in have]
    if missing:
        print(f"  note: absent from corpus, skipped: {missing}")

    frames = []
    for i in range(pf.num_row_groups):
        g = pf.read_row_group(i, columns=use).to_pandas()
        g = g[g.n_char > 0]
        if g.empty:
            continue
        meas = pd.DataFrame([page_measures(t, mattr_window)
                             for t in g.text_snap_content], index=g.index)
        # The corpus already carries n_char/n_words computed by our own
        # pipeline. Drop those copies here and use the recomputed ones, so the
        # aggregate is built from ICPSR's definitions throughout. (Our columns
        # are untouched in the corpus itself; this only affects this output.)
        g = g.drop(columns=[c for c in ("text_snap_content", "n_char", "n_words",
                                        "n_tags", "n_clean_tags")
                            if c in g.columns])
        # Our `date` is a Wayback YYYYMMDDHHMMSS timestamp; ICPSR's snapshot
        # grain is the DAY. Truncate so "mean over snapshot dates" means the
        # same thing on both sides.
        g["snap_day"] = g["date"].astype(str).str.slice(0, 8)
        g = pd.concat([g, meas], axis=1)
        frames.append(g)
        if (i + 1) % 1000 == 0 or i + 1 == pf.num_row_groups:
            print(f"  row group {i+1}/{pf.num_row_groups}", flush=True)

    pages = pd.concat(frames, ignore_index=True)
    del frames
    print(f"\ntotal non-empty pages: {len(pages):,}")

    keycols = [c for c in ("candidate_icpsr", "state", "office", "year")
               if c in pages.columns]
    pages["ck"] = pages[keycols].astype(str).agg("|".join, axis=1)

    agg = icpsr_aggregate(pages, ["n_char", "n_words", "ttr", "mattr"],
                          date_col="snap_day")
    agg = agg.rename(columns={"n_char": "icpsr_n_char",
                              "n_words": "icpsr_n_words",
                              "ttr": "icpsr_ttr_approx",
                              "mattr": "icpsr_mattr_approx"})
    carry = [c for c in ("cand_id", "name_key", "district_id", "party", "stage",
                         "data_source") if c in pages.columns]
    meta = pages.drop_duplicates("ck").set_index("ck")[keycols + carry]
    n_snap = (pages.groupby("ck").snap_day.nunique()
                   .rename("icpsr_n_valid_snap"))
    n_page = pages.groupby("ck").size().rename("icpsr_n_valid_pages")
    res = meta.join(agg).join(n_snap).join(n_page).reset_index(drop=True)

    dup = res.duplicated(subset=keycols).sum()
    print(f"key {keycols}: {len(res):,} rows, {dup} duplicates")
    for c in ("icpsr_n_char", "icpsr_n_words", "icpsr_ttr_approx",
              "icpsr_mattr_approx"):
        print(f"  {c:<22} missing {res[c].isna().sum():>5}  "
              f"median {res[c].median():.4f}")

    out.parent.mkdir(parents=True, exist_ok=True)
    res.to_csv(out, index=False)
    print(f"\nwrote {out}  ({len(res):,} candidate-years, {len(res.columns)} cols)")
    print("columns:", ", ".join(res.columns))
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--corpus", type=Path,
                    default=Path("data/deliverable/raw_corpus_icpsr.parquet"))
    ap.add_argument("--out", type=Path,
                    default=Path("data/deliverable/panel_icpsr_compat.csv"))
    ap.add_argument("--mattr-window", type=int, default=200,
                    help="verified optimum against ICPSR (searched 10..500)")
    ap.add_argument("--sample", type=int, default=200000,
                    help="pages sampled for the per-page validation (0 = all)")
    a = ap.parse_args()
    if not (a.validate or a.apply):
        ap.error("choose --validate and/or --apply")
    rc = 0
    if a.validate:
        rc |= run_validate(a.mattr_window, a.sample)
    if a.apply:
        rc |= run_apply(a.corpus, a.out, a.mattr_window)
    return rc


if __name__ == "__main__":
    sys.exit(main())
