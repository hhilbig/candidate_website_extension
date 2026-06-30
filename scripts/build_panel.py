#!/usr/bin/env python3
"""
Build the candidate-year panel from the scraped snapshot corpus.

Collapses each captured candidate-year (one CSV per candidate-year in the
tarballs) to a single representative observation: the LONGEST-TEXT snapshot
(the snapshot whose pages sum to the most characters), per KNOWLEDGE_BASE §2.
This normalizes away the cross-year snapshot-density inconsistency: regardless
of whether a candidate has 1 or 9,500 raw snapshots, the panel keeps one.

Read-only over the raw tarballs; writes a new derived dataset only.

Outputs (to --out-dir):
  panel_candidate_year.csv       one row/candidate-year, WITH concatenated text
  panel_candidate_year_meta.csv  same rows WITHOUT the text column (small)

Usage (repo root, venv active, on the droplet):
    python scripts/build_panel.py --out-dir data/panel
"""
import argparse
import glob
import io
import os
import re
import tarfile

import pandas as pd

# Page-type reading order for text assembly (homepage leads).
PT_PRIORITY = {"homepage": 0, "issues": 1, "biography": 2, "news": 3,
               "endorsements": 4, "constituent_services": 5, "action": 6, "other": 7}
META_COLS = ["candidate", "state", "district", "office", "year", "party", "stage",
             "sel_date", "n_snapshots_available", "n_pages", "page_types", "urlkey",
             "n_char", "n_words", "text_quality"]

# Placeholder / JS-shell / error markers — content that isn't real site text.
# Applied only to short texts so a long real page mentioning "loading" is safe.
PLACEHOLDER_RE = re.compile(
    r"\b(loading|coming soon|under construction|site is being (built|updated)|"
    r"check back|domain (is )?for sale|buy this domain|godaddy|namecheap|parked|"
    r"enable javascript|requires javascript|page not found|404|403 forbidden|"
    r"account suspended|website coming)\b", re.I)


def text_quality(text, n_char):
    """Tier a candidate-year's collapsed text. Thresholds documented in
    quality_reports/panel_build_2026-06-29.md; raw n_char is kept so downstream
    can re-threshold."""
    if n_char < 100:
        return "empty"
    if n_char < 1500 and PLACEHOLDER_RE.search(str(text)[:2000]):
        return "empty"
    if n_char < 1500:
        return "thin"
    return "usable"


def collapse_candidate(df, sep):
    """Return one panel-row dict for a single candidate-year CSV, or None."""
    df = df.copy()
    df["n_char"] = pd.to_numeric(df["n_char"], errors="coerce").fillna(0).astype(int)
    df["n_words"] = pd.to_numeric(df["n_words"], errors="coerce").fillna(0).astype(int)
    df["date"] = df["date"].astype(str)
    df = df[df["text_snap_content"].notna() & (df["n_char"] > 0)]
    if df.empty:
        return None

    n_snaps = df["date"].nunique()
    # longest-text snapshot: max sum(n_char) per date; tie-break latest date
    by_date = df.groupby("date")["n_char"].sum()
    best_total = by_date.max()
    sel_date = max(d for d, t in by_date.items() if t == best_total)  # latest on tie
    snap = df[df["date"] == sel_date].copy()

    # order pages: homepage-first by page-type priority, then descending n_char
    snap["_pt"] = snap["page_type"].astype(str).map(lambda p: PT_PRIORITY.get(p, 99))
    snap = snap.sort_values(["_pt", "n_char"], ascending=[True, False])
    text = sep.join(snap["text_snap_content"].astype(str).tolist())

    first = snap.iloc[0]
    total_nchar = int(snap["n_char"].sum())
    return {
        "candidate": first["candidate"], "state": first["state"],
        "district": first.get("district", ""), "office": first["office"],
        "year": int(first["year"]), "party": first.get("party", ""),
        "stage": first.get("stage", ""), "sel_date": sel_date,
        "n_snapshots_available": int(n_snaps), "n_pages": int(len(snap)),
        "page_types": ";".join(sorted(snap["page_type"].astype(str).unique())),
        "urlkey": first.get("urlkey", ""),
        "text": text, "n_char": total_nchar, "n_words": int(snap["n_words"].sum()),
        "text_quality": text_quality(text, total_nchar),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshots-dir", default="data/snapshots")
    ap.add_argument("--out-dir", default="data/panel")
    ap.add_argument("--sep", default="#+#")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    tarballs = sorted(glob.glob(f"{args.snapshots_dir}/house/*.tar.gz")) + \
        sorted(glob.glob(f"{args.snapshots_dir}/senate/*.tar.gz"))

    rows = []
    usecols = ["candidate", "state", "district", "office", "year", "party", "stage",
               "date", "urlkey", "page_type", "text_snap_content", "n_char", "n_words"]
    for path in tarballs:
        yr = os.path.basename(path).replace(".tar.gz", "")
        office = "house" if "/house/" in path else "senate"
        n_before = len(rows)
        with tarfile.open(path, "r:gz") as tf:
            for m in tf.getmembers():
                if not m.name.endswith(".csv"):
                    continue
                try:
                    df = pd.read_csv(io.BytesIO(tf.extractfile(m).read()),
                                     usecols=lambda c: c in usecols)
                except Exception as e:  # noqa: BLE001
                    print(f"  WARN {office} {yr} {os.path.basename(m.name)}: {e}")
                    continue
                row = collapse_candidate(df, args.sep)
                if row:
                    rows.append(row)
        print(f"[{office} {yr}] {len(rows) - n_before} candidate-years")

    panel = pd.DataFrame(rows)
    panel.to_csv(f"{args.out_dir}/panel_candidate_year.csv", index=False)
    panel[META_COLS].to_csv(f"{args.out_dir}/panel_candidate_year_meta.csv", index=False)

    print("\n==== PANEL ====")
    print(f"candidate-years: {len(panel)}")
    print(f"text n_char: median {int(panel['n_char'].median())} "
          f"mean {int(panel['n_char'].mean())} max {int(panel['n_char'].max())}")
    print(f"snapshots collapsed: median {int(panel['n_snapshots_available'].median())} "
          f"mean {panel['n_snapshots_available'].mean():.1f} max {int(panel['n_snapshots_available'].max())}")
    print(f"single-snapshot candidate-years: {(panel['n_snapshots_available'] == 1).sum()}")
    tq = panel["text_quality"].value_counts()
    print("text_quality: " + "  ".join(
        f"{k}={tq.get(k, 0)} ({100*tq.get(k, 0)/len(panel):.1f}%)"
        for k in ("usable", "thin", "empty")))
    print(f"empty-text rows: {(panel['n_char'] == 0).sum()}  -> {args.out_dir}/")


if __name__ == "__main__":
    main()
