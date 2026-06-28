#!/usr/bin/env python3
"""
Corpus-wide coverage + quality audit for all collected candidate-website data.

Streams every office-year tarball under data/snapshots/{house,senate}/*.tar.gz
(no extraction), aggregates per office-year, and emits tidy long-format CSVs for
downstream R/ggplot figures and a written report. Read-only over the data.

Reuses the project's own definitions (no reimplementation):
  - `_clean_campaign_url` (src/scrape_wayback.py) defines a "valid URL".
  - `is_cjk_spam` (src/extract_text.py) defines CJK spam (>30% CJK chars).

Only `text_snap_content` is heavy; it is read solely in the CJK pass, which by
default samples every Nth candidate file (--cjk sample). All other metrics read
the light columns only.

Usage (repo root, venv active, on the droplet):
    python scripts/coverage_audit.py --out-dir /tmp/coverage_audit
"""
import argparse
import glob
import io
import os
import re
import sys
import tarfile

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from src.scrape_wayback import _clean_campaign_url  # noqa: E402

EXPECTED_COLS = [
    "candidate", "state", "district", "office", "year", "party", "stage",
    "date", "urlkey", "snap_url", "page_type", "data_source", "n_tags",
    "n_clean_tags", "text_snap_content", "n_char", "n_words",
]
LIGHT_COLS = ["candidate", "state", "party", "date", "snap_url", "page_type", "n_char"]
CJK_RE = r"[　-鿿]"  # matches is_cjk_spam's char range
NCHAR_SAMPLE_PER_YEAR = 5000

VALID_STATES = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR", "GU", "VI", "AS", "MP",
}


def _med(s):
    return float(s.median()) if len(s) else 0.0


def _mean(s):
    return float(s.mean()) if len(s) else 0.0


def _max(s):
    return float(s.max()) if len(s) else 0.0


def audit_year(office, year, tarball, roster_path, progress_path, cjk_mode, cjk_n, anomalies):
    """Audit one office-year. Returns dict-of-rows for each output table."""
    yr = int(year)
    tag = f"{office} {year}"

    # ---- roster denominators ----
    roster = pd.read_csv(roster_path)
    roster["state"] = roster["state"].astype(str)
    roster["party"] = roster.get("party", pd.Series(["other"] * len(roster))).fillna("other").astype(str)
    roster["valid"] = roster["website_url"].map(_clean_campaign_url).notna()
    roster["key"] = roster["candidate"].astype(str) + " (" + roster["state"] + ").csv"
    n_roster = len(roster)
    n_valid = int(roster["valid"].sum())

    # ---- stream candidate CSVs from the tarball ----
    captured_keys = set()
    page_rows = 0
    schema_ok = True
    dup_rows = 0
    dates_out = 0
    bad_state_rows = 0
    bad_party_rows = 0
    bad_source_rows = 0
    date_min, date_max = None, None
    per_cand = []          # candidate-level summary rows
    nchar_all = []         # for richness summary (one year fits in memory)
    nchar_sample = []      # capped sample for the density figure
    pt_counts = {}         # page_type -> count
    month_snaps = {}       # month -> set of (candidate,date)
    cjk_rows_seen = 0
    cjk_rows_spam = 0

    lo = yr * 10 ** 10      # YYYY0000000000 lower bound (YYYYMMDDHHMMSS)
    hi = yr * 10 ** 10 + 1231_235959

    with tarfile.open(tarball, "r:gz") as tf:
        members = [m for m in tf.getmembers() if m.name.endswith(".csv")]
        for i, m in enumerate(members):
            base = os.path.basename(m.name)
            captured_keys.add(base)
            raw = tf.extractfile(m).read()
            want_text = (cjk_mode == "full") or (cjk_mode == "sample" and i % cjk_n == 0)
            usecols = LIGHT_COLS + (["text_snap_content"] if want_text else [])
            try:
                df = pd.read_csv(io.BytesIO(raw), usecols=lambda c: c in usecols)
            except Exception as e:  # noqa: BLE001
                anomalies.append((office, year, base, "unreadable_file", str(e)[:80]))
                continue
            # schema header check (read header of full file once, cheap)
            if i == 0:
                hdr = pd.read_csv(io.BytesIO(raw), nrows=0).columns.tolist()
                if hdr != EXPECTED_COLS:
                    schema_ok = False
                    anomalies.append((office, year, base, "schema_mismatch", ",".join(hdr)[:120]))
            n = len(df)
            if n == 0:
                anomalies.append((office, year, base, "empty_file", "0 rows"))
                continue
            page_rows += n

            # duplicate page rows within candidate
            d = df.drop_duplicates(subset=[c for c in ("candidate", "date", "snap_url") if c in df.columns])
            dup_rows += n - len(d)

            # dates
            dates = pd.to_numeric(df["date"], errors="coerce")
            valid_dates = dates.dropna()
            oo = int(((valid_dates < lo) | (valid_dates > hi)).sum()) + int(dates.isna().sum())
            if oo:
                dates_out += oo
            if len(valid_dates):
                dmin, dmax = int(valid_dates.min()), int(valid_dates.max())
                date_min = dmin if date_min is None else min(date_min, dmin)
                date_max = dmax if date_max is None else max(date_max, dmax)

            # value validity
            st = df["state"].astype(str)
            bad_state_rows += int((~st.isin(VALID_STATES)).sum())
            pty = df["party"].astype(str)
            bad_party_rows += int((~pty.isin({"R", "D", "other"})).sum())

            # richness
            nchar = pd.to_numeric(df["n_char"], errors="coerce").fillna(0)
            nchar_all.append(nchar)
            if len(nchar_sample) < NCHAR_SAMPLE_PER_YEAR:
                nchar_sample.extend(nchar.head(50).tolist())

            # page types
            for k, v in df["page_type"].astype(str).value_counts().items():
                pt_counts[k] = pt_counts.get(k, 0) + int(v)

            # months + snapshots (unique candidate,date)
            cand = df["candidate"].astype(str)
            snaps = set(zip(cand, df["date"].astype(str)))
            for (cc, dd) in snaps:
                mo = dd[4:6] if len(dd) >= 6 else "??"
                month_snaps.setdefault(mo, set()).add((cc, dd))

            # candidate-level
            name = cand.iloc[0] if n else base[:-4]
            per_cand.append({
                "candidate": name, "office": office, "year": yr,
                "state": st.iloc[0], "party": pty.iloc[0],
                "n_snaps": len(snaps), "n_pages": n,
                "max_nchar": int(nchar.max()), "total_nchar": int(nchar.sum()),
                "min_date": int(valid_dates.min()) if len(valid_dates) else 0,
                "max_date": int(valid_dates.max()) if len(valid_dates) else 0,
            })

            # CJK
            if want_text and "text_snap_content" in df.columns:
                t = df["text_snap_content"].fillna("").astype(str)
                tl = t.str.len().replace(0, 1)
                frac = t.str.count(CJK_RE) / tl
                cjk_rows_seen += n
                cjk_rows_spam += int((frac > 0.3).sum())

    pc = pd.DataFrame(per_cand)
    nchar_cat = pd.concat(nchar_all, ignore_index=True) if nchar_all else pd.Series([], dtype=float)

    # ---- coverage gap decomposition (roster-matched) ----
    valid_keys = set(roster.loc[roster["valid"], "key"])
    captured_valid = valid_keys & captured_keys
    captured_n = len(captured_keys)
    captured_not_in_roster = len(captured_keys - set(roster["key"]))
    if captured_not_in_roster:
        anomalies.append((office, year, "-", "captured_not_in_roster",
                          f"{captured_not_in_roster} captured files unmatched to roster"))
    no_url_n = n_roster - n_valid
    valid_no_capture_n = n_valid - len(captured_valid)

    # progress file
    prog_present = os.path.exists(progress_path)
    prog_complete = prog_error = -1
    if prog_present:
        pg = pd.read_csv(progress_path)
        prog_complete = int(pd.to_numeric(pg.get("scrape_complete", 0), errors="coerce").fillna(0).sum())
        prog_error = int(pd.to_numeric(pg.get("scrape_error", 0), errors="coerce").fillna(0).sum())

    snaps_per_cand = pc["n_snaps"] if len(pc) else pd.Series([], dtype=float)
    total_snaps = int(snaps_per_cand.sum())
    # pages/snapshot ≈ total pages / total snaps (per-candidate-date)
    pages_per_snap = (pc["n_pages"] / pc["n_snaps"].replace(0, 1)) if len(pc) else pd.Series([], dtype=float)

    if dup_rows:
        anomalies.append((office, year, "-", "duplicate_page_rows", str(dup_rows)))
    if dates_out:
        anomalies.append((office, year, "-", "dates_out_of_window", str(dates_out)))
    if bad_state_rows:
        anomalies.append((office, year, "-", "invalid_state_rows", str(bad_state_rows)))
    if bad_party_rows:
        anomalies.append((office, year, "-", "invalid_party_rows", str(bad_party_rows)))
    if len(pc):
        top = pc.sort_values("n_snaps", ascending=False).head(1).iloc[0]
        if top["n_snaps"] >= 150:
            anomalies.append((office, year, top["candidate"], "extreme_snapshots", str(int(top["n_snaps"]))))

    cjk_pct = round(100 * cjk_rows_spam / cjk_rows_seen, 3) if cjk_rows_seen else 0.0

    per_year = {
        "office": office, "year": yr,
        "roster_n": n_roster, "valid_url_n": n_valid,
        "valid_url_pct": round(100 * n_valid / n_roster, 1) if n_roster else 0,
        "captured_n": captured_n,
        "capture_pct_roster": round(100 * captured_n / n_roster, 1) if n_roster else 0,
        "capture_pct_validurl": round(100 * captured_n / n_valid, 1) if n_valid else 0,
        "no_url_n": no_url_n, "valid_no_capture_n": valid_no_capture_n,
        "page_rows": page_rows, "unique_cands": captured_n, "total_snaps": total_snaps,
        "snaps_per_cand_med": _med(snaps_per_cand), "snaps_per_cand_mean": round(_mean(snaps_per_cand), 2),
        "snaps_per_cand_max": int(_max(snaps_per_cand)),
        "pages_per_snap_med": _med(pages_per_snap), "pages_per_snap_mean": round(_mean(pages_per_snap), 2),
        "nchar_med": _med(nchar_cat), "nchar_mean": round(_mean(nchar_cat)), "nchar_max": int(_max(nchar_cat)),
        "pct_lt50": round(100 * float((nchar_cat < 50).mean()), 2) if len(nchar_cat) else 0,
        "pct_empty": round(100 * float((nchar_cat == 0).mean()), 2) if len(nchar_cat) else 0,
        "cjk_mode": cjk_mode, "cjk_pct": cjk_pct,
        "prog_present": prog_present, "prog_complete": prog_complete, "prog_error": prog_error,
        "dup_rows": dup_rows, "dates_out_of_window": dates_out, "schema_ok": schema_ok,
        "date_min": date_min or 0, "date_max": date_max or 0,
    }

    # by party
    by_party = []
    for pty_val, grp in roster.groupby("party"):
        cap = len(set(grp.loc[grp["valid"], "key"]) & captured_keys)
        vn = int(grp["valid"].sum())
        by_party.append({"office": office, "year": yr, "party": pty_val,
                         "roster_n": len(grp), "valid_url_n": vn, "captured_n": cap,
                         "capture_pct_validurl": round(100 * cap / vn, 1) if vn else 0})
    # by state
    by_state = []
    for st_val, grp in roster.groupby("state"):
        cap = len(set(grp["key"]) & captured_keys)
        by_state.append({"office": office, "year": yr, "state": st_val,
                         "roster_n": len(grp), "captured_n": cap,
                         "capture_pct": round(100 * cap / len(grp), 1) if len(grp) else 0})
        if cap == 0 and len(grp) >= 3:
            anomalies.append((office, year, st_val, "zero_capture_state", f"{len(grp)} roster cands"))
    # page types
    pagetype = [{"office": office, "year": yr, "page_type": k, "n_rows": v,
                 "pct_of_rows": round(100 * v / page_rows, 1) if page_rows else 0}
                for k, v in sorted(pt_counts.items(), key=lambda x: -x[1])]
    # months
    months = [{"office": office, "year": yr, "month": mo,
               "n_snapshots": len(s), "n_candidates": len({c for c, _ in s})}
              for mo, s in sorted(month_snaps.items())]
    # nchar sample
    nchar_s = [{"office": office, "year": yr, "n_char": int(x)} for x in nchar_sample]

    print(f"[{tag}] captured={captured_n}/{n_roster} ({per_year['capture_pct_validurl']}% valid-URL) "
          f"rows={page_rows} cjk={cjk_pct}% err={prog_error} dup={dup_rows} dates_out={dates_out}")
    return {"per_year": per_year, "by_party": by_party, "by_state": by_state,
            "pagetype": pagetype, "months": months, "nchar": nchar_s,
            "candidates": pc.to_dict("records")}


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--snapshots-dir", default="data/snapshots")
    ap.add_argument("--roster-dir", default="data/rosters")
    ap.add_argument("--progress-dir", default="data/progress")
    ap.add_argument("--out-dir", default="/tmp/coverage_audit")
    ap.add_argument("--cjk", choices=["full", "sample", "skip"], default="sample")
    ap.add_argument("--cjk-n", type=int, default=10, help="sample 1 of every N candidate files for CJK")
    args = ap.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)

    tarballs = sorted(glob.glob(f"{args.snapshots_dir}/house/*.tar.gz")) + \
        sorted(glob.glob(f"{args.snapshots_dir}/senate/*.tar.gz"))

    acc = {k: [] for k in ("per_year", "by_party", "by_state", "pagetype", "months", "nchar", "candidates")}
    anomalies = []
    for path in tarballs:
        office = "house" if "/house/" in path else "senate"
        year = os.path.basename(path).replace(".tar.gz", "")
        roster = f"{args.roster_dir}/roster_{office}_{year}.csv"
        progress = f"{args.progress_dir}/progress_roster_{office}_{year}.csv"
        if not os.path.exists(roster):
            anomalies.append((office, year, "-", "missing_roster", roster))
            print(f"[{office} {year}] MISSING ROSTER — skipped")
            continue
        res = audit_year(office, year, path, roster, progress, args.cjk, args.cjk_n, anomalies)
        for k in acc:
            acc[k].extend(res[k] if isinstance(res[k], list) else [res[k]])

    out = args.out_dir
    pd.DataFrame(acc["per_year"]).to_csv(f"{out}/audit_per_year.csv", index=False)
    pd.DataFrame(acc["by_party"]).to_csv(f"{out}/audit_by_party.csv", index=False)
    pd.DataFrame(acc["by_state"]).to_csv(f"{out}/audit_by_state.csv", index=False)
    pd.DataFrame(acc["pagetype"]).to_csv(f"{out}/audit_pagetype.csv", index=False)
    pd.DataFrame(acc["months"]).to_csv(f"{out}/audit_snap_month.csv", index=False)
    pd.DataFrame(acc["nchar"]).to_csv(f"{out}/audit_nchar_sample.csv", index=False)
    pd.DataFrame(acc["candidates"]).to_csv(f"{out}/audit_candidate_level.csv", index=False)
    pd.DataFrame(anomalies, columns=["office", "year", "candidate", "reason_code", "detail"]).to_csv(
        f"{out}/audit_anomalies.csv", index=False)

    py = pd.DataFrame(acc["per_year"])
    print("\n==== CORPUS TOTALS ====")
    print(f"office-years: {len(py)}  candidates(captured): {int(py['captured_n'].sum())}  "
          f"snapshots: {int(py['total_snaps'].sum())}  page_rows: {int(py['page_rows'].sum())}")
    print(f"anomalies flagged: {len(anomalies)}  -> {out}/")


if __name__ == "__main__":
    main()
