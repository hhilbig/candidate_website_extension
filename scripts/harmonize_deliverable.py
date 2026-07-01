#!/usr/bin/env python3
"""
Build the ICPSR/DIME-compatible deliverable from our collected corpus.

Products (data/deliverable/):
  candidate_crosswalk.csv          one row/candidate-year: our keys + FEC id +
                                   DIME id/covariates + reproduced ICPSR conventions
  panel_candidate_year_icpsr.csv   product (b): crosswalk + text + text_quality
                                   (candidates_scores-shaped, PRE-SCORE)
  raw_corpus_icpsr.parquet         product (a): snapshot-level, websites_clean schema

Scoring is deferred: NO score columns are produced. Read-only over raw tarballs.
Run on the droplet (repo root, venv active) after recover_fec_ids.py +
data/dime_cache/dime_cand_subset.csv are in place.
"""
import glob
import io
import os
import re
import tarfile

import numpy as np
import pandas as pd

OUT = "data/deliverable"
FEC_XWALK = f"{OUT}/fec_id_crosswalk.csv"
DIME = "data/dime_cache/dime_cand_subset.csv"
PANEL = "data/panel/panel_candidate_year.csv"
KEY = ["candidate", "state", "office", "year"]


# ---------- convention helpers ----------
def _title(s):
    return re.sub(r"\s+", " ", str(s)).strip().title()


def icpsr_name(fec_raw, fallback):
    """FEC 'LAST, FIRST MIDDLE SUFFIX' -> 'Last, First Middle Suffix' (title)."""
    if not isinstance(fec_raw, str) or "," not in fec_raw:
        return fallback
    last, rest = fec_raw.split(",", 1)
    return f"{_title(last)}, {_title(rest)}".strip().rstrip(",")


def name_key(fec_raw, fallback):
    """-> lowercase snake 'lastname_firstname' (websites_clean internal key)."""
    if not isinstance(fec_raw, str) or "," not in fec_raw:
        f = str(fallback).lower().split()
        return "_".join(f[::-1][:2]) if f else ""
    last, rest = fec_raw.split(",", 1)
    last = re.sub(r"[^a-z]", "", last.lower())
    toks = re.sub(r"[^a-z ]", "", rest.lower()).split()
    return f"{last}_{toks[0]}" if toks else last


def district_id(state, dist, office):
    if office != "house" or pd.isna(dist):
        return np.nan
    d = int(dist)
    return f"{state}{'01' if d == 0 else f'{d:02d}'}"


def norm_district(v):
    s = str(v).strip()
    if s in ("", "nan", "None"):
        return np.nan
    try:
        return int(float(s))
    except ValueError:
        return np.nan


# ---------- build master crosswalk ----------
def build_crosswalk():
    panel = pd.read_csv(PANEL, usecols=["candidate", "state", "district", "office",
                                        "year", "party"])
    panel["district_i"] = panel["district"].map(norm_district)

    fec = pd.read_csv(FEC_XWALK, dtype={"cand_id": str, "cand_pcc": str})
    fec["district_i"] = fec["district"].map(norm_district)

    # two-pass FEC join: unambiguous 4-key, then 5-key (with district) for rest
    un = fec.drop_duplicates(KEY, keep=False)
    m = panel.merge(un[KEY + ["cand_id", "fec_raw_name", "cand_ici"]], on=KEY, how="left")
    need = m["cand_id"].isna()
    if need.any():
        k5 = KEY + ["district_i"]
        fec5 = fec.drop_duplicates(k5, keep=False)[k5 + ["cand_id", "fec_raw_name", "cand_ici"]]
        m2 = m[need].drop(columns=["cand_id", "fec_raw_name", "cand_ici"]).merge(
            fec5, on=k5, how="left")
        m.loc[need, ["cand_id", "fec_raw_name", "cand_ici"]] = m2[
            ["cand_id", "fec_raw_name", "cand_ici"]].values
    print(f"FEC id match: {m['cand_id'].notna().sum()}/{len(m)} "
          f"({100*m['cand_id'].notna().mean():.1f}%)")

    # DIME join on cand_id (static: latest row) + (cand_id, year) (dynamic)
    dime = pd.read_csv(DIME, dtype={"Cand.ID": str})
    dime = dime.rename(columns={"Cand.ID": "cand_id", "bonica.rid": "bonica_rid",
                                "recipient.cfscore": "cfscore",
                                "recipient.cfscore.dyn": "cfscore_dyn",
                                "ico.status": "dime_ico"})
    stat_cols = ["cand_id", "bonica_rid", "ICPSR", "NID", "cfscore", "seat", "lname", "fname"]
    static = dime.sort_values("fecyear").drop_duplicates("cand_id", keep="last")[stat_cols]
    dyn = (dime.dropna(subset=["fecyear"])
           .assign(year=lambda d: d["fecyear"].astype(int))
           .sort_values("cycle").drop_duplicates(["cand_id", "year"], keep="last")
           [["cand_id", "year", "cfscore_dyn", "dwnom1", "dwnom2", "district", "dime_ico"]]
           .rename(columns={"district": "dime_district"}))
    m = m.merge(static, on="cand_id", how="left").merge(dyn, on=["cand_id", "year"], how="left")
    print(f"DIME (bonica_rid) match: {m['bonica_rid'].notna().sum()}/{len(m)} "
          f"({100*m['bonica_rid'].notna().mean():.1f}%)")

    # reproduce ICPSR conventions
    m["candidate_icpsr"] = [icpsr_name(r, c) for r, c in zip(m["fec_raw_name"], m["candidate"])]
    m["name_key"] = [name_key(r, c) for r, c in zip(m["fec_raw_name"], m["candidate"])]
    m["party_full"] = m["party"].map({"D": "democrat", "R": "republican"})
    m["stage"] = 2
    m["data_source"] = "general_wayback"
    # district_id: House only (state + zero-padded, at-large=01); Senate = NA.
    # DIME's senate rows carry seat codes, so do NOT coalesce them in.
    m["district_id"] = [district_id(s, d, o)
                        for s, d, o in zip(m["state"], m["district_i"], m["office"])]
    m["candidate_year_stage"] = m["candidate_icpsr"] + "_" + m["year"].astype(str) + "_2"
    m.to_csv(f"{OUT}/candidate_crosswalk.csv", index=False)
    print(f"crosswalk: {len(m)} rows -> {OUT}/candidate_crosswalk.csv")
    return m


# ---------- product (b): candidate-year panel, pre-score ----------
def build_panel(xwalk):
    panel = pd.read_csv(PANEL)
    keep = ["candidate", "state", "office", "year", "candidate_icpsr", "name_key",
            "candidate_year_stage", "cand_id", "bonica_rid", "ICPSR", "NID", "cand_ici",
            "party_full", "district_i", "district_id", "seat", "data_source",
            "cfscore", "cfscore_dyn", "dwnom1", "dwnom2"]  # panel already has 'stage'
    out = panel.merge(xwalk[keep], on=KEY, how="left")
    out = out.rename(columns={"party_full": "party_icpsr", "district_i": "district_num"})
    out.to_csv(f"{OUT}/panel_candidate_year_icpsr.csv", index=False)
    assert not any(c for c in out.columns if c in ("score", "score_all", "mnir", "nuance_score")), \
        "score columns must not appear (pre-score contract)"
    print(f"panel (pre-score): {len(out)} rows, {out.shape[1]} cols -> "
          f"{OUT}/panel_candidate_year_icpsr.csv")


# ---------- product (a): raw corpus, websites_clean schema ----------
# Streamed with a ParquetWriter (one candidate file at a time) to bound memory.
STR_COLS = ["candidate_icpsr", "name_key", "cand_id", "bonica_rid",
            "candidate_year_stage", "state", "district", "district_id", "office",
            "party", "data_source", "date", "urlkey", "snap_url", "page_type",
            "text_snap_content"]
INT_COLS = ["year", "stage", "n_char", "n_words", "n_tags", "n_clean_tags", "n_snap"]
CORPUS_COLS = (["candidate_icpsr", "name_key", "cand_id", "bonica_rid",
                "candidate_year_stage", "state", "district", "district_id", "office",
                "year", "party", "stage", "data_source", "date", "urlkey", "snap_url",
                "page_type", "n_char", "n_words", "n_tags", "n_clean_tags", "n_snap",
                "text_snap_content"])


def build_raw_corpus(xwalk=None):
    import pyarrow as pa
    import pyarrow.parquet as pq
    if xwalk is None:
        xwalk = pd.read_csv(f"{OUT}/candidate_crosswalk.csv")
    add = ["candidate_icpsr", "name_key", "candidate_year_stage", "cand_id",
           "bonica_rid", "party_full", "district_id", "data_source"]
    xk = xwalk.set_index(KEY)[add]
    usecols = ["candidate", "state", "district", "office", "year", "date", "urlkey",
               "snap_url", "page_type", "text_snap_content", "n_char", "n_words",
               "n_tags", "n_clean_tags"]
    out_path, writer, n = f"{OUT}/raw_corpus_icpsr.parquet", None, 0
    for path in sorted(glob.glob("data/snapshots/house/*.tar.gz")) + \
            sorted(glob.glob("data/snapshots/senate/*.tar.gz")):
        office = "house" if "/house/" in path else "senate"
        with tarfile.open(path, "r:gz") as tf:
            for mtar in tf.getmembers():
                if not mtar.name.endswith(".csv"):
                    continue
                try:
                    df = pd.read_csv(io.BytesIO(tf.extractfile(mtar).read()),
                                     usecols=lambda c: c in usecols)
                except Exception:  # noqa: BLE001
                    continue
                if df.empty:
                    continue
                cand, st, yr = df["candidate"].iloc[0], df["state"].iloc[0], int(df["year"].iloc[0])
                try:
                    meta = xk.loc[(cand, st, office, yr)]
                    if isinstance(meta, pd.DataFrame):
                        meta = meta.iloc[0]
                except KeyError:
                    continue
                for c in add:
                    df[c] = meta[c]
                df = df.rename(columns={"party_full": "party"})
                df["office"], df["stage"] = office, 2
                df["n_snap"] = df["date"].nunique()
                df = df.reindex(columns=CORPUS_COLS)
                for c in STR_COLS:
                    df[c] = df[c].astype("string").fillna("")
                for c in INT_COLS:
                    df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0).astype("int64")
                table = pa.Table.from_pandas(df, preserve_index=False)
                if writer is None:
                    writer = pq.ParquetWriter(out_path, table.schema, compression="snappy")
                writer.write_table(table.cast(writer.schema))
                n += len(df)
    if writer:
        writer.close()
    print(f"raw corpus: {n} rows -> {out_path}")


def main():
    os.makedirs(OUT, exist_ok=True)
    xwalk = build_crosswalk()
    build_panel(xwalk)
    build_raw_corpus(xwalk)


if __name__ == "__main__":
    main()
