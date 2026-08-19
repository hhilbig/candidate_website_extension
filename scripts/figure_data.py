#!/usr/bin/env python3
"""Build the tidy CSVs behind the coverage and validation figures.

Seven outputs, written to the requested output directory:

  coverage_by_office_year.csv  roster, url found, captured -- the denominator
  boundary_nchar.csv           median n_char by office-year, cleaned AND
                               uncleaned, plus ICPSR's own years
  validation_pairs.csv         our recomputed values against ICPSR's published
                               values, on ICPSR's own text
  topic_by_party.csv           topic shares by party and year, for face validity
  coverage_vs_ballot.csv       capture rates against independent ballot returns
  cfscore_correlations.csv     topic correlations with DIME CF-scores
  welfare_series.csv           ICPSR and extension House welfare attention

The uncleaned series exists to show what the release would look like without
replicating ICPSR's boilerplate filter: it is the difference between a series
that joins at the 2016/2018 boundary and one that jumps ~50%.

Usage: python scripts/figure_data.py --release-dir build/release_candidate
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
from icpsr_replicate_coding import (  # noqa: E402
    icpsr_clean_text, page_measures, icpsr_aggregate, icpsr_key, load_ngrams)
from coverage_vs_returns import ballot, match, theirs  # noqa: E402

ICPSR = Path.home() / ("Library/CloudStorage/Dropbox/Research/19_Great_Recession"
                       "/data/candidate_websites/226001-V1")
OUT = REPO / "quality_reports/figures/data"
TOPIC_PREFIX = "icpsr_topic_"


def coverage(release_dir: Path, out: Path) -> None:
    r = pd.read_csv(release_dir / "release_roster.csv", low_memory=False)
    g = (r.groupby(["office", "year"])
           .agg(roster=("captured", "size"),
                has_url=("has_url", "sum"),
                captured=("captured", "sum"))
           .reset_index())
    g["pct_of_roster"] = 100 * g.captured / g.roster
    g["pct_of_url"] = 100 * g.captured / g.has_url
    g.to_csv(out / "coverage_by_office_year.csv", index=False)
    print(f"coverage: {len(g)} office-years, "
          f"{g.captured.sum():,}/{g.roster.sum():,} captured")


def boundary(release_dir: Path, out: Path) -> None:
    """Median n_char per candidate-year, cleaned vs uncleaned, plus ICPSR."""
    pf = pq.ParquetFile(release_dir / "raw_corpus.parquet")
    rows = []
    for i in range(pf.num_row_groups):
        g = pf.read_row_group(i, columns=["candidate_icpsr", "state", "office",
                                          "year", "date", "n_char", "on_ballot",
                                          "text_snap_content"]).to_pandas()
        g = g[g.on_ballot & (g.n_char > 0)]
        if g.empty:
            continue
        raw = g.n_char.to_numpy(float)
        cln = np.array([len(icpsr_clean_text(t)["text"])
                        for t in g.text_snap_content])
        day = g.date.astype(str).str.slice(0, 8)
        d = pd.DataFrame({"day": day, "raw": raw, "cln": cln})
        # ICPSR aggregation: mean within snapshot-day, then across days.
        # Empty-after-cleaning pages drop out of the cleaned series only.
        per = d.groupby("day").agg(raw=("raw", "mean"))
        c = d[d.cln > 0].groupby("day").cln.mean()
        rows.append({"office": g.office.iloc[0], "year": int(g.year.iloc[0]),
                     "raw": per.raw.mean(),
                     "cln": c.mean() if len(c) else np.nan})
        if (i + 1) % 2000 == 0:
            print(f"  boundary: row group {i+1}/{pf.num_row_groups}", flush=True)
    ours = pd.DataFrame(rows)
    agg = (ours.groupby(["office", "year"])
               .agg(uncleaned=("raw", "median"), cleaned=("cln", "median"))
               .reset_index())
    agg["source"] = "extension"

    t = pd.read_csv(ICPSR / "candidates_complexity.csv")
    t = t[(t.stage == 2) & (t.data_source == "general_wayback")
          & t.party.isin(["democrat", "republican"])]
    th = (t.groupby("year").n_char.median().reset_index()
            .rename(columns={"n_char": "cleaned"}))
    th["office"], th["uncleaned"], th["source"] = "house", np.nan, "icpsr"
    pd.concat([th, agg], ignore_index=True).to_csv(
        out / "boundary_nchar.csv", index=False)
    print(f"boundary: {len(agg)} of ours + {len(th)} ICPSR years")


def validation(out: Path, n_cand: int) -> None:
    """Our recomputed values vs ICPSR's published values, on ICPSR's own text."""
    ng_path = REPO / "data/external/ngrams_en_2008.csv"
    ngrams = load_ngrams(ng_path) if ng_path.exists() else None
    w = pq.read_table(ICPSR / "websites_clean.parquet").to_pandas()
    w["ck"] = icpsr_key(w)
    truth = pd.read_csv(ICPSR / "candidates_complexity.csv")
    truth["ck"] = icpsr_key(truth)
    rng = np.random.default_rng(20260814)
    cks = rng.choice(truth.ck.unique(), size=n_cand, replace=False)
    s = w[w.ck.isin(set(cks)) & (w.n_char > 0)].copy()
    m = pd.DataFrame([page_measures(t, 200, ngrams)
                      for t in s.text_snap_content], index=s.index)
    cols = ["n_char", "n_words", "ttr", "mattr"] + (["entropy"] if ngrams else [])
    for c in cols:
        s[c] = m[c]
    agg = icpsr_aggregate(s, cols)
    j = (truth.set_index("ck").loc[cks][["n_char", "n_words", "TTR", "MATTR",
                                         "entropy"]]
         .join(agg, rsuffix="_ours").dropna())
    tidy = []
    for pub, ours in (("n_char", "n_char_ours"), ("n_words", "n_words_ours"),
                      ("TTR", "ttr"), ("MATTR", "mattr"),
                      ("entropy", "entropy_ours")):
        if ours not in j.columns:
            continue
        tidy.append(pd.DataFrame({"variable": pub, "published": j[pub],
                                  "ours": j[ours]}))
    v = pd.concat(tidy, ignore_index=True)
    v.to_csv(out / "validation_pairs.csv", index=False)
    for var, g in v.groupby("variable"):
        print(f"  {var:<8} n={len(g):4d} corr={np.corrcoef(g.published, g.ours)[0,1]:.4f}")


def topics_by_party(release_dir: Path, out: Path) -> None:
    p = pd.read_csv(release_dir / "panel_icpsr_compat.csv",
                    low_memory=False)
    tcols = [c for c in p.columns
             if c.startswith(TOPIC_PREFIX) and not c.endswith("_home")]
    # The compat panel carries ICPSR's party coding ("democrat"/"republican"),
    # not the raw FEC "D"/"R".
    p = p[p.on_ballot & p.party.isin(["democrat", "republican"])
          & p[tcols[0]].notna()].copy()
    p["party"] = p.party.map({"democrat": "D", "republican": "R"})
    g = (p.groupby(["party", "office", "year"])[tcols].mean().reset_index()
           .melt(id_vars=["party", "office", "year"],
                 var_name="topic", value_name="share"))
    g["topic"] = g.topic.str.replace(TOPIC_PREFIX, "", regex=False)
    g.to_csv(out / "topic_by_party.csv", index=False)
    n_t = max(g.topic.nunique(), 1)
    print(f"topics: {g.topic.nunique()} topics, {len(g)//n_t} party-office-years")


def coverage_vs_ballot(release_dir: Path, out: Path) -> None:
    """Coverage using the independent general-election ballot denominator."""
    b = ballot()
    t = theirs()
    b["in_icpsr"] = match(b, t)
    p = pd.read_csv(release_dir / "panel_icpsr_compat.csv", low_memory=False)
    rows = []
    for (office, year), g in b.groupby(["office", "year"]):
        icpsr_hit = g.in_icpsr
        if icpsr_hit.any():
            rows.append({
                "office": office, "year": year, "source": "icpsr",
                "n_ballot": len(g), "n_matched": int(icpsr_hit.sum()),
                "pct_cand": 100 * icpsr_hit.mean(),
                "votes_total": int(g.candidatevotes.sum()),
                "votes_matched": int(g.loc[icpsr_hit, "candidatevotes"].sum()),
                "pct_votes": 100 * g.loc[icpsr_hit, "candidatevotes"].sum()
                / g.candidatevotes.sum(),
            })
        q = p[(p.office == office) & (p.year == year) & p.on_ballot]
        if not q.empty:
            matched_votes = int(pd.to_numeric(q.general_votes, errors="coerce").sum())
            rows.append({
                "office": office, "year": year, "source": "extension",
                "n_ballot": len(g), "n_matched": len(q),
                "pct_cand": 100 * len(q) / len(g),
                "votes_total": int(g.candidatevotes.sum()),
                "votes_matched": matched_votes,
                "pct_votes": 100 * matched_votes / g.candidatevotes.sum(),
            })
    result = pd.DataFrame(rows).sort_values(["office", "source", "year"])
    result.to_csv(out / "coverage_vs_ballot.csv", index=False)
    ext = result[result.source.eq("extension")]
    print(f"ballot coverage: {int(ext.n_matched.sum()):,} captured ballot candidates")


def cfscore_correlations(release_dir: Path, out: Path) -> None:
    p = pd.read_csv(release_dir / "panel_icpsr_compat.csv", low_memory=False)
    x = pd.read_csv(release_dir / "candidate_crosswalk.csv", low_memory=False,
                    usecols=["candidate_cycle_id", "cfscore"])
    p = p.merge(x, on="candidate_cycle_id", how="left", validate="one_to_one")
    p = p[p.on_ballot].copy()
    tcols = [c for c in p if c.startswith(TOPIC_PREFIX) and not c.endswith("_home")]
    rows = []
    for col in tcols:
        row = {"topic": col.removeprefix(TOPIC_PREFIX)}
        for label, mask in (("all", pd.Series(True, index=p.index)),
                            ("D", p.party.eq("democrat")),
                            ("R", p.party.eq("republican"))):
            q = p.loc[mask, [col, "cfscore"]].dropna()
            row[f"r_{label}"] = q[col].corr(q.cfscore)
        rows.append(row)
    pd.DataFrame(rows).to_csv(out / "cfscore_correlations.csv", index=False)
    print(f"CF-score correlations: {len(rows)} topics")


def welfare_series(release_dir: Path, out: Path) -> None:
    topics = pd.read_csv(ICPSR / "candidates_topics.csv", low_memory=False)
    complexity = pd.read_csv(ICPSR / "candidates_complexity.csv", low_memory=False)
    complexity = complexity[(complexity.stage == 2)
                            & complexity.data_source.eq("general_wayback")
                            & complexity.party.isin(["democrat", "republican"])]
    old = topics.merge(complexity[["candidate", "year", "stage", "party"]],
                       on=["candidate", "year", "stage"], how="inner")
    old = (old.groupby(["year", "party"])["Welfare State"].mean().mul(100)
           .reset_index(name="share"))
    old["source"] = "icpsr"

    p = pd.read_csv(release_dir / "panel_icpsr_compat.csv", low_memory=False)
    p = p[p.office.eq("house") & p.on_ballot
          & p.party.isin(["democrat", "republican"])]
    new = (p.groupby(["year", "party"])[f"{TOPIC_PREFIX}Welfare State"].mean().mul(100)
             .reset_index(name="share"))
    new["source"] = "extension"
    pd.concat([old, new], ignore_index=True)[["year", "party", "source", "share"]].to_csv(
        out / "welfare_series.csv", index=False)
    print("welfare series: comparable D/R House ballot samples")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--sample-cands", type=int, default=400)
    ap.add_argument("--release-dir", type=Path,
                    default=REPO / "build/release_candidate")
    ap.add_argument("--out-dir", type=Path,
                    default=REPO / "quality_reports/figures/data")
    ap.add_argument("--skip-boundary", action="store_true",
                    help="the boundary pass reads the whole corpus (~10 min)")
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)
    print("coverage ..."); coverage(a.release_dir, a.out_dir)
    print("ballot coverage ..."); coverage_vs_ballot(a.release_dir, a.out_dir)
    print("topics ..."); topics_by_party(a.release_dir, a.out_dir)
    print("CF-score correlations ..."); cfscore_correlations(a.release_dir, a.out_dir)
    print("welfare series ..."); welfare_series(a.release_dir, a.out_dir)
    print("validation ..."); validation(a.out_dir, a.sample_cands)
    if not a.skip_boundary:
        print("boundary (full corpus pass) ..."); boundary(a.release_dir, a.out_dir)
    print(f"\nwrote to {a.out_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
