#!/usr/bin/env python3
"""Validate website diagnostics against a staged release and pin their hashes."""
from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path

import numpy as np
import pandas as pd


DATA_FILES = {
    "boundary_nchar.csv", "cfscore_correlations.csv",
    "coverage_by_office_year.csv", "coverage_vs_ballot.csv",
    "topic_by_party.csv", "validation_pairs.csv", "welfare_series.csv",
}
FIGURE_NAMES = {
    "boundary_continuity", "coverage_by_office_year", "coverage_counts",
    "external_validity_cfscore", "topics_party_gap", "topics_welfare_state",
    "validation_scatter", "welfare_state_series",
}
TOPIC_PREFIX = "icpsr_topic_"


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def assert_close(left: pd.DataFrame, right: pd.DataFrame, keys: list[str],
                 values: list[str], atol: float = 1e-10) -> None:
    a = left.sort_values(keys).reset_index(drop=True)
    b = right.sort_values(keys).reset_index(drop=True)
    assert a[keys].equals(b[keys]), f"diagnostic keys differ: {keys}"
    for col in values:
        assert np.allclose(a[col], b[col], equal_nan=True, atol=atol), col


def semantic_checks(release_dir: Path, data_dir: Path) -> dict[str, int]:
    panel = pd.read_csv(release_dir / "panel_icpsr_compat.csv", low_memory=False)
    roster = pd.read_csv(release_dir / "release_roster.csv", low_memory=False)
    crosswalk = pd.read_csv(release_dir / "candidate_crosswalk.csv", low_memory=False)

    coverage = pd.read_csv(data_dir / "coverage_by_office_year.csv")
    expected = (roster.groupby(["office", "year"])
                .agg(roster=("captured", "size"), has_url=("has_url", "sum"),
                     captured=("captured", "sum")).reset_index())
    expected["pct_of_roster"] = 100 * expected.captured / expected.roster
    expected["pct_of_url"] = 100 * expected.captured / expected.has_url
    assert_close(coverage, expected, ["office", "year"],
                 ["roster", "has_url", "captured", "pct_of_roster", "pct_of_url"])

    ballot = pd.read_csv(data_dir / "coverage_vs_ballot.csv")
    ext = ballot[ballot.source.eq("extension")]
    expected_ballot = (panel[panel.on_ballot]
                       .groupby(["office", "year"]).size()
                       .rename("n_matched").reset_index())
    assert_close(ext, expected_ballot, ["office", "year"], ["n_matched"])

    topic_cols = [c for c in panel if c.startswith(TOPIC_PREFIX)
                  and not c.endswith("_home")]
    p = panel[panel.party.isin(["democrat", "republican"])
              & panel[topic_cols[0]].notna()].copy()
    p["party"] = p.party.map({"democrat": "D", "republican": "R"})
    p = p[p.on_ballot].copy()
    expected_topics = (p.groupby(["party", "office", "year"])[topic_cols]
                       .mean().reset_index()
                       .melt(id_vars=["party", "office", "year"],
                             var_name="topic", value_name="share"))
    expected_topics["topic"] = expected_topics.topic.str.removeprefix(TOPIC_PREFIX)
    topics = pd.read_csv(data_dir / "topic_by_party.csv")
    assert_close(topics, expected_topics, ["party", "office", "year", "topic"],
                 ["share"])

    scores = crosswalk[["candidate_cycle_id", "cfscore"]]
    q = panel.merge(scores, on="candidate_cycle_id", how="left", validate="one_to_one")
    q = q[q.on_ballot].copy()
    expected_cf = []
    for col in topic_cols:
        row = {"topic": col.removeprefix(TOPIC_PREFIX)}
        for label, mask in (("all", pd.Series(True, index=q.index)),
                            ("D", q.party.eq("democrat")),
                            ("R", q.party.eq("republican"))):
            z = q.loc[mask, [col, "cfscore"]].dropna()
            row[f"r_{label}"] = z[col].corr(z.cfscore)
        expected_cf.append(row)
    cf = pd.read_csv(data_dir / "cfscore_correlations.csv")
    assert_close(cf, pd.DataFrame(expected_cf), ["topic"], ["r_all", "r_D", "r_R"])

    welfare = pd.read_csv(data_dir / "welfare_series.csv")
    expected_welfare = (panel[panel.office.eq("house") & panel.on_ballot]
                        .groupby(["year", "party"])
                        [f"{TOPIC_PREFIX}Welfare State"].mean().mul(100)
                        .reset_index(name="share"))
    expected_welfare["source"] = "extension"
    assert_close(welfare[welfare.source.eq("extension")], expected_welfare,
                 ["year", "party", "source"], ["share"])

    boundary = pd.read_csv(data_dir / "boundary_nchar.csv")
    extension_keys = boundary[boundary.source.eq("extension")][["office", "year"]]
    panel_keys = panel[["office", "year"]].drop_duplicates()
    assert (set(map(tuple, extension_keys.to_numpy()))
            == set(map(tuple, panel_keys.to_numpy())))
    assert len(pd.read_csv(data_dir / "validation_pairs.csv")) > 0

    release_manifest = json.loads((release_dir / "manifest.json").read_text())
    raw_rows = next(x["rows"] for x in release_manifest
                    if x["file"] == "raw_corpus.parquet")
    return {
        "candidate_years": len(panel),
        "raw_rows": int(raw_rows),
        "roster_rows": len(roster),
        "ballot_candidate_years": int(panel.on_ballot.sum()),
    }


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--release-dir", type=Path, required=True)
    ap.add_argument("--data-dir", type=Path, required=True)
    ap.add_argument("--figure-dir", type=Path, required=True)
    ap.add_argument("--manifest", type=Path, required=True)
    ap.add_argument("--write-manifest", action="store_true")
    a = ap.parse_args()

    missing_data = DATA_FILES - {p.name for p in a.data_dir.glob("*.csv")}
    assert not missing_data, f"missing diagnostic data: {sorted(missing_data)}"
    expected_figures = {f"{name}.{ext}" for name in FIGURE_NAMES
                        for ext in ("png", "pdf")}
    missing_figures = expected_figures - {p.name for p in a.figure_dir.iterdir()}
    assert not missing_figures, f"missing diagnostic figures: {sorted(missing_figures)}"
    for name in expected_figures:
        assert (a.figure_dir / name).stat().st_size > 10_000, f"trivial figure: {name}"

    counts = semantic_checks(a.release_dir, a.data_dir)
    release_manifest_hash = sha256(a.release_dir / "manifest.json")
    output_hashes = {
        f"data/{p.name}": sha256(p) for p in sorted(a.data_dir.glob("*.csv"))
    } | {
        f"figures/{p.name}": sha256(p)
        for p in sorted(a.figure_dir.iterdir()) if p.name in expected_figures
    }
    manifest = {
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "release_manifest_sha256": release_manifest_hash,
        "counts": counts,
        "outputs": output_hashes,
    }
    if a.write_manifest:
        a.manifest.parent.mkdir(parents=True, exist_ok=True)
        a.manifest.write_text(json.dumps(manifest, indent=2) + "\n")
    else:
        recorded = json.loads(a.manifest.read_text())
        assert recorded["release_manifest_sha256"] == release_manifest_hash, \
            "diagnostics were generated from a different release manifest"
        assert recorded["counts"] == counts, "diagnostic counts changed"
        assert recorded["outputs"] == output_hashes, "diagnostic output hashes changed"
    print(f"PASS: diagnostics match {counts['candidate_years']:,} candidate-years, "
          f"{counts['raw_rows']:,} raw rows, and {counts['roster_rows']:,} roster rows")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
