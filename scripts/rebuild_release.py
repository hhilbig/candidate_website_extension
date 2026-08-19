#!/usr/bin/env python3
"""Build the five repaired release files in a separate staging directory.

The candidate universe is same-election-year Democratic and Republican FEC
candidates, plus the reviewed general-ballot identity exceptions in
``config/candidate_identity_overrides.csv``. Confirmed non-campaign captures
are removed according to ``config/capture_adjudication.csv``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.build_panel import collapse_candidate  # noqa: E402
from scripts.drop_no_race_senate import in_scope, senate_races  # noqa: E402
from scripts.icpsr_replicate_coding import icpsr_clean_text  # noqa: E402
from src.build_candidate_roster import PARTY_MAP  # noqa: E402
from src.name_utils import clean_name  # noqa: E402
from src.release_universe import apply_capture_adjudication, select_captured_universe  # noqa: E402
from src.scrape_wayback import _clean_campaign_url  # noqa: E402

CURRENT = REPO / "data/deliverable"
CURRENT_CROSSWALK = REPO / "quality_reports/coverage_audit/csv/candidate_crosswalk.csv"
IDENTITY = REPO / "config/candidate_identity_overrides.csv"
CAPTURE_DECISIONS = REPO / "config/capture_adjudication.csv"
PANEL_KEY = ["candidate", "state", "office", "year"]
PANEL_COLUMNS = [
    "candidate", "state", "district", "office", "year", "party", "sel_date",
    "n_snapshots_available", "n_pages", "page_types", "urlkey", "text",
    "n_char", "n_words", "text_quality", "source_cand_id", "cand_id",
    "candidate_cycle_id", "cand_election_yr", "cand_status",
    "universe_source", "on_ballot", "stage", "data_source",
    "candidate_year_stage",
]


def load_fec(years: list[int]) -> pd.DataFrame:
    frames = []
    for year in years:
        d = pd.read_csv(REPO / f"data/fec_cache/cn{year}.csv", dtype=str)
        d = d[(d.cand_election_yr == str(year))
              & d.cand_office.isin(["H", "S"])
              & d.cand_pty_affiliation.isin(PARTY_MAP)].copy()
        d["year"] = year
        d["office"] = d.cand_office.map({"H": "house", "S": "senate"})
        d["state"] = d.cand_office_st
        d["party"] = d.cand_pty_affiliation.map(PARTY_MAP)
        d["district"] = pd.to_numeric(d.cand_office_district, errors="coerce").fillna(0).astype(int)
        frames.append(d)
    out = pd.concat(frames, ignore_index=True)
    out = out[in_scope(out, senate_races())].copy()
    return out.drop_duplicates(["year", "cand_id"], keep="first")


def stage_semantics(d: pd.DataFrame) -> pd.DataFrame:
    out = d.copy()
    out["on_ballot"] = out.on_ballot.fillna(False).astype(bool)
    out["stage"] = pd.Series(pd.NA, index=out.index, dtype="Int64")
    out.loc[out.on_ballot, "stage"] = 2
    out["data_source"] = "election_year_wayback"
    out.loc[out.on_ballot, "data_source"] = "general_wayback"
    if "candidate_icpsr" in out:
        out["candidate_year_stage"] = pd.NA
        out.loc[out.on_ballot, "candidate_year_stage"] = (
            out.loc[out.on_ballot, "candidate_icpsr"].astype(str) + "_"
            + out.loc[out.on_ballot, "year"].astype(str) + "_2"
        )
    return out


def captured_release_rows(fec: pd.DataFrame) -> pd.DataFrame:
    cw = pd.read_csv(CURRENT_CROSSWALK, low_memory=False, dtype={"cand_id": str})
    decisions = pd.read_csv(CAPTURE_DECISIONS, dtype={"cand_id": str})
    clean = apply_capture_adjudication(cw, decisions)
    overrides = pd.read_csv(IDENTITY, dtype=str)
    selected = select_captured_universe(clean, fec, overrides)

    canonical = fec[["year", "cand_id", "cand_election_yr", "cand_status"]].rename(
        columns={"cand_election_yr": "canonical_election_yr",
                 "cand_status": "canonical_status"})
    selected = selected.merge(canonical, on=["year", "cand_id"], how="left")
    source_frames = []
    for year in sorted(selected.year.unique()):
        d = pd.read_csv(REPO / f"data/fec_cache/cn{year}.csv", dtype=str)
        d["year"] = year
        source_frames.append(d[["year", "cand_id", "cand_election_yr", "cand_status"]])
    source = pd.concat(source_frames).rename(columns={
        "cand_id": "source_cand_id", "cand_election_yr": "source_election_yr",
        "cand_status": "source_status"})
    source = source.drop_duplicates(["year", "source_cand_id"])
    selected = selected.merge(source, on=["year", "source_cand_id"], how="left")
    selected["cand_election_yr"] = (selected.canonical_election_yr
                                      .fillna(selected.source_election_yr)
                                      .fillna(selected.cand_election_yr))
    selected["cand_status"] = (selected.canonical_status
                                 .fillna(selected.source_status)
                                 .fillna(selected.cand_status))
    selected = selected.drop(columns=["canonical_election_yr", "canonical_status",
                                      "source_election_yr", "source_status"])
    return stage_semantics(selected)


def _blank_unsupported_topics(coded: pd.DataFrame) -> pd.DataFrame:
    """Remove classifier output when no cleaned text supports the measure."""
    out = coded.copy()
    base_topics = [c for c in out if c.startswith("icpsr_topic_")
                   and not c.endswith("_home")]
    home_topics = [c for c in out if c.startswith("icpsr_topic_")
                   and c.endswith("_home")]
    assert len(base_topics) == len(home_topics) == 31

    base_support = out[["icpsr_n_char", "icpsr_n_words"]].notna().all(axis=1)
    home_support = out[["icpsr_n_char_home",
                        "icpsr_n_words_home"]].notna().all(axis=1)
    out.loc[~base_support, base_topics] = pd.NA
    out.loc[~home_support, home_topics] = pd.NA
    return out


def _panel_products(selected: pd.DataFrame, panel: pd.DataFrame,
                    out_dir: Path) -> None:
    assert panel.candidate_cycle_id.is_unique
    assert set(panel.candidate_cycle_id) == set(selected.candidate_cycle_id)
    panel[PANEL_COLUMNS].to_csv(out_dir / "panel_candidate_year.csv", index=False)

    coded = pd.read_csv(CURRENT / "panel_icpsr_compat.csv", low_memory=False,
                        dtype={"cand_id": str})
    coded = coded.rename(columns={"cand_id": "source_cand_id"})
    add = selected[["year", "source_cand_id", "cand_id", "candidate_cycle_id",
                    "cand_election_yr", "cand_status", "universe_source",
                    "on_ballot", "stage", "data_source", "candidate_year_stage"]]
    coded = coded.drop(columns=[c for c in ("on_ballot", "stage", "data_source",
                                             "candidate_year_stage") if c in coded])
    coded = coded.merge(add, on=["year", "source_cand_id"], how="inner",
                        validate="one_to_one")
    coded["icpsr_compatible"] = coded.on_ballot
    coded = _blank_unsupported_topics(coded)
    coded.to_csv(out_dir / "panel_icpsr_compat.csv", index=False)

    cw_cols = [c for c in selected.columns if not c.endswith("_x") and not c.endswith("_y")]
    selected[cw_cols].to_csv(out_dir / "candidate_crosswalk.csv", index=False)


def _roster(fec: pd.DataFrame, selected: pd.DataFrame, out_dir: Path) -> None:
    old = pd.read_csv(CURRENT / "release_roster.csv", dtype={"district": str})
    old["candidate"] = old.candidate.astype(str).str.strip()
    old["district_old"] = pd.to_numeric(old.district, errors="coerce").fillna(0).astype(int)
    old = old.sort_values("has_url", ascending=False).drop_duplicates(PANEL_KEY)

    f = fec.copy()
    f["candidate"] = f.cand_name.fillna("").map(clean_name).str.strip()
    f = f[PANEL_KEY + ["district", "party", "cand_id", "cand_election_yr",
                       "cand_status"]]
    r = old.merge(f, on=PANEL_KEY, how="inner", suffixes=("", "_fec"))
    r["candidate_cycle_id"] = r.cand_id + "-" + r.year.astype(str)

    # A handful of display-name keys map to multiple FEC IDs. Prefer the ID
    # actually attached to the captured corpus, then the matching district.
    captured = set(selected.candidate_cycle_id)
    r["_captured_id"] = r.candidate_cycle_id.isin(captured)
    r["_district_match"] = r.district_old.eq(r.district_fec)
    r = (r.sort_values(["_captured_id", "_district_match", "has_url"],
                       ascending=False)
          .drop_duplicates(PANEL_KEY, keep="first"))
    r["district"] = r.district_fec.astype(int)
    r["party"] = r.party_fec
    r = r.drop(columns=["district_old", "district_fec", "_captured_id",
                        "_district_match", "party_fec"])
    r["universe_source"] = "same_year_fec"

    # Add reviewed captured aliases under their canonical IDs when that ID is
    # not already represented in the attempted-URL roster.
    missing = selected[~selected.candidate_cycle_id.isin(r.candidate_cycle_id)].copy()
    if not missing.empty:
        url = old[PANEL_KEY + ["website_url", "has_url"]]
        extra = missing.merge(url, on=PANEL_KEY, how="left")
        extra["captured"] = True
        extra["capture_status"] = "captured"
        extra["url_status"] = "valid"
        for col in r.columns:
            if col not in extra:
                extra[col] = pd.NA
        r = pd.concat([r, extra[r.columns]], ignore_index=True)

    # The current ballot match is retained for same-year records. Reviewed
    # aliases are verified ballot candidates and therefore override it.
    alias_ids = set(selected.loc[selected.universe_source.eq("ballot_override"),
                                 "candidate_cycle_id"])
    r.loc[r.candidate_cycle_id.isin(alias_ids), "on_ballot"] = True

    # Captured rows must carry the same ballot decision and vote total in every
    # release product. The old roster's name match is not authoritative after
    # candidate-ID canonicalization.
    ballot_meta = selected.set_index("candidate_cycle_id")[["on_ballot",
                                                             "general_votes"]]
    captured_rows = r.candidate_cycle_id.isin(ballot_meta.index)
    aligned = ballot_meta.reindex(r.loc[captured_rows, "candidate_cycle_id"])
    r.loc[captured_rows, "on_ballot"] = aligned.on_ballot.to_numpy()
    r.loc[captured_rows, "general_votes"] = aligned.general_votes.to_numpy()

    r["captured"] = r.candidate_cycle_id.isin(captured)
    decisions = pd.read_csv(CAPTURE_DECISIONS, dtype={"cand_id": str})
    invalid = set((decisions[decisions.action.eq("exclude")].cand_id + "-"
                   + decisions[decisions.action.eq("exclude")].year.astype(str)))
    r["capture_status"] = "not_captured"
    r.loc[r.captured, "capture_status"] = "captured"
    r.loc[r.candidate_cycle_id.isin(invalid), "capture_status"] = "invalid_capture_excluded"

    retained = set((decisions[decisions.action.eq("retain")].cand_id + "-"
                    + decisions[decisions.action.eq("retain")].year.astype(str)))
    r["has_url"] = r.website_url.map(lambda x: _clean_campaign_url(x) is not None)
    r["url_status"] = "missing_or_invalid"
    r.loc[r.has_url, "url_status"] = "valid"
    r.loc[r.candidate_cycle_id.isin(retained), "url_status"] = "valid_manual_override"
    r.loc[r.candidate_cycle_id.isin(invalid), "url_status"] = "invalid_capture_source"
    r = stage_semantics(r)

    cols = ["candidate_cycle_id", "cand_id", "cand_election_yr", "cand_status",
            "candidate", "state", "district", "office", "year", "party",
            "universe_source", "website_url", "url_status", "has_url", "captured",
            "capture_status", "on_ballot", "general_votes", "stage", "data_source"]
    r[cols].sort_values(["office", "year", "state", "district", "candidate"]).to_csv(
        out_dir / "release_roster.csv", index=False)


def _correct_raw_counts(d: pd.DataFrame) -> pd.DataFrame:
    """Recompute every published page count from the preserved text."""
    out = d.copy()
    texts = out["text_snap_content"].fillna("").astype(str)
    out["n_char"] = texts.map(len)
    out["n_words"] = texts.map(lambda text: len(text.split()))
    cleaned = texts.map(icpsr_clean_text)
    out["n_tags"] = cleaned.map(lambda x: x["n_tags"])
    out["n_clean_tags"] = cleaned.map(lambda x: x["n_clean_tags"])
    assert out["n_char"].ge(0).all()
    assert out["n_words"].ge(0).all()
    assert out["n_tags"].ge(1).all()
    assert out["n_clean_tags"].between(0, out["n_tags"]).all()
    return out


def _raw(selected: pd.DataFrame, out_dir: Path) -> pd.DataFrame:
    import pyarrow as pa
    import pyarrow.parquet as pq

    source = CURRENT / "raw_corpus_icpsr.parquet"
    target = out_dir / "raw_corpus.parquet"
    pf = pq.ParquetFile(source)
    lookup_cols = [
        "candidate", "state", "district", "office", "party", "cand_id",
        "candidate_cycle_id", "cand_election_yr", "cand_status",
        "universe_source", "on_ballot", "stage", "data_source",
        "candidate_year_stage",
    ]
    lookup = selected.set_index(["year", "source_cand_id"])[lookup_cols]
    assert lookup.index.is_unique
    writer = None
    kept = 0
    panel_rows = []
    for i in range(pf.num_row_groups):
        d = pf.read_row_group(i).to_pandas()
        if d.empty:
            continue
        key = (int(d.year.iloc[0]), str(d.cand_id.iloc[0]))
        if key not in lookup.index:
            continue
        m = lookup.loc[key]
        for col in m.index:
            if col not in PANEL_KEY + ["district", "party"]:
                d[col] = m[col]
        d["cand_id"] = m.cand_id
        d = _correct_raw_counts(d)

        # Build the selected candidate-year row from these corrected counts.
        # Display identity and party fields come from the canonical selection,
        # while page text and snapshot choice come from the raw corpus.
        panel_input = d.copy()
        for col in ("candidate", "state", "district", "office", "party"):
            panel_input[col] = m[col]
        panel_row = collapse_candidate(panel_input, "#+#")
        if panel_row is None:
            raise AssertionError(f"no non-empty page text for {key}")
        for col in ("source_cand_id", "cand_id", "candidate_cycle_id",
                    "cand_election_yr", "cand_status", "universe_source",
                    "on_ballot", "stage", "data_source",
                    "candidate_year_stage"):
            panel_row[col] = key[1] if col == "source_cand_id" else m[col]
        panel_rows.append(panel_row)

        for col in ("cand_id", "candidate_cycle_id", "cand_election_yr",
                    "cand_status", "universe_source", "data_source",
                    "candidate_year_stage"):
            d[col] = d[col].astype("string").fillna("")
        d["on_ballot"] = d["on_ballot"].astype(bool)
        d["stage"] = pd.to_numeric(d["stage"], errors="coerce").astype("Int64")
        table = pa.Table.from_pandas(d, preserve_index=False)
        if writer is None:
            writer = pq.ParquetWriter(target, table.schema, compression="snappy")
        writer.write_table(table.cast(writer.schema))
        kept += len(d)
    if writer is None:
        raise RuntimeError("no raw corpus rows survived the release filter")
    writer.close()
    print(f"raw corpus: {kept:,}/{pf.metadata.num_rows:,} rows")
    panel = pd.DataFrame(panel_rows)
    assert len(panel) == len(selected)
    assert panel.candidate_cycle_id.is_unique
    print(f"reconstructed panel: {len(panel):,} candidate-years")
    return panel


def _manifest(out_dir: Path) -> None:
    rows = []
    for p in sorted(out_dir.iterdir()):
        if not p.is_file() or p.name == "manifest.json":
            continue
        if p.suffix == ".csv":
            d = pd.read_csv(p, low_memory=False)
            nrows, ncols = d.shape
        elif p.suffix == ".parquet":
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(p)
            nrows, ncols = pf.metadata.num_rows, pf.metadata.num_columns
        else:
            continue
        rows.append({"file": p.name, "rows": nrows, "columns": ncols,
                     "bytes": p.stat().st_size,
                     "sha256": hashlib.sha256(p.read_bytes()).hexdigest()})
    (out_dir / "manifest.json").write_text(json.dumps(rows, indent=2) + "\n")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--out-dir", type=Path, default=REPO / "build/release_candidate")
    a = ap.parse_args()
    a.out_dir.mkdir(parents=True, exist_ok=True)
    years = list(range(2002, 2025, 2))
    fec = load_fec(years)
    selected = captured_release_rows(fec)
    print(f"captured candidate-years: {len(selected):,}")
    panel = _raw(selected, a.out_dir)
    _panel_products(selected, panel, a.out_dir)
    _roster(fec, selected, a.out_dir)
    shutil.copy2(REPO / "docs/RELEASE_README.md", a.out_dir / "README.md")
    shutil.copy2(REPO / "docs/deliverable_codebook.md", a.out_dir / "codebook.md")
    _manifest(a.out_dir)
    print(f"wrote release candidate to {a.out_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
