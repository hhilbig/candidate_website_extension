"""Data for the mayoral social-media figures (scripts/mayoral_social_figures.R), from the release folder.

coverage_by_year_role.csv     share of eligible finalists with usable text, by election year and result role
page_types_by_period.csv      share of finalists with text that have at least one usable page of each page type
                              (page_type in the corpus), by four-year period of the election

Usage: python scripts/mayoral_social_figure_data.py [release_dir]
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

RELEASE = Path(sys.argv[1] if len(sys.argv) > 1 else "build/mayoral_release_v6_candidate")
OUT = Path("quality_reports/social_figure_candidates/mayoral/data")
PAGE_TYPES = {"issues": "Issues", "biography": "Biography", "endorsements": "Endorsements", "action": "Donate or volunteer", "news": "News"}
PERIODS = ([2009, 2013, 2017, 2021, 2025], ["2010–13", "2014–17", "2018–21", "2022–25"])


def main() -> None:
    OUT.mkdir(parents=True, exist_ok=True)
    finalists = pd.read_csv(RELEASE / "mayoral_finalists.csv", dtype=str)
    finalists = finalists[finalists["eligible"] == "True"].assign(text=lambda d: d["text_status"] == "text")
    coverage = finalists.groupby(["year", "official_result_role"])["text"].mean().mul(100).reset_index(name="share")
    coverage.to_csv(OUT / "coverage_by_year_role.csv", index=False)

    panel = pd.read_csv(RELEASE / "mayoral_finalist_panel.csv", usecols=["candidate_election_id", "year"])
    pages = pd.read_parquet(RELEASE / "mayoral_page_corpus.parquet",
                            columns=["candidate_election_id", "page_type", "release_usable"])
    has = (pages[pages["release_usable"]].groupby(["candidate_election_id", "page_type"]).size().unstack(fill_value=0) > 0)
    panel = panel.merge(has[list(PAGE_TYPES)], left_on="candidate_election_id", right_index=True, how="left").fillna(False)
    panel["period"] = pd.cut(panel["year"], PERIODS[0], labels=PERIODS[1])
    panel = panel.dropna(subset=["period"])
    shares = panel.groupby("period", observed=True)[list(PAGE_TYPES)].mean().mul(100).reset_index()
    shares = shares.melt(id_vars="period", var_name="page_type", value_name="share")
    shares["label"] = shares["page_type"].map(PAGE_TYPES)
    sizes = panel.groupby("period", observed=True).size().rename("n").reset_index()
    shares.merge(sizes, on="period").to_csv(OUT / "page_types_by_period.csv", index=False)

if __name__ == "__main__":
    main()
