#!/usr/bin/env python3
"""Build tidy data for internal social-media figure candidates.

The cross-dataset series use Democratic and Republican House candidates who
appeared in official general-election returns. Di Tella et al. supply
2002--2016; this release supplies 2018--2024. The handoff remains explicit in
every output through the ``source`` column.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import numpy as np
import pandas as pd


TOPIC_PREFIX = "icpsr_topic_"
PARTIES = ["democrat", "republican"]
DISPLAY_TOPICS = [
    "Welfare State",
    "Equality",
    "Education",
    "Labour Groups",
    "Political Authority",
    "Law and Order",
    "Military",
    "National Way of Life",
]
LONG_RUN_TOPICS = ["Welfare State", "Equality", "Law and Order", "Military"]


def topic_columns(frame: pd.DataFrame) -> list[str]:
    return [
        c for c in frame.columns
        if c.startswith(TOPIC_PREFIX) and not c.endswith("_home")
    ]


def weighted_mean(values: pd.Series, weights: pd.Series) -> float:
    keep = values.notna() & weights.notna() & weights.gt(0)
    if not keep.any():
        return np.nan
    return float(np.average(values[keep], weights=weights[keep]))


def combined_house_topics(
    panel: pd.DataFrame, icpsr_dir: Path
) -> tuple[pd.DataFrame, dict[str, int]]:
    old_topics = pd.read_csv(icpsr_dir / "candidates_topics.csv", low_memory=False)
    old_complexity = pd.read_csv(
        icpsr_dir / "candidates_complexity.csv", low_memory=False
    )
    old_complexity = old_complexity[
        old_complexity.stage.eq(2)
        & old_complexity.data_source.eq("general_wayback")
        & old_complexity.party.isin(PARTIES)
    ]
    old = old_topics.merge(
        old_complexity[["candidate", "year", "stage", "party"]],
        on=["candidate", "year", "stage"],
        how="inner",
        validate="one_to_one",
    )
    old_cols = [c.removeprefix(TOPIC_PREFIX) for c in topic_columns(panel)]
    old_long = (
        old.groupby(["year", "party"])[old_cols]
        .mean()
        .reset_index()
        .melt(id_vars=["year", "party"], var_name="topic", value_name="share")
    )
    old_long["source"] = "Di Tella et al."

    new = panel[
        panel.office.eq("house")
        & panel.on_ballot
        & panel.party.isin(PARTIES)
    ].copy()
    new_cols = topic_columns(new)
    new_long = (
        new.groupby(["year", "party"])[new_cols]
        .mean()
        .reset_index()
        .melt(id_vars=["year", "party"], var_name="topic", value_name="share")
    )
    new_long["topic"] = new_long.topic.str.removeprefix(TOPIC_PREFIX)
    new_long["source"] = "This release"

    combined = pd.concat([old_long, new_long], ignore_index=True)
    combined["share"] *= 100
    cell_counts = pd.concat(
        [
            old.groupby(["year", "party"]).size().rename("n").reset_index()
            .assign(source="Di Tella et al."),
            new.groupby(["year", "party"]).size().rename("n").reset_index()
            .assign(source="This release"),
        ],
        ignore_index=True,
    )
    assert cell_counts.n.min() >= 100, "headline House party-year cell below 100"
    counts = {
        "icpsr_house_ballot_candidate_years": int(len(old)),
        "release_house_ballot_candidate_years": int(len(new)),
        "minimum_house_party_year_n": int(cell_counts.n.min()),
    }
    return combined.merge(
        cell_counts, on=["year", "party", "source"], validate="many_to_one"
    ), counts


def website_evolution(panel: pd.DataFrame, icpsr_dir: Path) -> pd.DataFrame:
    """Build comparable House series for length and lexical diversity."""
    old = pd.read_csv(
        icpsr_dir / "candidates_complexity.csv", low_memory=False
    )
    old = old[
        old.stage.eq(2)
        & old.data_source.eq("general_wayback")
        & old.party.isin(PARTIES)
    ]
    old_year = (
        old.groupby("year")
        .agg(
            median_words=("n_words", "median"),
            median_mattr=("MATTR", "median"),
            n_words=("n_words", "count"),
            n_mattr=("MATTR", "count"),
        )
        .reset_index()
    )
    old_year["source"] = "Di Tella et al."

    new = panel[
        panel.office.eq("house")
        & panel.on_ballot
        & panel.party.isin(PARTIES)
    ]
    new_year = (
        new.groupby("year")
        .agg(
            median_words=("icpsr_n_words", "median"),
            median_mattr=("icpsr_mattr_approx", "median"),
            n_words=("icpsr_n_words", "count"),
            n_mattr=("icpsr_mattr_approx", "count"),
        )
        .reset_index()
    )
    new_year["source"] = "This release"

    wide = pd.concat([old_year, new_year], ignore_index=True)
    words = wide[["year", "source", "median_words", "n_words"]].rename(
        columns={"median_words": "value", "n_words": "n"}
    )
    words["measure"] = "Website length"
    mattr = wide[["year", "source", "median_mattr", "n_mattr"]].rename(
        columns={"median_mattr": "value", "n_mattr": "n"}
    )
    mattr["measure"] = "Lexical diversity"
    out = pd.concat([words, mattr], ignore_index=True)
    assert out.n.min() >= 100, "headline House year cell below 100"
    assert out.value.notna().all(), "missing annual median"
    return out.sort_values(["measure", "year"])


def party_gap(frame: pd.DataFrame, value: str = "share") -> pd.DataFrame:
    wide = frame.pivot(
        index=[c for c in frame.columns if c not in {"party", value, "n"}],
        columns="party",
        values=value,
    ).reset_index()
    wide["gap"] = wide["democrat"] - wide["republican"]
    return wide


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--release-dir", type=Path, required=True)
    parser.add_argument("--icpsr-dir", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    panel = pd.read_csv(args.release_dir / "panel_icpsr_compat.csv", low_memory=False)
    crosswalk = pd.read_csv(
        args.release_dir / "candidate_crosswalk.csv",
        usecols=["candidate_cycle_id", "cfscore"],
    )
    topics = topic_columns(panel)
    assert len(topics) == 31
    sums = panel[topics].sum(axis=1)
    assert np.allclose(sums, 1, atol=1e-8), "topic shares do not sum to one"

    combined, counts = combined_house_topics(panel, args.icpsr_dir)
    evolution = website_evolution(panel, args.icpsr_dir)
    evolution.to_csv(args.out_dir / "website_evolution.csv", index=False)
    gaps = party_gap(combined)
    gaps[gaps.topic.isin(LONG_RUN_TOPICS)].to_csv(
        args.out_dir / "long_run_topic_gaps.csv", index=False
    )

    profiles = combined.pivot_table(
        index=["year", "source", "topic"], columns="party", values="share"
    ).reset_index()
    profiles["abs_gap"] = (profiles.democrat - profiles.republican).abs()
    distance = (
        profiles.groupby(["year", "source"])
        .abs_gap.sum()
        .div(2)
        .reset_index(name="total_variation_pp")
    )
    distance.to_csv(args.out_dir / "agenda_distance.csv", index=False)

    current_house = panel[
        panel.office.eq("house") & panel.on_ballot & panel.party.isin(PARTIES)
    ].copy()
    agenda_2024 = (
        current_house[current_house.year.eq(2024)]
        .groupby("party")[[f"{TOPIC_PREFIX}{t}" for t in DISPLAY_TOPICS]]
        .mean()
        .mul(100)
        .T.reset_index(names="topic")
    )
    agenda_2024["topic"] = agenda_2024.topic.str.removeprefix(TOPIC_PREFIX)
    agenda_2024.to_csv(args.out_dir / "agenda_2024.csv", index=False)

    recent = panel[
        panel.on_ballot & panel.year.ge(2018) & panel.party.isin(PARTIES)
    ].copy()
    chamber = (
        recent.groupby(["office", "year", "party"])[topics]
        .mean()
        .reset_index()
        .melt(
            id_vars=["office", "year", "party"],
            var_name="topic",
            value_name="share",
        )
    )
    chamber["topic"] = chamber.topic.str.removeprefix(TOPIC_PREFIX)
    chamber_gap = party_gap(chamber).groupby(["office", "topic"]).gap.mean()
    chamber_plot = chamber_gap.unstack("office").reset_index()
    chamber_plot[["house", "senate"]] *= 100
    chamber_plot.to_csv(args.out_dir / "chamber_replication.csv", index=False)

    ballot = panel[panel.on_ballot & panel.party.isin(PARTIES)].copy()
    homepage_rows = []
    displacement_rows = []
    for topic_col in topics:
        home_col = f"{topic_col}_home"
        label = topic_col.removeprefix(TOPIC_PREFIX)
        for (office, year), cell in ballot.groupby(["office", "year"]):
            full = cell.groupby("party")[topic_col].mean()
            home = cell.groupby("party")[home_col].mean()
            homepage_rows.append(
                {
                    "office": office,
                    "year": year,
                    "topic": label,
                    "full_gap": 100 * (full["democrat"] - full["republican"]),
                    "home_gap": 100 * (home["democrat"] - home["republican"]),
                }
            )
        for party, group in ballot.groupby("party"):
            displacement_rows.append(
                {
                    "topic": label,
                    "party": party,
                    "full_minus_home_pp": 100 * (group[topic_col] - group[home_col]).mean(),
                }
            )
    homepage = pd.DataFrame(homepage_rows).groupby("topic")[["full_gap", "home_gap"]].mean().reset_index()
    homepage.to_csv(args.out_dir / "homepage_replication.csv", index=False)
    pd.DataFrame(displacement_rows).to_csv(
        args.out_dir / "home_full_displacement.csv", index=False
    )

    scores = ballot.merge(crosswalk, on="candidate_cycle_id", validate="one_to_one")
    cf_rows = []
    for party, group in scores.dropna(subset=["cfscore"]).groupby("party"):
        group = group.copy()
        group["quintile"] = pd.qcut(
            group.cfscore.rank(method="first"), 5, labels=False
        ) + 1
        for topic in DISPLAY_TOPICS:
            col = f"{TOPIC_PREFIX}{topic}"
            for quintile, value in group.groupby("quintile")[col].mean().items():
                cf_rows.append(
                    {
                        "party": party,
                        "quintile": int(quintile),
                        "topic": topic,
                        "share": 100 * value,
                    }
                )
    pd.DataFrame(cf_rows).to_csv(args.out_dir / "cfscore_quintiles.csv", index=False)

    vote_rows = []
    for (office, year), cell in ballot.groupby(["office", "year"]):
        for topic in DISPLAY_TOPICS:
            col = f"{TOPIC_PREFIX}{topic}"
            unweighted = cell.groupby("party")[col].mean()
            weighted = cell.groupby("party").apply(
                lambda g: weighted_mean(g[col], g.general_votes),
                include_groups=False,
            )
            vote_rows.append(
                {
                    "office": office,
                    "year": year,
                    "topic": topic,
                    "unweighted_gap": 100 * (unweighted["democrat"] - unweighted["republican"]),
                    "vote_weighted_gap": 100 * (weighted["democrat"] - weighted["republican"]),
                }
            )
    pd.DataFrame(vote_rows).to_csv(
        args.out_dir / "vote_weighted_gaps.csv", index=False
    )

    counts.update(
        {
            "release_ballot_candidate_years": int(len(ballot)),
            "release_senate_ballot_2018_2024": int(
                len(recent[recent.office.eq("senate")])
            ),
            "release_house_ballot_2024": int(
                len(current_house[current_house.year.eq(2024)])
            ),
        }
    )
    (args.out_dir / "analysis_counts.json").write_text(
        json.dumps(counts, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
