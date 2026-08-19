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
SITE_PAGE_TYPES = ["issues", "biography", "news", "action", "endorsements"]
MARGIN_BREAKS = [-0.001, 10, 20, 30, 40, 60, 100]
MARGIN_LABELS = ["0–10", "10–20", "20–30", "30–40", "40–60", "60–100"]


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


def website_length_by_party(panel: pd.DataFrame, icpsr_dir: Path) -> pd.DataFrame:
    """Build a comparable House series of median website length by party."""
    old = pd.read_csv(
        icpsr_dir / "candidates_complexity.csv", low_memory=False
    )
    old = old[
        old.stage.eq(2)
        & old.data_source.eq("general_wayback")
        & old.party.isin(PARTIES)
    ]
    old_year = (
        old.groupby(["year", "party"])
        .agg(
            median_words=("n_words", "median"),
            n=("n_words", "count"),
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
        new.groupby(["year", "party"])
        .agg(
            median_words=("icpsr_n_words", "median"),
            n=("icpsr_n_words", "count"),
        )
        .reset_index()
    )
    new_year["source"] = "This release"

    out = pd.concat([old_year, new_year], ignore_index=True)
    assert out.n.min() >= 100, "headline House year cell below 100"
    assert out.median_words.notna().all(), "missing annual median"
    return out.sort_values(["party", "year"])


def party_gap(frame: pd.DataFrame, value: str = "share") -> pd.DataFrame:
    wide = frame.pivot(
        index=[c for c in frame.columns if c not in {"party", value, "n"}],
        columns="party",
        values=value,
    ).reset_index()
    wide["gap"] = wide["democrat"] - wide["republican"]
    return wide


def site_development_by_race_margin(
    roster: pd.DataFrame, selected_panel: pd.DataFrame
) -> tuple[pd.DataFrame, dict[str, int]]:
    """Summarize captured House-site structure by two-party vote margin."""
    race_key = ["state", "district", "year"]
    eligible = roster[
        roster.office.eq("house")
        & roster.year.isin([2018, 2020, 2022, 2024])
        & roster.party.isin(["D", "R"])
        & roster.on_ballot
    ].copy()
    races = (
        eligible.groupby(race_key)
        .agg(
            n_candidates=("candidate_cycle_id", "size"),
            n_parties=("party", "nunique"),
            n_votes=("general_votes", "count"),
            max_votes=("general_votes", "max"),
            min_votes=("general_votes", "min"),
            total_votes=("general_votes", "sum"),
        )
        .reset_index()
    )
    races = races[
        races.n_candidates.eq(2)
        & races.n_parties.eq(2)
        & races.n_votes.eq(2)
        & races.total_votes.gt(0)
    ].copy()
    races["race_margin"] = (
        100 * (races.max_votes - races.min_votes) / races.total_votes
    )
    races["margin_bin"] = pd.cut(
        races.race_margin,
        MARGIN_BREAKS,
        labels=MARGIN_LABELS,
        include_lowest=True,
    )

    sites = eligible[eligible.captured].merge(
        races[race_key + ["margin_bin"]],
        on=race_key,
        how="inner",
        validate="many_to_one",
    )
    sites = sites.merge(
        selected_panel[["candidate_cycle_id", "page_types"]],
        on="candidate_cycle_id",
        how="inner",
        validate="one_to_one",
    )
    assert sites.page_types.notna().all(), "missing page types in figure sample"
    split_types = sites.page_types.str.split(";")
    for page_type in SITE_PAGE_TYPES:
        sites[page_type] = split_types.map(lambda values: page_type in values)
    sites["developed_site"] = sites[SITE_PAGE_TYPES].sum(axis=1).ge(3)

    summary = (
        sites.groupby("margin_bin", observed=False)
        .agg(
            captured_candidates=("candidate_cycle_id", "size"),
            share_developed=("developed_site", "mean"),
        )
        .reset_index()
    )
    summary["share_developed"] *= 100
    assert summary.captured_candidates.gt(0).all()
    counts = {
        "eligible_two_party_house_races": int(len(races)),
        "eligible_two_party_house_candidates": int(2 * len(races)),
        "captured_candidates_in_eligible_races": int(len(sites)),
    }
    return summary, counts


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
    roster = pd.read_csv(args.release_dir / "release_roster.csv", low_memory=False)
    selected_panel = pd.read_csv(
        args.release_dir / "panel_candidate_year.csv",
        usecols=["candidate_cycle_id", "page_types"],
    )
    topics = topic_columns(panel)
    assert len(topics) == 31
    sums = panel[topics].sum(axis=1)
    assert np.allclose(sums, 1, atol=1e-8), "topic shares do not sum to one"

    combined, counts = combined_house_topics(panel, args.icpsr_dir)
    lengths = website_length_by_party(panel, args.icpsr_dir)
    lengths.to_csv(args.out_dir / "website_length_by_party.csv", index=False)
    site_development, site_counts = site_development_by_race_margin(
        roster, selected_panel
    )
    site_development.to_csv(
        args.out_dir / "site_development_by_race_margin.csv", index=False
    )
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
    counts.update(site_counts)
    (args.out_dir / "analysis_counts.json").write_text(
        json.dumps(counts, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(counts, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
