#!/usr/bin/env python3
"""Build exact public-record request drafts for the 20 priority cities."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]


def _month_label(value: object) -> str:
    if pd.isna(value):
        return "month not recorded"
    return f"month {int(value):02d}"


def build_packet(
    priority: pd.DataFrame,
    inventory: pd.DataFrame,
    universe: pd.DataFrame,
) -> str:
    priority_keys = priority[["priority_rank", "geo_name", "state_abb", "unresolved_candidate_years"]].copy()
    inventory_keys = (
        inventory[["priority_rank", "city", "state", "unresolved_2000_2007"]]
        .drop_duplicates()
        .rename(columns={"city": "geo_name", "state": "state_abb", "unresolved_2000_2007": "unresolved_candidate_years"})
    )
    comparison = priority_keys.merge(
        inventory_keys,
        on=["priority_rank", "geo_name", "state_abb", "unresolved_candidate_years"],
        how="outer",
        indicator=True,
    )
    if not comparison["_merge"].eq("both").all() or len(priority_keys) != 20:
        raise ValueError("Official-source inventory does not match the 20-city priority table")

    scoped = universe.loc[
        universe["in_discovery_scope"].fillna(False)
        & universe["year"].between(2000, 2007)
    ].copy()
    lines = [
        "# Municipal historical-record request approval packet",
        "",
        "## Purpose and approval rule",
        "",
        "These drafts request native historical candidate records that can contain campaign website fields. No request can be sent until Hanno approves the exact text and the exact recipient or portal for that city.",
        "",
        "Each request asks for the source file once per election. The pipeline then matches all candidates in that contest. PDF copies remain useful, but the request gives priority to CSV, Excel, JSON, database exports, and original electronic forms.",
        "",
    ]
    for row in priority_keys.sort_values("priority_rank").itertuples(index=False):
        city_sources = inventory.loc[
            inventory["city"].eq(row.geo_name) & inventory["state"].eq(row.state_abb)
        ].sort_values("source_id")
        city_universe = scoped.loc[
            scoped["geo_name"].eq(row.geo_name) & scoped["state_abb"].eq(row.state_abb)
        ]
        contests = (
            city_universe.groupby(["year", "month"], dropna=False)
            .agg(candidate_years=("candidate_year_id", "nunique"), contests=("contest", "nunique"))
            .reset_index()
            .sort_values(["year", "month"], na_position="last")
        )
        if int(contests["candidate_years"].sum()) != int(row.unresolved_candidate_years):
            raise ValueError(f"Candidate total mismatch for {row.geo_name}, {row.state_abb}")
        scopes = "; ".join(
            f"{int(item.year)} ({_month_label(item.month)}, {int(item.candidate_years)} candidates)"
            for item in contests.itertuples(index=False)
        )
        years = ", ".join(str(int(year)) for year in contests["year"].tolist())
        lines.extend([
            f"## {int(row.priority_rank)}. {row.geo_name.title()}, {row.state_abb}",
            "",
            f"**Election scope:** {scopes}.",
            "",
            "**Recipient candidates:**",
            "",
        ])
        for source in city_sources.itertuples(index=False):
            lines.append(
                f"- `{source.source_id}`: [{source.source_type}]({source.official_url}) ({source.access_status})."
            )
        lines.extend([
            "",
            f"**Subject:** Native mayoral candidate contact records for {row.geo_name.title()}, {years}",
            "",
            "Hello,",
            "",
            f"I request the native electronic candidate contact lists, nomination or filing exports, voter-guide source data, campaign-finance registration records, and election-management extracts for the {row.geo_name.title()} mayoral elections in {years}.",
            "",
            "Please include ballot name, office sought, election date and round, campaign website, campaign email, phone, committee name or ID, treasurer, and any retained historical versions. Please provide CSV, Excel, database, JSON, or the original electronic files when available. A PDF copy is also useful when no native file remains. For PDFs, please preserve active hyperlink annotations.",
            "",
            "Please include the data dictionary and records-retention schedule. If the requested records no longer exist, please state the earliest year that remains available and whether the records were transferred to an archive.",
            "",
            "Best,",
            "Hanno Hilbig",
            "",
        ])
    lines.extend([
        "## Decision required before any request",
        "",
        "For each city, Hanno must select the exact recipient or portal and approve the final text. The current packet is a draft only. No request has been sent.",
        "",
    ])
    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--priority", type=Path, default=Path("data/municipal_discovery/screening/city_priority_2000_2007.csv"))
    parser.add_argument("--inventory", type=Path, default=Path("quality_reports/mayoral_url_discovery_2026-08-24/official_municipal_source_inventory.csv"))
    parser.add_argument("--universe", type=Path, default=Path("data/municipal_discovery/final/mayoral_candidate_universe.csv"))
    parser.add_argument("--output", type=Path, default=Path("quality_reports/mayoral_url_discovery_2026-08-24/municipal_public_record_request_packet.md"))
    args = parser.parse_args()
    rendered = build_packet(
        pd.read_csv(args.priority),
        pd.read_csv(args.inventory),
        pd.read_csv(args.universe, low_memory=False),
    )
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(rendered + "\n", encoding="utf-8")
    print({"cities": rendered.count("\n## ") - 2, "output": str(args.output)})


if __name__ == "__main__":
    main()
