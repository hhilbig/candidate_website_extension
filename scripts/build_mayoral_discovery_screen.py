#!/usr/bin/env python3
"""Build blinded mayoral URL-discovery screening and source-priority samples.

The builder uses the final candidate URL summary as the candidate-year frame and
the final mayoral universe only to add contest-level fields.  It does not make
network requests.  Candidate screening is stratified jointly on four requested
balance dimensions, with at least one draw from every observed joint stratum.
Contest size is balanced by equal-probability systematic sampling inside those
strata.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import tempfile
from pathlib import Path
from typing import Iterable

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SUMMARY = REPO_ROOT / "data/municipal_discovery/final/mayoral_candidate_url_summary.csv"
DEFAULT_UNIVERSE = REPO_ROOT / "data/municipal_discovery/final/mayoral_candidate_universe.csv"
DEFAULT_PILOT = REPO_ROOT / "data/municipal_discovery/search_pilot/query_manifest.csv"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data/municipal_discovery/screening"

DEFAULT_SEED = 20260824
ERAS = (
    (2000, 2007, "2000_2007"),
    (2008, 2013, "2008_2013"),
    (2014, 2017, "2014_2017"),
    (2018, 2021, "2018_2021"),
)
ERA_LABELS = tuple(label for _, _, label in ERAS)
BALANCE_COLUMNS = (
    "region",
    "population_quartile",
    "profile_availability",
    "stage_status",
    "contest_size_bin",
)
DESIGN_STRATA_COLUMNS = BALANCE_COLUMNS[:-1]

OUTPUT_FILENAMES = {
    "screening_sample": "candidate_screening_sample.csv",
    "contest_sample": "complete_contest_sample.csv",
    "city_priority": "city_priority_2000_2007.csv",
    "civic_provider_sample": "civic_provider_masked_sample.csv",
    "drop_audit": "screening_drop_audit.csv",
}

SUMMARY_REQUIRED = {
    "candidate_year_id",
    "candidate_contest_id",
    "ledb_candid",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "population_2020",
    "ballotpedia_url",
    "stage_scope_status",
    "election_stage_status",
    "has_validated_campaign_url",
}
UNIVERSE_REQUIRED = {
    "candidate_year_id",
    "contest",
    "place_fips",
    "month",
    "contest_n_candidates",
}
PILOT_REQUIRED = {"candidate_year_id"}

REGION_STATES = {
    "Northeast": "CT ME MA NH RI VT NJ NY PA".split(),
    "Midwest": "IN IL MI OH WI IA KS MN MO NE ND SD".split(),
    "South": "DE FL GA MD NC SC VA DC WV AL KY MS TN AR LA OK TX".split(),
    "West": "AZ CO ID MT NV NM UT WY AK CA HI OR WA".split(),
}
STATE_TO_REGION = {
    state: region for region, states in REGION_STATES.items() for state in states
}


class ScreenBuildError(ValueError):
    """The requested sample cannot be built without violating its design."""


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    columns = frame.columns.tolist()
    missing = sorted(set(required) - set(columns))
    if missing:
        raise ScreenBuildError(f"{label} is missing required columns: {missing}")


def _clean_id_series(series: pd.Series, label: str) -> pd.Series:
    values = series.astype("string").str.strip()
    invalid = values.isna() | values.eq("")
    if invalid.any():
        raise ScreenBuildError(f"{label} has {int(invalid.sum())} missing candidate_year_id values")
    return values


def _as_bool(series: pd.Series) -> pd.Series:
    true_values = {"1", "true", "t", "yes", "y"}
    false_values = {"0", "false", "f", "no", "n", "", "nan", "none", "<na>"}

    def convert(value: object) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, bool):
            return value
        normalized = str(value).strip().casefold()
        if normalized in true_values:
            return True
        if normalized in false_values:
            return False
        raise ScreenBuildError(f"Unrecognized boolean value: {value!r}")

    return series.map(convert).astype(bool)


def stable_rank(value: object, seed: int = DEFAULT_SEED, namespace: str = "screen") -> str:
    payload = f"{namespace}|{seed}|{value}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def era_for_year(year: object) -> str | None:
    if pd.isna(year):
        return None
    numeric = int(year)
    for start, end, label in ERAS:
        if start <= numeric <= end:
            return label
    return None


def _format_place_fips(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    try:
        numeric = float(text)
    except ValueError:
        return text
    if numeric.is_integer():
        return str(int(numeric))
    return text


def _contest_year_id(row: pd.Series) -> str:
    year = int(row["year"])
    place = _format_place_fips(row.get("place_fips"))
    if place:
        return f"{place}__{year}"
    city = str(row["geo_name"]).strip().casefold().replace(" ", "_")
    state = str(row["state_abb"]).strip().upper()
    return f"{state}__{city}__{year}"


def _election_date(row: pd.Series) -> str:
    exact = row.get("election_page_election_date")
    if pd.notna(exact) and str(exact).strip():
        return str(exact).strip()
    year = int(row["year"])
    month = pd.to_numeric(pd.Series([row.get("month")]), errors="coerce").iloc[0]
    if pd.notna(month) and 1 <= int(month) <= 12:
        return f"{year:04d}-{int(month):02d}"
    return f"{year:04d}"


def _election_round(row: pd.Series) -> str:
    for column in ("election_page_stage_label", "election_page_stage"):
        value = row.get(column)
        if pd.notna(value) and str(value).strip():
            return str(value).strip()
    status = row.get("election_stage_status")
    if pd.notna(status) and str(status).strip():
        normalized = str(status).strip()
        if normalized == "single_recorded_mayoral_contest_per_city_year_stage_not_explicit":
            return "unresolved"
        return normalized.removeprefix("verified_").removesuffix("_election")
    return "unresolved"


def enrich_candidate_frame(summary: pd.DataFrame, universe: pd.DataFrame) -> pd.DataFrame:
    """Validate and join the authoritative candidate-year inputs one-to-one."""
    _require_columns(summary, SUMMARY_REQUIRED, "candidate summary")
    _require_columns(universe, UNIVERSE_REQUIRED, "candidate universe")

    summary = summary.copy()
    universe = universe.copy()
    summary["candidate_year_id"] = _clean_id_series(
        summary["candidate_year_id"], "candidate summary"
    )
    universe["candidate_year_id"] = _clean_id_series(
        universe["candidate_year_id"], "candidate universe"
    )
    if summary["candidate_year_id"].duplicated().any():
        raise ScreenBuildError("candidate summary candidate_year_id must be unique")
    if universe["candidate_year_id"].duplicated().any():
        raise ScreenBuildError("candidate universe candidate_year_id must be unique")

    summary_columns = summary.columns.tolist()
    supplement_candidates = [
        "contest",
        "place_fips",
        "month",
        "contest_n_candidates",
        "election_page_election_date",
        "election_page_stage",
        "election_page_stage_label",
    ]
    supplement = [
        column for column in supplement_candidates
        if column in universe.columns and column not in summary_columns
    ]
    merged = summary.merge(
        universe[["candidate_year_id", *supplement]],
        on="candidate_year_id",
        how="left",
        validate="one_to_one",
        indicator=True,
    )
    missing_matches = merged["_merge"].ne("both")
    if missing_matches.any():
        examples = merged.loc[missing_matches, "candidate_year_id"].head(5).tolist()
        raise ScreenBuildError(
            f"{int(missing_matches.sum())} summary rows do not match the universe; examples: {examples}"
        )
    merged = merged.drop(columns="_merge")
    _require_columns(merged, UNIVERSE_REQUIRED - {"candidate_year_id"}, "joined candidate frame")

    merged["year"] = pd.to_numeric(merged["year"], errors="raise").astype(int)
    merged["contest_n_candidates"] = pd.to_numeric(
        merged["contest_n_candidates"], errors="coerce"
    )
    invalid_contest_size = (
        merged["contest_n_candidates"].isna()
        | merged["contest_n_candidates"].lt(1)
    )
    if invalid_contest_size.any():
        raise ScreenBuildError(
            f"{int(invalid_contest_size.sum())} rows have missing or invalid contest_n_candidates"
        )
    merged["contest_n_candidates"] = merged["contest_n_candidates"].astype(int)
    merged["_has_validated_url"] = _as_bool(merged["has_validated_campaign_url"])
    merged["era"] = merged["year"].map(era_for_year)
    merged["region"] = (
        merged["state_abb"].astype("string").str.upper().map(STATE_TO_REGION).fillna("Other/Unknown")
    )

    population = pd.to_numeric(merged["population_2020"], errors="coerce")
    population_rank = population.rank(method="average", pct=True)
    merged["population_quartile"] = "missing"
    merged.loc[population_rank.le(0.25), "population_quartile"] = "Q1"
    merged.loc[population_rank.gt(0.25) & population_rank.le(0.50), "population_quartile"] = "Q2"
    merged.loc[population_rank.gt(0.50) & population_rank.le(0.75), "population_quartile"] = "Q3"
    merged.loc[population_rank.gt(0.75), "population_quartile"] = "Q4"

    profile = merged["ballotpedia_url"].notna() & merged["ballotpedia_url"].astype("string").str.strip().ne("")
    merged["profile_availability"] = profile.map(
        {True: "profile", False: "no_profile"}
    )
    merged["stage_status"] = (
        merged["stage_scope_status"].astype("string").fillna("stage_unknown").replace("", "stage_unknown")
    )

    size = merged["contest_n_candidates"]
    merged["contest_size_bin"] = "5_plus"
    merged.loc[size.eq(1), "contest_size_bin"] = "1"
    merged.loc[size.eq(2), "contest_size_bin"] = "2"
    merged.loc[size.between(3, 4), "contest_size_bin"] = "3_4"

    for optional in (
        "election_page_election_date",
        "election_page_stage",
        "election_page_stage_label",
    ):
        if optional not in merged.columns:
            merged[optional] = pd.NA
    merged["contest_year_id"] = merged.apply(_contest_year_id, axis=1)
    merged["election_date"] = merged.apply(_election_date, axis=1)
    merged["election_round"] = merged.apply(_election_round, axis=1)
    merged["screening_stratum"] = (
        merged[list(DESIGN_STRATA_COLUMNS)].astype(str).agg(" | ".join, axis=1)
    )
    merged["balance_cell"] = merged[list(BALANCE_COLUMNS)].astype(str).agg(" | ".join, axis=1)
    return merged


def drop_audit_row(
    stage: str,
    before: int,
    after: int,
    *,
    unit: str = "candidate_year",
    operation_type: str = "filter",
    expected: bool,
    note: str,
) -> dict[str, object]:
    if before < 0 or after < 0 or after > before:
        raise ScreenBuildError(f"Invalid drop audit counts for {stage}: {before} -> {after}")
    dropped = before - after
    share = dropped / before if before else 0.0
    if share > 0.20 and not note.strip():
        raise ScreenBuildError(f"Large drop at {stage} requires an explicit audit note")
    return {
        "stage": stage,
        "unit": unit,
        "operation_type": operation_type,
        "before_n": int(before),
        "after_n": int(after),
        "dropped_n": int(dropped),
        "drop_share": float(share),
        "large_drop": bool(share > 0.20),
        "expected": bool(expected),
        "requires_investigation": bool(share > 0.20 and not expected),
        "note": note,
    }


def _allocation_table(
    frame: pd.DataFrame,
    quota: int,
    strata: tuple[str, ...],
    seed: int,
    era: str,
) -> pd.DataFrame:
    if quota < 1:
        raise ScreenBuildError("sample quota must be positive")
    if len(frame) < quota:
        raise ScreenBuildError(
            f"Era {era} has {len(frame)} eligible candidate-years, below quota {quota}"
        )
    sizes = (
        frame.groupby(list(strata), dropna=False, observed=True)
        .size()
        .rename("stratum_population")
        .reset_index()
    )
    sizes["screening_stratum"] = sizes[list(strata)].astype(str).agg(" | ".join, axis=1)
    sizes = sizes.sort_values("screening_stratum").reset_index(drop=True)
    if len(sizes) > quota:
        raise ScreenBuildError(
            f"Era {era} has {len(sizes)} observed balance strata but quota is {quota}; "
            "positive inclusion probabilities cannot be assigned to every stratum"
        )

    sizes["stratum_sample_n"] = 1
    remaining = quota - len(sizes)
    if remaining:
        capacity = sizes["stratum_population"] - sizes["stratum_sample_n"]
        total_capacity = int(capacity.sum())
        if total_capacity < remaining:
            raise ScreenBuildError(f"Era {era} cannot allocate its remaining {remaining} draws")
        ideal = remaining * capacity / total_capacity
        increments = ideal.map(math.floor).astype(int)
        sizes["stratum_sample_n"] += increments
        leftover = remaining - int(increments.sum())
        if leftover:
            capacity_left = sizes["stratum_population"] - sizes["stratum_sample_n"]
            choices = sizes.loc[capacity_left.gt(0), ["screening_stratum"]].copy()
            choices["remainder"] = (ideal - increments).loc[choices.index]
            choices["tie_rank"] = choices["screening_stratum"].map(
                lambda value: stable_rank(value, seed, f"allocation-{era}")
            )
            winners = choices.sort_values(
                ["remainder", "tie_rank"], ascending=[False, True]
            ).head(leftover).index
            if len(winners) != leftover:
                raise ScreenBuildError(f"Era {era} could not finish quota allocation")
            sizes.loc[winners, "stratum_sample_n"] += 1

    if int(sizes["stratum_sample_n"].sum()) != quota:
        raise ScreenBuildError(f"Era {era} allocation does not sum to quota {quota}")
    if (sizes["stratum_sample_n"] > sizes["stratum_population"]).any():
        raise ScreenBuildError(f"Era {era} allocated more rows than a stratum contains")
    return sizes


def stratified_candidate_sample(
    frame: pd.DataFrame,
    quota: int,
    *,
    seed: int = DEFAULT_SEED,
    strata: tuple[str, ...] = DESIGN_STRATA_COLUMNS,
) -> pd.DataFrame:
    """Draw a stable, positive-probability balanced sample within each era."""
    selected_parts: list[pd.DataFrame] = []
    for era in ERA_LABELS:
        era_frame = frame.loc[frame["era"].eq(era)].copy()
        allocation = _allocation_table(era_frame, quota, strata, seed, era)
        allocation_map = allocation.set_index("screening_stratum")[[
            "stratum_population", "stratum_sample_n"
        ]].to_dict("index")
        era_frame["_random_rank"] = era_frame["candidate_year_id"].map(
            lambda value: stable_rank(value, seed, f"candidate-{era}")
        )
        for stratum, group in era_frame.groupby("screening_stratum", sort=True):
            design = allocation_map[stratum]
            group = group.sort_values(
                ["contest_size_bin", "_random_rank", "candidate_year_id"]
            ).reset_index(drop=True)
            draw_n = int(design["stratum_sample_n"])
            if draw_n == len(group):
                selected_positions = list(range(len(group)))
            else:
                interval = len(group) / draw_n
                digest = stable_rank(stratum, seed, f"systematic-start-{era}")
                unit_fraction = int(digest, 16) / (1 << 256)
                start = unit_fraction * interval
                selected_positions = [
                    min(int(start + draw_index * interval), len(group) - 1)
                    for draw_index in range(draw_n)
                ]
                if len(set(selected_positions)) != draw_n:
                    raise ScreenBuildError(
                        f"Systematic draw in era {era}, stratum {stratum} produced duplicates"
                    )
            draw = group.iloc[selected_positions].copy()
            draw["stratum_population"] = int(design["stratum_population"])
            draw["stratum_sample_n"] = int(design["stratum_sample_n"])
            draw["inclusion_probability"] = (
                draw["stratum_sample_n"] / draw["stratum_population"]
            )
            draw["population_weight"] = 1.0 / draw["inclusion_probability"]
            selected_parts.append(draw)
    if not selected_parts:
        raise ScreenBuildError("No candidate screening rows were selected")
    selected = pd.concat(selected_parts, ignore_index=True)
    selected["sample_seed"] = int(seed)
    selected["sample_kind"] = "candidate_year_screen"
    selected = selected.sort_values(
        ["era", "screening_stratum", "_random_rank", "candidate_year_id"]
    ).reset_index(drop=True)
    selected["sample_order"] = selected.index + 1
    return selected.drop(columns="_random_rank")


def _balanced_unit_ids(
    frame: pd.DataFrame,
    n: int,
    *,
    id_column: str,
    balance_columns: tuple[str, ...],
    seed: int,
    namespace: str,
) -> list[str]:
    """Greedily match pool margins, with a stable hash used only for ties."""
    if len(frame) < n:
        raise ScreenBuildError(f"Only {len(frame)} units are available for a quota of {n}")
    pool = frame.copy().reset_index(drop=True)
    if pool[id_column].duplicated().any():
        raise ScreenBuildError(f"{id_column} must be unique for balanced unit selection")
    proportions = {
        column: pool[column].astype(str).value_counts(normalize=True).to_dict()
        for column in balance_columns
    }
    selected: list[int] = []
    remaining = set(pool.index.tolist())
    observed = {column: {} for column in balance_columns}
    for step in range(1, n + 1):
        best_index: int | None = None
        best_key: tuple[float, str] | None = None
        for index in remaining:
            score = 0.0
            for column in balance_columns:
                candidate_value = str(pool.at[index, column])
                categories = proportions[column]
                for value, proportion in categories.items():
                    count = observed[column].get(value, 0)
                    if value == candidate_value:
                        count += 1
                    target = step * proportion
                    score += (count - target) ** 2 / max(target, 1.0)
            tie = stable_rank(pool.at[index, id_column], seed, namespace)
            key = (score, tie)
            if best_key is None or key < best_key:
                best_key = key
                best_index = index
        assert best_index is not None
        selected.append(best_index)
        remaining.remove(best_index)
        for column in balance_columns:
            value = str(pool.at[best_index, column])
            observed[column][value] = observed[column].get(value, 0) + 1
    return pool.loc[selected, id_column].astype(str).tolist()


def select_complete_contests(
    full_frame: pd.DataFrame,
    pilot_ids: set[str],
    screening_ids: set[str],
    contests_per_era: int,
    *,
    seed: int = DEFAULT_SEED,
) -> tuple[pd.DataFrame, list[dict[str, object]]]:
    """Select disjoint, fully observed and fully unresolved city-election years."""
    supported = full_frame.loc[full_frame["era"].notna()].copy()
    contest_rows: list[dict[str, object]] = []
    for contest_year_id, group in supported.groupby("contest_year_id", sort=True):
        eras = group["era"].dropna().unique().tolist()
        if len(eras) != 1:
            raise ScreenBuildError(f"Contest-year {contest_year_id} crosses eras")
        declared_total = int(
            group.groupby("contest", dropna=False)["contest_n_candidates"].first().sum()
        )
        candidate_ids = set(group["candidate_year_id"].astype(str))
        contest_rows.append({
            "contest_year_id": str(contest_year_id),
            "era": eras[0],
            "region": str(group["region"].iloc[0]),
            "population_quartile": str(group["population_quartile"].iloc[0]),
            "stage_status": str(group["stage_status"].iloc[0]),
            "contest_size_bin": str(group["contest_size_bin"].iloc[0]),
            "observed_candidates": int(len(group)),
            "declared_candidates": declared_total,
            "is_complete": bool(len(group) == declared_total),
            "all_unresolved": bool((~group["_has_validated_url"]).all()),
            "contains_pilot": bool(candidate_ids & pilot_ids),
            "contains_screening_candidate": bool(candidate_ids & screening_ids),
        })
    contests = pd.DataFrame(contest_rows)
    audit: list[dict[str, object]] = []
    before = len(contests)
    eligible = contests.loc[contests["is_complete"] & contests["all_unresolved"]].copy()
    audit.append(drop_audit_row(
        "complete_unresolved_contests",
        before,
        len(eligible),
        unit="contest_year",
        expected=True,
        note="Contest screening requires every recorded candidate and no previously validated URL.",
    ))
    before = len(eligible)
    eligible = eligible.loc[~eligible["contains_pilot"]].copy()
    audit.append(drop_audit_row(
        "exclude_pilot_contests",
        before,
        len(eligible),
        unit="contest_year",
        expected=True,
        note="Complete-contest cases must be separate from the original pilot.",
    ))
    before = len(eligible)
    eligible = eligible.loc[~eligible["contains_screening_candidate"]].copy()
    audit.append(drop_audit_row(
        "exclude_candidate_screen_contests",
        before,
        len(eligible),
        unit="contest_year",
        expected=True,
        note="Complete contests are an additional sample disjoint from the 480 candidate-year screen.",
    ))

    chosen: list[str] = []
    contest_order: dict[str, int] = {}
    for era in ERA_LABELS:
        era_pool = eligible.loc[eligible["era"].eq(era)].copy()
        if len(era_pool) < contests_per_era:
            raise ScreenBuildError(
                f"Era {era} has {len(era_pool)} eligible complete contest-years, "
                f"below quota {contests_per_era}"
            )
        era_ids = _balanced_unit_ids(
            era_pool,
            contests_per_era,
            id_column="contest_year_id",
            balance_columns=("region", "population_quartile", "stage_status", "contest_size_bin"),
            seed=seed,
            namespace=f"complete-contest-{era}",
        )
        for within_era_order, contest_year_id in enumerate(era_ids, start=1):
            chosen.append(contest_year_id)
            contest_order[contest_year_id] = within_era_order

    result = supported.loc[supported["contest_year_id"].isin(chosen)].copy()
    result["contest_sample_order_within_era"] = result["contest_year_id"].map(contest_order)
    result["sample_seed"] = int(seed)
    result["sample_kind"] = "complete_contest_year"
    result = result.sort_values(
        ["era", "contest_sample_order_within_era", "contest", "candidate_year_id"]
    ).reset_index(drop=True)
    if set(result["candidate_year_id"].astype(str)) & screening_ids:
        raise ScreenBuildError("Complete contest sample overlaps the candidate screening sample")
    return result, audit


def build_city_priority(
    unresolved: pd.DataFrame,
    n_cities: int = 20,
) -> pd.DataFrame:
    early = unresolved.loc[unresolved["year"].between(2000, 2007)].copy()
    grouped = (
        early.groupby(["place_fips", "geo_name", "state_abb"], dropna=False)
        .agg(
            unresolved_candidate_years=("candidate_year_id", "size"),
            unresolved_candidates=("ledb_candid", "nunique"),
            unresolved_contest_years=("contest_year_id", "nunique"),
            election_years=("year", "nunique"),
            first_year=("year", "min"),
            last_year=("year", "max"),
            population_2020=("population_2020", "max"),
            profile_available_candidate_years=(
                "profile_availability", lambda values: int(values.eq("profile").sum())
            ),
        )
        .reset_index()
    )
    if len(grouped) < n_cities:
        raise ScreenBuildError(
            f"Only {len(grouped)} unresolved 2000-2007 cities are available; need {n_cities}"
        )
    grouped = grouped.sort_values(
        [
            "unresolved_candidate_years",
            "unresolved_contest_years",
            "population_2020",
            "state_abb",
            "geo_name",
        ],
        ascending=[False, False, False, True, True],
    ).head(n_cities).reset_index(drop=True)
    grouped.insert(0, "priority_rank", grouped.index + 1)
    return grouped


def _masked_id(candidate_year_id: object) -> str:
    digest = hashlib.sha256(
        f"mayoral-civic-provider-v1|{candidate_year_id}".encode("utf-8")
    ).hexdigest()[:16].upper()
    return f"MCP-{digest}"


def build_civic_provider_sample(
    screening_sample: pd.DataFrame,
    sample_n: int = 100,
    *,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    recent = screening_sample.loc[screening_sample["year"].between(2018, 2021)].copy()
    if len(recent) < sample_n:
        raise ScreenBuildError(
            f"Only {len(recent)} eligible 2018-2021 screening cases are available; need {sample_n}"
        )
    recent["_provider_rank"] = recent["candidate_year_id"].map(
        lambda value: stable_rank(value, seed, "civic-provider-sample")
    )
    selected = recent.sort_values(["_provider_rank", "candidate_year_id"]).head(sample_n).copy()
    selected["masked_candidate_id"] = selected["candidate_year_id"].map(_masked_id)
    if selected["masked_candidate_id"].duplicated().any():
        raise ScreenBuildError("Masked civic-provider IDs are not unique")
    selected["office"] = "mayor"
    result = selected[[
        "masked_candidate_id",
        "geo_name",
        "state_abb",
        "year",
        "office",
        "election_date",
        "election_round",
    ]].sort_values("masked_candidate_id").reset_index(drop=True)
    return result


def _public_screen_columns(frame: pd.DataFrame, complete_contests: bool = False) -> pd.DataFrame:
    columns = [
        "sample_order",
        "contest_sample_order_within_era",
        "sample_kind",
        "sample_seed",
        "candidate_year_id",
        "candidate_contest_id",
        "contest_year_id",
        "contest",
        "ledb_candid",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "month",
        "election_date",
        "election_round",
        "population_2020",
        "ballotpedia_url",
        "has_validated_campaign_url",
        "era",
        "region",
        "population_quartile",
        "profile_availability",
        "stage_status",
        "contest_n_candidates",
        "contest_size_bin",
        "screening_stratum",
        "balance_cell",
        "stratum_population",
        "stratum_sample_n",
        "inclusion_probability",
        "population_weight",
    ]
    available = [column for column in columns if column in frame.columns]
    return frame[available].copy()


def build_discovery_screen(
    summary: pd.DataFrame,
    universe: pd.DataFrame,
    pilot_manifest: pd.DataFrame,
    *,
    seed: int = DEFAULT_SEED,
    screen_per_era: int = 120,
    contests_per_era: int = 8,
    city_priority_n: int = 20,
    provider_sample_n: int = 100,
    expected_pilot_size: int | None = 120,
) -> dict[str, pd.DataFrame]:
    """Build all five screening outputs and enforce the requested quotas."""
    _require_columns(pilot_manifest, PILOT_REQUIRED, "pilot manifest")
    full = enrich_candidate_frame(summary, universe)
    pilot_ids_series = _clean_id_series(pilot_manifest["candidate_year_id"], "pilot manifest")
    pilot_ids = set(pilot_ids_series.tolist())
    if expected_pilot_size is not None and len(pilot_ids) != expected_pilot_size:
        raise ScreenBuildError(
            f"Pilot manifest has {len(pilot_ids)} unique candidate-years; expected {expected_pilot_size}"
        )
    missing_pilot_ids = pilot_ids - set(full["candidate_year_id"].astype(str))
    if missing_pilot_ids:
        raise ScreenBuildError(
            f"{len(missing_pilot_ids)} pilot candidate-years are absent from the final summary"
        )

    audit: list[dict[str, object]] = []
    unresolved = full.loc[~full["_has_validated_url"]].copy()
    audit.append(drop_audit_row(
        "exclude_validated_urls",
        len(full),
        len(unresolved),
        expected=True,
        note="The screen targets candidate-years without a validated campaign URL.",
    ))
    supported = unresolved.loc[unresolved["era"].notna()].copy()
    audit.append(drop_audit_row(
        "restrict_to_2000_2021_eras",
        len(unresolved),
        len(supported),
        expected=True,
        note="The approved screening eras cover elections from 2000 through 2021.",
    ))
    eligible = supported.loc[~supported["candidate_year_id"].astype(str).isin(pilot_ids)].copy()
    audit.append(drop_audit_row(
        "exclude_original_pilot_cases",
        len(supported),
        len(eligible),
        expected=True,
        note="The new screen must be independent of the original pilot's unique candidate-years.",
    ))

    screening = stratified_candidate_sample(eligible, screen_per_era, seed=seed)
    audit.append(drop_audit_row(
        "draw_candidate_screen",
        len(eligible),
        len(screening),
        operation_type="sampling",
        expected=True,
        note=(
            f"The approved design draws exactly {screen_per_era} candidate-years from each of "
            f"{len(ERA_LABELS)} eras and retains design weights."
        ),
    ))
    era_counts = screening.groupby("era", observed=True).size().to_dict()
    if any(era_counts.get(era, 0) != screen_per_era for era in ERA_LABELS):
        raise ScreenBuildError(f"Candidate screening era quotas failed: {era_counts}")
    if set(screening["candidate_year_id"].astype(str)) & pilot_ids:
        raise ScreenBuildError("Candidate screening sample overlaps the original pilot")

    screening_ids = set(screening["candidate_year_id"].astype(str))
    contests, contest_audit = select_complete_contests(
        full,
        pilot_ids,
        screening_ids,
        contests_per_era,
        seed=seed,
    )
    audit.extend(contest_audit)
    contest_counts = contests.groupby("era")["contest_year_id"].nunique().to_dict()
    if any(contest_counts.get(era, 0) != contests_per_era for era in ERA_LABELS):
        raise ScreenBuildError(f"Complete-contest era quotas failed: {contest_counts}")

    city_priority = build_city_priority(unresolved, city_priority_n)
    civic_sample = build_civic_provider_sample(
        screening, provider_sample_n, seed=seed
    )
    audit.append(drop_audit_row(
        "draw_masked_civic_provider_sample",
        int(screening["year"].between(2018, 2021).sum()),
        len(civic_sample),
        operation_type="sampling",
        expected=True,
        note="The provider outreach file is a masked subsample of the recent-era screen.",
    ))
    audit_frame = pd.DataFrame(audit)
    if audit_frame.loc[audit_frame["large_drop"], "note"].astype(str).str.strip().eq("").any():
        raise ScreenBuildError("At least one greater-than-20% drop lacks an explicit report")

    return {
        "screening_sample": _public_screen_columns(screening),
        "contest_sample": _public_screen_columns(contests, complete_contests=True),
        "city_priority": city_priority,
        "civic_provider_sample": civic_sample,
        "drop_audit": audit_frame,
    }


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".csv",
        dir=path.parent,
        delete=False,
        encoding="utf-8",
        newline="",
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    temporary.replace(path)


def write_outputs(
    outputs: dict[str, pd.DataFrame],
    output_dir: Path,
    *,
    input_paths: dict[str, Path] | None = None,
    seed: int = DEFAULT_SEED,
) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_metadata: dict[str, dict[str, object]] = {}
    for key, filename in OUTPUT_FILENAMES.items():
        path = output_dir / filename
        _write_csv_atomic(outputs[key], path)
        output_metadata[key] = {
            "path": filename,
            "rows": int(len(outputs[key])),
            "sha256": sha256_file(path),
        }
    manifest = {
        "schema_version": 1,
        "sample_seed": int(seed),
        "eras": [
            {"start": start, "end": end, "label": label} for start, end, label in ERAS
        ],
        "balance_columns": list(BALANCE_COLUMNS),
        "design_strata_columns": list(DESIGN_STRATA_COLUMNS),
        "systematic_order_columns": ["contest_size_bin"],
        "screening_method": "joint_stratification_with_equal_probability_systematic_sampling",
        "inputs": {
            key: {"path": str(path), "sha256": sha256_file(path)}
            for key, path in sorted((input_paths or {}).items())
        },
        "outputs": output_metadata,
    }
    manifest_path = output_dir / "screening_manifest.json"
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".json", dir=output_dir, delete=False, encoding="utf-8"
    ) as handle:
        temporary = Path(handle.name)
        json.dump(manifest, handle, indent=2, sort_keys=True)
        handle.write("\n")
    temporary.replace(manifest_path)
    return manifest_path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--summary", type=Path, default=DEFAULT_SUMMARY)
    parser.add_argument("--universe", type=Path, default=DEFAULT_UNIVERSE)
    parser.add_argument("--pilot-manifest", type=Path, default=DEFAULT_PILOT)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--screen-per-era", type=int, default=120)
    parser.add_argument("--contests-per-era", type=int, default=8)
    parser.add_argument("--city-priority-n", type=int, default=20)
    parser.add_argument("--provider-sample-n", type=int, default=100)
    parser.add_argument("--expected-pilot-size", type=int, default=120)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = pd.read_csv(args.summary, dtype={"candidate_year_id": "string"})
    universe = pd.read_csv(args.universe, dtype={"candidate_year_id": "string"})
    pilot = pd.read_csv(args.pilot_manifest, dtype={"candidate_year_id": "string"})
    outputs = build_discovery_screen(
        summary,
        universe,
        pilot,
        seed=args.seed,
        screen_per_era=args.screen_per_era,
        contests_per_era=args.contests_per_era,
        city_priority_n=args.city_priority_n,
        provider_sample_n=args.provider_sample_n,
        expected_pilot_size=args.expected_pilot_size,
    )
    manifest_path = write_outputs(
        outputs,
        args.output_dir,
        input_paths={
            "candidate_summary": args.summary,
            "candidate_universe": args.universe,
            "pilot_manifest": args.pilot_manifest,
        },
        seed=args.seed,
    )
    counts = {key: len(frame) for key, frame in outputs.items()}
    print(json.dumps({"manifest": str(manifest_path), "rows": counts}, sort_keys=True))


if __name__ == "__main__":
    main()
