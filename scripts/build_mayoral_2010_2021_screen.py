#!/usr/bin/env python3
"""Build the revised blinded 2010-2021 mayoral URL-discovery samples.

This is an offline sampler. It verifies the frozen current-coverage roster
against the frozen unresolved frame, excludes the approved earlier samples,
draws 160 candidate-years in each period, and then draws ten additional
complete contest-years in each period.
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
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
DEFAULT_ROSTER = DEFAULT_OUTPUT_DIR / "current_coverage_roster.csv"
DEFAULT_UNRESOLVED = DEFAULT_OUTPUT_DIR / "unresolved_2010_2021.csv"
DEFAULT_PILOT = REPO_ROOT / "data/municipal_discovery/search_pilot/query_manifest.csv"
DEFAULT_EARLIER_SCREEN = (
    REPO_ROOT / "data/municipal_discovery/screening/candidate_screening_sample.csv"
)
DEFAULT_EARLIER_CONTESTS = (
    REPO_ROOT / "data/municipal_discovery/screening/complete_contest_sample.csv"
)
DEFAULT_EARLIER_CONFIRMATION = (
    REPO_ROOT / "data/municipal_discovery/confirmation/candidate_confirmation_sample.csv"
)

DEFAULT_SEED = 20260825
PERIODS = (
    (2010, 2013, "2010_2013"),
    (2014, 2017, "2014_2017"),
    (2018, 2021, "2018_2021"),
)
PERIOD_LABELS = tuple(label for _, _, label in PERIODS)
DEFAULT_CANDIDATE_QUOTAS = {period: 160 for period in PERIOD_LABELS}
DEFAULT_CONTEST_QUOTAS = {period: 10 for period in PERIOD_LABELS}

BALANCE_COLUMNS = (
    "region",
    "population_group",
    "profile_availability",
    "stage_status",
    "contest_size_bin",
)
DESIGN_STRATA_COLUMNS = BALANCE_COLUMNS[:-1]

OUTPUT_FILENAMES = {
    "candidate_sample": "candidate_screening_sample_2010_2021.csv",
    "contest_sample": "complete_contest_sample_2010_2021.csv",
    "drop_audit": "sampling_drop_audit.csv",
    "balance_audit": "sampling_balance_audit.csv",
}

FRAME_REQUIRED = {
    "candidate_year_id",
    "candidate_contest_id",
    "contest",
    "ledb_candid",
    "full_name",
    "place_fips",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "contest_n_candidates",
    "population_2020",
    "ballotpedia_url",
    "election_stage_status",
    "covered",
    "period",
    *BALANCE_COLUMNS,
}


class RevisedScreenError(ValueError):
    """The approved revised sample cannot be built from the supplied inputs."""


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(frame.columns.tolist()))
    if missing:
        raise RevisedScreenError(f"{label} is missing required columns: {missing}")


def _clean_ids(frame: pd.DataFrame, label: str) -> pd.DataFrame:
    _require_columns(frame, {"candidate_year_id"}, label)
    result = frame.copy()
    result["candidate_year_id"] = result["candidate_year_id"].astype("string").str.strip()
    invalid = result["candidate_year_id"].isna() | result["candidate_year_id"].eq("")
    if invalid.any():
        raise RevisedScreenError(
            f"{label} has {int(invalid.sum())} missing candidate_year_id values"
        )
    return result


def _id_set(
    frame: pd.DataFrame,
    label: str,
    expected_unique: int | None = None,
) -> set[str]:
    cleaned = _clean_ids(frame, label)
    values = set(cleaned["candidate_year_id"].tolist())
    if expected_unique is not None and len(values) != expected_unique:
        raise RevisedScreenError(
            f"{label} has {len(values)} unique candidate-years; expected {expected_unique}"
        )
    return values


def _as_bool(series: pd.Series, label: str) -> pd.Series:
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
        raise RevisedScreenError(f"{label} has an unrecognized boolean value: {value!r}")

    return series.map(convert).astype(bool)


def stable_rank(value: object, seed: int, namespace: str) -> str:
    payload = f"{namespace}|{seed}|{value}".encode("utf-8")
    return hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _period_for_year(year: object) -> str | None:
    if pd.isna(year):
        return None
    value = int(year)
    for start, end, label in PERIODS:
        if start <= value <= end:
            return label
    return None


def _validate_frozen_frames(
    roster: pd.DataFrame,
    unresolved: pd.DataFrame,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return the roster, in-scope roster, and exact current unresolved frame."""
    _require_columns(roster, FRAME_REQUIRED, "current coverage roster")
    _require_columns(unresolved, FRAME_REQUIRED, "unresolved frame")
    roster = _clean_ids(roster, "current coverage roster")
    unresolved = _clean_ids(unresolved, "unresolved frame")
    if roster["candidate_year_id"].duplicated().any():
        raise RevisedScreenError("current coverage roster candidate_year_id must be unique")
    if unresolved["candidate_year_id"].duplicated().any():
        raise RevisedScreenError("unresolved frame candidate_year_id must be unique")

    for label, frame in (("current coverage roster", roster), ("unresolved frame", unresolved)):
        frame["year"] = pd.to_numeric(frame["year"], errors="raise").astype(int)
        expected_period = frame["year"].map(_period_for_year)
        recorded_period = frame["period"].astype("string")
        wrong_period = expected_period.notna() & recorded_period.ne(expected_period)
        if wrong_period.any():
            raise RevisedScreenError(
                f"{label} has {int(wrong_period.sum())} incorrect period labels"
            )
        missing_balance = frame[list(BALANCE_COLUMNS)].isna().any(axis=1)
        if missing_balance.any():
            raise RevisedScreenError(
                f"{label} has {int(missing_balance.sum())} rows missing balance fields"
            )
        invalid_size = pd.to_numeric(frame["contest_n_candidates"], errors="coerce")
        if invalid_size.isna().any() or invalid_size.lt(1).any():
            raise RevisedScreenError(f"{label} has missing or invalid contest sizes")
        frame["contest_n_candidates"] = invalid_size.astype(int)
        frame["covered"] = _as_bool(frame["covered"], f"{label}.covered")

    in_scope = roster.loc[roster["period"].astype("string").isin(PERIOD_LABELS)].copy()
    current_unresolved = in_scope.loc[~in_scope["covered"]].copy()
    expected_keys = set(current_unresolved["candidate_year_id"])
    observed_keys = set(unresolved["candidate_year_id"])
    if expected_keys != observed_keys or len(unresolved) != len(observed_keys):
        raise RevisedScreenError(
            "Frozen unresolved/coverage-roster key mismatch: "
            f"missing={len(expected_keys - observed_keys)}, "
            f"unexpected={len(observed_keys - expected_keys)}, "
            f"duplicate_rows={len(unresolved) - len(observed_keys)}"
        )
    if unresolved["covered"].any():
        raise RevisedScreenError("The frozen unresolved frame contains covered cases")
    unresolved = unresolved.sort_values("candidate_year_id", kind="mergesort").reset_index(drop=True)
    return roster, in_scope, unresolved


def _audit_transition(
    audit: list[dict[str, object]],
    stage: str,
    before: pd.DataFrame,
    after: pd.DataFrame,
    *,
    primary_id: str,
    primary_unit: str,
    operation_type: str,
    expected: bool,
    note: str,
) -> None:
    """Record each transition for its design unit and for unique cities."""
    for unit, id_column in ((primary_unit, primary_id), ("city", "place_fips")):
        before_n = int(before[id_column].nunique(dropna=False))
        after_n = int(after[id_column].nunique(dropna=False))
        if after_n > before_n:
            raise RevisedScreenError(f"Invalid audit transition {stage}: {before_n} -> {after_n}")
        dropped = before_n - after_n
        share = dropped / before_n if before_n else 0.0
        if share > 0.20 and not note.strip():
            raise RevisedScreenError(
                f"Large {unit} drop at {stage} requires an explicit report"
            )
        audit.append({
            "stage": stage,
            "unit": unit,
            "operation_type": operation_type,
            "before_n": before_n,
            "after_n": after_n,
            "dropped_n": dropped,
            "drop_share": float(share),
            "large_drop": bool(share > 0.20),
            "expected": bool(expected),
            "requires_investigation": bool(share > 0.20 and not expected),
            "note": note,
        })


def _allocation_table(
    frame: pd.DataFrame,
    quota: int,
    period: str,
    seed: int,
) -> pd.DataFrame:
    if len(frame) < quota:
        raise RevisedScreenError(
            f"Period {period} has {len(frame)} eligible candidate-years, below quota {quota}"
        )
    sizes = (
        frame.groupby(list(DESIGN_STRATA_COLUMNS), dropna=False, observed=True)
        .size()
        .rename("stratum_population")
        .reset_index()
    )
    sizes["design_stratum"] = sizes[list(DESIGN_STRATA_COLUMNS)].astype(str).agg(
        " | ".join, axis=1
    )
    sizes = sizes.sort_values("design_stratum", kind="mergesort").reset_index(drop=True)
    if len(sizes) > quota:
        raise RevisedScreenError(
            f"Period {period} has {len(sizes)} design strata, above quota {quota}"
        )
    sizes["stratum_sample_n"] = 1
    remaining = quota - len(sizes)
    capacity = sizes["stratum_population"] - 1
    if remaining:
        if int(capacity.sum()) < remaining:
            raise RevisedScreenError(f"Period {period} cannot allocate {quota} draws")
        ideal = remaining * capacity / capacity.sum()
        increments = ideal.map(math.floor).astype(int)
        sizes["stratum_sample_n"] += increments
        leftover = remaining - int(increments.sum())
        if leftover:
            choices = sizes.loc[
                sizes["stratum_sample_n"].lt(sizes["stratum_population"])
            ].copy()
            choices["remainder"] = (ideal - increments).loc[choices.index]
            choices["tie_rank"] = choices["design_stratum"].map(
                lambda value: stable_rank(value, seed, f"allocation-{period}")
            )
            winners = choices.sort_values(
                ["remainder", "tie_rank"], ascending=[False, True], kind="mergesort"
            ).head(leftover).index
            if len(winners) != leftover:
                raise RevisedScreenError(f"Period {period} could not finish allocation")
            sizes.loc[winners, "stratum_sample_n"] += 1
    if int(sizes["stratum_sample_n"].sum()) != quota:
        raise RevisedScreenError(f"Period {period} allocation does not sum to {quota}")
    if sizes["stratum_sample_n"].gt(sizes["stratum_population"]).any():
        raise RevisedScreenError(f"Period {period} allocation exceeds a stratum population")
    return sizes


def sample_candidate_years(
    eligible: pd.DataFrame,
    quotas: dict[str, int],
    *,
    seed: int,
) -> pd.DataFrame:
    if set(quotas) != set(PERIOD_LABELS):
        raise RevisedScreenError("Candidate quotas must name all three periods")
    selected_parts: list[pd.DataFrame] = []
    working = eligible.copy()
    working["design_stratum"] = working[list(DESIGN_STRATA_COLUMNS)].astype(str).agg(
        " | ".join, axis=1
    )
    working["balance_cell"] = working[list(BALANCE_COLUMNS)].astype(str).agg(
        " | ".join, axis=1
    )
    for period in PERIOD_LABELS:
        quota = quotas[period]
        period_frame = working.loc[working["period"].eq(period)].copy()
        allocation = _allocation_table(period_frame, quota, period, seed)
        allocation_map = allocation.set_index("design_stratum")[[
            "stratum_population", "stratum_sample_n",
        ]].to_dict("index")
        period_frame["_selection_rank"] = period_frame["candidate_year_id"].map(
            lambda value: stable_rank(value, seed, f"candidate-{period}")
        )
        for stratum, group in period_frame.groupby("design_stratum", sort=True):
            design = allocation_map[stratum]
            group = group.sort_values(
                ["contest_size_bin", "_selection_rank", "candidate_year_id"],
                kind="mergesort",
            ).reset_index(drop=True)
            draw_n = int(design["stratum_sample_n"])
            if draw_n == len(group):
                positions = list(range(len(group)))
            else:
                interval = len(group) / draw_n
                digest = stable_rank(stratum, seed, f"systematic-start-{period}")
                start = (int(digest, 16) / (1 << 256)) * interval
                positions = [
                    min(int(start + draw_index * interval), len(group) - 1)
                    for draw_index in range(draw_n)
                ]
                if len(set(positions)) != draw_n:
                    raise RevisedScreenError(
                        f"Systematic draw in {period}, stratum {stratum} produced duplicates"
                    )
            draw = group.iloc[positions].copy()
            draw["stratum_population"] = int(design["stratum_population"])
            draw["stratum_sample_n"] = int(design["stratum_sample_n"])
            draw["inclusion_probability"] = (
                draw["stratum_sample_n"] / draw["stratum_population"]
            )
            draw["population_weight"] = 1.0 / draw["inclusion_probability"]
            selected_parts.append(draw)

    sample = pd.concat(selected_parts, ignore_index=True)
    sample["sample_kind"] = "candidate_year_screen_2010_2021"
    sample["sample_seed"] = int(seed)
    sample = sample.sort_values(
        ["period", "design_stratum", "_selection_rank", "candidate_year_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    sample.insert(0, "sample_order", sample.index + 1)
    observed = sample.groupby("period", observed=True).size().to_dict()
    if observed != quotas:
        raise RevisedScreenError(
            f"Candidate period quotas failed: observed={observed}, expected={quotas}"
        )
    if sample["candidate_year_id"].duplicated().any():
        raise RevisedScreenError("Candidate sample contains duplicate keys")
    return sample.drop(columns="_selection_rank")


def _contest_frame(unresolved: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for contest, group in unresolved.groupby("contest", sort=True):
        periods = group["period"].dropna().astype(str).unique().tolist()
        if len(periods) != 1:
            raise RevisedScreenError(f"Contest {contest} crosses acquisition periods")
        for column in ("place_fips", "region", "population_group", "stage_status", "contest_size_bin"):
            if group[column].nunique(dropna=False) != 1:
                raise RevisedScreenError(f"Contest {contest} has inconsistent {column}")
        declared_values = group["contest_n_candidates"].unique().tolist()
        if len(declared_values) != 1:
            raise RevisedScreenError(f"Contest {contest} has inconsistent candidate totals")
        candidate_ids = tuple(sorted(group["candidate_year_id"].astype(str)))
        declared = int(declared_values[0])
        rows.append({
            "contest": str(contest),
            "contest_year_id": str(contest),
            "period": periods[0],
            "place_fips": group["place_fips"].iloc[0],
            "geo_name": str(group["geo_name"].iloc[0]),
            "state_abb": str(group["state_abb"].iloc[0]),
            "year": int(group["year"].iloc[0]),
            "region": str(group["region"].iloc[0]),
            "population_group": str(group["population_group"].iloc[0]),
            "profile_availability": (
                "profile" if group["profile_availability"].eq("profile").any() else "no_profile"
            ),
            "stage_status": str(group["stage_status"].iloc[0]),
            "contest_size_bin": str(group["contest_size_bin"].iloc[0]),
            "observed_candidate_years": int(len(candidate_ids)),
            "declared_candidate_years": declared,
            "is_complete": bool(len(candidate_ids) == declared),
            "candidate_ids": candidate_ids,
        })
    result = pd.DataFrame(rows)
    if result["contest"].duplicated().any():
        raise RevisedScreenError("Contest frame contains duplicate contest keys")
    return result


def sample_complete_contests(
    eligible: pd.DataFrame,
    quotas: dict[str, int],
    *,
    seed: int,
) -> pd.DataFrame:
    if set(quotas) != set(PERIOD_LABELS):
        raise RevisedScreenError("Contest quotas must name all three periods")
    selected_parts: list[pd.DataFrame] = []
    for period in PERIOD_LABELS:
        pool = eligible.loc[eligible["period"].eq(period)].copy()
        draw_n = quotas[period]
        if len(pool) < draw_n:
            raise RevisedScreenError(
                f"Period {period} has {len(pool)} eligible complete contests, below quota {draw_n}"
            )
        pool["_selection_rank"] = pool["contest"].map(
            lambda value: stable_rank(value, seed, f"complete-contest-{period}")
        )
        pool = pool.sort_values(
            [*BALANCE_COLUMNS, "_selection_rank", "contest"], kind="mergesort"
        ).reset_index(drop=True)
        interval = len(pool) / draw_n
        digest = stable_rank(period, seed, "complete-contest-systematic-start")
        start = (int(digest, 16) / (1 << 256)) * interval
        positions = [
            min(int(start + draw_index * interval), len(pool) - 1)
            for draw_index in range(draw_n)
        ]
        if len(set(positions)) != draw_n:
            raise RevisedScreenError(f"Complete-contest draw in {period} produced duplicates")
        draw = pool.iloc[positions].copy()
        draw["period_contest_population"] = int(len(pool))
        draw["period_contest_sample_n"] = int(draw_n)
        draw["inclusion_probability"] = draw_n / len(pool)
        draw["population_weight"] = len(pool) / draw_n
        draw["contest_sample_order_within_period"] = range(1, draw_n + 1)
        selected_parts.append(draw)
    selected = pd.concat(selected_parts, ignore_index=True)
    selected["sample_kind"] = "complete_contest_year_2010_2021"
    selected["sample_seed"] = int(seed)
    selected = selected.sort_values(
        ["period", "contest_sample_order_within_period", "contest"], kind="mergesort"
    ).reset_index(drop=True)
    observed = selected.groupby("period", observed=True)["contest"].nunique().to_dict()
    if observed != quotas:
        raise RevisedScreenError(
            f"Complete-contest period quotas failed: observed={observed}, expected={quotas}"
        )
    return selected.drop(columns="_selection_rank")


def _balance_audit(
    population: pd.DataFrame,
    sample: pd.DataFrame,
    *,
    sample_kind: str,
    weight_column: str,
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for period in PERIOD_LABELS:
        period_population = population.loc[population["period"].eq(period)]
        period_sample = sample.loc[sample["period"].eq(period)]
        for dimension in BALANCE_COLUMNS:
            population_counts = period_population[dimension].astype(str).value_counts()
            sample_counts = period_sample[dimension].astype(str).value_counts()
            weighted_counts = period_sample.assign(
                _value=period_sample[dimension].astype(str)
            ).groupby("_value", observed=True)[weight_column].sum()
            values = sorted(set(population_counts.index) | set(sample_counts.index))
            for value in values:
                population_n = int(population_counts.get(value, 0))
                sample_n = int(sample_counts.get(value, 0))
                weighted_n = float(weighted_counts.get(value, 0.0))
                population_share = population_n / len(period_population)
                sample_share = sample_n / len(period_sample)
                weighted_share = weighted_n / float(period_sample[weight_column].sum())
                rows.append({
                    "sample_kind": sample_kind,
                    "period": period,
                    "dimension": dimension,
                    "value": value,
                    "population_n": population_n,
                    "sample_n": sample_n,
                    "population_share": population_share,
                    "sample_share": sample_share,
                    "weighted_population_n": weighted_n,
                    "weighted_share": weighted_share,
                    "absolute_unweighted_share_difference": abs(sample_share - population_share),
                    "absolute_weighted_share_difference": abs(weighted_share - population_share),
                })
    return pd.DataFrame(rows)


def _public_candidate_sample(sample: pd.DataFrame) -> pd.DataFrame:
    columns = [
        "sample_order", "sample_kind", "sample_seed", "candidate_year_id",
        "candidate_contest_id", "contest", "ledb_candid", "full_name",
        "place_fips", "geo_name", "state_abb", "year", "month",
        "election_stage_status", "population_2020", "ballotpedia_url",
        "covered", "period", *BALANCE_COLUMNS, "design_stratum", "balance_cell",
        "stratum_population", "stratum_sample_n", "inclusion_probability",
        "population_weight",
    ]
    return sample[columns].copy()


def _public_contest_sample(
    unresolved: pd.DataFrame,
    selected_contests: pd.DataFrame,
) -> pd.DataFrame:
    design_columns = [
        "contest", "contest_year_id", "contest_sample_order_within_period",
        "period_contest_population", "period_contest_sample_n",
        "inclusion_probability", "population_weight", "sample_kind", "sample_seed",
    ]
    result = unresolved.merge(
        selected_contests[design_columns],
        on="contest",
        how="inner",
        validate="many_to_one",
        suffixes=("", "_design"),
    )
    if "period_design" in result.columns:
        result = result.drop(columns="period_design")
    result = result.sort_values(
        ["period", "contest_sample_order_within_period", "contest", "candidate_year_id"],
        kind="mergesort",
    ).reset_index(drop=True)
    result.insert(0, "sample_order", result.index + 1)
    columns = [
        "sample_order", "contest_sample_order_within_period", "sample_kind",
        "sample_seed", "candidate_year_id", "candidate_contest_id", "contest_year_id",
        "contest", "ledb_candid", "full_name", "place_fips", "geo_name", "state_abb",
        "year", "month", "election_stage_status", "population_2020", "ballotpedia_url",
        "covered", "period", *BALANCE_COLUMNS, "contest_n_candidates",
        "period_contest_population", "period_contest_sample_n", "inclusion_probability",
        "population_weight",
    ]
    return result[columns].copy()


def build_revised_screen(
    roster: pd.DataFrame,
    unresolved: pd.DataFrame,
    pilot_manifest: pd.DataFrame,
    earlier_screen: pd.DataFrame,
    earlier_contests: pd.DataFrame,
    earlier_confirmation: pd.DataFrame,
    *,
    seed: int = DEFAULT_SEED,
    candidate_quotas: dict[str, int] | None = None,
    contest_quotas: dict[str, int] | None = None,
    expected_pilot_size: int | None = 120,
    expected_screen_size: int | None = 480,
    expected_earlier_contests: int | None = 32,
    expected_confirmation_size: int | None = 800,
) -> dict[str, pd.DataFrame]:
    """Build both revised samples plus explicit drop and balance audits."""
    candidate_quotas = dict(DEFAULT_CANDIDATE_QUOTAS if candidate_quotas is None else candidate_quotas)
    contest_quotas = dict(DEFAULT_CONTEST_QUOTAS if contest_quotas is None else contest_quotas)
    roster, in_scope, unresolved = _validate_frozen_frames(roster, unresolved)

    pilot_ids = _id_set(pilot_manifest, "search pilot", expected_pilot_size)
    earlier_screen_ids = _id_set(earlier_screen, "earlier candidate screen", expected_screen_size)
    earlier_contest_ids = _id_set(earlier_contests, "earlier complete contests")
    if earlier_contests["candidate_year_id"].duplicated().any():
        raise RevisedScreenError("earlier complete contests have duplicate candidate rows")
    _require_columns(earlier_contests, {"contest"}, "earlier complete contests")
    observed_earlier_contests = int(earlier_contests["contest"].nunique())
    if expected_earlier_contests is not None and observed_earlier_contests != expected_earlier_contests:
        raise RevisedScreenError(
            f"earlier complete contests contain {observed_earlier_contests} contests; "
            f"expected {expected_earlier_contests}"
        )
    confirmation_ids = _id_set(
        earlier_confirmation, "earlier confirmation sample", expected_confirmation_size
    )

    explicit_candidate_exclusions = pilot_ids | earlier_screen_ids | earlier_contest_ids
    all_prior_ids = explicit_candidate_exclusions | confirmation_ids
    audit: list[dict[str, object]] = []
    _audit_transition(
        audit, "restrict_to_2010_2021", roster, in_scope,
        primary_id="candidate_year_id", primary_unit="candidate_year",
        operation_type="filter", expected=True,
        note="The approved acquisition screen covers 2010 through 2021; the full roster remains unchanged.",
    )
    _audit_transition(
        audit, "exclude_currently_covered_cases", in_scope, unresolved,
        primary_id="candidate_year_id", primary_unit="candidate_year",
        operation_type="filter", expected=True,
        note="Only unresolved candidate-years belong to the current acquisition frame.",
    )

    candidate_eligible = unresolved
    candidate_exclusion_steps = (
        ("exclude_search_pilot", pilot_ids, "The revised screen excludes the 120-case search pilot."),
        ("exclude_earlier_candidate_screen", earlier_screen_ids, "The revised screen excludes the earlier 480-case source screen."),
        ("exclude_earlier_complete_contests", earlier_contest_ids, "The revised screen excludes every candidate in the earlier complete-contest sample."),
    )
    for stage, excluded_ids, note in candidate_exclusion_steps:
        before = candidate_eligible
        candidate_eligible = before.loc[
            ~before["candidate_year_id"].astype(str).isin(excluded_ids)
        ].copy()
        _audit_transition(
            audit, stage, before, candidate_eligible,
            primary_id="candidate_year_id", primary_unit="candidate_year",
            operation_type="filter", expected=True, note=note,
        )

    _audit_transition(
        audit,
        "retain_earlier_confirmation_cases_for_candidate_screen",
        candidate_eligible,
        candidate_eligible,
        primary_id="candidate_year_id",
        primary_unit="candidate_year",
        operation_type="scope_decision",
        expected=True,
        note=(
            "The approved 480-case exclusion list does not include the earlier "
            "800-case confirmation sample; those IDs are excluded only from the "
            "new complete-contest sample."
        ),
    )

    candidate_sample = sample_candidate_years(
        candidate_eligible, candidate_quotas, seed=seed
    )
    _audit_transition(
        audit, "draw_revised_candidate_screen", candidate_eligible, candidate_sample,
        primary_id="candidate_year_id", primary_unit="candidate_year",
        operation_type="sampling", expected=True,
        note="The approved design draws 160 candidate-years from each of three periods and retains design weights.",
    )
    candidate_sample_ids = set(candidate_sample["candidate_year_id"].astype(str))
    if candidate_sample_ids & explicit_candidate_exclusions:
        raise RevisedScreenError("Revised candidate screen overlaps an explicitly excluded sample")

    contests = _contest_frame(unresolved)
    complete_contests = contests.loc[contests["is_complete"]].copy()
    _audit_transition(
        audit, "require_complete_unresolved_contests", contests, complete_contests,
        primary_id="contest", primary_unit="contest_year", operation_type="filter",
        expected=True,
        note="A contest is eligible only when every declared candidate remains in the unresolved frame.",
    )
    prior_free = complete_contests.loc[
        ~complete_contests["candidate_ids"].map(lambda ids: bool(set(ids) & all_prior_ids))
    ].copy()
    _audit_transition(
        audit, "exclude_all_prior_sample_contests", complete_contests, prior_free,
        primary_id="contest", primary_unit="contest_year", operation_type="filter",
        expected=True,
        note="The 30 complete contests must be disjoint from the pilot, earlier screens, and earlier confirmation sample.",
    )
    contest_eligible = prior_free.loc[
        ~prior_free["candidate_ids"].map(lambda ids: bool(set(ids) & candidate_sample_ids))
    ].copy()
    _audit_transition(
        audit, "exclude_revised_candidate_screen_contests", prior_free, contest_eligible,
        primary_id="contest", primary_unit="contest_year", operation_type="filter",
        expected=True,
        note="Complete contests are an additional sample disjoint from the revised 480-case candidate screen.",
    )
    selected_contests = sample_complete_contests(
        contest_eligible, contest_quotas, seed=seed
    )
    _audit_transition(
        audit, "draw_complete_contest_screen", contest_eligible, selected_contests,
        primary_id="contest", primary_unit="contest_year", operation_type="sampling",
        expected=True,
        note="The approved design draws ten complete contest-years from each of three periods with equal period-specific probabilities.",
    )
    contest_sample = _public_contest_sample(unresolved, selected_contests)
    contest_sample_ids = set(contest_sample["candidate_year_id"].astype(str))
    if contest_sample_ids & (all_prior_ids | candidate_sample_ids):
        raise RevisedScreenError("Complete-contest sample overlaps an earlier or revised sample")
    if contest_sample["covered"].any():
        raise RevisedScreenError("Complete-contest sample contains covered candidate-years")
    completeness = contest_sample.groupby("contest", observed=True).size()
    declared = contest_sample.groupby("contest", observed=True)["contest_n_candidates"].first()
    if not completeness.eq(declared).all():
        raise RevisedScreenError("Complete-contest output does not retain every declared candidate")

    candidate_balance = _balance_audit(
        candidate_eligible,
        candidate_sample,
        sample_kind="candidate_year_screen_2010_2021",
        weight_column="population_weight",
    )
    contest_balance = _balance_audit(
        contest_eligible,
        selected_contests,
        sample_kind="complete_contest_year_2010_2021",
        weight_column="population_weight",
    )
    audit_frame = pd.DataFrame(audit)
    unreported = audit_frame.loc[
        audit_frame["large_drop"] & audit_frame["note"].astype(str).str.strip().eq("")
    ]
    if not unreported.empty:
        raise RevisedScreenError("At least one greater-than-20% drop lacks an explicit report")
    if audit_frame["requires_investigation"].any():
        raise RevisedScreenError("At least one large drop is marked unexpected")

    return {
        "candidate_sample": _public_candidate_sample(candidate_sample),
        "contest_sample": contest_sample,
        "drop_audit": audit_frame,
        "balance_audit": pd.concat(
            [candidate_balance, contest_balance], ignore_index=True
        ),
    }


def _write_csv_atomic(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False,
        encoding="utf-8", newline="",
    ) as handle:
        temporary = Path(handle.name)
        frame.to_csv(handle, index=False)
    temporary.replace(path)


def write_outputs(
    outputs: dict[str, pd.DataFrame],
    output_dir: Path,
    *,
    input_paths: dict[str, Path],
    seed: int,
    candidate_quotas: dict[str, int] | None = None,
    contest_quotas: dict[str, int] | None = None,
) -> Path:
    """Write immutable-schema CSVs and a byte-level deterministic manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)
    metadata: dict[str, dict[str, object]] = {}
    for key, filename in OUTPUT_FILENAMES.items():
        path = output_dir / filename
        _write_csv_atomic(outputs[key], path)
        metadata[key] = {
            "path": filename,
            "rows": int(len(outputs[key])),
            "bytes": int(path.stat().st_size),
            "sha256": sha256_file(path),
        }
    manifest = {
        "schema_version": "mayoral-2010-2021-screen-v1",
        "sample_seed": int(seed),
        "periods": [
            {"start": start, "end": end, "label": label}
            for start, end, label in PERIODS
        ],
        "candidate_quotas": dict(
            DEFAULT_CANDIDATE_QUOTAS if candidate_quotas is None else candidate_quotas
        ),
        "contest_quotas": dict(
            DEFAULT_CONTEST_QUOTAS if contest_quotas is None else contest_quotas
        ),
        "balance_columns": list(BALANCE_COLUMNS),
        "candidate_design_strata": list(DESIGN_STRATA_COLUMNS),
        "candidate_systematic_order": ["contest_size_bin"],
        "candidate_excludes_earlier_confirmation": False,
        "complete_contests_exclude_earlier_confirmation": True,
        "inputs": {
            key: {
                "path": str(path),
                "bytes": int(path.stat().st_size),
                "sha256": sha256_file(path),
            }
            for key, path in sorted(input_paths.items())
        },
        "outputs": metadata,
    }
    manifest_path = output_dir / "sampling_manifest.json"
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
    parser.add_argument("--coverage-roster", type=Path, default=DEFAULT_ROSTER)
    parser.add_argument("--unresolved-frame", type=Path, default=DEFAULT_UNRESOLVED)
    parser.add_argument("--pilot-manifest", type=Path, default=DEFAULT_PILOT)
    parser.add_argument("--earlier-screen", type=Path, default=DEFAULT_EARLIER_SCREEN)
    parser.add_argument("--earlier-contests", type=Path, default=DEFAULT_EARLIER_CONTESTS)
    parser.add_argument(
        "--earlier-confirmation", type=Path, default=DEFAULT_EARLIER_CONFIRMATION
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--expected-pilot-size", type=int, default=120)
    parser.add_argument("--expected-screen-size", type=int, default=480)
    parser.add_argument("--expected-earlier-contests", type=int, default=32)
    parser.add_argument("--expected-confirmation-size", type=int, default=800)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    paths = {
        "coverage_roster": args.coverage_roster,
        "unresolved_frame": args.unresolved_frame,
        "pilot_manifest": args.pilot_manifest,
        "earlier_screen": args.earlier_screen,
        "earlier_contests": args.earlier_contests,
        "earlier_confirmation": args.earlier_confirmation,
    }
    frames = {
        key: pd.read_csv(path, low_memory=False, dtype={"candidate_year_id": "string"})
        for key, path in paths.items()
    }
    outputs = build_revised_screen(
        frames["coverage_roster"],
        frames["unresolved_frame"],
        frames["pilot_manifest"],
        frames["earlier_screen"],
        frames["earlier_contests"],
        frames["earlier_confirmation"],
        seed=args.seed,
        expected_pilot_size=args.expected_pilot_size,
        expected_screen_size=args.expected_screen_size,
        expected_earlier_contests=args.expected_earlier_contests,
        expected_confirmation_size=args.expected_confirmation_size,
    )
    manifest_path = write_outputs(
        outputs, args.output_dir, input_paths=paths, seed=args.seed
    )
    print(json.dumps({
        "candidate_rows": len(outputs["candidate_sample"]),
        "complete_contest_rows": len(outputs["contest_sample"]),
        "complete_contests": int(outputs["contest_sample"]["contest"].nunique()),
        "manifest": str(manifest_path),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
