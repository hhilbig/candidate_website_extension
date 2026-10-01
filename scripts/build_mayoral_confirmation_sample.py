#!/usr/bin/env python3
"""Build the independent 800-case mayoral discovery confirmation sample.

The sample excludes the original pilot, the 480-case source screen, and the
separate complete-contest screen. It draws 320 cases from 2000 through 2007
and 160 cases from each later era.
"""

from __future__ import annotations

import argparse
import json
import tempfile
from pathlib import Path
from typing import Iterable

import pandas as pd

from scripts.build_mayoral_discovery_screen import (
    DEFAULT_PILOT,
    DEFAULT_SEED,
    DEFAULT_SUMMARY,
    DEFAULT_UNIVERSE,
    DESIGN_STRATA_COLUMNS,
    ERA_LABELS,
    ScreenBuildError,
    _allocation_table,
    _public_screen_columns,
    drop_audit_row,
    enrich_candidate_frame,
    sha256_file,
    stable_rank,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SCREEN = (
    REPO_ROOT / "data/municipal_discovery/screening/candidate_screening_sample.csv"
)
DEFAULT_COMPLETE_CONTEST_SAMPLE = (
    REPO_ROOT / "data/municipal_discovery/screening/complete_contest_sample.csv"
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data/municipal_discovery/confirmation"
DEFAULT_QUOTAS = {
    "2000_2007": 320,
    "2008_2013": 160,
    "2014_2017": 160,
    "2018_2021": 160,
}


class ConfirmationSampleError(ValueError):
    """The inputs cannot support the approved confirmation sample."""


def _require_columns(frame: pd.DataFrame, required: Iterable[str], label: str) -> None:
    missing = sorted(set(required) - set(frame.columns.tolist()))
    if missing:
        raise ConfirmationSampleError(f"{label} is missing required columns: {missing}")


def _id_set(frame: pd.DataFrame, label: str) -> set[str]:
    _require_columns(frame, {"candidate_year_id"}, label)
    values = frame["candidate_year_id"].astype("string").str.strip()
    invalid = values.isna() | values.eq("")
    if invalid.any():
        raise ConfirmationSampleError(
            f"{label} has {int(invalid.sum())} missing candidate_year_id values"
        )
    return set(values.tolist())


def _contest_sample_ids(
    frame: pd.DataFrame,
    label: str,
    expected_contests: int | None,
) -> set[str]:
    candidate_ids = _id_set(frame, label)
    if len(candidate_ids) != len(frame):
        raise ConfirmationSampleError(f"{label} has duplicate candidate_year_id rows")
    contest_column = "contest" if "contest" in frame else "contest_year_id"
    _require_columns(frame, {contest_column}, label)
    contests = frame[contest_column].astype("string").str.strip()
    if contests.isna().any() or contests.eq("").any():
        raise ConfirmationSampleError(f"{label} has missing contest IDs")
    observed_contests = int(contests.nunique())
    if expected_contests is not None and observed_contests != expected_contests:
        raise ConfirmationSampleError(
            f"{label} has {observed_contests} contests; expected {expected_contests}"
        )
    return candidate_ids


def stratified_confirmation_sample(
    frame: pd.DataFrame,
    quotas: dict[str, int] | None = None,
    *,
    seed: int = DEFAULT_SEED,
) -> pd.DataFrame:
    """Draw each era quota with positive joint-stratum probabilities."""
    requested = dict(DEFAULT_QUOTAS if quotas is None else quotas)
    if set(requested) != set(ERA_LABELS):
        raise ConfirmationSampleError("Confirmation quotas must name all four eras")
    if any(not isinstance(value, int) or value < 1 for value in requested.values()):
        raise ConfirmationSampleError("Each confirmation quota must be a positive integer")

    selected_parts: list[pd.DataFrame] = []
    for era in ERA_LABELS:
        quota = requested[era]
        era_frame = frame.loc[frame["era"].eq(era)].copy()
        try:
            allocation = _allocation_table(
                era_frame,
                quota,
                DESIGN_STRATA_COLUMNS,
                seed,
                era,
            )
        except ScreenBuildError as error:
            raise ConfirmationSampleError(str(error)) from error
        allocation_map = allocation.set_index("screening_stratum")[[
            "stratum_population",
            "stratum_sample_n",
        ]].to_dict("index")
        era_frame["_random_rank"] = era_frame["candidate_year_id"].map(
            lambda value: stable_rank(value, seed, f"confirmation-candidate-{era}")
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
                digest = stable_rank(
                    stratum, seed, f"confirmation-systematic-start-{era}"
                )
                unit_fraction = int(digest, 16) / (1 << 256)
                start = unit_fraction * interval
                selected_positions = [
                    min(int(start + draw_index * interval), len(group) - 1)
                    for draw_index in range(draw_n)
                ]
                if len(set(selected_positions)) != draw_n:
                    raise ConfirmationSampleError(
                        f"Era {era}, stratum {stratum} produced duplicate positions"
                    )
            draw = group.iloc[selected_positions].copy()
            draw["stratum_population"] = int(design["stratum_population"])
            draw["stratum_sample_n"] = int(design["stratum_sample_n"])
            draw["inclusion_probability"] = (
                draw["stratum_sample_n"] / draw["stratum_population"]
            )
            draw["population_weight"] = 1.0 / draw["inclusion_probability"]
            selected_parts.append(draw)

    selected = pd.concat(selected_parts, ignore_index=True)
    selected["sample_seed"] = int(seed)
    selected["sample_kind"] = "candidate_year_confirmation"
    selected = selected.sort_values(
        ["era", "screening_stratum", "_random_rank", "candidate_year_id"]
    ).reset_index(drop=True)
    selected["sample_order"] = selected.index + 1
    counts = selected.groupby("era", observed=True).size().to_dict()
    if counts != requested:
        raise ConfirmationSampleError(
            f"Confirmation era quotas failed: observed {counts}, expected {requested}"
        )
    return selected.drop(columns="_random_rank")


def build_confirmation_sample(
    summary: pd.DataFrame,
    universe: pd.DataFrame,
    pilot_manifest: pd.DataFrame,
    candidate_screen: pd.DataFrame,
    complete_contest_sample: pd.DataFrame,
    *,
    seed: int = DEFAULT_SEED,
    quotas: dict[str, int] | None = None,
    expected_pilot_size: int | None = 120,
    expected_screen_size: int | None = 480,
    expected_complete_contests: int | None = 32,
) -> dict[str, pd.DataFrame]:
    """Build a weighted confirmation sample and an explicit drop audit."""
    requested = dict(DEFAULT_QUOTAS if quotas is None else quotas)
    try:
        full = enrich_candidate_frame(summary, universe)
    except ScreenBuildError as error:
        raise ConfirmationSampleError(str(error)) from error
    pilot_ids = _id_set(pilot_manifest, "pilot manifest")
    screen_ids = _id_set(candidate_screen, "candidate screen")
    contest_sample_ids = _contest_sample_ids(
        complete_contest_sample,
        "complete-contest sample",
        expected_complete_contests,
    )
    if expected_pilot_size is not None and len(pilot_ids) != expected_pilot_size:
        raise ConfirmationSampleError(
            f"Pilot manifest has {len(pilot_ids)} unique cases; expected {expected_pilot_size}"
        )
    if expected_screen_size is not None and len(screen_ids) != expected_screen_size:
        raise ConfirmationSampleError(
            f"Candidate screen has {len(screen_ids)} unique cases; expected {expected_screen_size}"
        )
    if pilot_ids & screen_ids:
        raise ConfirmationSampleError("The pilot and candidate screen overlap")
    if contest_sample_ids & (pilot_ids | screen_ids):
        raise ConfirmationSampleError(
            "The complete-contest sample overlaps the pilot or candidate screen"
        )
    universe_ids = set(full["candidate_year_id"].astype(str))
    missing_exclusions = (pilot_ids | screen_ids | contest_sample_ids) - universe_ids
    if missing_exclusions:
        examples = sorted(missing_exclusions)[:5]
        raise ConfirmationSampleError(
            f"{len(missing_exclusions)} excluded IDs are absent from the universe: {examples}"
        )

    audit: list[dict[str, object]] = []
    unresolved = full.loc[~full["_has_validated_url"]].copy()
    audit.append(drop_audit_row(
        "exclude_validated_urls",
        len(full),
        len(unresolved),
        expected=True,
        note="The confirmation sample targets candidate-years without a validated URL.",
    ))
    supported = unresolved.loc[unresolved["era"].notna()].copy()
    audit.append(drop_audit_row(
        "restrict_to_2000_2021_eras",
        len(unresolved),
        len(supported),
        expected=True,
        note="The approved confirmation design covers elections from 2000 through 2021.",
    ))
    eligible = supported.loc[
        ~supported["candidate_year_id"].astype(str).isin(pilot_ids)
    ].copy()
    audit.append(drop_audit_row(
        "exclude_original_pilot_cases",
        len(supported),
        len(eligible),
        expected=True,
        note="The confirmation sample must be separate from the original pilot.",
    ))
    before = len(eligible)
    eligible = eligible.loc[
        ~eligible["candidate_year_id"].astype(str).isin(screen_ids)
    ].copy()
    audit.append(drop_audit_row(
        "exclude_candidate_screen_cases",
        before,
        len(eligible),
        expected=True,
        note="The confirmation sample must be separate from the 480-case screen.",
    ))
    before = len(eligible)
    eligible = eligible.loc[
        ~eligible["candidate_year_id"].astype(str).isin(contest_sample_ids)
    ].copy()
    audit.append(drop_audit_row(
        "exclude_complete_contest_sample_cases",
        before,
        len(eligible),
        expected=True,
        note=(
            "The confirmation sample must also be separate from the 32 complete "
            "contest-years evaluated in the first stage."
        ),
    ))
    sample = stratified_confirmation_sample(eligible, requested, seed=seed)
    audit.append(drop_audit_row(
        "draw_confirmation_sample",
        len(eligible),
        len(sample),
        operation_type="sampling",
        expected=True,
        note=(
            "The approved design draws 320 early cases and 160 cases from each later era."
        ),
    ))
    sample_ids = set(sample["candidate_year_id"].astype(str))
    if sample_ids & (pilot_ids | screen_ids | contest_sample_ids):
        raise ConfirmationSampleError("The confirmation sample overlaps an earlier sample")
    audit_frame = pd.DataFrame(audit)
    if audit_frame.loc[
        audit_frame["large_drop"], "note"
    ].astype(str).str.strip().eq("").any():
        raise ConfirmationSampleError("A greater-than-20% filter lacks a report")
    return {
        "confirmation_sample": _public_screen_columns(sample),
        "drop_audit": audit_frame,
    }


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


def write_confirmation_outputs(
    outputs: dict[str, pd.DataFrame],
    output_dir: Path,
    *,
    input_paths: dict[str, Path] | None = None,
    seed: int = DEFAULT_SEED,
    quotas: dict[str, int] | None = None,
) -> Path:
    """Write the sample, the audit, and a hashed build manifest."""
    output_dir.mkdir(parents=True, exist_ok=True)
    files = {
        "confirmation_sample": output_dir / "candidate_confirmation_sample.csv",
        "drop_audit": output_dir / "confirmation_drop_audit.csv",
    }
    for key, path in files.items():
        _write_csv_atomic(outputs[key], path)
    manifest = {
        "schema_version": 1,
        "sample_seed": int(seed),
        "era_quotas": dict(DEFAULT_QUOTAS if quotas is None else quotas),
        "sample_is_acquisition_decision": False,
        "inputs": {
            key: {"path": str(path), "sha256": sha256_file(path)}
            for key, path in sorted((input_paths or {}).items())
        },
        "outputs": {
            key: {
                "path": path.name,
                "rows": int(len(outputs[key])),
                "sha256": sha256_file(path),
            }
            for key, path in files.items()
        },
    }
    manifest_path = output_dir / "confirmation_manifest.json"
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
    parser.add_argument("--screen", type=Path, default=DEFAULT_SCREEN)
    parser.add_argument(
        "--complete-contest-sample",
        type=Path,
        default=DEFAULT_COMPLETE_CONTEST_SAMPLE,
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--expected-pilot-size", type=int, default=120)
    parser.add_argument("--expected-screen-size", type=int, default=480)
    parser.add_argument("--expected-complete-contests", type=int, default=32)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    summary = pd.read_csv(args.summary, dtype={"candidate_year_id": "string"})
    universe = pd.read_csv(args.universe, dtype={"candidate_year_id": "string"})
    pilot = pd.read_csv(args.pilot_manifest, dtype={"candidate_year_id": "string"})
    screen = pd.read_csv(args.screen, dtype={"candidate_year_id": "string"})
    complete_contests = pd.read_csv(
        args.complete_contest_sample,
        dtype={"candidate_year_id": "string"},
    )
    outputs = build_confirmation_sample(
        summary,
        universe,
        pilot,
        screen,
        complete_contests,
        seed=args.seed,
        expected_pilot_size=args.expected_pilot_size,
        expected_screen_size=args.expected_screen_size,
        expected_complete_contests=args.expected_complete_contests,
    )
    manifest_path = write_confirmation_outputs(
        outputs,
        args.output_dir,
        input_paths={
            "candidate_summary": args.summary,
            "candidate_universe": args.universe,
            "pilot_manifest": args.pilot_manifest,
            "candidate_screen": args.screen,
            "complete_contest_sample": args.complete_contest_sample,
        },
        seed=args.seed,
    )
    print(json.dumps({
        "manifest": str(manifest_path),
        "confirmation_rows": len(outputs["confirmation_sample"]),
    }, sort_keys=True))


if __name__ == "__main__":
    main()
