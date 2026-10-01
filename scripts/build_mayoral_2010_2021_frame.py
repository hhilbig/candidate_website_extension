#!/usr/bin/env python3
"""Build the current mayoral coverage roster and unresolved 2010-2021 frame."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

import pandas as pd

from scripts.build_mayoral_discovery_screen import STATE_TO_REGION


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_UNIVERSE = REPO_ROOT / "data/municipal_discovery/final/mayoral_candidate_universe.csv"
DEFAULT_CLAIMS = REPO_ROOT / "data/municipal_discovery/final/mayoral_url_claims.csv"
DEFAULT_DISPOSITIONS = (
    REPO_ROOT / "quality_reports/mayoral_url_discovery_2026-08-25/batch20_evaluation/batch_claim_dispositions.csv",
    REPO_ROOT / "quality_reports/mayoral_url_discovery_2026-08-25/batch20_round2_evaluation/batch_claim_dispositions.csv",
)
DEFAULT_OUTPUT_DIR = REPO_ROOT / "data/municipal_discovery/screening_2010_2021"
PERIODS = ((2010, 2013, "2010_2013"), (2014, 2017, "2014_2017"), (2018, 2021, "2018_2021"))


class FrameBuildError(ValueError):
    """The authoritative inputs do not support a unique audited frame."""


def _as_bool(series: pd.Series) -> pd.Series:
    truthy = {"1", "true", "t", "yes", "y"}
    falsy = {"0", "false", "f", "no", "n", "", "nan", "none", "<na>"}

    def convert(value: object) -> bool:
        if pd.isna(value):
            return False
        if isinstance(value, bool):
            return value
        token = str(value).strip().casefold()
        if token in truthy:
            return True
        if token in falsy:
            return False
        raise FrameBuildError(f"Unrecognized boolean value: {value!r}")

    return series.map(convert).astype(bool)


def _period(year: object) -> str | None:
    if pd.isna(year):
        return None
    value = int(year)
    for start, end, label in PERIODS:
        if start <= value <= end:
            return label
    return None


def _require(frame: pd.DataFrame, columns: set[str], label: str) -> None:
    missing = sorted(columns - set(frame.columns))
    if missing:
        raise FrameBuildError(f"{label} is missing columns: {missing}")


def build_current_frame(
    universe: pd.DataFrame,
    claims: pd.DataFrame,
    dispositions: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, dict]:
    """Return in-scope coverage, unresolved 2010+, breakdowns, and an audit summary."""
    _require(
        universe,
        {
            "candidate_year_id", "in_discovery_scope", "year", "month", "geo_name",
            "state_abb", "population_2020", "ballotpedia_url", "stage_scope_status",
            "election_stage_status", "contest_n_candidates", "contest",
        },
        "candidate universe",
    )
    _require(
        claims,
        {"candidate_year_id", "claim_origin", "strict_custom_domain"},
        "claim table",
    )
    universe = universe.copy()
    universe["candidate_year_id"] = universe["candidate_year_id"].astype("string").str.strip()
    if universe["candidate_year_id"].isna().any() or universe["candidate_year_id"].eq("").any():
        raise FrameBuildError("candidate universe has missing candidate_year_id values")
    if universe["candidate_year_id"].duplicated().any():
        raise FrameBuildError("candidate universe candidate_year_id must be unique")
    in_scope = universe.loc[_as_bool(universe["in_discovery_scope"])].copy()
    if len(in_scope) != 4817:
        raise FrameBuildError(f"Expected 4,817 discovery-scope rows; found {len(in_scope)}")
    scope_keys = set(in_scope["candidate_year_id"].astype(str))

    claims = claims.copy()
    claims["candidate_year_id"] = claims["candidate_year_id"].astype("string").str.strip()
    claim_keys = set(claims["candidate_year_id"].dropna().astype(str))
    unexpected_claims = sorted(claim_keys - scope_keys)
    if unexpected_claims:
        raise FrameBuildError(f"Claim keys are absent from the 4,817-case universe: {unexpected_claims[:5]}")
    legacy_keys = set(
        claims.loc[claims["claim_origin"].eq("legacy_baseline"), "candidate_year_id"].astype(str)
    )
    direct_keys = set(
        claims.loc[
            ~claims["claim_origin"].eq("legacy_baseline")
            & _as_bool(claims["strict_custom_domain"]),
            "candidate_year_id",
        ].astype(str)
    )

    inferred_keys: set[str] = set()
    disposition_rows = 0
    for index, disposition in enumerate(dispositions):
        _require(
            disposition,
            {"candidate_year_id", "strict_under_batch_audit_rule", "batch_disposition"},
            f"disposition {index}",
        )
        disposition_rows += len(disposition)
        strict = disposition.loc[_as_bool(disposition["strict_under_batch_audit_rule"])].copy()
        invalid = strict.loc[
            ~strict["batch_disposition"].isin(
                ["human_accepted", "accepted_under_sampled_audit_rule"]
            )
        ]
        if len(invalid):
            raise FrameBuildError("Strict disposition has an unsupported acceptance basis")
        inferred_keys.update(
            strict.loc[
                strict["batch_disposition"].eq("accepted_under_sampled_audit_rule"),
                "candidate_year_id",
            ].astype(str)
        )
    unexpected_inferred = sorted(inferred_keys - scope_keys)
    if unexpected_inferred:
        raise FrameBuildError(f"Inferred coverage keys are absent from the universe: {unexpected_inferred[:5]}")

    roster = in_scope.copy()
    ids = roster["candidate_year_id"].astype(str)
    roster["legacy_source_backed"] = ids.isin(legacy_keys)
    roster["direct_new_strict"] = ids.isin(direct_keys)
    roster["sampled_audit_rule_inferred"] = ids.isin(inferred_keys)
    roster["directly_supported_combined"] = roster["legacy_source_backed"] | roster["direct_new_strict"]
    roster["covered"] = roster["directly_supported_combined"] | roster["sampled_audit_rule_inferred"]
    roster["coverage_basis"] = "uncovered"
    roster.loc[roster["legacy_source_backed"], "coverage_basis"] = "legacy_source_backed"
    roster.loc[roster["direct_new_strict"], "coverage_basis"] = "direct_new_strict"
    roster.loc[
        roster["legacy_source_backed"] & roster["direct_new_strict"], "coverage_basis"
    ] = "legacy_and_direct_new_strict"
    roster.loc[
        roster["sampled_audit_rule_inferred"] & ~roster["directly_supported_combined"],
        "coverage_basis",
    ] = "sampled_audit_rule_inferred"

    roster["year"] = pd.to_numeric(roster["year"], errors="raise").astype(int)
    roster["period"] = roster["year"].map(_period)
    roster["region"] = (
        roster["state_abb"].astype("string").str.upper().map(STATE_TO_REGION).fillna("Other/Unknown")
    )
    population = pd.to_numeric(roster["population_2020"], errors="coerce")
    roster["population_group"] = pd.cut(
        population,
        bins=[-float("inf"), 149999, 249999, 499999, float("inf")],
        labels=["100k_149999", "150k_249999", "250k_499999", "500k_plus"],
    ).astype("string").fillna("missing")
    roster["profile_availability"] = (
        roster["ballotpedia_url"].notna()
        & roster["ballotpedia_url"].astype("string").str.strip().ne("")
    ).map({True: "profile", False: "no_profile"})
    roster["stage_status"] = roster["stage_scope_status"].astype("string").fillna("missing").replace("", "missing")
    contest_size = pd.to_numeric(roster["contest_n_candidates"], errors="coerce")
    roster["contest_size_bin"] = "5_plus"
    roster.loc[contest_size.eq(2), "contest_size_bin"] = "2"
    roster.loc[contest_size.between(3, 4), "contest_size_bin"] = "3_4"
    roster.loc[contest_size.lt(2) | contest_size.isna(), "contest_size_bin"] = "missing_or_lt2"
    roster["missing_election_month"] = pd.to_numeric(roster["month"], errors="coerce").isna()

    unresolved = roster.loc[
        ~roster["covered"] & roster["year"].between(2010, 2021)
    ].copy()
    if unresolved["candidate_year_id"].duplicated().any():
        raise FrameBuildError("unresolved frame has duplicate candidate-year keys")

    dimensions = [
        "period", "year", "region", "population_group", "profile_availability",
        "stage_status", "election_stage_status", "contest_size_bin", "missing_election_month",
    ]
    breakdown_rows: list[dict[str, object]] = []
    for dimension in dimensions:
        counts = unresolved[dimension].astype("string").fillna("missing").value_counts(dropna=False)
        for value, count in counts.sort_index().items():
            breakdown_rows.append({"dimension": dimension, "value": value, "unresolved_n": int(count)})
    breakdown = pd.DataFrame(breakdown_rows)

    audit = {
        "schema_version": "mayoral-2010-2021-frame-v1",
        "universe_rows": int(len(universe)),
        "discovery_scope_rows": int(len(roster)),
        "claim_rows": int(len(claims)),
        "disposition_rows": int(disposition_rows),
        "legacy_source_backed_candidate_years": int(roster["legacy_source_backed"].sum()),
        "direct_new_strict_candidate_years": int(roster["direct_new_strict"].sum()),
        "legacy_and_direct_overlap_candidate_years": int((roster["legacy_source_backed"] & roster["direct_new_strict"]).sum()),
        "directly_supported_combined_candidate_years": int(roster["directly_supported_combined"].sum()),
        "sampled_audit_inferred_candidate_years_total": int(roster["sampled_audit_rule_inferred"].sum()),
        "sampled_audit_inferred_marginal_candidate_years": int((roster["sampled_audit_rule_inferred"] & ~roster["directly_supported_combined"]).sum()),
        "covered_candidate_years": int(roster["covered"].sum()),
        "uncovered_candidate_years": int((~roster["covered"]).sum()),
        "unresolved_2010_2021_candidate_years": int(len(unresolved)),
        "unresolved_by_period": {
            label: int(unresolved["period"].eq(label).sum()) for _, _, label in PERIODS
        },
        "unresolved_missing_election_month": int(unresolved["missing_election_month"].sum()),
        "duplicate_roster_keys": int(roster["candidate_year_id"].duplicated().sum()),
        "duplicate_unresolved_keys": int(unresolved["candidate_year_id"].duplicated().sum()),
        "unexpected_claim_keys": unexpected_claims,
        "unexpected_inferred_keys": unexpected_inferred,
    }
    return (
        roster.sort_values("candidate_year_id").reset_index(drop=True),
        unresolved.sort_values("candidate_year_id").reset_index(drop=True),
        breakdown.sort_values(["dimension", "value"]).reset_index(drop=True),
        audit,
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_outputs(
    roster: pd.DataFrame,
    unresolved: pd.DataFrame,
    breakdown: pd.DataFrame,
    audit: dict,
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "coverage_roster": output_dir / "current_coverage_roster.csv",
        "unresolved_frame": output_dir / "unresolved_2010_2021.csv",
        "breakdown": output_dir / "unresolved_2010_2021_breakdown.csv",
        "audit": output_dir / "frame_audit.json",
    }
    row_counts = {
        "coverage_roster": len(roster),
        "unresolved_frame": len(unresolved),
        "breakdown": len(breakdown),
        "audit": None,
    }
    roster.to_csv(paths["coverage_roster"], index=False)
    unresolved.to_csv(paths["unresolved_frame"], index=False)
    breakdown.to_csv(paths["breakdown"], index=False)
    paths["audit"].write_text(json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    manifest = {
        "schema_version": "mayoral-2010-2021-frame-manifest-v1",
        "files": [
            {
                "name": name,
                "path": path.name,
                "rows": row_counts[name],
                "bytes": path.stat().st_size,
                "sha256": _sha256(path),
            }
            for name, path in paths.items()
        ],
    }
    manifest_path = output_dir / "frame_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--universe", default=DEFAULT_UNIVERSE)
    parser.add_argument("--claims", default=DEFAULT_CLAIMS)
    parser.add_argument("--disposition", action="append", default=None)
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()
    disposition_paths = [Path(path) for path in args.disposition] if args.disposition else list(DEFAULT_DISPOSITIONS)
    roster, unresolved, breakdown, audit = build_current_frame(
        pd.read_csv(args.universe, low_memory=False),
        pd.read_csv(args.claims, low_memory=False),
        [pd.read_csv(path, low_memory=False) for path in disposition_paths],
    )
    write_outputs(roster, unresolved, breakdown, audit, Path(args.output_dir))
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
