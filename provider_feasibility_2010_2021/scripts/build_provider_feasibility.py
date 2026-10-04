#!/usr/bin/env python3
"""Freeze the unresolved frame and build a deterministic provider pilot packet."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pandas as pd


WORKSTREAM_ROOT = Path(__file__).resolve().parents[1]
REPO_ROOT = WORKSTREAM_ROOT.parent
DEFAULT_SOURCE = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/search_source_expansion"
    / "frozen_inputs/unresolved_2010_2021.csv"
)
DEFAULT_PARENT_SCREEN = (
    REPO_ROOT
    / "data/municipal_discovery/screening_2010_2021/candidate_screening_sample_2010_2021.csv"
)
DEFAULT_SCHEMA = WORKSTREAM_ROOT / "config/provider_response_schema.json"
DEFAULT_SEED = 20260825
DEFAULT_QUOTA_PER_YEAR = 25
FREEZE_START_YEAR = 2010
FREEZE_END_YEAR = 2021
PILOT_START_YEAR = 2018
PILOT_END_YEAR = 2021

FRAME_REQUIRED = [
    "candidate_contest_id",
    "candidate_year_id",
    "ledb_candid",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "contest",
    "population_2020",
    "stage_scope_status",
    "in_verified_general_scope",
    "election_page_stage",
    "election_page_stage_label",
    "election_page_election_date",
    "covered",
    "coverage_basis",
]

PARENT_REQUIRED = [
    "sample_order",
    "sample_kind",
    "sample_seed",
    "candidate_year_id",
    "candidate_contest_id",
    "ledb_candid",
    "full_name",
    "geo_name",
    "state_abb",
    "year",
    "month",
    "covered",
    "period",
    "design_stratum",
    "balance_cell",
    "inclusion_probability",
    "population_weight",
]

SHAREABLE_COLUMNS = [
    "masked_candidate_id",
    "candidate_name",
    "geo_name",
    "state_abb",
    "year",
    "office",
    "election_date_context",
    "election_date_precision",
    "election_round_context",
    "ballotready_civicengine_requested",
    "ballotpedia_requested",
    "democracy_works_requested",
]

PROHIBITED_SHAREABLE_COLUMNS = {
    "candidate_contest_id",
    "candidate_year_id",
    "ledb_candid",
    "contest",
    "has_validated_campaign_url",
    "covered",
    "coverage_basis",
    "discovery_status",
    "unresolved_reason",
    "campaign_urls",
    "campaign_hosts",
    "ballotpedia_url",
}


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _require_columns(frame: pd.DataFrame, columns: list[str], label: str) -> None:
    missing = sorted(set(columns) - set(frame.columns))
    if missing:
        raise ValueError(f"{label} missing required columns: {missing}")


def _truthy(series: pd.Series) -> pd.Series:
    return series.fillna(False).astype(str).str.strip().str.lower().isin({"true", "1", "yes"})


def _stable_rank(seed: int, candidate_year_id: str) -> str:
    raw = f"provider-pilot-rank-v1|{seed}|{candidate_year_id}".encode("utf-8")
    return hashlib.sha256(raw).hexdigest()


def _masked_id(seed: int, candidate_year_id: str) -> str:
    raw = f"provider-pilot-mask-v1|{seed}|{candidate_year_id}".encode("utf-8")
    return "PFS-" + hashlib.sha256(raw).hexdigest()[:16].upper()


def freeze_unresolved_frame(source: pd.DataFrame) -> pd.DataFrame:
    """Validate and normalize the authoritative unresolved 2010--2021 frame."""
    _require_columns(source, FRAME_REQUIRED, "unresolved frame")
    frame = source.copy()
    frame["year"] = pd.to_numeric(frame["year"], errors="raise").astype(int)
    if not frame["year"].between(FREEZE_START_YEAR, FREEZE_END_YEAR).all():
        bad_years = sorted(
            frame.loc[
                ~frame["year"].between(FREEZE_START_YEAR, FREEZE_END_YEAR), "year"
            ].unique()
        )
        raise ValueError(f"unresolved frame contains out-of-scope years: {bad_years}")
    if _truthy(frame["covered"]).any():
        ids = frame.loc[_truthy(frame["covered"]), "candidate_year_id"].astype(str).tolist()
        raise ValueError(f"unresolved frame contains covered rows: {ids[:5]}")

    if frame["candidate_year_id"].isna().any():
        raise ValueError("frozen unresolved frame contains missing candidate_year_id")
    if frame["candidate_year_id"].duplicated().any():
        duplicates = frame.loc[frame["candidate_year_id"].duplicated(), "candidate_year_id"].tolist()
        raise ValueError(f"candidate_year_id is not unique: {duplicates[:5]}")

    frame["month"] = pd.to_numeric(frame["month"], errors="coerce").astype("Int64")
    frame["population_2020"] = pd.to_numeric(frame["population_2020"], errors="coerce")
    frame = frame.sort_values(
        ["year", "state_abb", "geo_name", "candidate_year_id"], kind="mergesort"
    ).reset_index(drop=True)
    return frame


def validate_parent_screen(frozen: pd.DataFrame, parent_screen: pd.DataFrame) -> pd.DataFrame:
    """Validate that the common 480-case screen is an uncovered subset of the frame."""
    _require_columns(parent_screen, PARENT_REQUIRED, "parent screening sample")
    parent = parent_screen.copy()
    parent["year"] = pd.to_numeric(parent["year"], errors="raise").astype(int)
    if parent["candidate_year_id"].isna().any() or parent["candidate_year_id"].duplicated().any():
        raise ValueError("parent screening sample candidate_year_id must be nonmissing and unique")
    if _truthy(parent["covered"]).any():
        ids = parent.loc[_truthy(parent["covered"]), "candidate_year_id"].astype(str).tolist()
        raise ValueError(f"parent screening sample contains covered rows: {ids[:5]}")

    frozen_ids = set(frozen["candidate_year_id"].astype(str))
    parent_ids = set(parent["candidate_year_id"].astype(str))
    outside = sorted(parent_ids - frozen_ids)
    if outside:
        raise ValueError(f"parent screening sample is not a subset of unresolved frame: {outside[:5]}")

    agreement_fields = [
        "candidate_contest_id",
        "ledb_candid",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
    ]
    reference = frozen.set_index("candidate_year_id")
    mismatches: list[str] = []
    for field in agreement_fields:
        expected = parent["candidate_year_id"].map(reference[field])
        observed = parent[field]
        equal = observed.fillna("<NA>").astype(str).eq(expected.fillna("<NA>").astype(str))
        if not equal.all():
            mismatches.extend(parent.loc[~equal, "candidate_year_id"].astype(str).tolist())
    if mismatches:
        raise ValueError(f"parent screening sample disagrees with unresolved frame: {mismatches[:5]}")
    return parent.sort_values("sample_order", kind="mergesort").reset_index(drop=True)


def _date_context(row: pd.Series) -> tuple[str, str]:
    exact = str(row.get("election_page_election_date", "")).strip()
    if exact and exact.lower() != "nan":
        parsed = pd.to_datetime(exact, errors="coerce")
        if not pd.isna(parsed):
            return parsed.strftime("%Y-%m-%d"), "day"
    month = row.get("month")
    if not pd.isna(month):
        return f"{int(row['year']):04d}-{int(month):02d}", "month"
    return str(int(row["year"])), "year"


def _round_context(row: pd.Series) -> str:
    for column in ("election_page_stage_label", "election_page_stage"):
        value = str(row.get(column, "")).strip()
        if value and value.lower() != "nan":
            return value
    status = str(row.get("stage_scope_status", "")).strip()
    if status == "verified_general":
        return "general"
    if status == "verified_non_general_or_special":
        return "non-general or special; exact round unresolved"
    return "unresolved"


def draw_masked_sample(
    frozen: pd.DataFrame,
    parent_screen: pd.DataFrame,
    *,
    seed: int = DEFAULT_SEED,
    quota_per_year: int = DEFAULT_QUOTA_PER_YEAR,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Draw 25 cases/year from the 2018--2021 portion of the common screen."""
    parent = validate_parent_screen(frozen, parent_screen)
    eligible = parent.loc[
        parent["year"].between(PILOT_START_YEAR, PILOT_END_YEAR)
        & parent["period"].eq("2018_2021")
    ].copy()
    pieces: list[pd.DataFrame] = []
    for year in range(PILOT_START_YEAR, PILOT_END_YEAR + 1):
        stratum = eligible.loc[eligible["year"] == year].copy()
        if len(stratum) < quota_per_year:
            raise ValueError(
                f"year {year} contains {len(stratum)} eligible cases, below quota {quota_per_year}"
            )
        stratum["selection_rank"] = stratum["candidate_year_id"].astype(str).map(
            lambda value: _stable_rank(seed, value)
        )
        stratum = stratum.sort_values(
            ["selection_rank", "candidate_year_id"], kind="mergesort"
        ).head(quota_per_year)
        stratum["selection_stratum"] = str(year)
        stratum["provider_parent_stratum_n"] = int((eligible["year"] == year).sum())
        stratum["provider_stratum_quota"] = quota_per_year
        stratum["provider_conditional_inclusion_probability"] = (
            quota_per_year / stratum["provider_parent_stratum_n"]
        )
        stratum["parent_inclusion_probability"] = pd.to_numeric(
            stratum["inclusion_probability"], errors="raise"
        )
        stratum["parent_population_weight"] = pd.to_numeric(
            stratum["population_weight"], errors="raise"
        )
        stratum["combined_inclusion_probability"] = (
            stratum["parent_inclusion_probability"]
            * stratum["provider_conditional_inclusion_probability"]
        )
        stratum["combined_population_weight"] = 1 / stratum["combined_inclusion_probability"]
        pieces.append(stratum)

    internal = pd.concat(pieces, ignore_index=True)
    context = frozen.set_index("candidate_year_id")
    for column in (
        "stage_scope_status",
        "election_page_stage",
        "election_page_stage_label",
        "election_page_election_date",
    ):
        internal[column] = internal["candidate_year_id"].map(context[column])
    internal["masked_candidate_id"] = internal["candidate_year_id"].astype(str).map(
        lambda value: _masked_id(seed, value)
    )
    if internal["masked_candidate_id"].duplicated().any():
        raise ValueError("masked ID collision")

    date_values = internal.apply(_date_context, axis=1)
    internal["election_date_context"] = [item[0] for item in date_values]
    internal["election_date_precision"] = [item[1] for item in date_values]
    internal["election_round_context"] = internal.apply(_round_context, axis=1)

    shareable = pd.DataFrame(
        {
            "masked_candidate_id": internal["masked_candidate_id"],
            "candidate_name": internal["full_name"],
            "geo_name": internal["geo_name"],
            "state_abb": internal["state_abb"],
            "year": internal["year"],
            "office": "mayor",
            "election_date_context": internal["election_date_context"],
            "election_date_precision": internal["election_date_precision"],
            "election_round_context": internal["election_round_context"],
            "ballotready_civicengine_requested": True,
            "ballotpedia_requested": True,
            "democracy_works_requested": internal["year"].between(2020, 2021),
        }
    )
    if list(shareable.columns) != SHAREABLE_COLUMNS:
        raise AssertionError("shareable packet column contract changed")
    leaked = PROHIBITED_SHAREABLE_COLUMNS.intersection(shareable.columns)
    if leaked:
        raise AssertionError(f"shareable packet leaks internal/outcome fields: {sorted(leaked)}")

    crosswalk_columns = [
        "masked_candidate_id",
        "candidate_year_id",
        "candidate_contest_id",
        "ledb_candid",
        "full_name",
        "geo_name",
        "state_abb",
        "year",
        "sample_order",
        "sample_seed",
        "period",
        "design_stratum",
        "balance_cell",
        "selection_stratum",
        "provider_parent_stratum_n",
        "provider_stratum_quota",
        "provider_conditional_inclusion_probability",
        "parent_inclusion_probability",
        "parent_population_weight",
        "combined_inclusion_probability",
        "combined_population_weight",
        "selection_rank",
    ]
    crosswalk = internal[crosswalk_columns].copy()
    shareable = shareable.sort_values("masked_candidate_id", kind="mergesort").reset_index(drop=True)
    crosswalk = crosswalk.sort_values("masked_candidate_id", kind="mergesort").reset_index(drop=True)
    return shareable, crosswalk


def _nonempty(series: pd.Series) -> pd.Series:
    return series.fillna("").astype(str).str.strip().ne("")


def _valid_http_url(value: Any) -> bool:
    text = str(value).strip()
    if not text:
        return False
    parsed = urlsplit(text)
    return parsed.scheme.lower() in {"http", "https"} and bool(parsed.netloc)


def validate_provider_response(
    response: pd.DataFrame,
    schema: dict[str, Any],
    *,
    allowed_masked_ids: set[str] | None = None,
    expected_provider: str | None = None,
    require_complete_attempts: bool = False,
) -> pd.DataFrame:
    """Validate a provider result and append event_specific_eligible."""
    required_columns = list(schema["required_columns"])
    _require_columns(response, required_columns, "provider response")
    checked = response[required_columns].copy()

    key = list(schema["row_key"])
    for field in [*key, "match_status", "historical_version_status"]:
        missing = ~_nonempty(checked[field])
        if missing.any():
            ids = checked.loc[missing, "masked_candidate_id"].fillna("<missing>").astype(str).tolist()
            raise ValueError(f"provider response missing terminal {field}: {ids[:5]}")
    if checked.duplicated(key).any():
        duplicates = checked.loc[checked.duplicated(key, keep=False), key].to_dict("records")
        raise ValueError(f"provider response contains duplicate row keys: {duplicates[:5]}")

    invalid_providers = sorted(set(checked["provider_name"].dropna()) - set(schema["provider_name_values"]))
    if invalid_providers:
        raise ValueError(f"invalid provider_name values: {invalid_providers}")
    invalid_statuses = sorted(set(checked["match_status"].dropna()) - set(schema["match_status_values"]))
    if invalid_statuses:
        raise ValueError(f"invalid match_status values: {invalid_statuses}")
    invalid_history = sorted(
        set(checked["historical_version_status"].dropna())
        - set(schema["historical_version_status_values"])
    )
    if invalid_history:
        raise ValueError(f"invalid historical_version_status values: {invalid_history}")

    if allowed_masked_ids is not None:
        response_ids = set(checked["masked_candidate_id"].astype(str))
        unexpected = sorted(response_ids - allowed_masked_ids)
        if unexpected:
            raise ValueError(f"provider response contains unexpected masked IDs: {unexpected[:5]}")
        if require_complete_attempts:
            missing = sorted(allowed_masked_ids - response_ids)
            if missing:
                raise ValueError(f"provider response omits requested masked IDs: {missing[:5]}")

    if expected_provider is not None:
        observed = set(checked["provider_name"].astype(str))
        if observed != {expected_provider}:
            raise ValueError(f"expected only provider {expected_provider!r}; observed {sorted(observed)}")

    found = checked["match_status"].eq("found")
    for field in schema["found_required_fields"]:
        missing_found = found & ~_nonempty(checked[field])
        if missing_found.any():
            ids = checked.loc[missing_found, "masked_candidate_id"].astype(str).tolist()
            raise ValueError(f"found rows missing {field}: {ids[:5]}")
    bad_url = found & ~checked["campaign_website_url"].map(_valid_http_url)
    if bad_url.any():
        ids = checked.loc[bad_url, "masked_candidate_id"].astype(str).tolist()
        raise ValueError(f"found rows contain invalid campaign_website_url: {ids[:5]}")
    for field in schema["date_fields"]:
        present = found & _nonempty(checked[field])
        invalid_date = present & pd.to_datetime(checked[field], errors="coerce").isna()
        if invalid_date.any():
            ids = checked.loc[invalid_date, "masked_candidate_id"].astype(str).tolist()
            raise ValueError(f"found rows contain invalid {field}: {ids[:5]}")

    eligibility = schema["event_specific_eligibility"]
    event_specific = checked["match_status"].eq(eligibility["match_status"])
    event_specific &= checked["historical_version_status"].eq(
        eligibility["historical_version_status"]
    )
    for field in eligibility["required_fields"]:
        event_specific &= _nonempty(checked[field])
    event_specific &= checked["campaign_website_url"].map(_valid_http_url)
    checked["event_specific_eligible"] = event_specific
    return checked


def _write_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False, lineterminator="\n")


def _source_label(source_path: Path) -> str:
    try:
        return source_path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
    except ValueError:
        return source_path.resolve().as_posix()


def build(
    *,
    source_path: Path = DEFAULT_SOURCE,
    parent_screen_path: Path = DEFAULT_PARENT_SCREEN,
    output_root: Path = WORKSTREAM_ROOT,
    schema_path: Path = DEFAULT_SCHEMA,
    seed: int = DEFAULT_SEED,
    quota_per_year: int = DEFAULT_QUOTA_PER_YEAR,
) -> dict[str, Any]:
    source_path = Path(source_path)
    parent_screen_path = Path(parent_screen_path)
    output_root = Path(output_root)
    schema_path = Path(schema_path)
    source = pd.read_csv(source_path, low_memory=False)
    parent_source = pd.read_csv(parent_screen_path, low_memory=False)
    frozen = freeze_unresolved_frame(source)
    parent = validate_parent_screen(frozen, parent_source)
    shareable, crosswalk = draw_masked_sample(
        frozen, parent, seed=seed, quota_per_year=quota_per_year
    )
    schema = json.loads(schema_path.read_text(encoding="utf-8"))

    data_dir = output_root / "data"
    paths = {
        "frozen_unresolved_frame": data_dir / "frozen_unresolved_2010_2021.csv",
        "masked_provider_sample": data_dir / "masked_provider_sample_100.csv",
        "internal_sample_crosswalk": data_dir / "internal_masked_sample_crosswalk.csv",
        "provider_response_template": data_dir / "provider_response_template.csv",
    }
    _write_csv(frozen, paths["frozen_unresolved_frame"])
    _write_csv(shareable, paths["masked_provider_sample"])
    _write_csv(crosswalk, paths["internal_sample_crosswalk"])
    _write_csv(pd.DataFrame(columns=schema["required_columns"]), paths["provider_response_template"])

    freeze_counts = {str(k): int(v) for k, v in frozen.groupby("year").size().items()}
    provider_frame = frozen.loc[frozen["year"].between(PILOT_START_YEAR, PILOT_END_YEAR)]
    parent_provider_era = parent.loc[
        parent["year"].between(PILOT_START_YEAR, PILOT_END_YEAR)
        & parent["period"].eq("2018_2021")
    ]
    sample_counts = {str(k): int(v) for k, v in shareable.groupby("year").size().items()}
    generated_files = {
        key: {
            "path": path.relative_to(output_root).as_posix(),
            "sha256": sha256_file(path),
            "rows": int(pd.read_csv(path).shape[0]) if path.name != "provider_response_template.csv" else 0,
        }
        for key, path in paths.items()
    }
    manifest: dict[str, Any] = {
        "manifest_version": "provider-feasibility-build-v1",
        "generated_on": "2026-08-25",
        "unresolved_frame_source": {
            "path": _source_label(source_path),
            "sha256": sha256_file(source_path),
            "rows": int(len(source)),
        },
        "parent_screen_source": {
            "path": _source_label(parent_screen_path),
            "sha256": sha256_file(parent_screen_path),
            "rows": int(len(parent_source)),
        },
        "schema": {
            "path": schema_path.resolve().relative_to(REPO_ROOT.resolve()).as_posix()
            if schema_path.resolve().is_relative_to(REPO_ROOT.resolve())
            else schema_path.resolve().as_posix(),
            "sha256": sha256_file(schema_path),
            "schema_version": schema["schema_version"],
        },
        "sampling": {
            "seed": seed,
            "freeze_years": [FREEZE_START_YEAR, FREEZE_END_YEAR],
            "pilot_years": [PILOT_START_YEAR, PILOT_END_YEAR],
            "quota_per_year": quota_per_year,
            "frozen_unresolved_rows": int(len(frozen)),
            "frozen_unresolved_by_year": freeze_counts,
            "primary_provider_frame_rows": int(len(provider_frame)),
            "parent_screen_rows": int(len(parent)),
            "parent_provider_era_rows": int(len(parent_provider_era)),
            "sample_rows": int(len(shareable)),
            "sample_by_year": sample_counts,
            "democracy_works_requested_rows": int(shareable["democracy_works_requested"].sum()),
            "sample_is_exact_parent_subset": bool(
                set(crosswalk["candidate_year_id"]).issubset(
                    set(parent_provider_era["candidate_year_id"])
                )
            ),
            "unresolved_frame_has_covered_rows": bool(_truthy(frozen["covered"]).any()),
            "parent_screen_has_covered_rows": bool(_truthy(parent["covered"]).any()),
            "masking_definition": "Internal candidate/contest IDs and all known URL/outcome fields are withheld; candidate name and event context are retained because providers need them to match records.",
        },
        "filter_audit": [
            {
                "filter": "inherit common candidate screening sample",
                "before": int(len(frozen)),
                "after": int(len(parent)),
                "dropped": int(len(frozen) - len(parent)),
                "drop_share": round((len(frozen) - len(parent)) / len(frozen), 6),
                "expected": True,
                "reason": "This provider pilot is nested in the approved common 480-case acquisition screen rather than drawing from a separate universe.",
            },
            {
                "filter": "provider years 2018-2021 within common screen",
                "before": int(len(parent)),
                "after": int(len(parent_provider_era)),
                "dropped": int(len(parent) - len(parent_provider_era)),
                "drop_share": round(
                    (len(parent) - len(parent_provider_era)) / len(parent), 6
                ),
                "expected": True,
                "reason": "BallotReady and Ballotpedia public product documentation supports the primary test from 2018; Democracy Works is tested only for 2020-2021.",
            },
            {
                "filter": "deterministic provider subsample",
                "before": int(len(parent_provider_era)),
                "after": int(len(shareable)),
                "dropped": int(len(parent_provider_era) - len(shareable)),
                "drop_share": round(
                    (len(parent_provider_era) - len(shareable))
                    / len(parent_provider_era),
                    6,
                ),
                "expected": True,
                "reason": "Provider feasibility pilot size: 25 candidate-years from each of four years.",
            },
        ],
        "generated_files": generated_files,
    }
    manifest_path = output_root / "sample_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=DEFAULT_SOURCE)
    parser.add_argument("--parent-screen", type=Path, default=DEFAULT_PARENT_SCREEN)
    parser.add_argument("--output-root", type=Path, default=WORKSTREAM_ROOT)
    parser.add_argument("--schema", type=Path, default=DEFAULT_SCHEMA)
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--quota-per-year", type=int, default=DEFAULT_QUOTA_PER_YEAR)
    args = parser.parse_args()
    manifest = build(
        source_path=args.source,
        parent_screen_path=args.parent_screen,
        output_root=args.output_root,
        schema_path=args.schema,
        seed=args.seed,
        quota_per_year=args.quota_per_year,
    )
    print(json.dumps(manifest["sampling"], indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
