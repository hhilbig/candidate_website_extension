#!/usr/bin/env python3
"""Combine archive-discovery shards and produce coverage/audit datasets."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.mayoral_discovery import canonical_http_url, hostname, is_candidate_campaign_host, site_root_url


MANIFEST_KEYS = {
    "repo_root_relative", "universe_files", "evidence_files", "status_files",
    "election_page_status_files", "attempt_status_files",
}


def _read_combine_inputs(
    universe_paths: list[Path], evidence_paths: list[Path], status_paths: list[Path],
    election_page_status_paths: list[Path],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    universes = [pd.read_csv(path, low_memory=False) for path in universe_paths]
    evidence_parts = [pd.read_csv(path) for path in evidence_paths]
    status_parts = [pd.read_csv(path) for path in status_paths]
    election_page_status_parts = [
        pd.read_csv(path, dtype={"contest": "string"})
        for path in election_page_status_paths
    ]
    if not universes or not evidence_parts or not status_parts:
        raise ValueError("Combine inputs require universe, evidence, and status files")
    return _combine_frames(universes, evidence_parts, status_parts, election_page_status_parts)


def _resolve_manifest_paths(manifest_path: Path, values: list[str], repo_relative: bool) -> list[Path]:
    base = REPO_ROOT if repo_relative else manifest_path.parent
    return [(base / value).resolve() if not Path(value).is_absolute() else Path(value).resolve()
            for value in values]


def _flatten_registry_paths(value) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for value_item in value for item in _flatten_registry_paths(value_item)]
    if isinstance(value, dict):
        return [item for value_item in value.values() for item in _flatten_registry_paths(value_item)]
    return []


def enabled_registry_outputs(registry_path: Path) -> set[Path]:
    """Resolve output files declared by enabled source-registry entries."""
    payload = json.loads(registry_path.read_text(encoding="utf-8"))
    sources = payload.get("sources", []) if isinstance(payload, dict) else payload
    if not isinstance(sources, list):
        raise ValueError("Source registry must be a list or contain a sources list")
    output_paths: set[Path] = set()
    for source in sources:
        if not isinstance(source, dict) or not source.get("enabled", False):
            continue
        for value in _flatten_registry_paths(source.get("output_files", [])):
            path = Path(value)
            output_paths.add((REPO_ROOT / path).resolve() if not path.is_absolute() else path.resolve())
    return output_paths


def combine_manifest(
    manifest_path: Path, source_registry_path: Path | None = None
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Combine only the files explicitly named by a build manifest."""
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Build manifest must be a JSON object")
    unexpected_keys = set(payload) - MANIFEST_KEYS
    if unexpected_keys:
        raise ValueError(f"Unexpected build manifest keys: {sorted(unexpected_keys)}")
    required = ["universe_files", "evidence_files", "status_files"]
    missing_keys = [key for key in required if key not in payload]
    if missing_keys:
        raise ValueError(f"Build manifest missing keys: {missing_keys}")
    repo_relative = bool(payload.get("repo_root_relative", False))
    resolved = {
        key: _resolve_manifest_paths(manifest_path, payload.get(key, []), repo_relative)
        for key in required + ["election_page_status_files", "attempt_status_files"]
    }
    within_list_duplicates = {
        key: sorted(str(path) for path in set(paths) if paths.count(path) > 1)
        for key, paths in resolved.items()
    }
    within_list_duplicates = {
        key: paths for key, paths in within_list_duplicates.items() if paths
    }
    if within_list_duplicates:
        raise ValueError(
            f"Duplicate build manifest file paths within lists: {within_list_duplicates}"
        )
    occurrences: dict[Path, set[str]] = {}
    for key, paths in resolved.items():
        for path in paths:
            occurrences.setdefault(path, set()).add(key)
    allowed_dual_roles = {
        frozenset({"status_files", "attempt_status_files"}),
        frozenset({"election_page_status_files", "attempt_status_files"}),
    }
    invalid_cross_roles = {
        str(path): sorted(keys)
        for path, keys in occurrences.items()
        if len(keys) > 1 and frozenset(keys) not in allowed_dual_roles
    }
    if invalid_cross_roles:
        raise ValueError(
            f"Build manifest files have incompatible roles: {invalid_cross_roles}"
        )
    all_paths = list(occurrences)
    missing_files = sorted(str(path) for path in all_paths if not path.is_file())
    if missing_files:
        raise ValueError(f"Build manifest files are missing: {missing_files}")
    unexpected_files = [
        str(path) for key, paths in resolved.items() for path in paths
        if path.suffix.casefold() != ".csv"
        or (key == "universe_files" and path.name != "mayoral_candidate_universe.csv")
        or (key == "evidence_files" and not path.name.startswith("mayoral_url_evidence"))
        or (key == "status_files" and path.name != "mayoral_source_status.csv")
        or (key == "election_page_status_files" and path.name != "mayoral_source_status_election_pages.csv")
    ]
    if unexpected_files:
        raise ValueError(f"Unexpected configured build files: {sorted(unexpected_files)}")
    if source_registry_path is not None:
        enabled_outputs = enabled_registry_outputs(source_registry_path)
        adapter_paths = set(
            resolved["evidence_files"] + resolved["status_files"]
            + resolved["election_page_status_files"] + resolved["attempt_status_files"]
        )
        disabled_or_unregistered = sorted(str(path) for path in adapter_paths - enabled_outputs)
        if disabled_or_unregistered:
            raise ValueError(
                "Build manifest includes disabled or unregistered source outputs: "
                f"{disabled_or_unregistered}"
            )
    return _read_combine_inputs(
        resolved["universe_files"], resolved["evidence_files"],
        resolved["status_files"], resolved["election_page_status_files"],
    )


def combine_shards(shard_root: Path, shard_count: int) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    universe_paths, evidence_paths, status_paths, election_page_status_paths = [], [], [], []
    for index in range(shard_count):
        directory = shard_root / f"shard_{index}"
        universe_paths.append(directory / "mayoral_candidate_universe.csv")
        evidence_paths.append(directory / "mayoral_url_evidence.csv")
        status_paths.append(directory / "mayoral_source_status.csv")
        election_page_status_path = directory / "mayoral_source_status_election_pages.csv"
        if election_page_status_path.exists():
            election_page_status_paths.append(election_page_status_path)
    missing = [str(path) for path in universe_paths + evidence_paths + status_paths if not path.is_file()]
    if missing:
        raise ValueError(f"Shard build files are missing: {missing}")
    return _read_combine_inputs(
        universe_paths, evidence_paths, status_paths, election_page_status_paths
    )


def _combine_frames(
    universes: list[pd.DataFrame], evidence_parts: list[pd.DataFrame],
    status_parts: list[pd.DataFrame], election_page_status_parts: list[pd.DataFrame],
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:

    universe = universes[0]
    for column in [
        "election_stage_status", "election_stage_source_url", "election_stage_note",
        "stage_scope_status",
    ]:
        if column in universe:
            universe[column] = universe[column].fillna("").astype("object")
    expected_ids = set(universe.loc[
        universe["in_discovery_scope"] & universe["ballotpedia_url"].notna(),
        "candidate_year_id",
    ].astype(str))
    for other in universes[1:]:
        if set(other["candidate_contest_id"].astype(str)) != set(universe["candidate_contest_id"].astype(str)):
            raise ValueError("Shard universe files disagree")

    status = pd.concat(status_parts, ignore_index=True)
    if status["candidate_year_id"].astype(str).duplicated().any():
        duplicates = status.loc[
            status["candidate_year_id"].astype(str).duplicated(False), "candidate_year_id"
        ].tolist()
        raise ValueError(f"Duplicate candidate-year status rows: {duplicates[:5]}")
    observed_ids = set(status["candidate_year_id"].astype(str))
    if observed_ids != expected_ids:
        raise ValueError(
            f"Discovery incomplete: expected {len(expected_ids)} statuses, found {len(observed_ids)}; "
            f"missing={len(expected_ids - observed_ids)}, unexpected={len(observed_ids - expected_ids)}"
        )

    evidence = pd.concat(evidence_parts, ignore_index=True)
    context_columns = ["candidate_year_id", "candidate_contest_id", "contest", "month"]
    if universe["candidate_year_id"].astype(str).duplicated().any():
        raise ValueError("Universe has duplicate candidate-year rows; evidence context is ambiguous")
    evidence = evidence.drop(columns=[
        column for column in context_columns[1:] if column in evidence.columns
    ]).merge(
        universe[context_columns], on="candidate_year_id", how="left", validate="many_to_one"
    )
    if evidence["candidate_contest_id"].isna().any():
        raise ValueError("Evidence contains candidate-year IDs absent from the universe")
    evidence["validated_campaign_url"] = evidence["evidence_url"].map(canonical_http_url)
    evidence["campaign_host"] = evidence["validated_campaign_url"].map(hostname)
    evidence["campaign_site_url"] = evidence["validated_campaign_url"].map(site_root_url)
    evidence["validation_status"] = evidence["validated_campaign_url"].map(
        lambda value: "valid_source_backed_campaign_url" if is_candidate_campaign_host(value) else "invalid_or_excluded_host"
    )
    claim_key = [
        column for column in [
            "candidate_year_id", "validated_campaign_url", "url_source",
            "source_profile_url", "source_snapshot_timestamp", "discovery_method",
            "evidence_text",
        ] if column in evidence.columns
    ]
    evidence = evidence.drop_duplicates(claim_key).sort_values(
        ["year", "state_abb", "geo_name", "full_name", "validated_campaign_url"]
    )
    universe["election_page_source_status"] = "not_attempted"
    universe["election_page_source_url"] = ""
    universe["election_page_snapshots_checked"] = 0
    universe["election_page_candidate_urls_found"] = 0
    universe["election_page_error"] = ""
    universe["election_page_stage"] = ""
    universe["election_page_stage_label"] = ""
    universe["election_page_election_date"] = ""
    universe["election_page_stage_confidence"] = ""
    universe["election_page_stage_extraction_status"] = "not_attempted"
    if election_page_status_parts:
        page_status = pd.concat(election_page_status_parts, ignore_index=True)
        if page_status["contest"].astype(str).duplicated().any():
            duplicates = page_status.loc[
                page_status["contest"].astype(str).duplicated(False), "contest"
            ].astype(str).tolist()
            raise ValueError(f"Duplicate election-page contest status rows: {duplicates[:5]}")
        page_status = page_status.rename(columns={
            "source_status":"election_page_source_status_curated",
            "source_profile_url":"election_page_source_url_curated",
            "source_snapshots_checked":"election_page_snapshots_checked_curated",
            "candidate_urls_found":"election_page_candidate_urls_found_curated",
            "error":"election_page_error_curated",
            "source_election_stage":"election_page_stage_curated",
            "source_election_stage_label":"election_page_stage_label_curated",
            "source_election_date":"election_page_election_date_curated",
            "source_election_stage_confidence":"election_page_stage_confidence_curated",
            "stage_extraction_status":"election_page_stage_extraction_status_curated",
        })
        keep = ["contest"] + [column for column in page_status.columns if column.endswith("_curated")]
        universe = universe.merge(page_status[keep], on="contest", how="left", validate="many_to_one")
        for column in [
            "election_page_source_status", "election_page_source_url",
            "election_page_snapshots_checked", "election_page_candidate_urls_found",
            "election_page_error",
            "election_page_stage", "election_page_stage_label",
            "election_page_election_date", "election_page_stage_confidence",
            "election_page_stage_extraction_status",
        ]:
            curated = f"{column}_curated"
            if curated not in universe:
                continue
            universe[column] = universe[curated].fillna(universe[column])
            universe = universe.drop(columns=curated)
        use_stage = (
            universe["stage_scope_status"].eq("stage_unresolved")
            & universe["election_page_stage"].fillna("").ne("")
        )
        universe.loc[use_stage, "election_stage_status"] = universe.loc[
            use_stage, "election_page_stage"
        ]
        universe.loc[use_stage, "election_stage_source_url"] = universe.loc[
            use_stage, "election_page_source_url"
        ]
        universe.loc[use_stage, "election_stage_note"] = (
            "Ballotpedia archived election page date-matched label: "
            + universe.loc[use_stage, "election_page_stage_label"].astype(str)
            + " (" + universe.loc[use_stage, "election_page_election_date"].astype(str) + ")"
        )
        universe.loc[use_stage, "stage_scope_status"] = "verified_non_general_or_special"
        general = use_stage & universe["election_stage_status"].eq("verified_general_election")
        universe.loc[general, "stage_scope_status"] = "verified_general"
        universe["in_verified_general_scope"] = (
            universe["in_discovery_scope"]
            & universe["stage_scope_status"].eq("verified_general")
        )
    return universe, evidence.reset_index(drop=True), status.sort_values("candidate_year_id").reset_index(drop=True)


def build_candidate_summary(
    universe: pd.DataFrame, evidence: pd.DataFrame, status: pd.DataFrame
) -> pd.DataFrame:
    scoped = universe[universe["in_discovery_scope"]].copy()
    scoped = scoped.sort_values("candidate_contest_id").drop_duplicates("candidate_year_id")
    valid = evidence[evidence["validation_status"].eq("valid_source_backed_campaign_url")]
    url_counts = valid.groupby("candidate_year_id").agg(
        campaign_url_count=("campaign_site_url", "nunique"),
        campaign_urls=("campaign_site_url", lambda values: " | ".join(sorted(set(values)))),
        campaign_hosts=("campaign_host", lambda values: " | ".join(sorted(set(values)))),
        url_sources=("url_source", lambda values: " | ".join(sorted(set(values)))),
        confidence_levels=("confidence", lambda values: " | ".join(sorted(set(values)))),
    ).reset_index()
    columns = [
        "candidate_contest_id", "candidate_year_id", "ledb_candid", "full_name", "geo_name", "state_abb",
        "year", "month", "contest", "population_2020", "population_status", "election_stage_status",
        "election_stage_source_url", "election_stage_note", "stage_scope_status",
        "in_verified_general_scope",
        "direct_election_status", "contest_validation_status",
        "election_page_source_status", "election_page_source_url",
        "election_page_snapshots_checked", "election_page_candidate_urls_found",
        "election_page_error",
        "election_page_stage", "election_page_stage_label",
        "election_page_election_date", "election_page_stage_confidence",
        "election_page_stage_extraction_status",
        "ballotpedia_url", "source_dataset", "source_doi",
    ]
    summary = scoped[columns].merge(status, on="candidate_year_id", how="left", suffixes=("", "_status"))
    summary = summary.merge(url_counts, on="candidate_year_id", how="left")
    summary["campaign_url_count"] = summary["campaign_url_count"].fillna(0).astype(int)
    summary["has_validated_campaign_url"] = summary["campaign_url_count"].gt(0)
    summary["discovery_status"] = summary["source_status"].fillna("no_national_profile_source")
    summary.loc[summary["has_validated_campaign_url"], "discovery_status"] = "source_backed_url_found"
    summary["unresolved_reason"] = ""
    unresolved = ~summary["has_validated_campaign_url"]
    summary.loc[unresolved, "unresolved_reason"] = summary.loc[unresolved, "discovery_status"]
    summary.loc[
        unresolved & summary["discovery_status"].eq("profile_checked"),
        "unresolved_reason",
    ] = "profile_checked_no_campaign_url_evidence"
    summary.loc[
        unresolved & summary["election_page_source_status"].eq("page_checked"),
        "unresolved_reason",
    ] = "available_sources_checked_no_campaign_url_evidence"
    summary.loc[
        unresolved & summary["election_page_source_status"].eq("no_archived_page"),
        "unresolved_reason",
    ] = "no_archived_ballotpedia_election_page"
    summary.loc[
        unresolved & summary["election_page_source_status"].eq("error"),
        "unresolved_reason",
    ] = "ballotpedia_election_page_archive_error"
    return summary.sort_values(["year", "state_abb", "geo_name", "full_name"]).reset_index(drop=True)


def coverage_metrics(summary: pd.DataFrame, evidence: pd.DataFrame) -> dict:
    total = len(summary)
    with_profile = int(summary["ballotpedia_url"].notna().sum())
    with_url = int(summary["has_validated_campaign_url"].sum())
    profiled_with_url = int(
        (summary["ballotpedia_url"].notna() & summary["has_validated_campaign_url"]).sum()
    )
    unresolved_counts = summary.loc[
        ~summary["has_validated_campaign_url"], "unresolved_reason"
    ].value_counts(dropna=False).to_dict()
    source_candidate_counts = (
        evidence.loc[evidence["validation_status"].eq("valid_source_backed_campaign_url")]
        .groupby("url_source")["candidate_year_id"].nunique().sort_values(ascending=False).to_dict()
    )
    stage_counts = summary["stage_scope_status"].value_counts(dropna=False).to_dict()
    strict = summary[summary["in_verified_general_scope"]]
    return {
        "candidate_years_in_scope": total,
        "contests_in_scope": int(summary["candidate_contest_id"].str.rsplit("__", n=1).str[0].nunique()),
        "jurisdictions_in_scope": int(summary[["state_abb", "geo_name"]].drop_duplicates().shape[0]),
        "candidate_years_with_ballotpedia_profile": with_profile,
        "profile_source_coverage_rate": with_profile / total if total else 0,
        "candidate_years_with_validated_campaign_url": with_url,
        "candidate_years_without_validated_campaign_url": total - with_url,
        "overall_url_coverage_rate": with_url / total if total else 0,
        "profiled_candidate_years_with_validated_campaign_url": profiled_with_url,
        "url_yield_among_profiled_candidates": profiled_with_url / with_profile if with_profile else 0,
        "unique_validated_campaign_urls": int(evidence.loc[
            evidence["validation_status"].eq("valid_source_backed_campaign_url"),
            "campaign_site_url",
        ].nunique()),
        "unique_campaign_hosts": int(evidence.loc[
            evidence["validation_status"].eq("valid_source_backed_campaign_url"),
            "campaign_host",
        ].nunique()),
        "source_errors": int(summary["discovery_status"].eq("error").sum()),
        "election_page_source_errors": int(summary["election_page_source_status"].eq("error").sum()),
        "candidate_years_with_election_page_checked": int(
            summary["election_page_source_status"].eq("page_checked").sum()
        ),
        "candidate_years_without_archived_election_page": int(
            summary["election_page_source_status"].eq("no_archived_page").sum()
        ),
        "verified_general_candidate_years": int(len(strict)),
        "verified_general_contests": int(
            strict["candidate_contest_id"].str.rsplit("__", n=1).str[0].nunique()
        ),
        "verified_general_candidate_years_with_validated_campaign_url": int(
            strict["has_validated_campaign_url"].sum()
        ),
        "verified_general_url_coverage_rate": (
            float(strict["has_validated_campaign_url"].mean()) if len(strict) else 0
        ),
        "candidate_years_by_stage_scope_status": {
            str(key): int(value) for key, value in stage_counts.items()
        },
        "stage_not_explicit_candidate_years": int(summary["election_stage_status"].str.endswith("stage_not_explicit").sum()),
        "candidate_years_in_anomalous_winner_contests": int(summary["contest_validation_status"].ne("valid_single_winner").sum()),
        "unresolved_candidate_years_by_reason": {str(key): int(value) for key, value in unresolved_counts.items()},
        "candidate_years_with_url_by_source": {str(key): int(value) for key, value in source_candidate_counts.items()},
    }


def coverage_by_year(summary: pd.DataFrame) -> pd.DataFrame:
    return summary.groupby("year", as_index=False).agg(
        candidate_years_in_scope=("candidate_year_id", "nunique"),
        candidate_years_with_profile=("ballotpedia_url", lambda values: int(values.notna().sum())),
        candidate_years_with_url=("has_validated_campaign_url", "sum"),
        verified_general_candidate_years=("in_verified_general_scope", "sum"),
        stage_unresolved_candidate_years=(
            "stage_scope_status", lambda values: int(values.eq("stage_unresolved").sum())
        ),
    ).assign(
        url_coverage_rate=lambda frame: frame.candidate_years_with_url / frame.candidate_years_in_scope
    )


def evidence_source_summary(evidence: pd.DataFrame) -> pd.DataFrame:
    valid = evidence[evidence["validation_status"].eq("valid_source_backed_campaign_url")]
    return valid.groupby("url_source", as_index=False).agg(
        evidence_claims=("validated_campaign_url", "size"),
        candidate_years=("candidate_year_id", "nunique"),
        unique_campaign_sites=("campaign_site_url", "nunique"),
    ).sort_values(["candidate_years", "url_source"], ascending=[False, True])


def coverage_by_stage(summary: pd.DataFrame) -> pd.DataFrame:
    """Keep strict-general and stage-unresolved denominators visibly separate."""
    result = summary.groupby("stage_scope_status", as_index=False).agg(
        candidate_years=("candidate_year_id", "nunique"),
        contests=("candidate_contest_id", lambda values: values.str.rsplit("__", n=1).str[0].nunique()),
        candidate_years_with_url=("has_validated_campaign_url", "sum"),
    )
    result["url_coverage_rate"] = (
        result["candidate_years_with_url"] / result["candidate_years"]
    )
    return result.sort_values("stage_scope_status").reset_index(drop=True)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--manifest", default="config/mayoral_discovery_build_manifest.json"
    )
    parser.add_argument(
        "--source-registry", default="config/mayoral_url_source_registry.json"
    )
    parser.add_argument("--output-dir", default="data/municipal_discovery/final")
    args = parser.parse_args()

    output = Path(args.output_dir)
    output.mkdir(parents=True, exist_ok=True)
    universe, evidence, status = combine_manifest(
        Path(args.manifest), Path(args.source_registry)
    )
    summary = build_candidate_summary(universe, evidence, status)
    metrics = coverage_metrics(summary, evidence)
    universe.to_csv(output / "mayoral_candidate_universe.csv", index=False)
    evidence.to_csv(output / "mayoral_url_evidence.csv", index=False)
    status.to_csv(output / "mayoral_source_status.csv", index=False)
    summary.to_csv(output / "mayoral_candidate_url_summary.csv", index=False)
    coverage_by_year(summary).to_csv(output / "mayoral_coverage_by_year.csv", index=False)
    coverage_by_stage(summary).to_csv(output / "mayoral_coverage_by_stage.csv", index=False)
    evidence_source_summary(evidence).to_csv(output / "mayoral_evidence_source_summary.csv", index=False)
    (output / "mayoral_coverage_summary.json").write_text(
        json.dumps(metrics, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(metrics, indent=2))


if __name__ == "__main__":
    main()
