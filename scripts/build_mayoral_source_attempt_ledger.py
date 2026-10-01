#!/usr/bin/env python3
"""Build an exact contest-by-source universe and terminal source-attempt ledger."""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import tempfile
from pathlib import Path
from typing import Any

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))


TERMINAL_STATUSES = {
    "found", "no_result", "source_unavailable", "request_error",
    "match_unresolved",
}
LEDGER_PARSER_VERSION = "mayoral-source-attempt-ledger-v1"

CONTEST_UNIVERSE_COLUMNS = [
    "contest_source_target_id", "source_id", "contest_id", "geo_name",
    "state_abb", "place_fips", "year", "month", "election_round",
    "contest_n_candidates", "candidate_year_ids", "adapter",
    "source_scope_years", "source_scope_jurisdictions",
    "expected_parser_version", "ledger_parser_version",
]
LEDGER_COLUMNS = [
    "attempt_id", "contest_source_target_id", "source_id", "contest_id",
    "geo_name", "state_abb", "place_fips", "year", "month",
    "election_round", "source_status", "raw_source_statuses",
    "source_attempt_ids", "source_parser_versions", "mapping_version",
    "adapter", "retrieved_on", "error", "attempt_file",
    "source_attempt_json", "ledger_parser_version",
]
FILTER_REPORT_COLUMNS = [
    "source_id", "filter_name", "before", "after", "dropped",
    "drop_rate", "exceeds_20_percent",
]

STATE_ABBREVIATIONS = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "florida": "FL", "georgia": "GA", "hawaii": "HI",
    "idaho": "ID", "illinois": "IL", "indiana": "IN", "iowa": "IA",
    "kansas": "KS", "kentucky": "KY", "louisiana": "LA", "maine": "ME",
    "maryland": "MD", "massachusetts": "MA", "michigan": "MI",
    "minnesota": "MN", "mississippi": "MS", "missouri": "MO",
    "montana": "MT", "nebraska": "NE", "nevada": "NV",
    "new hampshire": "NH", "new jersey": "NJ", "new mexico": "NM",
    "new york": "NY", "north carolina": "NC", "north dakota": "ND",
    "ohio": "OH", "oklahoma": "OK", "oregon": "OR",
    "pennsylvania": "PA", "rhode island": "RI", "south carolina": "SC",
    "south dakota": "SD", "tennessee": "TN", "texas": "TX", "utah": "UT",
    "vermont": "VT", "virginia": "VA", "washington": "WA",
    "west virginia": "WV", "wisconsin": "WI", "wyoming": "WY",
    "district of columbia": "DC",
}


def _stable_id(kind: str, *parts: object) -> str:
    payload = "\x1f".join([kind, *(str(part) for part in parts)])
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _clean_scalar(value: Any) -> Any:
    if value is None or (not isinstance(value, (list, dict)) and pd.isna(value)):
        return None
    if not isinstance(value, str) and hasattr(value, "isoformat"):
        return value.isoformat()
    if hasattr(value, "item"):
        return value.item()
    return value


def _json_record(record: dict[str, Any]) -> dict[str, Any]:
    return {str(key): _clean_scalar(value) for key, value in record.items()}


def _write_csv_atomic(frame: pd.DataFrame, path: Path, columns: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile(
        mode="w", suffix=".csv", dir=path.parent, delete=False,
        encoding="utf-8", newline="",
    ) as handle:
        temporary = Path(handle.name)
        frame.reindex(columns=columns).to_csv(handle, index=False)
    temporary.replace(path)


def _consistent_value(group: pd.DataFrame, column: str, contest_id: str) -> Any:
    if column not in group:
        return pd.NA
    values = group[column].dropna().drop_duplicates().tolist()
    if len(values) > 1:
        raise ValueError(
            f"Contest {contest_id} has inconsistent {column} values: {values[:5]}"
        )
    return values[0] if values else pd.NA


def _as_scope_mask(values: pd.Series) -> pd.Series:
    if pd.api.types.is_bool_dtype(values):
        return values.fillna(False)
    normalized = values.astype("string").str.casefold()
    return normalized.isin({"true", "1", "yes"})


def _filter_record(
    rows: list[dict[str, Any]], source_id: str, name: str, before: int, after: int,
) -> None:
    dropped = before - after
    rate = dropped / before if before else 0.0
    rows.append({
        "source_id": source_id,
        "filter_name": name,
        "before": int(before),
        "after": int(after),
        "dropped": int(dropped),
        "drop_rate": float(rate),
        "exceeds_20_percent": bool(rate > 0.20),
    })


def _base_contests(
    universe: pd.DataFrame, filter_rows: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    required = {
        "candidate_year_id", "contest", "geo_name", "state_abb", "year",
        "month", "in_discovery_scope",
    }
    missing = required - set(universe.columns)
    if missing:
        raise ValueError(f"Authoritative universe missing columns: {sorted(missing)}")
    if universe["candidate_year_id"].astype(str).duplicated().any():
        raise ValueError("Authoritative universe has duplicate candidate-year rows")
    scoped = universe.loc[_as_scope_mask(universe["in_discovery_scope"])].copy()
    _filter_record(
        filter_rows, "__authoritative_universe__", "in_discovery_scope_candidate_rows",
        len(universe), len(scoped),
    )
    scoped["contest_id"] = scoped["contest"].astype("string")
    if scoped["contest_id"].isna().any() or scoped["contest_id"].str.strip().eq("").any():
        raise ValueError("In-scope universe contains a missing contest ID")
    rows: list[dict[str, Any]] = []
    for contest_id, group in scoped.groupby("contest_id", sort=False, dropna=False):
        rows.append({
            "contest_id": str(contest_id),
            "geo_name": _consistent_value(group, "geo_name", str(contest_id)),
            "state_abb": _consistent_value(group, "state_abb", str(contest_id)),
            "place_fips": _consistent_value(group, "place_fips", str(contest_id)),
            "year": int(_consistent_value(group, "year", str(contest_id))),
            "month": _consistent_value(group, "month", str(contest_id)),
            "election_round": _consistent_value(
                group, "election_stage_status", str(contest_id)
            ),
            "contest_n_candidates": int(group["candidate_year_id"].nunique()),
            "candidate_year_ids": " | ".join(
                sorted(group["candidate_year_id"].astype(str).unique())
            ),
        })
    contests = pd.DataFrame(rows)
    if contests["contest_id"].duplicated().any():
        raise AssertionError("Contest universe construction produced duplicate contests")
    return contests, scoped


def _tracked_sources(registry: list[dict[str, Any]]) -> list[dict[str, Any]]:
    tracked: list[dict[str, Any]] = []
    seen: set[str] = set()
    for source in registry:
        if not isinstance(source, dict) or not source.get("enabled", False):
            continue
        source_id = str(source.get("source_id", "")).strip()
        if not source_id or source_id in seen:
            raise ValueError(f"Enabled registry has missing or duplicate source_id: {source_id!r}")
        seen.add(source_id)
        if "attempt_tracking" not in source:
            raise ValueError(
                f"Enabled source {source_id} must declare attempt_tracking true or false"
            )
        if not isinstance(source["attempt_tracking"], bool):
            raise ValueError(
                f"Source {source_id} attempt_tracking must be a JSON boolean"
            )
        if not source["attempt_tracking"]:
            if not str(source.get("attempt_tracking_reason", "")).strip():
                raise ValueError(
                    f"Source {source_id} sets attempt_tracking false without a reason"
                )
            continue
        tracked.append(source)
    if not tracked:
        raise ValueError("Registry has no enabled sources with attempt_tracking true")
    return tracked


def _year_mask(frame: pd.DataFrame, years: Any, source_id: str) -> pd.Series:
    if isinstance(years, dict) and {"start", "end"}.issubset(years):
        return frame["year"].between(int(years["start"]), int(years["end"]))
    if isinstance(years, list) and years:
        return frame["year"].isin([int(value) for value in years])
    raise ValueError(f"Source {source_id} has an invalid years scope: {years!r}")


def _jurisdiction_mask(
    frame: pd.DataFrame, jurisdictions: Any, source_id: str, all_in_scope: bool,
) -> pd.Series:
    if all_in_scope:
        return pd.Series(True, index=frame.index)
    if not isinstance(jurisdictions, list) or not jurisdictions:
        raise ValueError(f"Source {source_id} has no machine-readable jurisdiction scope")
    mask = pd.Series(False, index=frame.index)
    for jurisdiction in jurisdictions:
        if isinstance(jurisdiction, dict):
            city = str(jurisdiction.get("geo_name", "")).strip().casefold()
            state = str(jurisdiction.get("state_abb", "")).strip().upper()
            if not city or len(state) != 2:
                raise ValueError(
                    f"Source {source_id} has invalid structured jurisdiction: {jurisdiction}"
                )
            mask |= (
                frame["geo_name"].astype(str).str.casefold().eq(city)
                & frame["state_abb"].astype(str).str.upper().eq(state)
            )
            continue
        label = str(jurisdiction).strip()
        if label == "United States":
            mask |= True
            continue
        parts = [part.strip() for part in label.rsplit(",", 1)]
        state = STATE_ABBREVIATIONS.get(parts[1].casefold(), "") if len(parts) == 2 else ""
        if not state:
            raise ValueError(
                f"Source {source_id} jurisdiction is not machine-readable: {label!r}; "
                "use {'geo_name': ..., 'state_abb': ...} in attempt_scope.jurisdictions"
            )
        mask |= (
            frame["geo_name"].astype(str).str.casefold().eq(parts[0].casefold())
            & frame["state_abb"].astype(str).str.upper().eq(state)
        )
    return mask


def _candidate_filter_contests(
    scoped_candidates: pd.DataFrame, rule: dict[str, Any], source_id: str,
) -> set[str]:
    column = str(rule.get("column", ""))
    operator = str(rule.get("operator", ""))
    if column not in scoped_candidates:
        raise ValueError(f"Source {source_id} candidate filter column is missing: {column}")
    if operator == "notna":
        keep = scoped_candidates[column].notna()
    elif operator == "eq":
        keep = scoped_candidates[column].eq(rule.get("value"))
    elif operator == "truthy":
        keep = _as_scope_mask(scoped_candidates[column])
    else:
        raise ValueError(f"Source {source_id} has unsupported candidate filter: {operator}")
    return set(scoped_candidates.loc[keep, "contest_id"].astype(str))


def build_contest_source_universe(
    universe: pd.DataFrame, registry: list[dict[str, Any]],
) -> tuple[pd.DataFrame, pd.DataFrame, list[dict[str, Any]]]:
    """Cross authoritative contests with explicit scopes for tracked sources."""
    filter_rows: list[dict[str, Any]] = []
    contests, scoped_candidates = _base_contests(universe, filter_rows)
    tracked = _tracked_sources(registry)
    output: list[dict[str, Any]] = []
    for source in tracked:
        source_id = str(source["source_id"])
        scope = source.get("attempt_scope") or {}
        selected = contests.copy()
        before = len(selected)
        years = scope.get("years", source.get("years"))
        selected = selected.loc[_year_mask(selected, years, source_id)].copy()
        _filter_record(filter_rows, source_id, "years", before, len(selected))

        before = len(selected)
        jurisdictions = scope.get("jurisdictions", source.get("jurisdictions"))
        selected = selected.loc[_jurisdiction_mask(
            selected, jurisdictions, source_id, bool(scope.get("all_in_scope", False))
        )].copy()
        _filter_record(filter_rows, source_id, "jurisdictions", before, len(selected))

        if scope.get("candidate_filter"):
            before = len(selected)
            eligible = _candidate_filter_contests(
                scoped_candidates, scope["candidate_filter"], source_id
            )
            selected = selected[selected["contest_id"].isin(eligible)].copy()
            _filter_record(filter_rows, source_id, "candidate_filter", before, len(selected))
        if selected.empty:
            raise ValueError(f"Tracked source {source_id} has zero contest targets")
        mapping = source.get("attempt_status_mapping") or {}
        expected_parser = source.get("parser_version", mapping.get("parser_version", ""))
        for row in selected.to_dict("records"):
            contest_id = str(row["contest_id"])
            output.append({
                **row,
                "contest_source_target_id": _stable_id(
                    "contest_source_target", source_id, contest_id
                ),
                "source_id": source_id,
                "adapter": source.get("adapter", ""),
                "source_scope_years": json.dumps(years, sort_keys=True),
                "source_scope_jurisdictions": json.dumps(
                    jurisdictions, sort_keys=True
                ),
                "expected_parser_version": expected_parser,
                "ledger_parser_version": LEDGER_PARSER_VERSION,
            })
    targets = pd.DataFrame(output).reindex(columns=CONTEST_UNIVERSE_COLUMNS)
    if targets.duplicated(["source_id", "contest_id"]).any():
        raise AssertionError("Contest-source target construction produced duplicates")
    targets = targets.sort_values(
        ["source_id", "year", "month", "contest_id"], na_position="last"
    ).reset_index(drop=True)
    filters = pd.DataFrame(filter_rows).reindex(columns=FILTER_REPORT_COLUMNS)
    return targets, filters, tracked


def _resolve_repo_path(value: str, root: Path = REPO_ROOT) -> Path:
    path = Path(value)
    return path.resolve() if path.is_absolute() else (root / path).resolve()


def load_attempt_manifest(manifest_path: Path) -> list[Path]:
    payload = json.loads(manifest_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("Build manifest must be a JSON object")
    values = payload.get("attempt_status_files")
    if not isinstance(values, list) or not values:
        raise ValueError("Build manifest must list at least one attempt_status_file")
    base = REPO_ROOT if payload.get("repo_root_relative", False) else manifest_path.parent
    paths = [
        _resolve_repo_path(str(value), base)
        for value in values
    ]
    duplicates = sorted(str(path) for path in set(paths) if paths.count(path) > 1)
    if duplicates:
        raise ValueError(f"Duplicate attempt_status_files: {duplicates}")
    missing = sorted(str(path) for path in paths if not path.is_file())
    if missing:
        raise ValueError(f"Missing attempt_status_files: {missing}")
    unexpected = sorted(str(path) for path in paths if path.suffix.casefold() != ".csv")
    if unexpected:
        raise ValueError(f"Unexpected attempt status file types: {unexpected}")
    return paths


def _mapping_files(source: dict[str, Any], root: Path) -> set[Path]:
    mapping = source.get("attempt_status_mapping") or {}
    files = mapping.get("files", [])
    if isinstance(files, str):
        files = [files]
    return {_resolve_repo_path(str(value), root) for value in files}


def _registered_output_files(source: dict[str, Any], root: Path) -> set[Path]:
    files = source.get("output_files", [])
    if isinstance(files, str):
        files = [files]
    return {_resolve_repo_path(str(value), root) for value in files}


def _mapping_source_for_file(
    path: Path, tracked: list[dict[str, Any]], registry_root: Path,
) -> list[dict[str, Any]]:
    return [
        source for source in tracked
        if path in _mapping_files(source, registry_root)
    ]


def _resolve_contest_id(
    record: dict[str, Any], source_id: str, mapping: dict[str, Any],
    candidate_to_contest: dict[str, str], source_targets: pd.DataFrame,
) -> str:
    key = mapping.get("key") or {}
    contest_column = key.get("contest_id_column", "contest_id" if "contest_id" in record else "")
    if contest_column and pd.notna(record.get(contest_column)):
        return str(record[contest_column])
    candidate_column = key.get("candidate_year_id_column")
    if candidate_column and pd.notna(record.get(candidate_column)):
        candidate_id = str(record[candidate_column])
        if candidate_id not in candidate_to_contest:
            raise ValueError(
                f"Source {source_id} attempt has unknown candidate-year ID: {candidate_id}"
            )
        return candidate_to_contest[candidate_id]
    fallback = key.get("scope_year_unique")
    if fallback:
        year_column = str(fallback.get("year_column", "year"))
        if pd.isna(record.get(year_column)):
            raise ValueError(f"Source {source_id} fallback attempt lacks {year_column}")
        matches = source_targets[source_targets["year"].eq(int(record[year_column]))]
        if len(matches) != 1:
            raise ValueError(
                f"Source {source_id} scope-year fallback is not unique for "
                f"{record[year_column]}: {len(matches)} targets"
            )
        return str(matches.iloc[0]["contest_id"])
    raise ValueError(
        f"Source {source_id} attempt cannot be assigned to a contest; "
        "configure attempt_status_mapping.key"
    )


def _normalize_status(
    record: dict[str, Any], source_id: str, mapping: dict[str, Any],
    found_override: bool = False,
) -> tuple[str, str]:
    status_column = str(mapping.get("status_column", "source_status"))
    raw = str(record.get(status_column, "")).strip()
    mapped = (mapping.get("status_map") or {}).get(raw, raw)
    found_column = mapping.get("found_count_column")
    if found_column and mapping.get("found_when_positive", False):
        count = pd.to_numeric(pd.Series([record.get(found_column)]), errors="coerce").iloc[0]
        if pd.notna(count) and float(count) > 0:
            mapped = "found"
    if found_override:
        mapped = "found"
    if mapped not in TERMINAL_STATUSES:
        raise ValueError(
            f"Source {source_id} status {raw!r} does not map to one of "
            f"{sorted(TERMINAL_STATUSES)}"
        )
    return str(mapped), raw


def _found_evidence_keys(
    source: dict[str, Any], mapping: dict[str, Any], registry_root: Path,
) -> set[str]:
    files = mapping.get("found_evidence_files", [])
    if isinstance(files, str):
        files = [files]
    if not files:
        return set()
    key = str(mapping.get("found_evidence_key", ""))
    if key not in {"candidate_year_id", "contest_id"}:
        raise ValueError(
            f"Source {source['source_id']} found_evidence_key must be "
            "candidate_year_id or contest_id"
        )
    values: set[str] = set()
    for value in files:
        path = _resolve_repo_path(str(value), registry_root)
        if not path.is_file():
            raise ValueError(f"Source {source['source_id']} found evidence file is missing: {path}")
        frame = pd.read_csv(path, low_memory=False)
        column = key
        if column not in frame and key == "contest_id" and "contest" in frame:
            column = "contest"
        if column not in frame:
            raise ValueError(
                f"Source {source['source_id']} found evidence file {path} lacks {key}"
            )
        values.update(frame[column].dropna().astype(str))
    return values


def _normalize_source_rows(
    source: dict[str, Any], raw_rows: list[dict[str, Any]],
    targets: pd.DataFrame, candidate_to_contest: dict[str, str],
    registry_root: Path,
) -> list[dict[str, Any]]:
    source_id = str(source["source_id"])
    mapping = source.get("attempt_status_mapping") or {}
    found_keys = _found_evidence_keys(source, mapping, registry_root)
    found_key_type = str(mapping.get("found_evidence_key", ""))
    normalized: list[dict[str, Any]] = []
    for record in raw_rows:
        contest_id = _resolve_contest_id(
            record, source_id, mapping, candidate_to_contest, targets
        )
        if found_key_type == "contest_id":
            found_override = contest_id in found_keys
        elif found_key_type == "candidate_year_id":
            candidate_column = (mapping.get("key") or {}).get(
                "candidate_year_id_column", "candidate_year_id"
            )
            candidate_value = record.get(candidate_column)
            found_override = pd.notna(candidate_value) and str(candidate_value) in found_keys
        else:
            found_override = False
        status, raw_status = _normalize_status(
            record, source_id, mapping, found_override=found_override
        )
        parser_column = mapping.get("parser_version_column", "parser_version")
        parser_version = record.get(parser_column, mapping.get("parser_version", ""))
        if pd.isna(parser_version) or not str(parser_version).strip():
            parser_version = mapping.get("parser_version", source.get("parser_version", ""))
        if pd.isna(parser_version) or not str(parser_version).strip():
            raise ValueError(
                f"Source {source_id} attempt for {contest_id} lacks a parser version"
            )
        normalized.append({
            "source_id": source_id,
            "contest_id": contest_id,
            "source_status": status,
            "raw_source_status": raw_status,
            "source_attempt_id": record.get("attempt_id", ""),
            "source_parser_version": parser_version,
            "mapping_version": mapping.get("mapping_version", "standard_terminal_v1"),
            "retrieved_on": record.get(mapping.get("retrieved_on_column", "retrieved_on"), ""),
            "error": record.get(mapping.get("error_column", "error"), ""),
            "attempt_file": record["__attempt_file"],
            "raw_record": {key: value for key, value in record.items() if key != "__attempt_file"},
        })

    if not normalized:
        return []
    groups: list[dict[str, Any]] = []
    precedence = mapping.get("aggregation_precedence")
    if precedence is not None:
        if set(precedence) != TERMINAL_STATUSES or len(precedence) != len(TERMINAL_STATUSES):
            raise ValueError(
                f"Source {source_id} aggregation_precedence must list each terminal status once"
            )
        rank = {status: index for index, status in enumerate(precedence)}
    else:
        rank = {}
    for contest_id, contest_rows in pd.DataFrame(normalized).groupby("contest_id", sort=False):
        records = contest_rows.to_dict("records")
        if len(records) > 1 and precedence is None:
            raise ValueError(
                f"Duplicate source-attempt target without explicit aggregation for "
                f"{source_id}/{contest_id}"
            )
        selected_status = (
            min((item["source_status"] for item in records), key=rank.get)
            if precedence is not None else records[0]["source_status"]
        )
        groups.append({
            "source_id": source_id,
            "contest_id": str(contest_id),
            "source_status": selected_status,
            "raw_source_statuses": " | ".join(sorted({item["raw_source_status"] for item in records})),
            "source_attempt_ids": " | ".join(sorted({
                str(item["source_attempt_id"]) for item in records
                if pd.notna(item["source_attempt_id"]) and str(item["source_attempt_id"])
            })),
            "source_parser_versions": " | ".join(sorted({
                str(item["source_parser_version"]) for item in records
                if pd.notna(item["source_parser_version"]) and str(item["source_parser_version"])
            })),
            "mapping_version": str(records[0]["mapping_version"]),
            "retrieved_on": " | ".join(sorted({
                str(item["retrieved_on"]) for item in records
                if pd.notna(item["retrieved_on"]) and str(item["retrieved_on"])
            })),
            "error": " | ".join(sorted({
                str(item["error"]) for item in records
                if pd.notna(item["error"]) and str(item["error"])
            }))[:1000],
            "attempt_file": " | ".join(sorted({item["attempt_file"] for item in records})),
            "source_attempt_json": json.dumps(
                [_json_record(item["raw_record"]) for item in records],
                ensure_ascii=False, sort_keys=True,
            ),
        })
    return groups


def build_source_attempt_ledger(
    universe: pd.DataFrame,
    registry: list[dict[str, Any]],
    attempt_frames: dict[Path, pd.DataFrame],
    registry_root: Path = REPO_ROOT,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    targets, filters, tracked = build_contest_source_universe(universe, registry)
    tracked_by_id = {str(source["source_id"]): source for source in tracked}
    candidate_to_contest = dict(zip(
        universe.loc[_as_scope_mask(universe["in_discovery_scope"]), "candidate_year_id"].astype(str),
        universe.loc[_as_scope_mask(universe["in_discovery_scope"]), "contest"].astype(str),
    ))
    rows_by_source: dict[str, list[dict[str, Any]]] = {
        source_id: [] for source_id in tracked_by_id
    }
    configured_files = set()
    for source in tracked:
        configured_files |= _mapping_files(source, registry_root)
        configured_files |= _registered_output_files(source, registry_root)

    for raw_path, frame in attempt_frames.items():
        path = raw_path.resolve()
        if path not in configured_files:
            raise ValueError(f"Unexpected attempt status file not registered by an enabled source: {path}")
        mapping_sources = _mapping_source_for_file(path, tracked, registry_root)
        if "source_id" in frame.columns and frame["source_id"].notna().any():
            row_source_ids = frame["source_id"].astype("string")
        elif len(mapping_sources) == 1:
            row_source_ids = pd.Series(
                str(mapping_sources[0]["source_id"]), index=frame.index, dtype="string"
            )
        else:
            raise ValueError(
                f"Attempt file {path} lacks source_id and maps to "
                f"{len(mapping_sources)} registry sources"
            )
        for index, record in frame.to_dict("index").items():
            source_id = str(row_source_ids.loc[index])
            if source_id not in tracked_by_id:
                raise ValueError(
                    f"Attempt file {path} contains disabled or unexpected source_id {source_id}"
                )
            record["__attempt_file"] = str(path)
            rows_by_source[source_id].append(record)

    normalized: list[dict[str, Any]] = []
    for source_id, source in tracked_by_id.items():
        source_targets = targets[targets["source_id"].eq(source_id)]
        normalized.extend(_normalize_source_rows(
            source, rows_by_source[source_id], source_targets, candidate_to_contest,
            registry_root,
        ))
    normalized_frame = pd.DataFrame(normalized)
    if normalized_frame.empty:
        raise ValueError("No source-attempt rows were ingested")
    if normalized_frame.duplicated(["source_id", "contest_id"]).any():
        raise ValueError("Normalized ledger contains duplicate contest-source targets")

    target_keys = set(zip(targets["source_id"], targets["contest_id"].astype(str)))
    attempt_keys = set(zip(
        normalized_frame["source_id"], normalized_frame["contest_id"].astype(str)
    ))
    missing = sorted(target_keys - attempt_keys)
    unexpected = sorted(attempt_keys - target_keys)
    if missing or unexpected:
        raise ValueError(
            "Source-attempt target mismatch: "
            f"missing={len(missing)} {missing[:5]}, "
            f"unexpected={len(unexpected)} {unexpected[:5]}"
        )
    ledger = targets.merge(
        normalized_frame, on=["source_id", "contest_id"], how="left",
        validate="one_to_one",
    )
    ledger["attempt_id"] = ledger.apply(
        lambda row: _stable_id("source_attempt_ledger", row["source_id"], row["contest_id"]),
        axis=1,
    )
    ledger["ledger_parser_version"] = LEDGER_PARSER_VERSION
    if not set(ledger["source_status"]).issubset(TERMINAL_STATUSES):
        raise AssertionError("Ledger contains a non-terminal source status")
    return (
        targets.reindex(columns=CONTEST_UNIVERSE_COLUMNS),
        ledger.reindex(columns=LEDGER_COLUMNS).sort_values(
            ["source_id", "year", "month", "contest_id"], na_position="last"
        ).reset_index(drop=True),
        filters,
    )


def load_registry(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sources = payload.get("sources", []) if isinstance(payload, dict) else payload
    if not isinstance(sources, list):
        raise ValueError("Source registry must be a list or contain a sources list")
    return sources


def build_from_files(
    universe_path: Path, registry_path: Path, manifest_path: Path, output_dir: Path,
) -> dict[str, Any]:
    attempt_paths = load_attempt_manifest(manifest_path)
    attempt_frames = {path: pd.read_csv(path, low_memory=False) for path in attempt_paths}
    contests, ledger, filters = build_source_attempt_ledger(
        pd.read_csv(universe_path, low_memory=False),
        load_registry(registry_path),
        attempt_frames,
    )
    _write_csv_atomic(
        contests, output_dir / "mayoral_contest_universe.csv", CONTEST_UNIVERSE_COLUMNS
    )
    _write_csv_atomic(
        ledger, output_dir / "mayoral_source_attempt_ledger.csv", LEDGER_COLUMNS
    )
    _write_csv_atomic(
        filters,
        output_dir / "mayoral_source_scope_filter_audit.csv",
        FILTER_REPORT_COLUMNS,
    )
    large = filters[filters["exceeds_20_percent"]].to_dict("records")
    return {
        "contest_source_targets": len(contests),
        "source_attempts": len(ledger),
        "terminal_statuses": {
            str(key): int(value)
            for key, value in ledger["source_status"].value_counts().sort_index().items()
        },
        "large_filter_reports": large,
        "outputs": {
            "contest_universe": str(output_dir / "mayoral_contest_universe.csv"),
            "source_attempt_ledger": str(output_dir / "mayoral_source_attempt_ledger.csv"),
            "source_scope_filter_audit": str(
                output_dir / "mayoral_source_scope_filter_audit.csv"
            ),
        },
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--universe", required=True)
    parser.add_argument("--source-registry", required=True)
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--output-dir", required=True)
    args = parser.parse_args()
    summary = build_from_files(
        Path(args.universe), Path(args.source_registry), Path(args.manifest),
        Path(args.output_dir),
    )
    print(json.dumps(summary, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
