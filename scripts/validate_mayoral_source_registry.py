#!/usr/bin/env python3
"""Validate the operational mayoral source registry."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_FIELDS = {
    "source_id",
    "enabled",
    "adapter",
    "input_files",
    "output_files",
    "years",
    "jurisdictions",
}

TERMINAL_ATTEMPT_STATUSES = {
    "found",
    "no_result",
    "source_unavailable",
    "request_error",
    "match_unresolved",
}


def _flatten_paths(value: Any) -> list[str]:
    if isinstance(value, str):
        return [value]
    if isinstance(value, list):
        return [item for child in value for item in _flatten_paths(child)]
    if isinstance(value, dict):
        return [item for child in value.values() for item in _flatten_paths(child)]
    if value in (None, ""):
        return []
    raise ValueError(f"File declarations must contain strings, lists, or objects: {value!r}")


def _validate_years(value: Any, source_id: str) -> None:
    if isinstance(value, list):
        if not value or not all(isinstance(year, int) for year in value):
            raise ValueError(f"{source_id}: years list must contain integers")
        if len(set(value)) != len(value):
            raise ValueError(f"{source_id}: years list contains duplicates")
        return
    if isinstance(value, dict) and set(value) == {"start", "end"}:
        start, end = value["start"], value["end"]
        if not isinstance(start, int) or not isinstance(end, int) or start > end:
            raise ValueError(f"{source_id}: invalid start/end year range")
        return
    raise ValueError(f"{source_id}: years must be an integer list or start/end object")


def load_registry(path: Path) -> list[dict[str, Any]]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    sources = payload.get("sources") if isinstance(payload, dict) else payload
    if not isinstance(sources, list) or not sources:
        raise ValueError("Source registry must be a non-empty list or contain a sources list")
    if not all(isinstance(source, dict) for source in sources):
        raise ValueError("Every source registry entry must be an object")
    return sources


def _validate_attempt_tracking(
    source: dict[str, Any], source_id: str, repo_root: Path
) -> None:
    if "attempt_tracking" not in source:
        raise ValueError(f"{source_id}: enabled source must declare attempt_tracking")
    tracked = source["attempt_tracking"]
    if not isinstance(tracked, bool):
        raise ValueError(f"{source_id}: attempt_tracking must be true or false")
    if not tracked:
        if not str(source.get("attempt_tracking_reason", "")).strip():
            raise ValueError(
                f"{source_id}: attempt_tracking false requires attempt_tracking_reason"
            )
        return

    scope = source.get("attempt_scope")
    if not isinstance(scope, dict):
        raise ValueError(f"{source_id}: tracked source requires an attempt_scope object")

    mapping = source.get("attempt_status_mapping")
    if mapping is None:
        return
    if not isinstance(mapping, dict):
        raise ValueError(f"{source_id}: attempt_status_mapping must be an object")
    required = {
        "mapping_version",
        "files",
        "key",
        "status_column",
        "status_map",
        "aggregation_precedence",
    }
    missing = sorted(required - set(mapping))
    if missing:
        raise ValueError(
            f"{source_id}: attempt_status_mapping missing fields {missing}"
        )
    files = _flatten_paths(mapping["files"])
    if not files or len(files) != len(set(files)):
        raise ValueError(
            f"{source_id}: attempt_status_mapping files must be nonempty and unique"
        )
    missing_files = [
        value for value in files
        if not value.startswith(("http://", "https://"))
        and not (repo_root / value).is_file()
    ]
    if missing_files:
        raise ValueError(
            f"{source_id}: attempt status files do not exist: {missing_files}"
        )
    key = mapping["key"]
    if not isinstance(key, dict) or not any(
        name in key
        for name in ("contest_id_column", "candidate_year_id_column", "scope_year_unique")
    ):
        raise ValueError(f"{source_id}: attempt status mapping needs an explicit key")
    status_map = mapping["status_map"]
    if not isinstance(status_map, dict) or not status_map:
        raise ValueError(f"{source_id}: status_map must be a nonempty object")
    invalid_statuses = sorted(set(status_map.values()) - TERMINAL_ATTEMPT_STATUSES)
    if invalid_statuses:
        raise ValueError(
            f"{source_id}: status_map has invalid terminal statuses {invalid_statuses}"
        )
    precedence = mapping["aggregation_precedence"]
    if (
        not isinstance(precedence, list)
        or len(precedence) != len(set(precedence))
        or set(precedence) != TERMINAL_ATTEMPT_STATUSES
    ):
        raise ValueError(
            f"{source_id}: aggregation_precedence must list each terminal status once"
        )


def validate_registry(path: Path, repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    sources = load_registry(path)
    source_ids = [str(source.get("source_id", "")).strip() for source in sources]
    if any(not source_id for source_id in source_ids):
        raise ValueError("Every source registry entry needs a non-empty source_id")
    duplicates = sorted({source_id for source_id in source_ids if source_ids.count(source_id) > 1})
    if duplicates:
        raise ValueError(f"Duplicate source IDs: {duplicates}")

    enabled_outputs: set[str] = set()
    enabled_inputs: set[str] = set()
    for source, source_id in zip(sources, source_ids):
        missing = sorted(REQUIRED_FIELDS - set(source))
        if missing:
            raise ValueError(f"{source_id}: missing operational fields {missing}")
        if not isinstance(source["enabled"], bool):
            raise ValueError(f"{source_id}: enabled must be true or false")
        _validate_years(source["years"], source_id)
        jurisdictions = source["jurisdictions"]
        valid_jurisdiction = lambda value: (
            isinstance(value, str) and bool(value.strip())
        ) or (
            isinstance(value, dict)
            and isinstance(value.get("geo_name"), str)
            and bool(value["geo_name"].strip())
            and isinstance(value.get("state_abb"), str)
            and bool(value["state_abb"].strip())
        )
        if (
            not isinstance(jurisdictions, list)
            or not jurisdictions
            or not all(valid_jurisdiction(value) for value in jurisdictions)
        ):
            raise ValueError(
                f"{source_id}: jurisdictions must contain names or geo_name/state_abb objects"
            )
        inputs = _flatten_paths(source["input_files"])
        outputs = _flatten_paths(source["output_files"])
        if len(inputs) != len(set(inputs)) or len(outputs) != len(set(outputs)):
            raise ValueError(f"{source_id}: duplicate input or output file declaration")
        if not source["enabled"]:
            continue
        adapter = source["adapter"]
        if not isinstance(adapter, str) or not adapter.strip():
            raise ValueError(f"{source_id}: enabled source needs an adapter")
        adapter_path = repo_root / adapter
        if not adapter_path.is_file():
            raise ValueError(f"{source_id}: adapter does not exist: {adapter}")
        missing_inputs = [
            value for value in inputs
            if not value.startswith(("http://", "https://")) and not (repo_root / value).is_file()
        ]
        if missing_inputs:
            raise ValueError(f"{source_id}: input files do not exist: {missing_inputs}")
        _validate_attempt_tracking(source, source_id, repo_root)
        enabled_inputs.update(inputs)
        enabled_outputs.update(outputs)

    return {
        "schema_version": 1,
        "registry_path": str(path),
        "sources": len(sources),
        "enabled_sources": sum(bool(source["enabled"]) for source in sources),
        "disabled_sources": sum(not bool(source["enabled"]) for source in sources),
        "enabled_input_files": len(enabled_inputs),
        "enabled_output_files": len(enabled_outputs),
        "status": "valid",
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--registry", type=Path, default=Path("config/mayoral_url_source_registry.json")
    )
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    report = validate_registry(args.registry)
    rendered = json.dumps(report, indent=2) + "\n"
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(rendered, encoding="utf-8")
    print(rendered, end="")


if __name__ == "__main__":
    main()
