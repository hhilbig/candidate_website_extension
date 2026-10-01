#!/usr/bin/env python3
"""Freeze the current mayoral discovery inputs and outputs in one manifest."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PATHS = [
    "data/external/municipal/ledb_candidatelevel.csv",
    "data/external/municipal/cities_constituency_data.csv",
    "data/municipal_discovery/mayoral_discovery_metadata.json",
    "data/municipal_discovery/final/mayoral_candidate_universe.csv",
    "data/municipal_discovery/final/mayoral_candidate_url_summary.csv",
    "data/municipal_discovery/final/mayoral_url_evidence.csv",
    "data/municipal_discovery/final/mayoral_source_status.csv",
    "data/municipal_discovery/final/mayoral_url_archive_validation.csv",
    "data/municipal_discovery/final/mayoral_coverage_summary.json",
    "data/municipal_discovery/search_pilot/query_manifest.csv",
    "data/municipal_discovery/search_pilot/search_results.jsonl",
    "data/municipal_discovery/search_pilot/search_lead_review.csv",
    "data/municipal_discovery/search_pilot/source_page_expansion.csv",
    "data/municipal_discovery/search_pilot/pilot_domain_archive_checks.csv",
    "data/municipal_discovery/search_pilot/search_results_failed_no_credits_20260824.jsonl",
    "config/curated_mayoral_contest_stages.json",
    "config/curated_mayoral_url_evidence.json",
    "config/mayoral_url_source_registry.json",
]


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def csv_shape(path: Path) -> tuple[int, int]:
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            with path.open(newline="", encoding=encoding) as handle:
                reader = csv.reader(handle)
                header = next(reader, [])
                return sum(1 for _ in reader), len(header)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Cannot decode {path}")


def record_count(path: Path) -> int | None:
    suffix = path.suffix.casefold()
    if suffix == ".csv":
        return csv_shape(path)[0]
    if suffix == ".jsonl":
        return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())
    if suffix == ".json":
        payload = json.loads(path.read_text(encoding="utf-8"))
        return len(payload) if isinstance(payload, list) else None
    return None


def build_manifest(paths: list[str], root: Path = REPO_ROOT) -> dict:
    records = []
    missing = []
    for relative in paths:
        path = root / relative
        if not path.is_file():
            missing.append(relative)
            continue
        rows = record_count(path)
        columns = csv_shape(path)[1] if path.suffix.casefold() == ".csv" else None
        records.append({
            "path": relative,
            "bytes": path.stat().st_size,
            "sha256": file_sha256(path),
            "records": rows,
            "columns": columns,
        })
    if missing:
        raise FileNotFoundError(f"Missing freeze targets: {missing}")
    return {
        "manifest_version": "mayoral-discovery-state-v1",
        "generated_at_utc": datetime.now(timezone.utc).isoformat(),
        "repository_root_name": root.name,
        "files": records,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--output",
        default="quality_reports/mayoral_url_discovery_2026-08-24/initial_state_manifest.json",
    )
    parser.add_argument("--path", action="append", dest="paths")
    args = parser.parse_args()
    manifest = build_manifest(args.paths or DEFAULT_PATHS)
    output = REPO_ROOT / args.output
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"files": len(manifest["files"]), "output": str(output)}))


if __name__ == "__main__":
    main()
