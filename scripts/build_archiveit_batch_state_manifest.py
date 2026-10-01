#!/usr/bin/env python3
"""Freeze the exact files used and produced by an Archive-It collection batch."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
from pathlib import Path


def _record(path: Path, root: Path) -> dict:
    resolved = path.resolve()
    if not resolved.is_file():
        raise FileNotFoundError(resolved)
    try:
        label = resolved.relative_to(root.resolve()).as_posix()
    except ValueError:
        label = str(resolved)
    row_count = None
    if resolved.suffix.lower() == ".csv":
        with resolved.open(newline="", encoding="utf-8-sig") as handle:
            row_count = sum(1 for _ in csv.DictReader(handle))
    return {
        "path": label,
        "bytes": resolved.stat().st_size,
        "sha256": hashlib.sha256(resolved.read_bytes()).hexdigest(),
        "csv_rows": row_count,
    }


def build_manifest(
    config_path: Path,
    payload_dir: Path,
    artifacts: list[Path],
    root: Path,
) -> dict:
    config = json.loads(config_path.read_text(encoding="utf-8"))
    collection_ids = sorted(int(row["collection_id"]) for row in config["collections"])
    if len(collection_ids) != 20 or len(set(collection_ids)) != 20:
        raise ValueError("The frozen batch must contain exactly 20 unique collections")
    payloads = sorted(payload_dir.glob("*.json"))
    payload_ids = sorted(int(path.stem) for path in payloads)
    if payload_ids != collection_ids:
        raise ValueError(
            f"Payload collection IDs disagree with config: expected {collection_ids}, found {payload_ids}"
        )
    paths = [config_path, *payloads, *artifacts]
    resolved = [path.resolve() for path in paths]
    if len(resolved) != len(set(resolved)):
        raise ValueError("Manifest inputs contain duplicate files")
    return {
        "manifest_version": "archiveit-batch-state-v1",
        "batch_id": config.get("batch_id", ""),
        "collection_count": 20,
        "collection_ids": collection_ids,
        "serper_requests": 0,
        "paid_cost_usd": 0.0,
        "files": [_record(path, root) for path in paths],
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True)
    parser.add_argument("--payload-dir", required=True)
    parser.add_argument("--artifact", action="append", default=[])
    parser.add_argument("--output", required=True)
    parser.add_argument("--root", default=".")
    args = parser.parse_args()
    manifest = build_manifest(
        Path(args.config), Path(args.payload_dir), [Path(path) for path in args.artifact], Path(args.root),
    )
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({"files": len(manifest["files"]), "output": str(output)}, indent=2))


if __name__ == "__main__":
    main()
