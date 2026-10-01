#!/usr/bin/env python3
"""Build an uncapped fetch queue from a completed mayoral Wayback inventory."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import tempfile
from pathlib import Path
from urllib.parse import urlsplit

import pandas as pd

from scripts.scrape_mayoral_wayback_batch import (
    CAPTURE_COLUMNS,
    FETCH_QUEUE_COLUMNS,
    RUN_CONTRACT_VERSION,
)


QUEUE_CONTRACT_VERSION = "mayoral-wayback-all-inventory-captures-v1"


class AllSnapshotQueueError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _atomic_csv(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, suffix=".tmp") as handle:
        temp = Path(handle.name)
        frame.to_csv(handle, index=False)
    os.replace(temp, path)


def _atomic_json(payload: dict[str, object], path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", dir=path.parent, delete=False, suffix=".tmp") as handle:
        temp = Path(handle.name)
        json.dump(payload, handle, indent=2, sort_keys=True)
        handle.write("\n")
    os.replace(temp, path)


def build_all_snapshot_queue(captures: pd.DataFrame) -> pd.DataFrame:
    if captures.columns.tolist() != CAPTURE_COLUMNS:
        raise AllSnapshotQueueError("capture inventory schema differs from the frozen contract")
    if captures.empty:
        raise AllSnapshotQueueError("capture inventory is empty")
    if captures["capture_id"].isna().any() or captures["capture_id"].astype(str).str.strip().eq("").any():
        raise AllSnapshotQueueError("capture inventory has missing capture IDs")
    if captures["capture_id"].duplicated().any():
        raise AllSnapshotQueueError("capture inventory has duplicate capture IDs")

    queue = captures.copy()
    queue["path_depth"] = queue["page_identity"].map(
        lambda value: len([part for part in urlsplit(str(value)).path.split("/") if part])
    )
    queue = queue.sort_values(
        ["scrape_key", "capture_days_absolute", "timestamp", "original_url", "capture_id"],
        kind="stable",
    ).reset_index(drop=True)
    queue["target_queue_rank"] = queue.groupby("target_id", sort=False).cumcount() + 1
    queue["selection_reason"] = "all_inventoried_unique_content_captures_uncapped"
    queue["max_pages_per_target"] = -1
    queue = queue.reindex(columns=FETCH_QUEUE_COLUMNS)

    if len(queue) != len(captures) or set(queue.capture_id) != set(captures.capture_id):
        raise AllSnapshotQueueError("uncapped queue does not exactly recover inventory capture keys")
    if queue.capture_id.duplicated().any():
        raise AllSnapshotQueueError("uncapped queue has duplicate capture IDs")
    return queue


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture-inventory", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--audit-output", type=Path, required=True)
    args = parser.parse_args()

    captures = pd.read_csv(args.capture_inventory, keep_default_na=False, low_memory=False)
    queue = build_all_snapshot_queue(captures)
    _atomic_csv(queue, args.output)
    audit = {
        "queue_contract_version": QUEUE_CONTRACT_VERSION,
        "fetch_run_contract_version": RUN_CONTRACT_VERSION,
        "source_inventory": str(args.capture_inventory),
        "source_inventory_sha256": _sha256(args.capture_inventory),
        "inventory_rows": len(captures),
        "inventory_capture_keys": int(captures.capture_id.nunique()),
        "queue_rows": len(queue),
        "queue_capture_keys": int(queue.capture_id.nunique()),
        "exact_inventory_key_agreement": set(queue.capture_id) == set(captures.capture_id),
        "duplicate_queue_keys": int(queue.capture_id.duplicated().sum()),
        "target_rows": int(queue.target_id.nunique()),
        "candidate_years": int(queue.candidate_year_id.nunique()),
        "selection_rule": "all inventoried unique-content captures; no path or per-target cap",
        "output": str(args.output),
        "output_sha256": _sha256(args.output),
        "output_bytes": args.output.stat().st_size,
    }
    _atomic_json(audit, args.audit_output)
    print(json.dumps(audit, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
