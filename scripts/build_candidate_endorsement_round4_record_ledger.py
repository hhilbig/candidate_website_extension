#!/usr/bin/env python3
"""Build the round-four endorsement record ledger from the production runs.

The projection library already exists in `src/candidate_endorsement_round4_release.py`.
This script is the driver it needed: it reads the completed Stage-A and Stage-B
artifacts, projects every Stage-B batch against its exact parent inventory,
summarizes every parent, and writes the two files the importer reads.

It never re-infers and it never repairs. A Stage-B response that carries a
retryable infrastructure failure stops the build, because a ledger must not
record an open failure as a settled outcome.

Outputs, in `--output-dir`:

  endorsement_round4_record_ledger_private.jsonl.gz
      One row per resolved record, carrying its canonical segment.
  endorsement_round4_record_ledger.json
      `summary`, `parents`, and the `private_file` spec, which is exactly the
      block the bundle manifest carries under `endorsement_round4_record_ledger`.

Usage:
  python scripts/build_candidate_endorsement_round4_record_ledger.py \
    --export-dir build/hive_candidate_extraction/v2_1_full_export_20260824p \
    --stage-a-output-dir build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_a_outputs_20260916a \
    --stage-b-input-dir build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_b_20260916a \
    --stage-b-output-dir build/hive_candidate_extraction/v2_1_endorsement_round4_production_stage_b_outputs_20260916a \
    --output-dir build/hive_candidate_extraction/round4_record_ledger_20260918
"""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterator, Mapping

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.import_candidate_extraction_v2 import (  # noqa: E402
    ENDORSEMENT_ROUND4_LEDGER_FILENAME,
    round4_resolved_record,
    FROZEN_ROUND4_PRODUCTION_STAGE_A_GENERATION_CONFIG_SHA256,
    FROZEN_ROUND4_PRODUCTION_STAGE_A_INPUT_MANIFEST_SHA256,
    FROZEN_ROUND4_PRODUCTION_STAGE_B_GENERATION_CONFIG_SHA256,
    FROZEN_ROUND4_PRODUCTION_STAGE_B_INPUT_MANIFEST_SHA256,
)
from src.candidate_endorsement_round4_release import (  # noqa: E402
    Round4ProjectionError,
    build_round4_record_ledger,
    parent_inventory_from_export_rows,
    project_round4_stage_b_batch,
    round4_parent_coverage,
    validate_endorsement_round4_record_ledger_private_rows,
)

LEDGER_SUMMARY_FILENAME = "endorsement_round4_record_ledger.json"


class LedgerBuildError(RuntimeError):
    """The artifacts do not support a ledger. The build stops rather than guess."""


def _require(condition: Any, message: str) -> None:
    if not bool(condition):
        raise LedgerBuildError(message)


def read_jsonl_gz(path: Path) -> Iterator[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                yield json.loads(line)


def read_chunk_dir(directory: Path, pattern: str) -> Iterator[dict[str, Any]]:
    paths = sorted(directory.glob(pattern))
    _require(paths, f"no {pattern} under {directory}")
    for path in paths:
        yield from read_jsonl_gz(path)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def check_run_identity(
    rows: Mapping[str, Mapping[str, Any]],
    *,
    stage: str,
    manifest_sha256: str,
    generation_config_sha256: str,
) -> None:
    """Require one frozen identity across every output row of a stage.

    The run metadata already reports one identity per stage. This repeats the
    check at row level, because the ledger binds the whole corpus to these two
    hashes and a single foreign row would make that claim false.
    """
    manifests = {str(row.get("input_manifest_sha256")) for row in rows.values()}
    configs = {str(row.get("generation_config_sha256")) for row in rows.values()}
    _require(
        manifests == {manifest_sha256},
        f"stage {stage} output rows carry input manifests {sorted(manifests)}, "
        f"not the frozen {manifest_sha256}",
    )
    _require(
        configs == {generation_config_sha256},
        f"stage {stage} output rows carry generation configs {sorted(configs)}, "
        f"not the frozen {generation_config_sha256}",
    )


def load_export_parents(export_dir: Path) -> dict[str, list[dict[str, Any]]]:
    """Group the export's endorsement windows by parent document."""
    parents: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in read_chunk_dir(export_dir / "chunks", "chunk_*.jsonl.gz"):
        if row.get("route") == "endorsements":
            parents[str(row["parent_document_id"])].append(row)
    _require(parents, f"{export_dir} holds no endorsement window")
    return dict(parents)


def load_by_batch(
    directory: Path, pattern: str, label: str
) -> dict[str, dict[str, Any]]:
    rows: dict[str, dict[str, Any]] = {}
    for row in read_chunk_dir(directory, pattern):
        batch_id = str(row["batch_id"])
        _require(batch_id not in rows, f"{label} repeats batch {batch_id}")
        rows[batch_id] = row
    return rows


def build(
    *,
    export_dir: Path,
    stage_a_output_dir: Path,
    stage_b_input_dir: Path,
    stage_b_output_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    export_parents = load_export_parents(export_dir)
    stage_a_outputs = load_by_batch(
        stage_a_output_dir, "chunk_*.out.jsonl.gz", "stage A output"
    )
    stage_b_batches = load_by_batch(
        stage_b_input_dir / "chunks", "chunk_*.jsonl.gz", "stage B input"
    )
    stage_b_outputs = load_by_batch(
        stage_b_output_dir, "chunk_*.out.jsonl.gz", "stage B output"
    )
    check_run_identity(
        stage_a_outputs,
        stage="A",
        manifest_sha256=FROZEN_ROUND4_PRODUCTION_STAGE_A_INPUT_MANIFEST_SHA256,
        generation_config_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_A_GENERATION_CONFIG_SHA256
        ),
    )
    check_run_identity(
        stage_b_outputs,
        stage="B",
        manifest_sha256=FROZEN_ROUND4_PRODUCTION_STAGE_B_INPUT_MANIFEST_SHA256,
        generation_config_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_B_GENERATION_CONFIG_SHA256
        ),
    )
    missing = sorted(set(stage_b_batches) - set(stage_b_outputs))
    _require(not missing, f"stage B batches without an output: {missing[:5]}")
    foreign = sorted(set(stage_b_outputs) - set(stage_b_batches))
    _require(not foreign, f"stage B outputs without a batch: {foreign[:5]}")

    stage_a_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in stage_a_outputs.values():
        stage_a_by_parent[str(row["parent_document_id"])].append(row)

    inventories: dict[str, dict[str, Any]] = {}
    projections_by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    accounted = 0
    for batch_id, batch in sorted(stage_b_batches.items()):
        parent_id = str(batch["parent_document_id"])
        rows = export_parents.get(parent_id)
        _require(rows, f"batch {batch_id} names parent {parent_id}, which the export lacks")
        if parent_id not in inventories:
            inventories[parent_id] = parent_inventory_from_export_rows(rows)
        try:
            projection = project_round4_stage_b_batch(
                batch, stage_b_outputs[batch_id], inventories[parent_id]
            )
        except Round4ProjectionError as error:
            raise LedgerBuildError(f"batch {batch_id}: {error}") from error
        projections_by_parent[parent_id].append(projection)
        accounted += 1

    # Every endorsement parent in the corpus gets a summary, not only those with
    # a Stage-B batch. 319 parents reached Stage A and produced no candidate
    # anchor, and 2 never reached Stage A because their pages hold only
    # navigation. All of them are parents with nothing to extract, which
    # `round4_parent_coverage` reports as complete coverage and zero records.
    # Leaving them out made the importer refuse the bundle, correctly, because an
    # endorsement segment would have had no summary behind it.
    summaries = []
    for parent_id in sorted(export_parents):
        summaries.append(
            round4_parent_coverage(
                parent_id,
                sorted(
                    stage_a_by_parent.get(parent_id, []),
                    key=lambda row: str(row["batch_id"]),
                ),
                projections_by_parent.get(parent_id, []),
            )
        )
    retryable = [str(item["parent_document_id"]) for item in summaries
                 if item.get("retry_required")]
    if retryable:
        raise LedgerBuildError(
            f"retryable Stage-B failures remain open for {len(retryable)} "
            f"parents, first {retryable[0]}. Repair them before building the "
            "ledger; the ledger must not record an open failure as settled."
        )
    ledger = build_round4_record_ledger(
        summaries,
        stage_a_input_manifest_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_A_INPUT_MANIFEST_SHA256
        ),
        stage_a_generation_config_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_A_GENERATION_CONFIG_SHA256
        ),
        stage_b_input_manifest_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_B_INPUT_MANIFEST_SHA256
        ),
        stage_b_generation_config_sha256=(
            FROZEN_ROUND4_PRODUCTION_STAGE_B_GENERATION_CONFIG_SHA256
        ),
        stage_b_response_count=len(stage_b_outputs),
        accounted_output_count=accounted,
    )
    records: list[dict[str, Any]] = []
    for parent_id in sorted(projections_by_parent):
        segment_by_id = {
            str(row["document_id"]): row for row in export_parents[parent_id]
        }
        for projection in projections_by_parent[parent_id]:
            for aligned in projection["aligned_records"]:
                records.append(
                    round4_resolved_record(aligned, segment_by_id, parent_id)
                )
    records.sort(
        key=lambda row: (str(row["canonical_segment_id"]), str(row["record_id"]))
    )
    identities = {str(row["record_id"]) for row in records}
    _require(
        len(identities) == len(records),
        "two resolved records share one record identity",
    )
    _require(
        len(records) == int(ledger["summary"]["aligned_record_count"]),
        "resolved records differ from the ledger's aligned record count",
    )
    return ledger, records


def write_outputs(
    output_dir: Path, ledger: Mapping[str, Any], records: list[dict[str, Any]]
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    private_path = output_dir / ENDORSEMENT_ROUND4_LEDGER_FILENAME
    with gzip.open(private_path, "wt", encoding="utf-8") as handle:
        for row in records:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    block = {
        "summary": ledger["summary"],
        "parents": ledger["parents"],
        "private_file": {
            "file": ENDORSEMENT_ROUND4_LEDGER_FILENAME,
            "rows": len(records),
            "sha256": sha256_file(private_path),
        },
    }
    (output_dir / LEDGER_SUMMARY_FILENAME).write_text(
        json.dumps(block, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    return block


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--stage-a-output-dir", type=Path, required=True)
    parser.add_argument("--stage-b-input-dir", type=Path, required=True)
    parser.add_argument("--stage-b-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    args = parser.parse_args()

    ledger, records = build(
        export_dir=args.export_dir,
        stage_a_output_dir=args.stage_a_output_dir,
        stage_b_input_dir=args.stage_b_input_dir,
        stage_b_output_dir=args.stage_b_output_dir,
    )
    validate_endorsement_round4_record_ledger_private_rows(ledger)
    block = write_outputs(args.output_dir, ledger, records)
    summary = block["summary"]
    print(
        json.dumps(
            {
                "parents": summary["parent_count"],
                "aligned_records": summary["aligned_record_count"],
                "withheld_records": summary["withheld_record_count"],
                "parents_complete": summary["parents_complete"],
                "unresolved_target_blocks": summary["stage_b_unresolved_target_blocks"],
                "optional_field_repairs": summary["stage_b_optional_field_repairs"],
                "all_model_records_accounted": summary["all_model_records_accounted"],
                "unaccounted_outputs": summary["unaccounted_outputs"],
                "private_file": block["private_file"],
            },
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
