#!/usr/bin/env python3
"""Write the run metadata of a re-aligned (schema 2.2) one-stage output folder.

`realign_candidate_extraction_v2_2.py` re-aligns the output rows but writes no
run metadata. The importer requires one `run_metadata_*.json` for each shard,
with the 2.2 contract identity and with alignment totals that agree with the
re-aligned rows. In September 2026 this step ran by hand. This script does the
same step for any corpus.

For each shard file of the 2.1 folder the script:
  * selects the chunks of the shard (sorted chunk index modulo shard count),
  * computes the four alignment totals from the re-aligned rows, with the
    definitions of `all_output_aggregate` in the importer,
  * replaces the contract fields that differ between 2.1 and 2.2,
  * keeps each generation field, and records the 2.1 values that it replaced.

No inference runs. The script fails if the new generation config hash is not
the hash that the importer expects for schema 2.2.

With `--repair-attempt N` the folders are seed-vary repair folders. The script
then writes `run_metadata.json` with the 2.2 repair identity of attempt N, and
`repair_manifest.json` with the 2.2 identity and with the file hash and the
generation hash of each re-aligned target row. The run totals stay: they
describe the last Slurm job of the attempt, not the rows.

Usage:
  CANDIDATE_EXTRACTION_SCHEMA_VERSION=2.2 python scripts/realign_run_metadata_v2_2.py \
      --source-dir <2.1 output folder> --destination <2.2 output folder>
"""
from __future__ import annotations

import argparse
import gzip
import json
import os
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

NOTE = (
    "Alignment moved from 2.1 to 2.2. Generation was not re-run; every "
    "generation input and every raw output is the 2.1 artifact."
)
AGGREGATES = (
    "aligned_items_this_run",
    "alignment_status_counts_this_run",
    "documents_exactly_supported_this_run",
    "withheld_items_this_run",
)


def shard_totals(paths: list[Path]) -> dict:
    statuses: Counter[str] = Counter()
    exact = aligned = withheld = 0
    for path in paths:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for line in handle:
                row = json.loads(line)
                validation = row.get("validation")
                if isinstance(validation, dict):
                    exact += int(
                        validation.get("schema_valid") is True
                        and validation.get("n_items") == validation.get("n_supported")
                    )
                alignment = row.get("exact_alignment")
                if isinstance(alignment, dict):
                    statuses[str(alignment.get("status") or "missing")] += 1
                    aligned += int(alignment.get("aligned_item_count"))
                    withheld += int(alignment.get("withheld_item_count"))
    return {
        "aligned_items_this_run": aligned,
        "alignment_status_counts_this_run": dict(sorted(statuses.items())),
        "documents_exactly_supported_this_run": exact,
        "withheld_items_this_run": withheld,
    }


def repair_folder(args, importer, new) -> int:
    attempt = args.repair_attempt
    expected_hash = importer.EXPECTED_REPAIR_GENERATION_CONFIG_SHA256S[attempt]
    row = json.loads((args.source_dir / "run_metadata.json").read_text())
    if row.get("schema_version") != "2.1":
        raise SystemExit("run_metadata.json: not a 2.1 run metadata file")
    row["realigned_from_schema_version"] = "2.1"
    row["realigned_from_generation_config_sha256"] = row["generation_config_sha256"]
    row["realignment_note"] = NOTE
    for holder in (row, row["generation_config"]):
        holder["schema_version"] = "2.2"
        holder["extraction_contract_sha256"] = new["extraction_contract_sha256"]
        holder["alignment_spec_sha256"] = new["alignment_spec_sha256"]
    row["alignment_id"] = new["alignment_id"]
    row["generation_config_sha256"] = importer.sha256_text(importer.canonical_json(row["generation_config"]))
    if row["generation_config_sha256"] != expected_hash:
        raise SystemExit("run_metadata.json: the 2.2 repair generation config hash differs from the importer")
    (args.destination / "run_metadata.json").write_text(json.dumps(row, indent=2, sort_keys=True) + "\n")

    manifest = json.loads((args.source_dir / "repair_manifest.json").read_text())
    if manifest.get("schema_version") != "2.1" or manifest.get("attempt_index") != attempt:
        raise SystemExit("repair_manifest.json: not the 2.1 manifest of this attempt")
    manifest["realigned_from_schema_version"] = "2.1"
    manifest["realigned_from_repair_generation_config_sha256"] = manifest["repair_generation_config_sha256"]
    manifest["realignment_note"] = NOTE
    manifest["schema_version"] = "2.2"
    manifest["extraction_contract_sha256"] = new["extraction_contract_sha256"]
    manifest["primary_generation_config_sha256"] = importer.EXPECTED_GENERATION_CONFIG_SHA256
    manifest["repair_generation_config_sha256"] = expected_hash
    for entry in manifest["targets"]:
        path = args.destination / entry["repair_output_file"]
        entry["repair_output_sha256"] = importer.sha256_file(path)
        rows = {r["document_id"]: r for r in importer.read_jsonl_gz(path)}
        target = rows[entry["document_id"]]
        for field in ("finish_reason", "parse_status", "output_tokens"):
            if target.get(field) != entry[field]:
                raise SystemExit(f"{entry['document_id']}: {field} changed in the re-alignment")
        if target["generation_config_sha256"] != expected_hash:
            raise SystemExit(f"{entry['document_id']}: row generation hash is not the 2.2 repair hash")
        entry["generation_config_sha256"] = expected_hash
    (args.destination / "repair_manifest.json").write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({key: manifest[key] for key in ("attempt_index", "target_count", "repaired_count", "unrepaired_count")}))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source-dir", type=Path, required=True)
    parser.add_argument("--destination", type=Path, required=True)
    parser.add_argument("--repair-attempt", type=int, choices=(2, 3))
    args = parser.parse_args()
    if os.environ.get("CANDIDATE_EXTRACTION_SCHEMA_VERSION") != "2.2":
        raise SystemExit("set CANDIDATE_EXTRACTION_SCHEMA_VERSION=2.2")
    from scripts import import_candidate_extraction_v2 as importer
    from src.candidate_extraction import get_extraction_contract

    new = get_extraction_contract("2.2")
    if args.repair_attempt:
        return repair_folder(args, importer, new)
    chunks = sorted(args.destination.glob("chunk_*.out.jsonl.gz"))
    if not chunks or len(chunks) != len(list(args.source_dir.glob("chunk_*.out.jsonl.gz"))):
        raise SystemExit("the two folders do not hold the same chunks")
    report = {}
    for path in sorted(args.source_dir.glob("run_metadata*.json")):
        row = json.loads(path.read_text())
        if row.get("schema_version") != "2.1":
            raise SystemExit(f"{path.name}: not a 2.1 run metadata file")
        index, count = row["shard_index"], row["shard_count"]
        selected = [c for i, c in enumerate(chunks) if i % count == index]
        totals = shard_totals(selected)
        row["realigned_from_schema_version"] = "2.1"
        row["realigned_from_generation_config_sha256"] = row["generation_config_sha256"]
        row["realigned_from_run_aggregates"] = {key: row[key] for key in AGGREGATES}
        row["realignment_note"] = NOTE
        row.update(totals)
        for holder in (row, row["generation_config"]):
            holder["schema_version"] = "2.2"
            holder["extraction_contract_sha256"] = new["extraction_contract_sha256"]
            holder["alignment_spec_sha256"] = new["alignment_spec_sha256"]
        row["alignment_id"] = new["alignment_id"]
        row["generation_config_sha256"] = importer.sha256_text(
            importer.canonical_json(row["generation_config"])
        )
        if row["generation_config_sha256"] != importer.EXPECTED_GENERATION_CONFIG_SHA256:
            raise SystemExit(f"{path.name}: the 2.2 generation config hash differs from the importer")
        if row["model_items_parsed_this_run"] != totals["aligned_items_this_run"] + totals["withheld_items_this_run"]:
            raise SystemExit(f"{path.name}: the item totals do not reconcile")
        (args.destination / path.name).write_text(json.dumps(row, indent=2, sort_keys=True) + "\n")
        report[path.name] = {"chunks": len(selected), **totals}
    print(json.dumps(report, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
