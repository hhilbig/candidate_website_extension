#!/usr/bin/env python3
"""Validate and import evidence-linked candidate-page extraction outputs."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.candidate_extraction import (
    LIST_KEY,
    MODEL_ID,
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    evidence_is_supported,
    validate_extraction,
)


def read_jsonl_gz(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def load_inputs(export_dir: Path) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    manifest = json.loads((export_dir / "manifest.json").read_text())
    records: dict[str, dict[str, Any]] = {}
    for chunk in manifest["chunks"]:
        path = export_dir / "chunks" / chunk["file"]
        if sha256_file(path) != chunk["sha256"]:
            raise ValueError(f"input chunk hash mismatch: {path}")
        for row in read_jsonl_gz(path):
            document_id = str(row["document_id"])
            if document_id in records:
                raise ValueError(f"duplicate input document: {document_id}")
            records[document_id] = row
    if len(records) != int(manifest["exported_records"]):
        raise ValueError("input manifest count differs from chunk contents")
    return manifest, records


def load_outputs(output_dir: Path) -> dict[str, dict[str, Any]]:
    records: dict[str, dict[str, Any]] = {}
    for path in sorted(output_dir.glob("chunk_*.out.jsonl.gz")):
        for row in read_jsonl_gz(path):
            document_id = str(row["document_id"])
            if document_id in records:
                raise ValueError(f"duplicate output document: {document_id}")
            records[document_id] = row
    return records


def load_run_metadata(output_dir: Path) -> list[dict[str, Any]]:
    paths = sorted(output_dir.glob("run_metadata*.json"))
    return [json.loads(path.read_text()) for path in paths]


def validate_run_metadata(
    metadata: list[dict[str, Any]], manifest: dict[str, Any], output_count: int
) -> dict[str, Any]:
    if not metadata:
        if manifest.get("mode") == "full":
            raise ValueError("full extraction lacks shard run metadata")
        return {"files": 0, "shard_count": None}
    for row in metadata:
        if row.get("model_id") != MODEL_ID or row.get("model_revision") != MODEL_REVISION:
            raise ValueError("run metadata has unexpected model identity")
        if row.get("runtime_lock_sha256") != RUNTIME_LOCK_SHA256:
            raise ValueError("run metadata has unexpected runtime lock")
    shard_counts = {row.get("shard_count") for row in metadata}
    if shard_counts == {None}:
        if len(metadata) != 1:
            raise ValueError("unsharded extraction has multiple run metadata files")
        expected_indices = {None}
    else:
        if len(shard_counts) != 1 or None in shard_counts:
            raise ValueError("run metadata shard counts disagree")
        shard_count = int(next(iter(shard_counts)))
        expected_indices = set(range(shard_count))
    observed_indices = {row.get("shard_index") for row in metadata}
    if observed_indices != expected_indices:
        raise ValueError(
            f"run metadata shard coverage differs: expected={len(expected_indices)}, "
            f"observed={len(observed_indices)}"
        )
    processed = sum(int(row.get("documents_processed_this_run", 0)) for row in metadata)
    if processed < 1 or processed > output_count:
        raise ValueError(
            f"run metadata processed count is impossible: {processed} for {output_count} outputs"
        )
    return {
        "files": len(metadata),
        "shard_count": None if expected_indices == {None} else len(expected_indices),
        "documents_processed": processed,
    }


def import_results(
    export_dir: Path,
    hive_output_dir: Path,
) -> tuple[list[dict[str, Any]], pd.DataFrame, dict[str, Any]]:
    manifest, inputs = load_inputs(export_dir)
    outputs = load_outputs(hive_output_dir)
    missing = set(inputs) - set(outputs)
    extra = set(outputs) - set(inputs)
    if missing or extra:
        raise ValueError(
            f"input/output coverage differs: missing={len(missing)}, extra={len(extra)}"
        )
    metadata_summary = validate_run_metadata(
        load_run_metadata(hive_output_dir), manifest, len(outputs)
    )

    enriched: list[dict[str, Any]] = []
    summaries: list[dict[str, Any]] = []
    for document_id in sorted(inputs):
        source = inputs[document_id]
        output = outputs[document_id]
        for column in ("candidate_cycle_id", "route", "raw_text_sha256"):
            if str(output.get(column)) != str(source[column]):
                raise ValueError(f"{document_id}: output {column} differs from input")
        if output.get("model_id") != MODEL_ID or output.get("model_revision") != MODEL_REVISION:
            raise ValueError(f"{document_id}: unexpected model identity")
        if output.get("runtime_lock_sha256") != RUNTIME_LOCK_SHA256:
            raise ValueError(f"{document_id}: unexpected runtime lock")

        if output.get("parse_status") == "parsed":
            check = validate_extraction(
                str(source["route"]), output.get("extraction"), str(source["source_text"])
            )
        else:
            check = {
                "schema_valid": False,
                "n_items": 0,
                "n_supported": 0,
                "evidence_support_rate": None,
                "status": str(output.get("parse_status") or "missing_parse_status"),
            }
        max_output_tokens = output.get("max_output_tokens")
        if output.get("subsegment_repair"):
            repair_method = "recursive_subsegment"
        elif max_output_tokens is not None and int(max_output_tokens) != 8_192:
            repair_method = "higher_output_ceiling"
        else:
            repair_method = "none"
        row = {
            **{key: value for key, value in source.items() if key != "source_text"},
            "source_text": source["source_text"],
            "extraction": output.get("extraction"),
            "raw_output": output.get("raw_output"),
            "parse_status": output.get("parse_status"),
            "model_id": output.get("model_id"),
            "model_revision": output.get("model_revision"),
            "runtime_lock_sha256": output.get("runtime_lock_sha256"),
            "input_truncated": bool(output.get("input_truncated", False)),
            "input_tokens_used": output.get("input_tokens_used"),
            "input_chars_used": output.get("input_chars_used"),
            "max_output_tokens": max_output_tokens,
            "compact_repair": bool(output.get("compact_repair", False)),
            "subsegment_repair": bool(output.get("subsegment_repair", False)),
            "repair_method": repair_method,
            "repair_depth": output.get("repair_depth"),
            "repair_segment_count": output.get("repair_segment_count"),
            "repair_source_normalizations": output.get("repair_source_normalizations") or [],
            "validation": check,
            "human_validation_status": "pending",
        }
        enriched.append(row)
        summaries.append({
            "document_id": document_id,
            "candidate_cycle_id": source["candidate_cycle_id"],
            "route": source["route"],
            "year": source["year"],
            "party": source["party"],
            "parse_status": output.get("parse_status"),
            "input_truncated": bool(output.get("input_truncated", False)),
            "repair_method": repair_method,
            **check,
            "human_validation_status": "pending",
        })

    summary = pd.DataFrame(summaries)
    n_documents = len(summary)
    parsed = int(summary["parse_status"].eq("parsed").sum())
    schema_valid = int(summary["schema_valid"].sum())
    n_items = int(summary["n_items"].sum())
    n_supported = int(summary["n_supported"].sum())
    parse_error_rate = 1 - parsed / n_documents if n_documents else 1.0
    schema_error_rate = 1 - schema_valid / parsed if parsed else 1.0
    evidence_support_rate = n_supported / n_items if n_items else None
    gates = {
        "complete_coverage": len(outputs) == len(inputs),
        "parse_error_rate_at_most_0_05": parse_error_rate <= 0.05,
        "schema_error_rate_zero": schema_error_rate == 0,
        "evidence_support_rate_at_least_0_95": (
            evidence_support_rate is not None and evidence_support_rate >= 0.95
        ),
    }
    report = {
        "schema_version": "1.0",
        "input_manifest": manifest,
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "documents": n_documents,
        "documents_by_route": dict(sorted(Counter(summary["route"]).items())),
        "parsed_documents": parsed,
        "parse_error_rate": parse_error_rate,
        "schema_valid_documents": schema_valid,
        "schema_error_rate_among_parsed": schema_error_rate,
        "extracted_items": n_items,
        "evidence_supported_items": n_supported,
        "evidence_support_rate": evidence_support_rate,
        "truncated_documents": int(summary["input_truncated"].sum()),
        "repair_documents": int(summary["repair_method"].ne("none").sum()),
        "repair_documents_by_method": dict(sorted(
            Counter(summary.loc[summary["repair_method"].ne("none"), "repair_method"]).items()
        )),
        "technical_gates": gates,
        "technical_gate_passed": all(gates.values()),
        "substantive_validation_status": "pending_human_review",
        "run_metadata": metadata_summary,
    }
    return enriched, summary, report


def _identity_value(value: Any) -> str:
    return " ".join(str(value or "").split()).casefold()


def _item_identity(route: str, item: dict[str, Any]) -> tuple[str, ...]:
    fields = {
        "issues": ("heading", "claim"),
        "biography": ("field", "value"),
        "endorsements": ("name", "organization", "endorser_type"),
    }[route]
    return tuple(_identity_value(item.get(field)) for field in fields)


def aggregate_parent_extractions(
    enriched: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    """Merge overlapping segments while retaining every model-output variant."""
    grouped: dict[str, list[dict[str, Any]]] = {}
    for row in enriched:
        parent_id = str(row.get("parent_document_id") or row["document_id"])
        grouped.setdefault(parent_id, []).append(row)

    parents: list[dict[str, Any]] = []
    for parent_id, segments in sorted(grouped.items()):
        segments.sort(key=lambda row: int(row.get("segment_index", 0)))
        route = str(segments[0]["route"])
        records: dict[tuple[str, ...], dict[str, Any]] = {}
        for segment in segments:
            if segment.get("parse_status") != "parsed":
                continue
            extraction = segment.get("extraction") or {}
            for item in extraction.get(LIST_KEY[route], []):
                identity = _item_identity(route, item)
                variant = {
                    "segment_id": segment["document_id"],
                    "segment_index": segment.get("segment_index", 0),
                    "item": item,
                    "evidence_supported": evidence_is_supported(
                        str(item.get("evidence", "")), str(segment["source_text"])
                    ),
                }
                if identity not in records:
                    records[identity] = {
                        "record": item,
                        "variants": [variant],
                    }
                else:
                    records[identity]["variants"].append(variant)
        parent_records = []
        for identity, payload in sorted(records.items()):
            supported = [
                variant for variant in payload["variants"]
                if variant["evidence_supported"]
            ]
            representative = (supported or payload["variants"])[0]["item"]
            parent_records.append({
                "record_id": hashlib.sha256(
                    f"{parent_id}\n{route}\n{json.dumps(identity, ensure_ascii=False)}".encode()
                ).hexdigest(),
                "record": representative,
                "variant_count": len(payload["variants"]),
                "supported_variant_count": len(supported),
                "variants": payload["variants"],
            })
        parse_failures = sum(
            segment.get("parse_status") != "parsed" for segment in segments
        )
        first = segments[0]
        parents.append({
            "parent_document_id": parent_id,
            "candidate_cycle_id": first["candidate_cycle_id"],
            "candidate_name": first["candidate_name"],
            "state": first["state"],
            "office": first["office"],
            "year": first["year"],
            "party": first["party"],
            "route": route,
            "raw_text_sha256": first["raw_text_sha256"],
            "segment_count": len(segments),
            "parsed_segment_count": len(segments) - parse_failures,
            "parent_parse_status": (
                "parsed" if parse_failures == 0 else "partially_parsed"
            ),
            "records": parent_records,
            "human_validation_status": "pending",
        })
    return parents


def flatten_parent_records(parents: list[dict[str, Any]]) -> dict[str, pd.DataFrame]:
    """Create analysis-facing route tables from provenance-rich parent records."""
    rows: dict[str, list[dict[str, Any]]] = {
        "issues": [], "biography": [], "endorsements": []
    }
    metadata_fields = (
        "parent_document_id", "candidate_cycle_id", "candidate_name", "state",
        "office", "year", "party", "route", "raw_text_sha256", "segment_count",
        "parsed_segment_count", "parent_parse_status",
    )
    for parent in parents:
        route = str(parent["route"])
        for payload in parent["records"]:
            item = payload["record"]
            rows[route].append({
                **{field: parent[field] for field in metadata_fields},
                "record_id": payload["record_id"],
                "variant_count": payload["variant_count"],
                "supported_variant_count": payload["supported_variant_count"],
                "representative_evidence_supported": (
                    payload["supported_variant_count"] > 0
                ),
                **item,
                "model_id": MODEL_ID,
                "model_revision": MODEL_REVISION,
                "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
            })
    return {route: pd.DataFrame(values) for route, values in rows.items()}


def write_imported(
    enriched: list[dict[str, Any]],
    summary: pd.DataFrame,
    report: dict[str, Any],
    output_dir: Path,
) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    prefix = "full" if report.get("input_manifest", {}).get("mode") == "full" else "pilot"
    targets = [
        output_dir / f"{prefix}_extractions.jsonl.gz",
        output_dir / f"{prefix}_document_summary.csv",
        output_dir / f"{prefix}_validation_report.json",
        output_dir / f"{prefix}_parent_extractions.jsonl.gz",
        output_dir / f"{prefix}_issue_records.parquet",
        output_dir / f"{prefix}_biography_records.parquet",
        output_dir / f"{prefix}_endorsement_records.parquet",
    ]
    if any(path.exists() for path in targets):
        raise FileExistsError(f"refusing to overwrite imported outputs in {output_dir}")

    jsonl = targets[0]
    temp = jsonl.with_suffix(jsonl.suffix + f".tmp-{os.getpid()}")
    with gzip.open(temp, "wt", encoding="utf-8") as handle:
        for row in enriched:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(temp, jsonl)
    summary.to_csv(targets[1], index=False)
    targets[2].write_text(json.dumps(report, indent=2, sort_keys=True) + "\n")
    parents = aggregate_parent_extractions(enriched)
    parent_path = targets[3]
    temp = parent_path.with_suffix(parent_path.suffix + f".tmp-{os.getpid()}")
    with gzip.open(temp, "wt", encoding="utf-8") as handle:
        for row in parents:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(temp, parent_path)
    flat = flatten_parent_records(parents)
    flat["issues"].to_parquet(targets[4], index=False)
    flat["biography"].to_parquet(targets[5], index=False)
    flat["endorsements"].to_parquet(targets[6], index=False)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--export-dir", type=Path, required=True)
    parser.add_argument("--hive-output-dir", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--fail-on-technical-gate", action="store_true")
    args = parser.parse_args()
    enriched, summary, report = import_results(args.export_dir, args.hive_output_dir)
    write_imported(enriched, summary, report, args.output_dir)
    print(json.dumps({
        "documents": report["documents"],
        "parse_error_rate": report["parse_error_rate"],
        "evidence_support_rate": report["evidence_support_rate"],
        "technical_gate_passed": report["technical_gate_passed"],
    }, sort_keys=True))
    if args.fail_on_technical_gate and not report["technical_gate_passed"]:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
