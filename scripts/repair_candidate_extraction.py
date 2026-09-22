#!/usr/bin/env python3
"""Prepare and apply same-model repairs for candidate extraction parse failures."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.import_candidate_extraction import (  # noqa: E402
    load_inputs,
    load_outputs,
    sha256_file,
)
from src.candidate_extraction import (  # noqa: E402
    MODEL_REVISION,
    RUNTIME_LOCK_SHA256,
    evidence_is_supported,
    validate_extraction,
)


def read_rows(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def write_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
    with gzip.open(temp, "wt", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
    os.replace(temp, path)


def reanchor_normalized_evidence(evidence: str, original: str) -> str | None:
    quote_chars = set('\"“”‘’')

    def normalize_with_map(text: str) -> tuple[str, list[int]]:
        chars: list[str] = []
        positions: list[int] = []
        pending_space: int | None = None
        for index, char in enumerate(text):
            if char in quote_chars:
                continue
            if char.isspace():
                if chars and chars[-1] != " ":
                    pending_space = index
                continue
            if pending_space is not None:
                chars.append(" ")
                positions.append(pending_space)
                pending_space = None
            chars.append(char)
            positions.append(index)
        return "".join(chars).strip(), positions

    normalized_source, positions = normalize_with_map(original)
    normalized_evidence, _ = normalize_with_map(str(evidence))
    if not normalized_evidence:
        return None
    start = normalized_source.find(normalized_evidence)
    if start < 0:
        return None
    end = start + len(normalized_evidence) - 1
    if end >= len(positions):
        return None
    return original[positions[start]:positions[end] + 1]


def prepare(export_dir: Path, output_dir: Path, repair_dir: Path) -> int:
    _, inputs = load_inputs(export_dir)
    outputs = load_outputs(output_dir)
    failed_ids = sorted(
        document_id
        for document_id, row in outputs.items()
        if row.get("parse_status") != "parsed"
    )
    if set(failed_ids) - set(inputs):
        raise ValueError("repair candidates include IDs absent from the input export")
    repair_dir.mkdir(parents=True, exist_ok=True)
    write_rows(repair_dir / "chunks" / "chunk_00000.jsonl.gz", [inputs[x] for x in failed_ids])
    manifest = {
        "schema_version": "1.0",
        "repair_records": len(failed_ids),
        "document_ids": failed_ids,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
    }
    (repair_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n"
    )
    return len(failed_ids)


def apply_repairs(output_dir: Path, repair_output_dir: Path) -> int:
    repairs = load_outputs(repair_output_dir)
    if not repairs:
        return 0
    for document_id, row in repairs.items():
        if row.get("parse_status") != "parsed":
            raise ValueError(f"repair remains unparsed: {document_id}")
        if row.get("model_revision") != MODEL_REVISION:
            raise ValueError(f"repair model revision differs: {document_id}")
        if row.get("runtime_lock_sha256") != RUNTIME_LOCK_SHA256:
            raise ValueError(f"repair runtime lock differs: {document_id}")

    found: set[str] = set()
    for path in sorted(output_dir.glob("chunk_*.out.jsonl.gz")):
        rows = read_rows(path)
        changed = False
        for index, old in enumerate(rows):
            document_id = str(old["document_id"])
            if document_id not in repairs:
                continue
            new = repairs[document_id]
            for field in ("candidate_cycle_id", "route", "raw_text_sha256"):
                if str(new.get(field)) != str(old.get(field)):
                    raise ValueError(f"repair identity differs for {document_id}: {field}")
            rows[index] = new
            found.add(document_id)
            changed = True
        if changed:
            write_rows(path, rows)
    missing = set(repairs) - found
    if missing:
        raise ValueError(f"repair IDs absent from original outputs: {len(missing)}")
    return len(found)


def prepare_subsegments(
    export_dir: Path,
    output_dir: Path,
    repair_dir: Path,
    max_chars: int = 2_000,
    overlap_chars: int = 200,
) -> int:
    if max_chars < 500 or overlap_chars < 0 or overlap_chars >= max_chars:
        raise ValueError("invalid subsegment size/overlap")
    _, inputs = load_inputs(export_dir)
    outputs = load_outputs(output_dir)
    failed_ids = sorted(x for x, row in outputs.items() if row.get("parse_status") != "parsed")
    rows: list[dict[str, Any]] = []
    mapping: dict[str, list[str]] = {}
    step = max_chars - overlap_chars
    for parent_id in failed_ids:
        source = inputs[parent_id]
        text = str(source["source_text"])
        children = []
        for index, start in enumerate(range(0, len(text), step)):
            end = min(start + max_chars, len(text))
            child_id = hashlib.sha256(
                f"subsegment-repair-v1|{parent_id}|{index}|{start}|{end}".encode()
            ).hexdigest()
            child = {
                **source,
                "document_id": child_id,
                "source_text": text[start:end],
                "repair_parent_document_id": parent_id,
                "repair_segment_index": index,
                "repair_segment_start_char": start,
                "repair_segment_end_char": end,
            }
            rows.append(child)
            children.append(child_id)
            if end == len(text):
                break
        mapping[parent_id] = children
    repair_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = repair_dir / "chunks" / "chunk_00000.jsonl.gz"
    write_rows(chunk_path, rows)
    (repair_dir / "manifest.json").write_text(json.dumps({
        "schema_version": "1.0",
        "exported_records": len(rows),
        "chunks": [{
            "file": chunk_path.name,
            "sha256": sha256_file(chunk_path),
        }],
        "repair_parent_records": len(failed_ids),
        "repair_segment_records": len(rows),
        "max_chars": max_chars,
        "overlap_chars": overlap_chars,
        "mapping": mapping,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
    }, indent=2, sort_keys=True) + "\n")
    return len(rows)


def prepare_quote_normalized(
    export_dir: Path,
    output_dir: Path,
    repair_dir: Path,
) -> int:
    _, inputs = load_inputs(export_dir)
    outputs = load_outputs(output_dir)
    failed_ids = sorted(x for x, row in outputs.items() if row.get("parse_status") != "parsed")
    quote_chars = str.maketrans({char: " " for char in '\"“”‘’'})
    rows = []
    mapping = {}
    for parent_id in failed_ids:
        source = inputs[parent_id]
        child_id = hashlib.sha256(f"quote-normalized-repair-v1|{parent_id}".encode()).hexdigest()
        rows.append({
            **source,
            "document_id": child_id,
            "source_text": str(source["source_text"]).translate(quote_chars),
            "repair_original_source_text": str(source["source_text"]),
            "repair_parent_document_id": parent_id,
            "repair_segment_index": 0,
            "repair_source_normalization": "quotation_marks_to_spaces",
        })
        mapping[parent_id] = [child_id]
    repair_dir.mkdir(parents=True, exist_ok=True)
    chunk_path = repair_dir / "chunks" / "chunk_00000.jsonl.gz"
    write_rows(chunk_path, rows)
    (repair_dir / "manifest.json").write_text(json.dumps({
        "schema_version": "1.0",
        "exported_records": len(rows),
        "chunks": [{"file": chunk_path.name, "sha256": sha256_file(chunk_path)}],
        "repair_parent_records": len(rows),
        "repair_segment_records": len(rows),
        "mapping": mapping,
        "source_normalization": "quotation_marks_to_spaces",
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
    }, indent=2, sort_keys=True) + "\n")
    return len(rows)


def collapse_subsegments(
    repair_dir: Path,
    repair_output_dir: Path,
    output_path: Path,
) -> int:
    inputs = {
        row["document_id"]: row
        for row in read_rows(repair_dir / "chunks" / "chunk_00000.jsonl.gz")
    }
    outputs = load_outputs(repair_output_dir)
    if set(outputs) != set(inputs):
        raise ValueError("subsegment repair input/output coverage differs")
    grouped: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    for child_id, source in inputs.items():
        output = outputs[child_id]
        if output.get("parse_status") != "parsed":
            raise ValueError(f"subsegment remains unparsed: {child_id}")
        extraction = json.loads(json.dumps(output.get("extraction")))
        validation_source = source.get("repair_original_source_text", source["source_text"])
        if source.get("repair_source_normalization") == "quotation_marks_to_spaces":
            for item in extraction["issue_blocks"]:
                if not evidence_is_supported(item.get("evidence"), validation_source):
                    anchored = reanchor_normalized_evidence(
                        str(item.get("evidence") or ""), str(validation_source)
                    )
                    if anchored is not None:
                        item["evidence"] = anchored
            output = {**output, "extraction": extraction}
        check = validate_extraction("issues", extraction, validation_source)
        if not check["schema_valid"] or check["n_supported"] != check["n_items"]:
            raise ValueError(f"invalid subsegment extraction: {child_id}")
        grouped.setdefault(source["repair_parent_document_id"], []).append((source, output))

    collapsed = []
    for parent_id, pairs in sorted(grouped.items()):
        pairs.sort(key=lambda x: x[0]["repair_segment_index"])
        records: dict[tuple[str, str], dict[str, Any]] = {}
        for _, output in pairs:
            for item in output["extraction"]["issue_blocks"]:
                key = tuple(" ".join(str(item.get(x) or "").split()).casefold() for x in ("heading", "claim"))
                records.setdefault(key, item)
        source0, output0 = pairs[0]
        normalizations = sorted({
            value
            for source, output in pairs
            for value in (
                source.get("repair_source_normalization"),
                *(output.get("repair_source_normalizations") or []),
            )
            if value
        })
        collapsed.append({
            **output0,
            "document_id": parent_id,
            "candidate_cycle_id": source0["candidate_cycle_id"],
            "route": source0["route"],
            "raw_text_sha256": source0["raw_text_sha256"],
            "extraction": {"issue_blocks": list(records.values())},
            "parse_status": "parsed",
            "subsegment_repair": True,
            "repair_segment_count": len(pairs),
            "repair_depth": 1 + max(int(output.get("repair_depth", 0)) for _, output in pairs),
            "repair_source_normalizations": normalizations,
        })
    write_rows(output_path, collapsed)
    return len(collapsed)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    subparsers = parser.add_subparsers(dest="command", required=True)
    prep = subparsers.add_parser("prepare")
    prep.add_argument("--export-dir", type=Path, required=True)
    prep.add_argument("--output-dir", type=Path, required=True)
    prep.add_argument("--repair-dir", type=Path, required=True)
    apply_parser = subparsers.add_parser("apply")
    apply_parser.add_argument("--output-dir", type=Path, required=True)
    apply_parser.add_argument("--repair-output-dir", type=Path, required=True)
    segments = subparsers.add_parser("prepare-subsegments")
    segments.add_argument("--export-dir", type=Path, required=True)
    segments.add_argument("--output-dir", type=Path, required=True)
    segments.add_argument("--repair-dir", type=Path, required=True)
    segments.add_argument("--max-chars", type=int, default=2_000)
    segments.add_argument("--overlap-chars", type=int, default=200)
    collapse = subparsers.add_parser("collapse-subsegments")
    collapse.add_argument("--repair-dir", type=Path, required=True)
    collapse.add_argument("--repair-output-dir", type=Path, required=True)
    collapse.add_argument("--output-path", type=Path, required=True)
    normalized = subparsers.add_parser("prepare-quote-normalized")
    normalized.add_argument("--export-dir", type=Path, required=True)
    normalized.add_argument("--output-dir", type=Path, required=True)
    normalized.add_argument("--repair-dir", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        count = prepare(args.export_dir, args.output_dir, args.repair_dir)
    elif args.command == "apply":
        count = apply_repairs(args.output_dir, args.repair_output_dir)
    elif args.command == "prepare-subsegments":
        count = prepare_subsegments(
            args.export_dir, args.output_dir, args.repair_dir,
            args.max_chars, args.overlap_chars,
        )
    elif args.command == "collapse-subsegments":
        count = collapse_subsegments(
            args.repair_dir, args.repair_output_dir, args.output_path
        )
    else:
        count = prepare_quote_normalized(
            args.export_dir, args.output_dir, args.repair_dir
        )
    print(json.dumps({"command": args.command, "records": count}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
