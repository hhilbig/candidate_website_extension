#!/usr/bin/env python3
"""Create deterministic human-coding files for extraction precision and recall."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any

import pandas as pd

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.candidate_extraction import LIST_KEY, evidence_is_supported  # noqa: E402


def read_jsonl(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def stable_score(seed: int, value: str) -> str:
    return hashlib.sha256(f"{seed}\n{value}".encode()).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def record_subtype(route: str, item: dict[str, Any]) -> str:
    if route == "biography":
        return str(item["field"])
    if route == "endorsements":
        return str(item["endorser_type"])
    return "issue_statement"


def model_record_identity(route: str, item: dict[str, Any]) -> tuple[str, ...]:
    fields = {
        "issues": ("heading", "claim"),
        "biography": ("field", "value"),
        "endorsements": ("name", "organization", "endorser_type"),
    }[route]
    return tuple(" ".join(str(item.get(field) or "").split()).casefold() for field in fields)


def source_context(source: str, evidence: str, radius: int = 300) -> str:
    translated = str(source).translate(str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"'}))
    normalized_chars: list[str] = []
    source_positions: list[int] = []
    in_whitespace = False
    for position, char in enumerate(translated):
        if char.isspace():
            if normalized_chars and not in_whitespace:
                normalized_chars.append(" ")
                source_positions.append(position)
            in_whitespace = True
        else:
            normalized_chars.append(char)
            source_positions.append(position)
            in_whitespace = False
    while normalized_chars and normalized_chars[-1] == " ":
        normalized_chars.pop()
        source_positions.pop()
    normalized_source = "".join(normalized_chars)
    normalized_target = " ".join(
        str(evidence)
        .translate(str.maketrans({"’": "'", "‘": "'", "“": '"', "”": '"'}))
        .split()
    )
    normalized_start = normalized_source.find(normalized_target)
    if normalized_start < 0 or not normalized_target:
        return source[: 2 * radius]
    normalized_end = normalized_start + len(normalized_target) - 1
    start = source_positions[normalized_start]
    evidence_end = source_positions[normalized_end] + 1
    left = max(0, start - radius)
    right = min(len(source), evidence_end + radius)
    return source[left:right]


def reconstruct_parent_text(segments: list[dict[str, Any]]) -> str:
    ordered = sorted(segments, key=lambda row: int(row.get("segment_index", 0)))
    if len(ordered) == 1:
        return str(ordered[0]["source_text"])
    size = max(int(row["segment_end_char"]) for row in ordered)
    chars = [""] * size
    for row in ordered:
        start = int(row["segment_start_char"])
        text = str(row["source_text"])
        for offset, char in enumerate(text):
            position = start + offset
            if chars[position] and chars[position] != char:
                raise ValueError(f"overlap mismatch for {row['parent_document_id']}")
            chars[position] = char
    if any(char == "" for char in chars):
        raise ValueError(f"segmentation gap for {ordered[0]['parent_document_id']}")
    return "".join(chars)


def select_by_stratum(
    rows: list[dict[str, Any]],
    per_stratum: int,
    seed: int,
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[(str(row["route"]), str(row["subtype"]))].append(row)
    selected: list[dict[str, Any]] = []
    for key in sorted(grouped):
        values = sorted(
            grouped[key], key=lambda row: stable_score(seed, str(row["validation_id"]))
        )
        selected.extend(values[:per_stratum])
    return selected


def build_validation_files(
    imported_jsonl: Path,
    record_per_stratum: int,
    recall_pages_per_route: int,
    seed: int,
) -> tuple[pd.DataFrame, pd.DataFrame, dict[str, Any]]:
    segments = read_jsonl(imported_jsonl)
    by_parent: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in segments:
        parent_id = str(row.get("parent_document_id") or row["document_id"])
        by_parent[parent_id].append(row)

    candidates: list[dict[str, Any]] = []
    parent_payloads: list[dict[str, Any]] = []
    for parent_id, group in sorted(by_parent.items()):
        group.sort(key=lambda row: int(row.get("segment_index", 0)))
        first = group[0]
        route = str(first["route"])
        parent_source = reconstruct_parent_text(group)
        variants: dict[tuple[str, ...], list[dict[str, Any]]] = defaultdict(list)
        for segment in group:
            if segment.get("parse_status") != "parsed":
                continue
            for item in (segment.get("extraction") or {}).get(LIST_KEY[route], []):
                variants[model_record_identity(route, item)].append(item)
        parent_records: list[dict[str, Any]] = []
        for identity, item_variants in sorted(variants.items()):
            item = next(
                (
                    value for value in item_variants
                    if evidence_is_supported(str(value.get("evidence", "")), parent_source)
                ),
                item_variants[0],
            )
            parent_records.append(item)
            evidence = str(item.get("evidence", ""))
            identity_json = json.dumps(identity, ensure_ascii=False)
            validation_id = hashlib.sha256(
                f"{parent_id}\n{route}\n{identity_json}".encode()
            ).hexdigest()
            candidates.append({
                    "validation_id": validation_id,
                    "parent_document_id": parent_id,
                    "candidate_cycle_id": first["candidate_cycle_id"],
                    "candidate_name": first["candidate_name"],
                    "year": first["year"],
                    "party": first["party"],
                    "route": route,
                    "subtype": record_subtype(route, item),
                    "model_record_json": json.dumps(
                        item, ensure_ascii=False, sort_keys=True
                    ),
                    "model_variant_count": len(item_variants),
                    "evidence": evidence,
                    "evidence_exact_match": evidence_is_supported(evidence, parent_source),
                    "source_context": source_context(parent_source, evidence),
                    "record_present_in_page": "",
                    "normalized_value_faithful": "",
                    "record_type_correct": "",
                    "evidence_alone_supports_record": "",
                    "coding_status": "pending",
                    "coder_notes": "",
            })
        parent_payloads.append({
            "parent_document_id": parent_id,
            "candidate_cycle_id": first["candidate_cycle_id"],
            "candidate_name": first["candidate_name"],
            "year": first["year"],
            "party": first["party"],
            "route": route,
            "segment_count": len(group),
            "parse_failure_count": sum(row.get("parse_status") != "parsed" for row in group),
            "model_records_json": json.dumps(parent_records, ensure_ascii=False, sort_keys=True),
            "source_text": parent_source,
            "all_requested_records_captured": "",
            "missed_records_json": "",
            "coding_status": "pending",
            "coder_notes": "",
        })

    record_rows = select_by_stratum(candidates, record_per_stratum, seed)
    recall_rows: list[dict[str, Any]] = []
    for route in sorted(LIST_KEY):
        pool = [row for row in parent_payloads if row["route"] == route]
        pool.sort(key=lambda row: stable_score(seed, str(row["parent_document_id"])))
        recall_rows.extend(pool[:recall_pages_per_route])
    report = {
        "schema_version": "1.0",
        "seed": seed,
        "record_sample_rows": len(record_rows),
        "record_sample_by_stratum": {
            f"{route}|{subtype}": int(count)
            for (route, subtype), count in sorted(
                pd.DataFrame(record_rows).groupby(["route", "subtype"]).size().items()
            )
        },
        "recall_sample_pages": len(recall_rows),
        "recall_sample_by_route": dict(
            (str(key), int(value)) for key, value in sorted(
                pd.Series([row["route"] for row in recall_rows]).value_counts().items()
            )
        ),
        "coding_status": "pending_human_review",
        "source_imported_jsonl": str(imported_jsonl),
        "source_imported_jsonl_sha256": file_sha256(imported_jsonl),
    }
    return pd.DataFrame(record_rows), pd.DataFrame(recall_rows), report


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--imported-jsonl", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--record-per-stratum", type=int, default=30)
    parser.add_argument("--recall-pages-per-route", type=int, default=30)
    parser.add_argument("--seed", type=int, default=20260822)
    args = parser.parse_args()
    if args.output_dir.exists() and any(args.output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {args.output_dir}")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    records, recall, report = build_validation_files(
        args.imported_jsonl,
        args.record_per_stratum,
        args.recall_pages_per_route,
        args.seed,
    )
    precision_csv = args.output_dir / "record_precision_coding.csv"
    recall_csv = args.output_dir / "page_recall_coding.csv"
    precision_json = args.output_dir / "record_precision_coding.json"
    recall_json = args.output_dir / "page_recall_coding.json"
    records.to_csv(precision_csv, index=False)
    recall.to_csv(recall_csv, index=False)
    precision_json.write_text(
        records.to_json(orient="records", force_ascii=False, indent=2) + "\n"
    )
    recall_json.write_text(
        recall.to_json(orient="records", force_ascii=False, indent=2) + "\n"
    )
    report["coding_file_sha256"] = {
        path.name: file_sha256(path)
        for path in (precision_csv, recall_csv, precision_json, recall_json)
    }
    (args.output_dir / "validation_manifest.json").write_text(
        json.dumps(report, indent=2, sort_keys=True) + "\n"
    )
    print(json.dumps(report, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
