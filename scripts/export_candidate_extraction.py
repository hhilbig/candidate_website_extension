#!/usr/bin/env python3
"""Export deduplicated candidate pages for evidence-linked Hive extraction."""
from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import os
import sys
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import pandas as pd
import pyarrow.parquet as pq

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from src.candidate_extraction import (
    ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
    ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
    ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256,
    ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256,
    INFERENCE_DISPLAY_ESCAPE_ID,
    INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256,
    MODEL_ID,
    MODEL_REVISION,
    ROUTES,
    build_endorsement_context_provenance,
)
from src.candidate_extraction_provenance import (
    OFFSET_COORDINATE_SYSTEM,
    SOURCE_TEXT_TRANSFORM_RULE,
    SOURCE_TEXT_TRANSFORM_VERSION,
)


REQUIRED_COLUMNS = {
    "candidate_cycle_id",
    "candidate_icpsr",
    "state",
    "office",
    "year",
    "party",
    "date",
    "snap_url",
    "page_type",
    "text_snap_content",
}

def sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def deduplicate_target_pages(frame: pd.DataFrame) -> list[dict[str, Any]]:
    """Collapse exact repeated page text within candidacy and extraction route."""
    missing = REQUIRED_COLUMNS - set(frame.columns)
    if missing:
        raise ValueError(f"raw corpus lacks required columns: {sorted(missing)}")
    work = frame.loc[frame["page_type"].isin(ROUTES)].copy()
    work["_raw_text"] = work["text_snap_content"].fillna("").astype(str)
    work = work.loc[work["_raw_text"].str.strip().ne("")].copy()
    work["_text_sha256"] = work["_raw_text"].map(sha256_text)
    work["_document_id"] = work.apply(
        lambda row: sha256_text(
            f"{row['candidate_cycle_id']}\n{row['page_type']}\n{row['_text_sha256']}"
        ),
        axis=1,
    )
    work = work.sort_values(
        ["candidate_cycle_id", "page_type", "_text_sha256", "date", "snap_url"],
        kind="stable",
    )

    records: list[dict[str, Any]] = []
    for document_id, rows in work.groupby("_document_id", sort=True):
        first = rows.iloc[0]
        raw_text = str(first["_raw_text"])
        source_text = raw_text.replace("#+#", "\n")
        dates = sorted(rows["date"].astype(str).unique())
        urls = sorted(rows["snap_url"].dropna().astype(str).unique())
        records.append({
            "document_id": str(document_id),
            "candidate_cycle_id": str(first["candidate_cycle_id"]),
            "candidate_icpsr": str(first["candidate_icpsr"]),
            "state": str(first["state"]),
            "office": str(first["office"]),
            "year": int(first["year"]),
            "party": str(first["party"]),
            "route": str(first["page_type"]),
            "raw_text_sha256": str(first["_text_sha256"]),
            "extraction_source_text_sha256": sha256_text(source_text),
            "extraction_source_n_chars": len(source_text),
            "source_text_transform_version": SOURCE_TEXT_TRANSFORM_VERSION,
            "source_text_transform_changed": source_text != raw_text,
            "source_text": source_text,
            "n_raw_occurrences": int(len(rows)),
            "first_capture_timestamp": dates[0],
            "last_capture_timestamp": dates[-1],
            "source_urls": urls,
        })
    return records


def _stable_score(document_id: str, seed: int) -> str:
    return sha256_text(f"{seed}\n{document_id}")


def select_stratified_pilot(
    records: Iterable[dict[str, Any]],
    per_route: int,
    seed: int,
) -> list[dict[str, Any]]:
    """Select a deterministic year-party round-robin pilot within each route."""
    if per_route < 1:
        raise ValueError("per_route must be positive")
    by_route: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_route[str(record["route"])].append(record)

    selected: list[dict[str, Any]] = []
    for route in ROUTES:
        route_records = by_route.get(route, [])
        if len(route_records) < per_route:
            raise ValueError(
                f"route {route} has {len(route_records)} documents; "
                f"cannot sample {per_route}"
            )
        strata: dict[tuple[int, str], deque[dict[str, Any]]] = {}
        grouped: dict[tuple[int, str], list[dict[str, Any]]] = defaultdict(list)
        for record in route_records:
            grouped[(int(record["year"]), str(record["party"]))].append(record)
        for key, values in grouped.items():
            values.sort(key=lambda row: _stable_score(row["document_id"], seed))
            strata[key] = deque(values)
        keys = sorted(strata)
        route_selected: list[dict[str, Any]] = []
        while len(route_selected) < per_route:
            progressed = False
            for key in keys:
                if strata[key] and len(route_selected) < per_route:
                    route_selected.append(strata[key].popleft())
                    progressed = True
            if not progressed:
                raise RuntimeError(f"pilot selection exhausted route {route}")
        selected.extend(route_selected)
    return sorted(selected, key=lambda row: (row["route"], row["document_id"]))


def segment_document(
    record: dict[str, Any],
    max_chars: int,
    overlap_chars: int,
) -> list[dict[str, Any]]:
    """Split one page into deterministic overlapping, source-exact windows."""
    if max_chars < 1:
        raise ValueError("max_chars must be positive")
    if overlap_chars < 0 or overlap_chars >= max_chars:
        raise ValueError("overlap_chars must be nonnegative and smaller than max_chars")
    text = str(record["source_text"])
    parent_id = str(record["document_id"])
    bounds: list[tuple[int, int]] = []
    start = 0
    while start < len(text):
        hard_end = min(start + max_chars, len(text))
        end = hard_end
        if hard_end < len(text):
            break_at = text.rfind("\n", start + max_chars // 2, hard_end)
            if break_at > start:
                end = break_at + 1
        bounds.append((start, end))
        if end == len(text):
            break
        next_start = max(0, end - overlap_chars)
        if next_start <= start:
            raise RuntimeError("segmentation did not advance")
        start = next_start

    descriptors: list[dict[str, Any]] = []
    for index, (start, end) in enumerate(bounds):
        segment_text = text[start:end]
        segment_hash = sha256_text(segment_text)
        segment_id = sha256_text(f"{parent_id}\n{index}\n{start}\n{end}\n{segment_hash}")
        descriptors.append({
            "document_id": segment_id,
            "segment_index": index,
            "segment_start_char": start,
            "segment_end_char": end,
            "segment_text_sha256": segment_hash,
            "source_text": segment_text,
        })

    segments: list[dict[str, Any]] = []
    for descriptor in descriptors:
        segment_id = str(descriptor["document_id"])
        start = int(descriptor["segment_start_char"])
        end = int(descriptor["segment_end_char"])
        segment = {
            **record,
            "parent_document_id": parent_id,
            **descriptor,
            "segment_count": len(bounds),
        }
        if str(record["route"]) == "endorsements" and "source_urls" in record:
            segment["endorsement_context_provenance"] = (
                build_endorsement_context_provenance(
                    parent_document_id=parent_id,
                    parent_source_text=text,
                    source_urls=record.get("source_urls") or [],
                    segment_id=segment_id,
                    segment_start_char=start,
                    segment_end_char=end,
                    parent_segments=descriptors,
                )
            )
        segments.append(segment)
    return segments


def segment_documents(
    records: Iterable[dict[str, Any]],
    max_chars: int,
    overlap_chars: int,
    route_overrides: dict[str, tuple[int, int]] | None = None,
) -> list[dict[str, Any]]:
    route_overrides = route_overrides or {}
    segments = [
        segment
        for record in records
        for segment in segment_document(
            record,
            *route_overrides.get(str(record["route"]), (max_chars, overlap_chars)),
        )
    ]
    return sorted(
        segments,
        key=lambda row: (row["route"], row["parent_document_id"], row["segment_index"]),
    )


def write_chunks(
    records: list[dict[str, Any]],
    output_dir: Path,
    chunk_size: int,
) -> list[dict[str, Any]]:
    if chunk_size < 1:
        raise ValueError("chunk_size must be positive")
    output_dir.mkdir(parents=True, exist_ok=True)
    if any(output_dir.iterdir()):
        raise FileExistsError(f"refusing nonempty output directory: {output_dir}")
    manifest_rows: list[dict[str, Any]] = []
    for index, start in enumerate(range(0, len(records), chunk_size)):
        subset = records[start:start + chunk_size]
        path = output_dir / f"chunk_{index:05d}.jsonl.gz"
        temp = path.with_suffix(path.suffix + f".tmp-{os.getpid()}")
        with gzip.open(temp, "wt", encoding="utf-8") as handle:
            for record in subset:
                handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
        os.replace(temp, path)
        manifest_rows.append({
            "file": path.name,
            "records": len(subset),
            "sha256": file_sha256(path),
            "first_document_id": subset[0]["document_id"],
            "last_document_id": subset[-1]["document_id"],
        })
    return manifest_rows


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--pilot-per-route", type=int, default=120)
    parser.add_argument("--full", action="store_true")
    parser.add_argument("--chunk-size", type=int, default=60)
    parser.add_argument("--seed", type=int, default=20260822)
    parser.add_argument("--segment-max-chars", type=int, default=6_000)
    parser.add_argument("--segment-overlap-chars", type=int, default=600)
    parser.add_argument("--endorsements-segment-max-chars", type=int, default=3_000)
    parser.add_argument("--endorsements-segment-overlap-chars", type=int, default=300)
    args = parser.parse_args()

    parquet = pq.ParquetFile(args.corpus)
    missing = REQUIRED_COLUMNS - set(parquet.schema_arrow.names)
    if missing:
        raise ValueError(f"raw corpus lacks required columns: {sorted(missing)}")
    frame = pq.read_table(args.corpus, columns=sorted(REQUIRED_COLUMNS)).to_pandas()
    all_records = deduplicate_target_pages(frame)
    parent_records = (
        all_records
        if args.full
        else select_stratified_pilot(all_records, args.pilot_per_route, args.seed)
    )
    records = segment_documents(
        parent_records,
        args.segment_max_chars,
        args.segment_overlap_chars,
        {"endorsements": (
            args.endorsements_segment_max_chars,
            args.endorsements_segment_overlap_chars,
        )},
    )
    chunks_dir = args.output_dir / "chunks"
    chunk_rows = write_chunks(records, chunks_dir, args.chunk_size)
    counts = pd.Series([record["route"] for record in records]).value_counts().sort_index()
    parent_counts = (
        pd.Series([record["route"] for record in parent_records]).value_counts().sort_index()
    )
    universe_counts = (
        pd.Series([record["route"] for record in all_records]).value_counts().sort_index()
    )
    transformed_universe = sum(
        bool(record["source_text_transform_changed"]) for record in all_records
    )
    transformed_exported = sum(
        bool(record["source_text_transform_changed"]) for record in parent_records
    )
    endorsement_segments = [
        record for record in records if record["route"] == "endorsements"
    ]
    endorsement_contexts = [
        record["endorsement_context_provenance"]
        for record in endorsement_segments
    ]
    endorsement_has_complete_line = sum(
        context["segment_has_complete_line"] is True
        for context in endorsement_contexts
    )
    endorsement_empty_complete_line = sum(
        context["segment_has_complete_line"] is False
        for context in endorsement_contexts
    )
    endorsement_first_display_differs = sum(
        context["segment_has_complete_line"] is True
        and context["segment_first_complete_line_start_char"]
        != context["segment_first_complete_line_inference_display_start_char"]
        for context in endorsement_contexts
    )
    endorsement_last_display_differs = sum(
        context["segment_has_complete_line"] is True
        and context["segment_last_complete_line_end_char"]
        != context["segment_last_complete_line_inference_display_end_char"]
        for context in endorsement_contexts
    )
    manifest = {
        "schema_version": "1.4",
        "created_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "mode": "full" if args.full else "pilot",
        "seed": args.seed,
        "pilot_per_route": None if args.full else args.pilot_per_route,
        "chunk_size": args.chunk_size,
        "segmentation": {
            "max_chars": args.segment_max_chars,
            "overlap_chars": args.segment_overlap_chars,
            "route_overrides": {
                "endorsements": {
                    "max_chars": args.endorsements_segment_max_chars,
                    "overlap_chars": args.endorsements_segment_overlap_chars,
                }
            },
            "boundary_preference": "last_newline_in_second_half_else_hard_boundary",
        },
        "source_corpus": str(args.corpus),
        "source_corpus_sha256": file_sha256(args.corpus),
        "source_text_transform": {
            "version": SOURCE_TEXT_TRANSFORM_VERSION,
            "rule": SOURCE_TEXT_TRANSFORM_RULE,
            "offset_coordinate_system": OFFSET_COORDINATE_SYSTEM,
            "transformed_universe_parent_documents": transformed_universe,
            "transformed_exported_parent_documents": transformed_exported,
        },
        "endorsement_context_provenance": {
            "version": ENDORSEMENT_CONTEXT_PROVENANCE_VERSION,
            "provenance_spec_sha256": (
                ENDORSEMENT_CONTEXT_PROVENANCE_SPEC_SHA256
            ),
            "url_classifier_id": ENDORSEMENT_CONTEXT_CLASSIFIER_ID,
            "url_classifier_spec_sha256": (
                ENDORSEMENT_CONTEXT_CLASSIFIER_SPEC_SHA256
            ),
            "structural_heading_spec_sha256": (
                ENDORSEMENT_CONTEXT_STRUCTURAL_HEADING_SPEC_SHA256
            ),
            "inference_display_escape_id": INFERENCE_DISPLAY_ESCAPE_ID,
            "inference_display_escape_spec_sha256": (
                INFERENCE_DISPLAY_ESCAPE_SPEC_SHA256
            ),
            "scope_preference": [
                "segment", "parent_document", "page_or_section_route_context",
            ],
            "segment_complete_line_census": {
                "endorsement_segments": len(endorsement_segments),
                "segments_with_complete_line": endorsement_has_complete_line,
                "segments_without_complete_line": endorsement_empty_complete_line,
                "first_source_display_boundary_differs": (
                    endorsement_first_display_differs
                ),
                "last_source_display_boundary_differs": (
                    endorsement_last_display_differs
                ),
            },
        },
        "parent_hash_fields": {
            "raw_text_sha256": "SHA-256 of text_snap_content before the source-text transform",
            "extraction_source_text_sha256": "SHA-256 of the exact transformed text supplied to inference",
        },
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "routes": list(ROUTES),
        "deduplicated_universe_records": len(all_records),
        "deduplicated_universe_by_route": {
            key: int(value) for key, value in universe_counts.items()
        },
        "exported_parent_documents": len(parent_records),
        "exported_parent_documents_by_route": {
            key: int(value) for key, value in parent_counts.items()
        },
        "exported_records": len(records),
        "exported_by_route": {key: int(value) for key, value in counts.items()},
        "chunks": chunk_rows,
    }
    path = args.output_dir / "manifest.json"
    path.write_text(json.dumps(manifest, indent=2, sort_keys=True) + "\n")
    print(json.dumps({
        "output_dir": str(args.output_dir),
        "exported_records": len(records),
        "exported_parent_documents": len(parent_records),
        "exported_by_route": manifest["exported_by_route"],
        "universe_records": len(all_records),
        "chunks": len(chunk_rows),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
