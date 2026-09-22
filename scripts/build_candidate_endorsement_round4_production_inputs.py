#!/usr/bin/env python3
"""Build round-four production inputs and the full-corpus batch census.

This script calls no model.  The ``census`` subcommand reconstructs every
endorsement parent from the frozen source export and reports the exact Stage-A
batch count and the Stage-B upper bound.  Step 2 uses that census to size its
inference request before it asks for full-run authority.

The ``stage-a`` subcommand writes the production Stage-A batches.  The
``stage-b`` subcommand writes Stage-B batches from authenticated Stage-A
output.  Both carry the production artifact scope, which the production runner
requires and the development runner rejects.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import math
from collections import Counter
from collections.abc import Mapping, Sequence
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

import scripts.audit_candidate_endorsement_40_parent_feasibility as base
from src.candidate_endorsement_round4 import (
    STAGE_A_SLOT_COUNT,
    STAGE_B_SLOT_COUNT,
    canonical_json,
)
from src.candidate_endorsement_round4_production import (
    ROUND4_LINE_INVENTORY_SCOPE_RULE,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
    get_round4_production_contract,
)
from src.candidate_endorsement_round4_release import (
    ANCHOR_ROLES,
    parent_inventory_from_export_rows,
)

SOURCE_EXPORT = "build/hive_candidate_extraction/v2_1_full_export_20260824p"
FROZEN_SOURCE_MANIFEST_SHA256 = (
    "f5e6545a2d2ec35a83f4184e0d328635e5fba9d3c80cd4c8a7466178b7d9172c"
)
EXPECTED_ENDORSEMENT_PARENTS = 2_583
EXPECTED_ENDORSEMENT_SEGMENTS = 6_096


def endorsement_rows_by_parent() -> dict[str, list[dict]]:
    """Group every frozen endorsement segment by its parent document."""
    rows, _ = base.authenticated_full_rows()
    grouped: dict[str, list[dict]] = {}
    for row in rows:
        if row.get("route") != "endorsements":
            continue
        grouped.setdefault(str(row["parent_document_id"]), []).append(row)
    return grouped


def full_corpus_census() -> dict:
    """Compute the exact production batch census with no model call.

    The Stage-A count is exact, because every model-decision line is known
    before inference.  The Stage-B count is an upper bound, because only
    Stage-A anchors become Stage-B targets.
    """
    grouped = endorsement_rows_by_parent()
    stage_a_batches = 0
    model_decision_lines = 0
    inventory_lines = 0
    dispositions: Counter = Counter()
    per_parent: list[dict] = []
    for parent_id in sorted(grouped):
        parent = parent_inventory_from_export_rows(grouped[parent_id])
        lines = parent["lines"]
        inventory_lines += len(lines)
        targets = sum(1 for line in lines if line["model_decision_required"])
        model_decision_lines += targets
        for line in lines:
            dispositions[str(line["deterministic_disposition"])] += 1
        batches = math.ceil(targets / STAGE_A_SLOT_COUNT) if targets else 0
        stage_a_batches += batches
        per_parent.append({
            "parent_document_id": parent_id,
            "segments": len(grouped[parent_id]),
            "inventory_lines": len(lines),
            "model_decision_lines": targets,
            "stage_a_batches": batches,
        })
    return {
        "version": "candidate_endorsement_round4_production_census_v1",
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "model_calls": 0,
        "source_export": SOURCE_EXPORT,
        "source_export_manifest_sha256": FROZEN_SOURCE_MANIFEST_SHA256,
        "line_inventory_scope_rule": ROUND4_LINE_INVENTORY_SCOPE_RULE,
        "round4_production_contract_sha256": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "parents": len(grouped),
        "segments": sum(len(value) for value in grouped.values()),
        "inventory_lines": inventory_lines,
        "model_decision_lines": model_decision_lines,
        "deterministic_dispositions": dict(sorted(dispositions.items())),
        "stage_a_slot_count": STAGE_A_SLOT_COUNT,
        "stage_b_slot_count": STAGE_B_SLOT_COUNT,
        "stage_a_batches": stage_a_batches,
        "stage_b_batches_upper_bound": math.ceil(
            model_decision_lines / STAGE_B_SLOT_COUNT
        ),
        "stage_b_batches_note": (
            "Only Stage-A anchors become Stage-B targets. The exact Stage-B "
            "count is known after Stage A completes."
        ),
        # The independent sample returned 2,512 anchors from 4,391 model
        # decision lines.  That sample was selected for endorsement content,
        # so this ratio overstates the corpus rate.  It is a planning figure,
        # not a prediction.
        "stage_b_batches_enriched_sample_estimate": math.ceil(
            model_decision_lines * (2_512 / 4_391) / STAGE_B_SLOT_COUNT
        ),
        "per_parent_sha256": hashlib.sha256(
            canonical_json(per_parent).encode("utf-8")
        ).hexdigest(),
    }


def _census_report(args: argparse.Namespace) -> int:
    census = full_corpus_census()
    if census["parents"] != EXPECTED_ENDORSEMENT_PARENTS:
        raise ValueError(
            f"endorsement parent count differs: {census['parents']}"
        )
    if census["segments"] != EXPECTED_ENDORSEMENT_SEGMENTS:
        raise ValueError(
            f"endorsement segment count differs: {census['segments']}"
        )
    text = json.dumps(census, ensure_ascii=False, indent=2, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0


def stage_a_batches_for_production(
    grouped: Mapping[str, Sequence[Mapping]],
) -> list[dict]:
    """Build production Stage-A batches with the reviewed round-four builder."""
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        build_fresh_stage_a_batches,
    )

    parents = [
        parent_inventory_from_export_rows(grouped[parent_id])
        for parent_id in sorted(grouped)
    ]
    return build_fresh_stage_a_batches(parents)


def anchors_from_stage_a(
    stage_a_rows: Sequence[Mapping],
) -> dict[str, list[tuple[str, str]]]:
    """Return every Stage-A anchor as (batch_id, slot) keyed by parent."""
    anchors: dict[str, list[tuple[str, str]]] = {}
    for row in stage_a_rows:
        if row.get("technical_pass") is not True:
            continue
        output = row.get("model_output") or {}
        for slot, decision in sorted(output.items()):
            if not isinstance(decision, Mapping):
                continue
            if decision.get("role") in ANCHOR_ROLES:
                anchors.setdefault(
                    str(row["parent_document_id"]), []
                ).append((str(row["batch_id"]), slot))
    return anchors


def _production_stage_a_parents(batches, manifest):
    """Group Stage-A batches into parents, allowing a parent with no batches.

    The development check requires every selected parent to appear in the
    batches. That does not hold on the full corpus. A parent whose every line
    falls outside the attested context scope has no line for the model to
    decide, so the builder emits no batch for it. Two of the 2,583 endorsement
    parents are exactly that: pages of 403 and 300 characters whose lines are
    all `outside_attested_context_scope`.

    Such a parent can never yield an endorsement record, and that is a
    deterministic fact from the inventory rather than a gap. This check keeps
    the guarantee that matters, that no batch names a parent outside the
    selection, and verifies that every absent parent really has nothing to
    decide instead of assuming it.
    """
    from collections import defaultdict

    from scripts.build_candidate_endorsement_round4_development_inputs import (
        _validated_parent_from_stage_a_batches,
    )

    grouped = defaultdict(list)
    for batch in batches:
        grouped[str(batch.get("parent_document_id"))].append(batch)
    selected = set(manifest["selected_parent_ids"])
    unexpected = set(grouped) - selected
    if unexpected:
        raise ValueError(
            f"Stage-A batches name {len(unexpected)} parents outside the selection"
        )
    absent = sorted(selected - set(grouped))
    if absent:
        grouped_rows = endorsement_rows_by_parent()
        for parent_id in absent:
            parent = parent_inventory_from_export_rows(grouped_rows[parent_id])
            decidable = sum(
                1 for line in parent["lines"] if line["model_decision_required"]
            )
            if decidable:
                raise ValueError(
                    f"selected parent {parent_id} has {decidable} decidable "
                    "lines but produced no Stage-A batch"
                )
    return [
        _validated_parent_from_stage_a_batches(grouped[parent_id])
        for parent_id in sorted(grouped)
    ]


def authenticate_production_stage_a_run(
    stage_a_input_dir: Path,
    stage_a_output_dir: Path,
    *,
    expected_input_manifest_sha256: str,
) -> tuple[list[dict], list[dict], dict, dict]:
    """Authenticate a complete production Stage-A run and bind its bytes.

    The development authenticator cannot be reused. It requires
    ``development_only`` to be true and it compares against the development
    authorizations and generation configuration, all of which differ by design
    on a production artifact.

    This check keeps the same guarantees: the manifest and every chunk match
    their digests, the output chunk set is exactly the expected one, the run
    metadata states the production scope and binds the same manifest, every
    output row is technically complete, and the row order matches the manifest.
    """
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        _validated_parent_from_stage_a_batches,
        sha256_file,
        strict_json,
    )
    from scripts.hive import (
        extract_candidate_endorsement_round4_development_vllm as core,
    )
    from scripts.hive import (
        extract_candidate_endorsement_round4_production_vllm as prod,
    )

    manifest = prod.validate_production_manifest(
        stage_a_input_dir / "manifest.json",
        stage_a_input_dir / "chunks",
        stage="line_decision",
        expected_manifest_sha256=expected_input_manifest_sha256,
    )
    input_paths = [
        stage_a_input_dir / "chunks" / str(entry["file"])
        for entry in manifest["chunks"]
    ]
    batches = [
        row for path in input_paths for row in core.read_gzip_jsonl(path)
    ]
    # Stage B reasons over whole parents. A bounded pilot selects individual
    # batches, so its parents are partial and cannot produce Stage-B input.
    if manifest.get("full_corpus") is not True:
        raise ValueError(
            "Stage B requires a full-corpus Stage-A run. This artifact is a "
            "bounded pilot, so its parents are incomplete."
        )
    if stage_a_output_dir.is_symlink() or not stage_a_output_dir.is_dir():
        raise ValueError("production Stage-A output directory is absent")
    expected_output_names = [
        path.name.replace(".jsonl.gz", ".out.jsonl.gz") for path in input_paths
    ]
    observed = sorted(
        p.name for p in stage_a_output_dir.glob("chunk_*.out.jsonl.gz")
    )
    if observed != sorted(expected_output_names):
        raise ValueError(
            "production Stage-A output chunks are incomplete: "
            f"{len(observed)} of {len(expected_output_names)}"
        )
    output_paths = [stage_a_output_dir / name for name in expected_output_names]
    output_rows = [
        row for path in output_paths for row in core.read_gzip_jsonl(path)
    ]

    metadata_paths = sorted(stage_a_output_dir.glob("run_metadata*.json"))
    if not metadata_paths:
        raise ValueError("production Stage-A run metadata is absent")
    generation_hashes: set[str] = set()
    for path in metadata_paths:
        metadata = strict_json(path)
        if type(metadata) is not dict:
            raise ValueError("production Stage-A run metadata is not an object")
        if (
            metadata.get("artifact_scope") != ROUND4_PRODUCTION_ARTIFACT_SCOPE
            or metadata.get("development_only") is not False
            or metadata.get("authorizations") != ROUND4_PRODUCTION_AUTHORIZATIONS
            or metadata.get("stage") != "line_decision"
            or metadata.get("input_manifest_sha256")
            != expected_input_manifest_sha256
        ):
            raise ValueError("production Stage-A run metadata provenance differs")
        if metadata.get("technical_fail_count") != 0:
            raise ValueError(
                "production Stage-A run reports a technical failure; repair it "
                "before building Stage B"
            )
        generation_hashes.add(str(metadata.get("generation_config_sha256")))
    if len(generation_hashes) != 1:
        raise ValueError("production Stage-A shards disagree on the generation identity")

    for row in output_rows:
        if row.get("technical_pass") is not True:
            raise ValueError(
                f"production Stage-A row {row.get('batch_id')} did not pass"
            )
    observed_ids = [row["batch_id"] for row in output_rows]
    if sorted(observed_ids) != sorted(manifest["expected_batch_ids"]):
        raise ValueError("production Stage-A output batch identities differ")

    order = {bid: i for i, bid in enumerate(manifest["expected_batch_ids"])}
    batches.sort(key=lambda b: order[str(b["batch_id"])])
    output_rows.sort(key=lambda r: order[str(r["batch_id"])])

    # Reconstruct parents only after the scope, output set, metadata and row
    # checks have passed. A development-scoped or failed run is then refused on
    # that ground, rather than on a confusing batch-field message raised while
    # parsing parents it should never have reached.
    parents = _production_stage_a_parents(batches, manifest)

    inventory_payload = {
        "version": "candidate_endorsement_round4_production_stage_a_output_inventory_v1",
        "input_manifest_sha256": expected_input_manifest_sha256,
        "generation_config_sha256": sorted(generation_hashes)[0],
        "ordered_output_batch_ids_sha256": _sha256_json_local(
            [row["batch_id"] for row in output_rows]
        ),
        "output_files": [
            {
                "file": p.name,
                "bytes": p.stat().st_size,
                "sha256": sha256_file(p),
            }
            for p in output_paths
        ],
        "run_metadata_files": [
            {
                "file": p.name,
                "bytes": p.stat().st_size,
                "sha256": sha256_file(p),
            }
            for p in metadata_paths
        ],
    }
    provenance = {
        **inventory_payload,
        "output_inventory_sha256": _sha256_json_local(inventory_payload),
    }
    return batches, output_rows, {"manifest": manifest, "parents": parents}, provenance


def _sha256_json_local(value) -> str:
    return hashlib.sha256(canonical_json(value).encode("utf-8")).hexdigest()


def _stage_b(args: argparse.Namespace) -> int:
    """Build production Stage-B batches from authenticated Stage-A output.

    Stage A proposes anchors. Only those anchors become Stage-B targets, so
    the exact Stage-B scale is unknown until Stage A finishes.
    """
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        build_fresh_stage_b_batches,
    )

    batches, output_rows, authenticated, stage_a_provenance = (
        authenticate_production_stage_a_run(
            args.stage_a_input_dir,
            args.stage_a_output_dir,
            expected_input_manifest_sha256=(
                args.expected_stage_a_input_manifest_sha256
            ),
        )
    )
    # The context-radius experiment was not promoted, so production keeps the
    # reviewed radius of zero.
    stage_b_batches, diagnostics = build_fresh_stage_b_batches(
        parents=authenticated["parents"],
        stage_a_batches=batches,
        stage_a_output_rows=output_rows,
        stage_a_input_manifest_sha256=(
            args.expected_stage_a_input_manifest_sha256
        ),
        stage_a_output_inventory_sha256=stage_a_provenance[
            "output_inventory_sha256"
        ],
        read_only_context_radius=0,
    )
    if not stage_b_batches:
        raise ValueError("Stage A produced no Stage-B anchor proposals")
    manifest = write_production_artifact(
        output_dir=args.output_dir,
        stage="record_extraction",
        batches=stage_b_batches,
        selected_parent_ids=list(
            authenticated["manifest"]["selected_parent_ids"]
        ),
        chunk_size=args.chunk_size,
        full_corpus=bool(
            authenticated["manifest"].get("full_corpus", False)
        ),
    )
    digest = hashlib.sha256(
        (args.output_dir / "manifest.json").read_bytes()
    ).hexdigest()
    anchors = sum(len(b.get("targets") or []) for b in stage_b_batches)
    print(canonical_json({
        "stage": "record_extraction",
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "stage_a_batches": len(batches),
        "stage_a_outputs": len(output_rows),
        "stage_b_batches": manifest["batch_count"],
        "stage_b_anchors": anchors,
        "chunks": len(manifest["chunks"]),
        "manifest_sha256": digest,
        "stage_a_output_inventory_sha256": stage_a_provenance[
            "output_inventory_sha256"
        ],
        "read_only_context_radius": 0,
        "output_dir": str(args.output_dir),
        "inference_submitted": False,
    }))
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    census = sub.add_parser(
        "census", help="report the exact full-corpus batch census"
    )
    census.add_argument("--report", type=Path)
    census.set_defaults(handler=_census_report)

    stage_a = sub.add_parser(
        "stage-a", help="write the production Stage-A batches"
    )
    stage_a.add_argument("--output-dir", type=Path, required=True)
    stage_a.add_argument("--chunk-size", type=int, default=60)
    stage_a.add_argument(
        "--largest", type=int,
        help="build a bounded pilot from the N largest prompts",
    )
    stage_a.set_defaults(handler=_stage_a)

    stage_b = sub.add_parser(
        "stage-b", help="build Stage-B batches from authenticated Stage-A output"
    )
    stage_b.add_argument("--stage-a-input-dir", type=Path, required=True)
    stage_b.add_argument("--stage-a-output-dir", type=Path, required=True)
    stage_b.add_argument(
        "--expected-stage-a-input-manifest-sha256", required=True,
    )
    stage_b.add_argument("--output-dir", type=Path, required=True)
    stage_b.add_argument("--chunk-size", type=int, default=60)
    stage_b.set_defaults(handler=_stage_b)

    args = parser.parse_args()
    return args.handler(args)


def write_production_artifact(
    *,
    output_dir: Path,
    stage: str,
    batches: Sequence[Mapping[str, Any]],
    selected_parent_ids: Sequence[str],
    chunk_size: int,
    full_corpus: bool,
) -> dict:
    """Write a production input artifact with the production scope.

    This mirrors the development writer but carries the production scope and
    authorizations, and it enforces no development sample binding.
    """
    from scripts.build_candidate_endorsement_round4_development_inputs import (
        BUILDER_VERSION,
        _round4_module,
        _schema_catalog,
        deterministic_gzip_jsonl,
        sha256_json,
    )

    if output_dir.exists():
        raise ValueError(f"output directory already exists: {output_dir}")
    if stage not in ("line_decision", "record_extraction"):
        raise ValueError(f"unsupported production stage: {stage}")
    if not batches or chunk_size <= 0:
        raise ValueError("a production artifact needs batches and a chunk size")
    batch_ids = [str(batch["batch_id"]) for batch in batches]
    if len(batch_ids) != len(set(batch_ids)):
        raise ValueError("production batch identities duplicate")
    if {str(batch["stage"]) for batch in batches} != {stage}:
        raise ValueError("production batch stages differ")

    round4 = _round4_module()
    prompt = (
        round4.ROUND4_STAGE_A_PROMPT if stage == "line_decision"
        else round4.ROUND4_STAGE_B_PROMPT
    )
    slot_count = (
        STAGE_A_SLOT_COUNT if stage == "line_decision" else STAGE_B_SLOT_COUNT
    )
    schema_catalog = _schema_catalog(stage, slot_count)
    chunk_payloads = []
    for index, offset in enumerate(range(0, len(batches), chunk_size)):
        rows = list(batches[offset : offset + chunk_size])
        payload = deterministic_gzip_jsonl(rows)
        chunk_payloads.append((
            f"chunk_{index:05d}.jsonl.gz", payload,
            [str(row["batch_id"]) for row in rows],
        ))
    manifest: dict[str, Any] = {
        "version": "candidate_endorsement_round4_production_input_v1",
        "stage": stage,
        "pipeline_revision": 4,
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "authorizations": dict(ROUND4_PRODUCTION_AUTHORIZATIONS),
        "full_corpus": bool(full_corpus),
        "prompt_sha256": hashlib.sha256(prompt.encode("utf-8")).hexdigest(),
        "schema_catalog": schema_catalog,
        "schema_catalog_sha256": sha256_json(schema_catalog),
        "slot_count": slot_count,
        "chunk_size": chunk_size,
        "batch_count": len(batches),
        "expected_batch_ids": batch_ids,
        "expected_batch_ids_sha256": sha256_json(batch_ids),
        "selected_parent_count": len(selected_parent_ids),
        "selected_parent_ids": list(selected_parent_ids),
        "selected_parent_ids_sha256": sha256_json(list(selected_parent_ids)),
        "chunks": [
            {
                "file": name,
                "bytes": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "records": len(ids),
                "batch_ids_sha256": sha256_json(ids),
            }
            for name, payload, ids in chunk_payloads
        ],
        "source_export_manifest_sha256": FROZEN_SOURCE_MANIFEST_SHA256,
        "line_inventory_scope_rule": ROUND4_LINE_INVENTORY_SCOPE_RULE,
        "round4_production_contract_sha256": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "builder_version": BUILDER_VERSION,
    }
    manifest["manifest_payload_sha256"] = sha256_json(manifest)
    output_dir.mkdir(parents=True)
    (output_dir / "chunks").mkdir()
    for name, payload, _ids in chunk_payloads:
        (output_dir / "chunks" / name).write_bytes(payload)
    (output_dir / "manifest.json").write_text(
        canonical_json(manifest) + "\n", encoding="utf-8"
    )
    return manifest


def _stage_a(args: argparse.Namespace) -> int:
    grouped = endorsement_rows_by_parent()
    batches = stage_a_batches_for_production(grouped)
    selected = sorted(grouped)
    full_corpus = True
    if args.largest:
        # A bounded pilot over the largest prompts.  These carry the context
        # risk, so they are the batches worth proving on a GPU first.
        from scripts.hive import (
            extract_candidate_endorsement_round4_development_vllm as core,
        )
        ranked = sorted(
            batches,
            key=lambda b: -sum(
                len(m["content"]) for m in core.build_conversation(b)
            ),
        )
        batches = ranked[: args.largest]
        selected = sorted({str(b["parent_document_id"]) for b in batches})
        full_corpus = False
    manifest = write_production_artifact(
        output_dir=args.output_dir,
        stage="line_decision",
        batches=batches,
        selected_parent_ids=selected,
        chunk_size=args.chunk_size,
        full_corpus=full_corpus,
    )
    digest = hashlib.sha256(
        (args.output_dir / "manifest.json").read_bytes()
    ).hexdigest()
    print(canonical_json({
        "stage": "line_decision",
        "artifact_scope": ROUND4_PRODUCTION_ARTIFACT_SCOPE,
        "full_corpus": full_corpus,
        "parents": len(selected),
        "batches": manifest["batch_count"],
        "chunks": len(manifest["chunks"]),
        "manifest_sha256": digest,
        "output_dir": str(args.output_dir),
        "inference_submitted": False,
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
