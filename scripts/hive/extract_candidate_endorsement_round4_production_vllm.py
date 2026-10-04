#!/usr/bin/env python3
"""Run the round-four endorsement stages over the full production corpus.

The reviewed round-four semantics live in ``src/candidate_endorsement_round4``.
The development runner implements the batch validation, prompt rendering, and
output checks against those semantics.  This runner reuses that pure logic and
changes only the scope: it accepts a production artifact, it refuses a
development artifact, and it never claims release authority.

The development runner stays byte-identical, because an origin attestation
pins its digest.  This file is additive.
"""
from __future__ import annotations

import argparse
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

from scripts.hive import (  # noqa: E402
    extract_candidate_endorsement_round4_development_vllm as core,
)
from src.candidate_endorsement_round4_production import (  # noqa: E402
    PRODUCTION_STAGES,
    ROUND4_PRODUCTION_ARTIFACT_SCOPE,
    ROUND4_PRODUCTION_AUTHORIZATIONS,
    get_round4_production_contract,
    validate_scope_authorizations,
)

RUNNER_VERSION = "candidate_endorsement_round4_production_vllm_v1"
MANIFEST_VERSION = "candidate_endorsement_round4_production_input_v1"
OUTPUT_VERSION = "candidate_endorsement_round4_production_output_v1"
RUN_METADATA_VERSION = (
    "candidate_endorsement_round4_production_run_metadata_v1"
)
ARTIFACT_SCOPE = ROUND4_PRODUCTION_ARTIFACT_SCOPE
PRODUCTION_AUTHORIZATIONS = dict(ROUND4_PRODUCTION_AUTHORIZATIONS)

# The production run reuses the reviewed prompts, schemas, seed, and decoding
# settings without change.  Only the scope and the manifest identity differ.
MODEL_ID = core.MODEL_ID
MODEL_REVISION = core.MODEL_REVISION
RUNTIME_LOCK_SHA256 = core.RUNTIME_LOCK_SHA256
GENERATION_SEED = core.GENERATION_SEED
MAX_OUTPUT_TOKENS = dict(core.MAX_OUTPUT_TOKENS)

# The development runs used a 32,768-token window, which fits a 40-parent
# sample.  A token preflight over the full corpus on September 15, 2026 found
# 106 Stage-A prompts above the budget that window allows, and the largest
# needs 34,248 tokens.  The runner refuses to truncate an input, so that
# window would abort the production job before any inference.
#
# The checkpoint supports 262,144 positions, so the window is a runner
# setting, not a model limit.  Raising it changes no prompt, schema, seed, or
# frozen contract hash.  It only lets the exact same prompts fit.
MAX_MODEL_LEN = 49_152
DEVELOPMENT_MAX_MODEL_LEN = core.MAX_MODEL_LEN
MEASURED_MAX_STAGE_A_PROMPT_TOKENS = 34_248

stage_prompt = core.stage_prompt
stage_prompt_sha256 = core.stage_prompt_sha256
schema_for_batch = core.schema_for_batch
render_batch_prompt = core.render_batch_prompt
build_conversation = core.build_conversation
validate_stage_output = core.validate_stage_output
canonical_json = core.canonical_json
sha256_json = core.sha256_json
file_sha256 = core.file_sha256


def validate_production_manifest(
    manifest_path: Path, chunks_dir: Path, *, stage: str,
    expected_manifest_sha256: str,
) -> dict:
    """Authenticate a production input manifest and refuse any other scope."""
    if stage not in PRODUCTION_STAGES:
        raise ValueError(f"unsupported production stage: {stage}")
    manifest = core.strict_json_loads(manifest_path.read_text())
    if file_sha256(manifest_path) != expected_manifest_sha256:
        raise ValueError("production input manifest bytes differ")
    if manifest.get("version") != MANIFEST_VERSION:
        raise ValueError("production input manifest version differs")
    # A production artifact is either the full corpus or a bounded pilot over
    # it.  Both carry the production scope, and the flag must be explicit.
    if manifest.get("full_corpus") not in (True, False):
        raise ValueError("production input manifest omits its corpus scope")
    validate_scope_authorizations(
        str(manifest.get("artifact_scope")),
        dict(manifest.get("authorizations") or {}),
    )
    if manifest.get("artifact_scope") != ARTIFACT_SCOPE:
        raise ValueError("production input manifest scope differs")
    contract = get_round4_production_contract()
    if manifest.get("round4_production_contract_sha256") != contract[
        "round4_production_contract_sha256"
    ]:
        raise ValueError("production input manifest contract differs")
    for entry in manifest.get("chunks") or []:
        path = chunks_dir / str(entry["file"])
        if not path.is_file() or file_sha256(path) != entry["sha256"]:
            raise ValueError(f"production chunk bytes differ: {entry['file']}")
    return manifest


CONTEXT_SAFETY_MARGIN_TOKENS = getattr(
    core, "CONTEXT_SAFETY_MARGIN_TOKENS", 256
)


def prompt_budget(stage: str, max_model_len: int = MAX_MODEL_LEN) -> int:
    """Return the largest prompt this stage may send, in tokens."""
    return (
        int(max_model_len)
        - int(MAX_OUTPUT_TOKENS[stage])
        - int(CONTEXT_SAFETY_MARGIN_TOKENS)
    )


def assert_prompt_budget_fits(
    stage: str, observed_max_prompt_tokens: int,
    max_model_len: int = MAX_MODEL_LEN,
) -> None:
    """Fail before submission when a measured prompt cannot fit.

    The runner refuses to truncate an input, so one oversized batch aborts a
    whole job after the model has loaded.  A preflight must therefore run on
    the exact tokenizer before any production submission.
    """
    budget = prompt_budget(stage, max_model_len)
    if int(observed_max_prompt_tokens) > budget:
        raise ValueError(
            f"{stage}: the largest prompt needs "
            f"{int(observed_max_prompt_tokens)} tokens and the budget is "
            f"{budget}; raise max_model_len before submitting"
        )


PRODUCTION_SCOPE_FIELDS = {
    "artifact_scope": ARTIFACT_SCOPE,
    "development_only": False,
    "authorizations": dict(PRODUCTION_AUTHORIZATIONS),
}


def _restamp(row: dict, *, version: str) -> dict:
    """Replace the development scope labels with the production ones.

    The parsing, validation, and provenance logic is shared with the reviewed
    development runner, so the substantive content of a row is identical.  Only
    the scope labels differ, and they must never say development on a
    production artifact.
    """
    restamped = dict(row)
    restamped["version"] = version
    restamped["runner_version"] = RUNNER_VERSION
    restamped.update(PRODUCTION_SCOPE_FIELDS)
    return restamped


def derive_production_output_row(**kwargs) -> dict:
    """Derive one output row and stamp it with the production scope."""
    return _restamp(core.derive_output_row(**kwargs), version=OUTPUT_VERSION)


def process_production_chunk(
    *,
    input_path,
    output_path,
    llm,
    tokenizer,
    sampling_params_factory,
    args,
    input_manifest_sha256: str,
    expected_input_chunk_sha256: str,
    generation_config_sha256_value: str,
) -> list[dict]:
    """Infer one authenticated chunk and publish production-scoped rows."""
    if output_path.exists() or output_path.is_symlink():
        raise FileExistsError(f"refusing to overwrite output: {output_path}")
    core._require_regular_file(input_path, "input chunk")
    input_bytes = input_path.read_bytes()
    observed = core.sha256_bytes(input_bytes)
    if observed != expected_input_chunk_sha256:
        raise ValueError("input chunk bytes differ from the manifest")
    batches = core.read_gzip_jsonl_bytes(input_bytes, label=input_path.name)

    conversations, token_counts, sampling = [], [], []
    cache: dict = {}
    budget = prompt_budget(args.stage, args.max_model_len)
    for index, batch in enumerate(batches):
        core._validate_batch(batch, stage=args.stage, label=f"batch[{index}]")
        conversation = core.build_conversation(batch)
        tokens = core.prompt_token_count(tokenizer, conversation)
        if tokens > budget:
            raise ValueError(
                f"{batch['batch_id']}: prompt needs {tokens} tokens and the "
                f"budget is {budget}; input truncation is forbidden"
            )
        schema = core.schema_for_batch(batch)
        digest = core.sha256_json(schema)
        if digest not in cache:
            cache[digest] = sampling_params_factory(
                temperature=0.0,
                max_tokens=int(args.max_output_tokens),
                seed=int(args.seed),
                **core.structured_parameters(schema),
            )
        conversations.append(conversation)
        token_counts.append(tokens)
        sampling.append(cache[digest])

    outputs = llm.chat(
        conversations, sampling, use_tqdm=True,
        chat_template_kwargs={"enable_thinking": False},
    )
    if len(outputs) != len(batches):
        raise RuntimeError("vLLM returned the wrong number of outputs")
    rows = []
    for batch, output, tokens in zip(batches, outputs, token_counts):
        raw, finish, stop, output_tokens = core._completion_fields(output)
        rows.append(derive_production_output_row(
            batch=batch,
            raw_output=raw,
            finish_reason=finish,
            stop_reason=stop,
            output_tokens=output_tokens,
            prompt_tokens=tokens,
            args=args,
            input_manifest_sha256=input_manifest_sha256,
            input_chunk_sha256=observed,
            generation_config_sha256_value=generation_config_sha256_value,
        ))
    core._exclusive_publish_bytes(output_path, core._gzip_jsonl_bytes(rows))
    return rows


def generation_config_payload(
    stage: str, *, input_manifest_sha256: str,
) -> dict:
    """Build the production generation identity for one stage."""
    return {
        "version": RUNNER_VERSION,
        "stage": stage,
        "artifact_scope": ARTIFACT_SCOPE,
        "authorizations": dict(PRODUCTION_AUTHORIZATIONS),
        "production_binding": get_round4_production_contract()[
            "production_binding_sha256"
        ],
        "round4_production_contract_sha256": get_round4_production_contract()[
            "round4_production_contract_sha256"
        ],
        "model_id": MODEL_ID,
        "model_revision": MODEL_REVISION,
        "runtime_lock_sha256": RUNTIME_LOCK_SHA256,
        "seed": GENERATION_SEED,
        "temperature": 0.0,
        "thinking_enabled": False,
        "max_model_len": MAX_MODEL_LEN,
        "max_output_tokens": MAX_OUTPUT_TOKENS[stage],
        "structured_decoding_backend": core.STRUCTURED_DECODING_BACKEND
        if hasattr(core, "STRUCTURED_DECODING_BACKEND") else "xgrammar",
        "system_prompt_sha256": stage_prompt_sha256(stage),
        "input_manifest_sha256": input_manifest_sha256,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--chunks-dir", type=Path, required=True)
    parser.add_argument("--input-manifest", type=Path, required=True)
    parser.add_argument(
        "--expected-input-manifest-sha256", required=True,
    )
    parser.add_argument(
        "--expected-generation-config-sha256",
        help="bind the externally reviewed generation identity",
    )
    parser.add_argument("--output-dir", type=Path, required=True)
    parser.add_argument("--model-path", required=True)
    parser.add_argument(
        "--stage", choices=sorted(PRODUCTION_STAGES), required=True,
    )
    parser.add_argument("--max-model-len", type=int, default=MAX_MODEL_LEN)
    parser.add_argument("--max-output-tokens", type=int)
    parser.add_argument("--gpu-memory-utilization", type=float, default=0.92)
    parser.add_argument("--seed", type=int, default=GENERATION_SEED)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument(
        "--dry-run", action="store_true",
        help="authenticate the inputs and exit without loading the model",
    )
    parser.add_argument(
        "--resume", action="store_true",
        help="skip chunks that already published and continue the run",
    )
    args = parser.parse_args()

    manifest = validate_production_manifest(
        args.input_manifest, args.chunks_dir, stage=args.stage,
        expected_manifest_sha256=args.expected_input_manifest_sha256,
    )
    config = generation_config_payload(
        args.stage,
        input_manifest_sha256=args.expected_input_manifest_sha256,
    )
    observed_config_sha256 = sha256_json(config)
    if (
        args.expected_generation_config_sha256
        and args.expected_generation_config_sha256 != observed_config_sha256
    ):
        raise ValueError(
            "generation configuration differs from the reviewed expectation"
        )
    if args.stage == "line_decision":
        # The corpus-wide measurement is known, so refuse a window that the
        # largest prompt cannot fit before the model loads.
        assert_prompt_budget_fits(
            args.stage,
            MEASURED_MAX_STAGE_A_PROMPT_TOKENS,
            args.max_model_len,
        )
    if args.dry_run:
        print(canonical_json({
            "stage": args.stage,
            "artifact_scope": ARTIFACT_SCOPE,
            "batches": manifest.get("batch_count"),
            "chunks": len(manifest.get("chunks") or []),
            "generation_config_sha256": sha256_json(config),
            "model_loaded": False,
            "inference_submitted": False,
        }))
        return 0
    import os
    import socket
    from collections import Counter
    from datetime import datetime, timezone

    chunk_entries = list(manifest.get("chunks") or [])
    chunk_paths = [args.chunks_dir / str(e["file"]) for e in chunk_entries]
    selected = core.select_shard(
        chunk_paths, args.shard_index, args.shard_count
    )
    if not selected:
        raise ValueError("the selected shard holds no chunk")

    input_root = args.input_manifest.parent.resolve()
    output_root = args.output_dir.resolve()
    if (
        output_root == input_root
        or output_root in input_root.parents
        or input_root in output_root.parents
    ):
        raise ValueError("input and output roots must not overlap")
    args.output_dir.mkdir(parents=True, exist_ok=True)
    output_paths = [
        args.output_dir / p.name.replace(".jsonl.gz", ".out.jsonl.gz")
        for p in selected
    ]
    metadata_path = args.output_dir / (
        "run_metadata.json" if args.shard_count == 1
        else f"run_metadata_shard_{args.shard_index:05d}.json"
    )
    for path in [*output_paths, metadata_path]:
        if path.is_symlink():
            raise ValueError(f"artifact path is a symlink: {path}")
    stale = sorted(args.output_dir.glob(".*.tmp-*"))
    if stale:
        raise ValueError("stale temporary artifacts require a manual audit")

    # A chunk publishes atomically, so an existing output file is complete.
    # On a preemptible partition the job is requeued without warning, so a
    # resume skips the finished chunks and never rewrites one.
    completed = [p for p in output_paths if p.exists()]
    if completed and not args.resume:
        raise FileExistsError(
            "refusing to overwrite production artifacts: "
            + ", ".join(str(p) for p in completed)
            + "; pass --resume to continue an interrupted run"
        )
    pending = [
        (chunk, out) for chunk, out in zip(selected, output_paths)
        if not out.exists()
    ]
    if not pending:
        print(canonical_json({
            "stage": args.stage,
            "artifact_scope": ARTIFACT_SCOPE,
            "resumed": True,
            "already_complete_chunks": len(completed),
            "pending_chunks": 0,
            "note": "every selected chunk is already published",
        }))
        return 0
    # Every attempt keeps its own metadata, so the retry provenance survives.
    if metadata_path.exists():
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        metadata_path = args.output_dir / (
            f"{metadata_path.stem}_resume_{stamp}.json"
        )

    from vllm import LLM, SamplingParams

    started = datetime.now(timezone.utc).isoformat(timespec="seconds")
    llm = LLM(**core.llm_engine_kwargs(args))
    tokenizer = llm.get_tokenizer()
    by_name = {str(e["file"]): e for e in chunk_entries}
    rows: list[dict] = []
    for input_path, output_path in pending:
        rows.extend(process_production_chunk(
            input_path=input_path,
            output_path=output_path,
            llm=llm,
            tokenizer=tokenizer,
            sampling_params_factory=SamplingParams,
            args=args,
            input_manifest_sha256=args.expected_input_manifest_sha256,
            expected_input_chunk_sha256=str(by_name[input_path.name]["sha256"]),
            generation_config_sha256_value=sha256_json(config),
        ))

    metadata = {
        "version": RUN_METADATA_VERSION,
        "runner_version": RUNNER_VERSION,
        "round": 4,
        **PRODUCTION_SCOPE_FIELDS,
        "full_corpus": manifest.get("full_corpus"),
        "stage": args.stage,
        "input_manifest_sha256": args.expected_input_manifest_sha256,
        "generation_config_sha256": sha256_json(config),
        "generation_config": config,
        "max_model_len": int(args.max_model_len),
        "selected_chunk_names": [p.name for p in selected],
        "processed_chunk_names": [out.name for _chunk, out in pending],
        "skipped_complete_chunk_count": len(completed),
        "resumed": bool(completed),
        "processed_output_count": len(rows),
        "technical_pass_count": sum(
            r.get("technical_pass") is True for r in rows
        ),
        "technical_fail_count": sum(
            r.get("technical_pass") is not True for r in rows
        ),
        "parse_status_counts": dict(sorted(
            Counter(str(r.get("parse_status")) for r in rows).items()
        )),
        "max_prompt_tokens_used": max(
            (int(r["prompt_tokens_used"]) for r in rows), default=0
        ),
        "min_context_margin_tokens": min(
            (int(r["context_margin_tokens"]) for r in rows), default=0
        ),
        "started_at": started,
        "completed_at": datetime.now(timezone.utc).isoformat(
            timespec="seconds"
        ),
        "host": socket.gethostname(),
        "slurm_job_id": os.environ.get("SLURM_JOB_ID"),
    }
    core._exclusive_publish_bytes(
        metadata_path, (canonical_json(metadata) + "\n").encode("utf-8")
    )
    print(canonical_json({
        "stage": args.stage,
        "artifact_scope": ARTIFACT_SCOPE,
        "outputs": len(rows),
        "technical_pass": metadata["technical_pass_count"],
        "technical_fail": metadata["technical_fail_count"],
        "max_prompt_tokens_used": metadata["max_prompt_tokens_used"],
        "min_context_margin_tokens": metadata["min_context_margin_tokens"],
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
