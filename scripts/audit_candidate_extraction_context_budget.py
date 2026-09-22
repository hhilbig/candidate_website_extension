#!/usr/bin/env python3
"""Census exact v2.1 prompt-token context use before Hive inference."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Iterable

REPO = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO))

from scripts.build_candidate_extraction_v2_pilot import (  # noqa: E402
    read_jsonl_gz,
    sha256_file,
    source_chunk_paths,
)
from scripts.hive.extract_candidate_pages_vllm import (  # noqa: E402
    build_user_message,
)
from src.candidate_extraction import (  # noqa: E402
    MODEL_ID,
    MODEL_REVISION,
    get_extraction_contract,
    get_prompts,
)


def _sha256_text(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def audit_context_budget(
    rows: Iterable[dict[str, Any]],
    tokenizer: Any,
    *,
    max_model_len: int,
    max_output_tokens: int,
    reserved_tokens: int = 500,
    batch_size: int = 128,
) -> dict[str, Any]:
    """Apply the runner's exact v2.1 context preflight to every source row."""
    prompts = get_prompts("2.1")
    system_tokens = {
        route: len(tokenizer.encode(prompt)) for route, prompt in prompts.items()
    }
    route_counts: Counter[str] = Counter()
    over_budget: list[dict[str, Any]] = []
    largest: list[dict[str, Any]] = []
    total_rows = 0
    minimum_margin: int | None = None

    pending_rows: list[dict[str, Any]] = []
    pending_messages: list[str] = []

    def consume() -> None:
        nonlocal minimum_margin, total_rows
        if not pending_rows:
            return
        encoded = tokenizer(
            pending_messages,
            add_special_tokens=True,
            padding=False,
            truncation=False,
            return_length=True,
        )
        lengths = list(encoded["length"])
        if len(lengths) != len(pending_rows):
            raise RuntimeError("tokenizer returned the wrong number of lengths")
        for row, user_tokens in zip(pending_rows, lengths):
            route = str(row["route"])
            total = (
                int(system_tokens[route])
                + int(user_tokens)
                + int(max_output_tokens)
                + int(reserved_tokens)
            )
            margin = int(max_model_len) - total
            result = {
                "document_id": str(row["document_id"]),
                "parent_document_id": str(row["parent_document_id"]),
                "candidate_cycle_id": str(row["candidate_cycle_id"]),
                "route": route,
                "system_tokens": int(system_tokens[route]),
                "user_message_tokens": int(user_tokens),
                "reserved_tokens": int(reserved_tokens),
                "max_output_tokens": int(max_output_tokens),
                "total_reserved_context_tokens": total,
                "context_margin_tokens": margin,
            }
            route_counts[route] += 1
            total_rows += 1
            minimum_margin = margin if minimum_margin is None else min(
                minimum_margin, margin
            )
            largest.append(result)
            largest.sort(
                key=lambda item: (
                    -int(item["total_reserved_context_tokens"]),
                    str(item["document_id"]),
                )
            )
            del largest[10:]
            if margin < 0:
                over_budget.append(result)
        pending_rows.clear()
        pending_messages.clear()

    for row in rows:
        source_text = row.get("source_text")
        if not isinstance(source_text, str):
            raise TypeError(f"{row.get('document_id')}: source_text must be a string")
        route = str(row.get("route"))
        if route not in prompts:
            raise ValueError(f"{row.get('document_id')}: unknown route {route!r}")
        pending_rows.append(row)
        pending_messages.append(build_user_message(row, source_text, "2.1"))
        if len(pending_rows) >= batch_size:
            consume()
    consume()
    return {
        "rows": total_rows,
        "rows_by_route": dict(sorted(route_counts.items())),
        "max_model_len": int(max_model_len),
        "max_output_tokens": int(max_output_tokens),
        "reserved_tokens": int(reserved_tokens),
        "system_prompt_tokens_by_route": dict(sorted(system_tokens.items())),
        "minimum_context_margin_tokens": minimum_margin,
        "over_budget_rows": len(over_budget),
        "over_budget_document_ids": [
            str(item["document_id"]) for item in over_budget
        ],
        "largest_total_context_rows": largest,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--full-export-dir", type=Path, required=True)
    parser.add_argument("--model-id", default=MODEL_ID)
    parser.add_argument("--model-revision", default=MODEL_REVISION)
    parser.add_argument("--max-model-len", type=int, default=32_768)
    parser.add_argument("--max-output-tokens", type=int, default=24_576)
    parser.add_argument("--reserved-tokens", type=int, default=500)
    parser.add_argument("--batch-size", type=int, default=128)
    parser.add_argument("--output", type=Path)
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    from transformers import AutoTokenizer

    manifest, chunks = source_chunk_paths(args.full_export_dir)
    tokenizer = AutoTokenizer.from_pretrained(
        args.model_id,
        revision=args.model_revision,
        trust_remote_code=True,
    )
    rows = (row for path in chunks for row in read_jsonl_gz(path))
    census = audit_context_budget(
        rows,
        tokenizer,
        max_model_len=args.max_model_len,
        max_output_tokens=args.max_output_tokens,
        reserved_tokens=args.reserved_tokens,
        batch_size=args.batch_size,
    )
    backend_tokenizer = tokenizer.backend_tokenizer.to_str()
    contract = get_extraction_contract("2.1")
    report = {
        "audit_type": "candidate_extraction_v2_1_exact_tokenizer_context_budget",
        "status": "passed" if census["over_budget_rows"] == 0 else "failed",
        "source_export_directory": str(args.full_export_dir),
        "source_export_manifest_sha256": sha256_file(
            args.full_export_dir / "manifest.json"
        ),
        "source_corpus_sha256": manifest["source_corpus_sha256"],
        "model_id": args.model_id,
        "model_revision": args.model_revision,
        "tokenizer_class": type(tokenizer).__name__,
        "tokenizer_vocabulary_size": len(tokenizer),
        "backend_tokenizer_json_sha256": _sha256_text(backend_tokenizer),
        "schema_version": "2.1",
        "extraction_contract_sha256": contract["extraction_contract_sha256"],
        "user_message_template_sha256": contract["user_message_template_sha256"],
        "inference_display_escape_spec_sha256": contract[
            "inference_display_escape_spec_sha256"
        ],
        "counting_rule": (
            "len(tokenizer.encode(system_prompt)) + "
            "len(tokenizer.encode(full_user_message)) + max_output_tokens + "
            "500 reserved tokens <= max_model_len; full_user_message is built "
            "by the production runner and includes carried endorsement context"
        ),
        **census,
    }
    payload = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    if args.output:
        if args.output.exists():
            raise FileExistsError(f"refusing to overwrite {args.output}")
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(payload, encoding="utf-8")
    print(payload, end="")


if __name__ == "__main__":
    main()
