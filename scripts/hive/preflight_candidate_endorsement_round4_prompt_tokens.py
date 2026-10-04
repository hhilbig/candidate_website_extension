#!/usr/bin/env python3
"""Measure exact prompt token counts before a round-four production run.

Character counts do not predict token counts for archived campaign pages.
The measured ratio on the largest Stage-A prompts is about 3.47 characters per
token, not the four characters a reader might assume.

The runner refuses to truncate an input, so one oversized batch aborts a whole
job after the model has loaded.  Run this preflight on Hive, where the exact
tokenizer is cached, and submit only when it reports that every prompt fits.

The script loads no model weights and needs no GPU.  Run it on the login node.
"""
from __future__ import annotations

import argparse
import glob
import gzip
import json
from pathlib import Path
import sys

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO))

DEFAULT_SNAPSHOT_GLOB = (
    "/nfs/hive/scratch/$USER/hf/hub/"
    "models--Qwen--Qwen3.6-27B-FP8/snapshots/*"
)


def load_tokenizer(snapshot_glob: str):
    from transformers import AutoTokenizer

    matches = sorted(glob.glob(snapshot_glob))
    if not matches:
        raise SystemExit(f"no model snapshot matched: {snapshot_glob}")
    return AutoTokenizer.from_pretrained(matches[0])


def prompt_token_counts(tokenizer, conversations):
    counts = []
    for entry in conversations:
        encoded = tokenizer.apply_chat_template(
            entry["conversation"], tokenize=True, add_generation_prompt=True,
        )
        # apply_chat_template returns a BatchEncoding.  Counting the mapping
        # itself yields the number of keys, not the number of tokens.
        token_ids = encoded["input_ids"]
        counts.append({
            "batch_id": entry.get("batch_id"),
            "tokens": len(token_ids),
            "chars": entry.get("chars"),
        })
    counts.sort(key=lambda row: -row["tokens"])
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--conversations", type=Path, required=True,
        help="gzipped JSONL with batch_id, chars, and conversation",
    )
    parser.add_argument("--snapshot-glob", default=DEFAULT_SNAPSHOT_GLOB)
    parser.add_argument(
        "--stage", choices=("line_decision", "record_extraction"),
        required=True,
    )
    parser.add_argument("--max-model-len", type=int, default=49_152)
    parser.add_argument("--max-output-tokens", type=int)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    max_output = args.max_output_tokens or (
        2_048 if args.stage == "line_decision" else 16_384
    )
    budget = int(args.max_model_len) - max_output - 256

    tokenizer = load_tokenizer(args.snapshot_glob)
    with gzip.open(args.conversations, "rt", encoding="utf-8") as handle:
        entries = [json.loads(line) for line in handle if line.strip()]
    counts = prompt_token_counts(tokenizer, entries)
    over = [row for row in counts if row["tokens"] > budget]
    report = {
        "version": "candidate_endorsement_round4_prompt_token_preflight_v1",
        "stage": args.stage,
        "max_model_len": int(args.max_model_len),
        "max_output_tokens": max_output,
        "context_safety_margin_tokens": 256,
        "prompt_budget_tokens": budget,
        "prompts_checked": len(counts),
        "max_prompt_tokens": counts[0]["tokens"] if counts else 0,
        "median_prompt_tokens": (
            counts[len(counts) // 2]["tokens"] if counts else 0
        ),
        "over_budget_count": len(over),
        "over_budget_batch_ids": [row["batch_id"] for row in over[:50]],
        "fits": not over,
    }
    text = json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True)
    if args.report:
        args.report.parent.mkdir(parents=True, exist_ok=True)
        args.report.write_text(text + "\n", encoding="utf-8")
    print(text)
    return 0 if report["fits"] else 2


if __name__ == "__main__":
    raise SystemExit(main())
