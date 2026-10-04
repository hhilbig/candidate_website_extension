"""Hive vLLM check: is a saved archived page the finalist's own mayoral campaign site?

One item is one finalist x one URL, with up to two saved archived pages from
within 365 days of the election. The model answers yes / no / unclear under the
same rule the stage 2 agents used. Items carrying a "gold" label are calibration
items (known positives and negatives); they are scored, not integrated.

Input:  JSONL, one item per line (item_id, candidate_name, city_name, state_abb,
        election_date, url, captures[{timestamp, text}], optional gold).
Output: JSONL with the item_id, decision, quote, reason, and run identity.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import time
from pathlib import Path

PROMPT_VERSION = "mayoral-site-attribution-v1"
MAX_PAGE_CHARS = 5000
SYSTEM = """You check archived web pages for a research dataset of US mayoral campaign websites.
You receive one mayoral finalist (name, city, state, election date), a website URL, and the text of up to two archived copies of that site saved within a year of the election.

Answer "yes" only if the pages show that this site is THIS candidate's own campaign website for a mayoral campaign in THIS city, active for THIS election. Signs: the candidate's name together with "for Mayor", "elect", "vote", "re-elect", donate or volunteer calls for the mayoral race, or a campaign committee footer ("Paid for by ... for Mayor").
Answer "no" if the site belongs to a different person (same surname, other city or state), is for a different office (city council, state legislature, Congress, judge) with no mayoral campaign, is an official government page, a news or fan page, a parked, for-sale, spam or empty page, or if its campaign content is clearly only for an earlier or later election and does not apply to this one.
Answer "unclear" if the text is too thin or ambiguous to decide (for example only a title, or a page that names the candidate without any campaign content).
Quote the shortest passage that supports your answer, copied exactly from the page text, or "" if there is none. Give a one-sentence reason."""

SCHEMA = {
    "type": "object",
    "properties": {
        "decision": {"type": "string", "enum": ["yes", "no", "unclear"]},
        "quote": {"type": "string", "maxLength": 300},
        "reason": {"type": "string", "maxLength": 300},
    },
    "required": ["decision", "quote", "reason"],
    "additionalProperties": False,
}


def clean_city(name: str) -> str:
    return re.sub(r"\s+(city|CDP|urban county|city \(balance\)|metro government \(balance\))$", "", name).split("/")[0]


def user_message(item: dict) -> str:
    pages = []
    for i, capture in enumerate(item["captures"][:2], 1):
        stamp = capture["timestamp"]
        pages.append(f"--- Archived page {i}, saved {stamp[:4]}-{stamp[4:6]}-{stamp[6:8]} ---\n"
                     + capture["text"][:MAX_PAGE_CHARS])
    return (f"Finalist: {item['candidate_name']}\nCity: {clean_city(item['city_name'])}, {item['state_abb']}\n"
            f"Mayoral election date: {item['election_date']}\nWebsite: {item['url']}\n\n" + "\n\n".join(pages))


def structured(schema: dict) -> dict:
    try:
        from vllm.sampling_params import StructuredOutputsParams
        return {"structured_outputs": StructuredOutputsParams(json=schema)}
    except ImportError:
        from vllm.sampling_params import GuidedDecodingParams
        return {"guided_decoding": GuidedDecodingParams(json=schema)}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--model-path", required=True)
    parser.add_argument("--seed", type=int, default=20261003)
    parser.add_argument("--max-model-len", type=int, default=16384)
    parser.add_argument("--max-output-tokens", type=int, default=512)
    args = parser.parse_args()
    from vllm import LLM, SamplingParams

    items = [json.loads(line) for line in args.input.open() if line.strip()]
    llm = LLM(model=args.model_path, tokenizer=args.model_path, max_model_len=args.max_model_len,
              gpu_memory_utilization=0.92, seed=args.seed, trust_remote_code=False, language_model_only=True,
              structured_outputs_config={"backend": "xgrammar", "disable_any_whitespace": True})
    tokenizer = llm.get_tokenizer()
    conversations, kept, skipped = [], [], []
    budget = args.max_model_len - args.max_output_tokens - 64
    for item in items:
        conversation = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user_message(item)}]
        ids = tokenizer.apply_chat_template(conversation, tokenize=True, add_generation_prompt=True,
                                            enable_thinking=False)
        ids = ids["input_ids"] if hasattr(ids, "keys") else ids
        if len(ids) > budget:
            skipped.append(item["item_id"])
            continue
        conversations.append(conversation)
        kept.append(item)
    params = SamplingParams(temperature=0.0, max_tokens=args.max_output_tokens, seed=args.seed, **structured(SCHEMA))
    started = time.time()
    outputs = llm.chat(conversations, params, use_tqdm=True, chat_template_kwargs={"enable_thinking": False})
    script_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    temp = args.output.with_suffix(".tmp")
    with temp.open("w") as handle:
        for item, output in zip(kept, outputs):
            completion = output.outputs[0]
            try:
                answer = json.loads(completion.text)
                status = "ok"
            except json.JSONDecodeError:
                answer, status = {}, "parse_error"
            handle.write(json.dumps({"item_id": item["item_id"], "status": status, "finish_reason": completion.finish_reason,
                                     **answer, "gold": item.get("gold", ""), "prompt_version": PROMPT_VERSION,
                                     "script_sha256": script_sha, "model_path": args.model_path, "seed": args.seed}) + "\n")
        for item_id in skipped:
            handle.write(json.dumps({"item_id": item_id, "status": "over_context_budget", "prompt_version": PROMPT_VERSION,
                                     "script_sha256": script_sha}) + "\n")
    os.replace(temp, args.output)
    print(json.dumps({"items": len(items), "classified": len(kept), "over_budget": len(skipped),
                      "seconds": round(time.time() - started, 1)}), flush=True)


if __name__ == "__main__":
    main()
