"""Hive vLLM check: does a usable archived page belong to the finalist's campaign for THIS election?

The site check judges whole sites. A site reused across campaigns can still hold pages from an earlier race or
another office (Opus audit, October 16, 2026). This check reads each usable page alone and labels it:
  this_campaign     content of this candidate's campaign for this mayoral election, or campaign site pages that fit it
  unclear           a generic page that cannot be tied to one election (contact form, undated biography)
  earlier_campaign  content only for an earlier (or later) race of this candidate, nothing for this election
  other_office      a campaign or office page for another office (council, county, Congress, governor)
  not_campaign      not about this candidate's campaign (spam, another person, business, hosting placeholder)
The release keeps this_campaign and unclear (approved October 16, 2026).

Input:  JSONL, one page per line (item_id, candidate_name, city_name, state_abb, election_date, capture_date, url, text).
Output: JSONL with item_id, label, quote, reason and run identity; written in chunks so a requeued job resumes.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import re
import time
from pathlib import Path

PROMPT_VERSION = "mayoral-page-campaign-v1"
MAX_PAGE_CHARS = 5000
CHUNK = 1000
LABELS = ["this_campaign", "unclear", "earlier_campaign", "other_office", "not_campaign"]
SYSTEM = """You check archived web pages for a research dataset of US mayoral campaign websites.
You receive one mayoral finalist (name, city, state, election date) and the text of ONE page from a website that a review already confirmed as this candidate's campaign site for this election. Sites are often reused across campaigns, so a single page can still belong to another race. Decide what this page is.

Labels:
- "this_campaign": the page is part of this candidate's campaign for THIS mayoral election. Includes re-election pages that describe the candidate's record as mayor, issue and biography pages written for this race, donate, volunteer, event and endorsement pages for this race, and news posts from the campaign period.
- "unclear": a generic page that fits the campaign but cannot be tied to one election (a contact form, an undated biography, a short donate page without dates). Use it when the page gives no sign of another race.
- "earlier_campaign": the page is clearly about an earlier or later race of this candidate (other election year or date, a victory letter or endorsement for a previous election), with nothing for this election.
- "other_office": the page is a campaign or office page for a different office (city council, county, state legislature, Congress, governor, school board), with nothing for this mayoral race.
- "not_campaign": the page is not about this candidate's campaign at all (spam, another person, a business, a hosting or default page).

Compare dates on the page with the election date. A page saved after the election can still be "this_campaign" if its content is for this race. Quote the shortest passage that supports your answer, copied exactly from the page text, or "" if there is none. Give a one-sentence reason."""

SCHEMA = {
    "type": "object",
    "properties": {
        "label": {"type": "string", "enum": LABELS},
        "quote": {"type": "string", "maxLength": 300},
        "reason": {"type": "string", "maxLength": 300},
    },
    "required": ["label", "quote", "reason"],
    "additionalProperties": False,
}


def clean_city(name: str) -> str:
    return re.sub(r"\s+(city|CDP|urban county|city \(balance\)|metro government \(balance\))$", "", name).split("/")[0]


def user_message(item: dict) -> str:
    return (f"Finalist: {item['candidate_name']}\nCity: {clean_city(item['city_name'])}, {item['state_abb']}\n"
            f"Mayoral election date: {item['election_date']}\nPage address: {item['url']}\n"
            f"Page saved: {item['capture_date']}\n\n--- Page text ---\n" + item["text"][:MAX_PAGE_CHARS])


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
    parser.add_argument("--seed", type=int, default=20261016)
    parser.add_argument("--max-model-len", type=int, default=8192)
    parser.add_argument("--max-output-tokens", type=int, default=384)
    parser.add_argument("--done-from", type=Path, nargs="*", default=[],
                        help="other label files whose items count as done (a second job on another partition)")
    args = parser.parse_args()
    from vllm import LLM, SamplingParams

    done = set()
    for path in [args.output, *args.done_from]:
        if path.exists():
            done |= {json.loads(line)["item_id"] for line in path.open() if line.strip()}
    items = [json.loads(line) for line in args.input.open() if line.strip()]
    items = [i for i in items if i["item_id"] not in done]
    print(json.dumps({"already_done": len(done), "to_do": len(items)}), flush=True)
    if not items:
        return
    llm = LLM(model=args.model_path, tokenizer=args.model_path, max_model_len=args.max_model_len,
              gpu_memory_utilization=0.92, seed=args.seed, trust_remote_code=False, language_model_only=True,
              structured_outputs_config={"backend": "xgrammar", "disable_any_whitespace": True})
    tokenizer = llm.get_tokenizer()
    params = SamplingParams(temperature=0.0, max_tokens=args.max_output_tokens, seed=args.seed, **structured(SCHEMA))
    script_sha = hashlib.sha256(Path(__file__).read_bytes()).hexdigest()
    budget = args.max_model_len - args.max_output_tokens - 64
    started = time.time()
    for start in range(0, len(items), CHUNK):
        chunk = items[start:start + CHUNK]
        conversations, kept, rows = [], [], []
        for item in chunk:
            conversation = [{"role": "system", "content": SYSTEM}, {"role": "user", "content": user_message(item)}]
            ids = tokenizer.apply_chat_template(conversation, tokenize=True, add_generation_prompt=True,
                                                enable_thinking=False)
            ids = ids["input_ids"] if hasattr(ids, "keys") else ids
            if len(ids) > budget:
                rows.append({"item_id": item["item_id"], "status": "over_context_budget"})
                continue
            conversations.append(conversation)
            kept.append(item)
        outputs = llm.chat(conversations, params, use_tqdm=False, chat_template_kwargs={"enable_thinking": False})
        for item, output in zip(kept, outputs):
            completion = output.outputs[0]
            try:
                answer, status = json.loads(completion.text), "ok"
            except json.JSONDecodeError:
                answer, status = {}, "parse_error"
            rows.append({"item_id": item["item_id"], "status": status, "finish_reason": completion.finish_reason, **answer})
        with args.output.open("a") as handle:
            for row in rows:
                handle.write(json.dumps({**row, "prompt_version": PROMPT_VERSION, "script_sha256": script_sha,
                                         "model_path": args.model_path, "seed": args.seed}) + "\n")
        print(json.dumps({"done": start + len(chunk), "of": len(items), "seconds": round(time.time() - started, 1)}),
              flush=True)


if __name__ == "__main__":
    main()
