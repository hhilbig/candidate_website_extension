"""Build the exact human ledger for Stage-B job 21470382 technical residuals."""
from __future__ import annotations

import argparse
import copy
import gzip
import itertools
import json
import re
import sys
from pathlib import Path
from typing import Any, Mapping

REPO = Path(__file__).resolve().parents[1]
if str(REPO) not in sys.path:
    sys.path.insert(0, str(REPO))

from scripts.hive.extract_candidate_endorsement_line_ledger_vllm import (
    validate_expected_input_manifest_sha256,
    validate_input_manifest,
)
from src.candidate_endorsement_line_ledger import (
    RECORD_EXTRACTION_STAGE,
    get_line_ledger_contract,
    sha256_json,
    validate_record_extraction_output,
)

SPEC = {
    "version": "candidate_endorsement_stage_b_exact_source_residual_v1",
    "scope": "job_21470382_exact_13_invalid_batches_23_nonexact_fields",
    "decision_rule": (
        "human_enters_one_exact_source_string_from_the_authenticated_record_"
        "block;_optional_fields_may_use_NULL;_no_fuzzy_matching_or_semantic_"
        "substitution;_complete_run_is_revalidated_after_merge"
    ),
}
SPEC_SHA256 = sha256_json(SPEC)
FROZEN_INPUT_MANIFEST_SHA256 = (
    "aef6d6263d51873854cfa7fb1e4a1d2be6a7c3bf5f1244d0be93d57931dec178"
)
FROZEN_OUTPUT_FILES = {
    "chunk_00000.out.jsonl.gz": "f157d463b034314012274b13b011f73c6f01fa2fb72234260a7191498d3b4343",
    "chunk_00001.out.jsonl.gz": "e65089ed146a60d1b8c1c04570a32c1e4dd066ccd850fdf0c240109e8e5ff96f",
    "chunk_00002.out.jsonl.gz": "d55246ea42a1049e61966b5ad21ec1859fd162c07bc7e157c498f0003666ed7f",
    "run_metadata.json": "a05a4bd0b2d4cc1fa97fc4debbd3232d58d7f58b8abcfea2528902e77f06fd95",
}
ERROR_SUFFIX_TO_FIELD = {
    "name_not_exact_in_block": "endorser_name",
    "title_not_exact_in_block": "title",
    "affiliation_not_exact_in_block": "affiliation",
}


def file_sha256(path: Path) -> str:
    import hashlib
    return hashlib.sha256(path.read_bytes()).hexdigest()


def read_gzip(path: Path) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def load_frozen_failed_run(
    export_dir: Path, output_dir: Path,
) -> tuple[dict[str, Any], list[dict[str, Any]], list[dict[str, Any]]]:
    manifest_path = export_dir / "manifest.json"
    validate_expected_input_manifest_sha256(
        manifest_path, FROZEN_INPUT_MANIFEST_SHA256
    )
    manifest = json.loads(manifest_path.read_text())
    listed = manifest["files"]["chunks"]
    paths = [export_dir / "chunks" / entry["file"] for entry in listed]
    if sorted(path.name for path in (export_dir / "chunks").iterdir()) != sorted(
        entry["file"] for entry in listed
    ):
        raise ValueError("frozen Stage-B input inventory differs")
    for path, entry in zip(paths, listed):
        if file_sha256(path) != entry["sha256"] or path.stat().st_size != entry["bytes"]:
            raise ValueError("frozen Stage-B input chunk differs")
    batches = [row for path in paths for row in read_gzip(path)]
    for name, expected in FROZEN_OUTPUT_FILES.items():
        path = output_dir / name
        if not path.is_file() or file_sha256(path) != expected:
            raise ValueError(f"frozen failed Stage-B artifact differs: {name}")
    if sorted(path.name for path in output_dir.iterdir()) != sorted(FROZEN_OUTPUT_FILES):
        raise ValueError("failed Stage-B exact four-file inventory differs")
    output_paths = sorted(output_dir.glob("chunk_*.out.jsonl.gz"))
    if [path.name for path in output_paths] != sorted(
        name for name in FROZEN_OUTPUT_FILES if name.endswith(".gz")
    ):
        raise ValueError("failed Stage-B output inventory differs")
    rows = [row for path in output_paths for row in read_gzip(path)]
    if [row.get("batch_id") for row in rows] != [
        batch.get("batch_id") for batch in batches
    ]:
        raise ValueError("failed Stage-B output order differs")
    if len(rows) != 152:
        raise ValueError("failed Stage-B row count differs")
    return manifest, batches, rows


def _word_tokens(value: str) -> list[tuple[str, int, int]]:
    return [
        (match.group(0), match.start(), match.end())
        for match in re.finditer(r"[^\W_]+(?:[-’'][^\W_]+)*", value, re.UNICODE)
    ]


def _name_options(line: str, original: str) -> list[str]:
    source = _word_tokens(line)
    wanted = [token.casefold() for token, _, _ in _word_tokens(original)]
    options: list[str] = []
    for start in range(len(source)):
        if not wanted or source[start][0].casefold() != wanted[0]:
            continue
        cursor = start
        matched = [start]
        for word in wanted[1:]:
            cursor += 1
            while cursor < len(source) and source[cursor][0].casefold() != word:
                between = line[source[cursor][1]:source[cursor][2]]
                if not re.fullmatch(r"[A-Za-z]+", between):
                    break
                cursor += 1
            if cursor >= len(source) or source[cursor][0].casefold() != word:
                matched = []
                break
            matched.append(cursor)
        if matched:
            first, last = matched[0], matched[-1]
            if first > 0 and source[first - 1][0].isdigit():
                first -= 1
            options.append(line[source[first][1]:source[last][2]])
    return list(dict.fromkeys(options))


def _optional_field_options(
    line: str, original: str, field: str,
) -> list[str | None]:
    options: list[str] = []
    if field == "title" and original.casefold().startswith("former "):
        original_words = [word for word, _, _ in _word_tokens(original)]
        if len(original_words) >= 3:
            tail = r"\s+".join(re.escape(word) for word in original_words[-2:])
            timing_match = re.search(
                rf"\bFormer\b.*?\b{tail}\b", line, re.IGNORECASE
            )
            if timing_match:
                options.append(line[timing_match.start():timing_match.end()])
    for component in [part.strip() for part in original.split(",") if part.strip()]:
        match = re.search(re.escape(component), line, re.IGNORECASE)
        if match:
            options.append(line[match.start():match.end()])
            continue
        if field == "title":
            wanted = {word.casefold() for word, _, _ in _word_tokens(component)}
            source = _word_tokens(line)
            for end, (word, _, word_end) in enumerate(source):
                if word.casefold() not in wanted:
                    continue
                # A title option must end in an exact word from the proposed title.
                # Extend left only across adjacent title words from that same proposal;
                # this yields e.g. "City Councilor", never the arbitrary token "City".
                start = end
                while (
                    start > 0
                    and source[start - 1][0].casefold() in wanted
                    and line[source[start - 1][2]:source[start][1]].isspace()
                ):
                    start -= 1
                candidate = line[source[start][1]:word_end]
                candidate_words = [w.casefold() for w, _, _ in _word_tokens(candidate)]
                if len(candidate_words) > 1 or word.casefold() in {
                    "councilor", "scientist", "founder", "representative", "senator",
                    "mayor", "chair", "trustee", "attorney", "commissioner",
                }:
                    options.append(candidate)
    unique = list(dict.fromkeys(options))
    maximal = [
        option for option in unique
        if not any(option != other and option in other for other in unique)
    ]
    return [*maximal, None]


def residual_options(residual: Mapping[str, Any]) -> list[str | None]:
    lines = [str(line["line_text"]) for line in residual["record_block_lines"]]
    original = str(residual["original_value"])
    if residual["field"] == "endorser_name":
        options = [option for line in lines for option in _name_options(line, original)]
    else:
        options = [
            option for line in lines for option in _optional_field_options(
                line, original, residual["field"]
            )
        ]
    options = list(dict.fromkeys(options))
    if not options or (residual["field"] == "endorser_name" and None in options):
        raise ValueError(f"residual has no exact semantic options: {residual['residual_id']}")
    return options


def derive_residuals(
    batches: list[dict[str, Any]], rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    residuals: list[dict[str, Any]] = []
    for batch, row in zip(batches, rows):
        errors = row.get("stage_validation", {}).get("errors", [])
        targets = {target["slot"]: target for target in batch["targets"]}
        for error in errors:
            parts = str(error).split(":")
            if len(parts) != 3 or parts[2] not in ERROR_SUFFIX_TO_FIELD:
                continue
            slot, record_label, suffix = parts
            record_index = int(record_label.removeprefix("record_"))
            field = ERROR_SUFFIX_TO_FIELD[suffix]
            target = targets[slot]
            record = row["model_output"][slot]["records"][record_index]
            block_lines = [{
                "source_line_id": line["source_line_id"],
                "parent_line_index": line["parent_line_index"],
                "parent_start_char": line["parent_start_char"],
                "parent_end_char": line["parent_end_char"],
                "line_text": line["line_text"],
            } for line in target["record_block_lines"]]
            identity = {
                "spec_sha256": SPEC_SHA256,
                "batch_id": batch["batch_id"], "slot": slot,
                "record_index": record_index, "field": field,
                "source_line_id": target["source_line_id"],
                "original_value": record[field],
            }
            residual = {
                "residual_id": sha256_json(identity), **identity,
                "parent_document_id": batch["parent_document_id"],
                "candidate_cycle_id": batch["candidate_cycle_id"],
                "model_record": record,
                "record_block_lines": block_lines,
                "optional_null_allowed": field != "endorser_name",
            }
            residual["allowed_values"] = residual_options(residual)
            residuals.append(residual)
    if len(residuals) != 23 or len({r["residual_id"] for r in residuals}) != 23:
        raise ValueError("expected exact 23-field residual ledger")
    if len({r["batch_id"] for r in residuals}) != 13:
        raise ValueError("expected exact 13 residual batches")
    return residuals


def prune_to_technically_feasible_options(
    residuals: list[dict[str, Any]], batches: list[dict[str, Any]],
    rows: list[dict[str, Any]],
) -> None:
    batch_by_id = {batch["batch_id"]: batch for batch in batches}
    row_by_id = {row["batch_id"]: row for row in rows}
    grouped: dict[str, list[dict[str, Any]]] = {}
    for residual in residuals:
        grouped.setdefault(residual["batch_id"], []).append(residual)
    for batch_id, items in grouped.items():
        feasible: list[tuple[Any, ...]] = []
        for values in itertools.product(*(item["allowed_values"] for item in items)):
            output = copy.deepcopy(row_by_id[batch_id]["model_output"])
            for item, value in zip(items, values, strict=True):
                output[item["slot"]]["records"][item["record_index"]][item["field"]] = value
            validation = validate_record_extraction_output(batch_by_id[batch_id], output)
            if validation["schema_valid"] and validation["coverage_complete"]:
                feasible.append(values)
        if not feasible:
            raise ValueError(f"residual batch has no technically feasible option tuple: {batch_id}")
        for index, item in enumerate(items):
            item["allowed_values"] = [
                value for value in item["allowed_values"]
                if any(values[index] == value for values in feasible)
            ]


def semantic_compatibility_groups(
    residuals: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    grouped: dict[tuple[str, str, int], list[dict[str, Any]]] = {}
    for residual in residuals:
        grouped.setdefault(
            (residual["batch_id"], residual["slot"], residual["record_index"]), []
        ).append(residual)
    result = []
    for items in grouped.values():
        fields = {item["field"] for item in items}
        if fields != {"title", "affiliation"}:
            continue
        title = next(item for item in items if item["field"] == "title")
        affiliation = next(item for item in items if item["field"] == "affiliation")
        line = str(title["record_block_lines"][0]["line_text"])
        match = re.search(
            r"-\s*(.+?)\s+at\s+(.+?),\s*(.+?)\s+of\s+(.+?)\s*$", line
        )
        if match is None:
            raise ValueError("paired title/affiliation residual lacks structural clauses")
        pairs = [
            {title["residual_id"]: match.group(1), affiliation["residual_id"]: match.group(2)},
            {title["residual_id"]: match.group(3), affiliation["residual_id"]: match.group(4)},
            {title["residual_id"]: None, affiliation["residual_id"]: None},
        ]
        if any(
            pair[title["residual_id"]] not in title["allowed_values"]
            or pair[affiliation["residual_id"]] not in affiliation["allowed_values"]
            for pair in pairs
        ):
            raise ValueError("paired title/affiliation options differ from source clauses")
        result.append({
            "item_ids": [title["residual_id"], affiliation["residual_id"]],
            "allowed_combinations": pairs,
        })
    return result


def build_task(
    export_dir: Path, output_dir: Path,
) -> dict[str, Any]:
    manifest, batches, rows = load_frozen_failed_run(export_dir, output_dir)
    residuals = derive_residuals(batches, rows)
    prune_to_technically_feasible_options(residuals, batches, rows)
    compatibility_groups = semantic_compatibility_groups(residuals)
    provenance = {
        "spec": SPEC, "spec_sha256": SPEC_SHA256,
        "input_manifest_sha256": FROZEN_INPUT_MANIFEST_SHA256,
        "output_files": FROZEN_OUTPUT_FILES,
        "ordered_residual_ids_sha256": sha256_json(
            [row["residual_id"] for row in residuals]
        ),
        "residual_count": 23, "invalid_batch_count": 13,
        "line_ledger_contract_sha256": manifest["line_ledger_contract_sha256"],
        "repair_validation_contract_sha256": get_line_ledger_contract()[
            "line_ledger_contract_sha256"
        ],
    }
    return {
        "task_id": "candidate_endorsement_stage_b_residual_21470382",
        "title": "Stage-B exact-source technical residuals (job 21470382)",
        "instructions": (
            "### Rule\nEnter the exact source string that should replace the displayed "
            "field. Copy characters exactly, including punctuation and nonbreaking spaces. "
            "Do not paraphrase or infer. For title or affiliation only, enter `NULL` when no "
            "exact source value should be released. Names may not be NULL. The importer rejects "
            "any answer that does not make the complete frozen run pass exact validation."
        ),
        "provenance": provenance,
        "items": [{
            "id": residual["residual_id"],
            "label": f"{index + 1}/23 · {residual['field']}",
            "display": [
                {"label": "Identity", "format": "badges", "content": {
                    "Candidate": residual["candidate_cycle_id"],
                    "Field": residual["field"], "Batch": residual["batch_id"][:12],
                }},
                {"label": "Original model record", "format": "json", "content": residual["model_record"]},
                {"label": "Original invalid value", "format": "text", "content": str(residual["original_value"])},
                {"label": "Authenticated exact source block and offsets", "format": "json", "content": residual["record_block_lines"]},
            ],
            "residual": residual,
            "allowed_values": residual["allowed_values"],
        } for index, residual in enumerate(residuals)],
        "compatibility_groups": compatibility_groups,
        "response_schema": "one_item_specific_allowed_value_plus_optional_string_notes",
    }


def render_html(task: Mapping[str, Any]) -> str:
    payload = json.dumps(task, ensure_ascii=False).replace("</", "<\\/")
    return f"""<!doctype html><meta charset=utf-8><title>{task['title']}</title>
<style>body{{font:16px system-ui;max-width:1100px;margin:24px auto;padding:0 16px}}nav button{{margin:3px}}pre{{white-space:pre-wrap;background:#f5f5f5;padding:12px}}select,textarea{{width:100%;font:inherit;padding:8px;margin:8px 0}}.bad{{color:#a00}}.ok{{color:#075}}</style>
<h1>{task['title']}</h1><p id=progress></p><nav id=nav></nav><main id=main></main>
<button id=exportButton>Export completed results</button><pre id=out></pre>
<script>const task={payload}; const key=[task.task_id,task.provenance.spec_sha256,task.provenance.repair_validation_contract_sha256,task.provenance.ordered_residual_ids_sha256].join(':')+':responses'; let saved=JSON.parse(localStorage.getItem(key)||'{{}}'); let current=0;
function optionLabel(v){{return v===null?'NULL':v.replace(/\u00a0/g,'⍽NBSP⍽')}}
function render(){{const item=task.items[current],r=saved[item.id]||{{}};nav.innerHTML=task.items.map((x,i)=>`<button onclick="current=${{i}};render()">${{i+1}}</button>`).join('');progress.textContent=`${{Object.keys(saved).length}} / ${{task.items.length}} completed`;main.innerHTML=`<h2>${{item.label}}</h2><pre>${{JSON.stringify(item.display,null,2)}}</pre><label>Exact allowed replacement<select id=value><option value="">Choose…</option>${{item.allowed_values.map((v,i)=>`<option value="${{i}}" ${{r.value_index===i?'selected':''}}>${{optionLabel(v)}}</option>`).join('')}}</select></label><label>Notes (optional)<textarea id=notes>${{r.notes||''}}</textarea></label>`;value.onchange=save;notes.onchange=save}}
function save(){{if(value.value===''){{delete saved[task.items[current].id]}}else{{saved[task.items[current].id]={{value_index:Number(value.value),notes:notes.value}}}}localStorage.setItem(key,JSON.stringify(saved));render()}}
function chosen(itemId){{const item=task.items.find(x=>x.id===itemId),r=saved[itemId];return r?item.allowed_values[r.value_index]:undefined}}
function compatibilityValid(){{return task.compatibility_groups.every(group=>group.allowed_combinations.some(combo=>group.item_ids.every(id=>chosen(id)===combo[id])))}}
document.getElementById('exportButton').onclick=()=>{{if(Object.keys(saved).length!==task.items.length){{out.textContent='Complete all 23 rows first.';return}}if(!compatibilityValid()){{out.textContent='Selected paired fields do not describe the same authenticated source clause.';return}}const responses=task.items.map(item=>({{item_id:item.id,status:'complete',answers:{{exact_source_value:item.allowed_values[saved[item.id].value_index],notes:saved[item.id].notes||''}}}}));const result={{task_id:task.task_id,title:task.title,format:'handcode-results-v1',exported_at:new Date().toISOString(),n_items:task.items.length,n_complete:task.items.length,responses}};out.textContent=JSON.stringify(result,null,2);const a=document.createElement('a');a.href=URL.createObjectURL(new Blob([out.textContent+'\\n'],{{type:'application/json'}}));a.download=task.task_id+'_results.json';a.click()}};render();</script>"""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--stage-b-export-dir", type=Path, required=True)
    parser.add_argument("--stage-b-output-dir", type=Path, required=True)
    parser.add_argument("--task-json", type=Path, required=True)
    parser.add_argument("--html", type=Path, required=True)
    args = parser.parse_args()
    task = build_task(args.stage_b_export_dir, args.stage_b_output_dir)
    args.task_json.parent.mkdir(parents=True, exist_ok=True)
    args.task_json.write_text(json.dumps(task, indent=2, sort_keys=True) + "\n")
    args.html.write_text(render_html(task), encoding="utf-8")
    print(json.dumps({"items": len(task["items"]), "task_json": str(args.task_json)}, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
