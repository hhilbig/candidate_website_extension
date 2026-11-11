"""Tools for a model coder of one validation task file.

  python coder_tools.py show TASK.json            instructions, questions, item count
  python coder_tools.py item TASK.json N          item N (0-based) as text
  python coder_tools.py answer TASK.json N JSON   store the answers of item N
  python coder_tools.py finish TASK.json          write TASK_results.json if all items have answers

`item` omits character offsets and alignment bookkeeping of the model records.
All other content is the content of the task file. Answers go to
TASK.answers.jsonl first, one line for each item, so that no answer is lost.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

DROP = ("_char", "_alignment_method", "_exact_match_count", "source_model_item_id", "parent_page_offsets",
        "overlap_resolution")


def slim(value):
    if isinstance(value, dict):
        return {k: slim(v) for k, v in value.items() if not any(d in k for d in DROP)}
    if isinstance(value, list):
        return [slim(v) for v in value]
    return value


def main():
    command, path = sys.argv[1], Path(sys.argv[2])
    task = json.loads(path.read_text())
    store = path.with_suffix(".answers.jsonl")
    if command == "show":
        print(task["title"], "\n\n", task["instructions"], "\n\nQUESTIONS")
        print(json.dumps(task["questions"], indent=1))
        print("\nITEMS:", len(task["items"]))
    elif command == "item":
        item = task["items"][int(sys.argv[3])]
        print("ITEM", sys.argv[3], "id", item["id"], "|", item["label"])
        for block in item["display"]:
            print(f"\n=== {block['label']} ===")
            content = block["content"]
            if block["format"] == "text":
                print(content)
            else:
                content = slim(content)
                if isinstance(content, list):
                    for i, row in enumerate(content):
                        print(i, json.dumps(row.get("record", row), ensure_ascii=False))
                else:
                    print(json.dumps(content, ensure_ascii=False, indent=1))
    elif command == "answer":
        index, answers = int(sys.argv[3]), json.loads(sys.argv[4])
        ids = {q["id"] for q in task["questions"]} | {"notes"}
        assert set(answers) <= ids, f"unknown question ids: {set(answers) - ids}"
        for q in task["questions"]:
            if q["type"] == "yesno":
                assert answers.get(q["id"]) in ("yes", "no"), f"{q['id']} must be yes or no"
        with store.open("a") as handle:
            handle.write(json.dumps({"index": index, "item_id": task["items"][index]["id"], "answers": answers}) + "\n")
        print("stored", index)
    elif command == "finish":
        rows = {}
        for line in store.read_text().splitlines():
            row = json.loads(line)
            rows[row["index"]] = row
        missing = [i for i in range(len(task["items"])) if i not in rows]
        assert not missing, f"items without answers: {missing}"
        out = path.with_name(path.stem + "_results.json")
        out.write_text(json.dumps({
            "task_id": task["task_id"], "format": "handcode-results-v1",
            "exported_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "n_items": len(rows), "n_complete": len(rows),
            "responses": [{"item_id": rows[i]["item_id"], "status": "complete", "answers": rows[i]["answers"]}
                          for i in sorted(rows)]}, indent=1))
        print("wrote", out)


if __name__ == "__main__":
    main()
