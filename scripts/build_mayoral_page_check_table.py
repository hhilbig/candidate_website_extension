"""Merge the Hive page-check label files into the table the corpus build reads (page_id, label).

Label files are given oldest first; a later file replaces the label of a page that an earlier file labeled (a relabel
with a newer prompt). Records with a parse error carry no label, so the page keeps no page-check label.
Hand decisions (--overrides, a JSON list of page_id, label, reason) replace the model label last. On October 17, 2026
an Opus hand check decided the 71 earlier_campaign pages whose race_year is the election year or the year before,
because the model often calls an earlier round of the same race a different election.
Same text (--corpus): a corpus page without a label inherits the label of a labeled page of the same finalist with the
same text_sha256, when those pages agree on one label. A rebuild that drops a kept copy otherwise makes its unlabeled
copy usable, round after round.

Usage:
  python scripts/build_mayoral_page_check_table.py --items <items.jsonl ...> --labels <labels.jsonl ...> \\
      [--overrides <decisions.json>] [--corpus <mayoral_page_corpus.parquet>] --output <csv>
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import Counter
from pathlib import Path


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--items", type=Path, nargs="+", required=True, help="Hive item files (checks that each label has an item)")
    parser.add_argument("--labels", type=Path, nargs="+", required=True, help="label files, oldest first")
    parser.add_argument("--overrides", type=Path, help="hand decisions, JSON list of page_id, label, reason")
    parser.add_argument("--corpus", type=Path, help="page corpus, to pass labels to copies with the same text")
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    item_ids = {json.loads(line)["item_id"] for path in args.items for line in path.open()}
    final = {}
    for path in args.labels:
        for line in path.open():
            record = json.loads(line)
            if record.get("status") == "ok":
                final[record["item_id"]] = record
    assert set(final) <= item_ids, "a label names a page without an item"
    hand = {d["page_id"]: d["label"] for d in json.loads(args.overrides.read_text())} if args.overrides else {}
    assert set(hand) <= set(final), "a hand decision names a page without a model label"
    labels = {page_id: (hand.get(page_id, record["label"]),
                        "opus-hand-check-2026-10-17" if page_id in hand else record["prompt_version"], record.get("race_year"))
              for page_id, record in final.items()}
    inherited = 0
    if args.corpus:
        import pandas as pd
        corpus = pd.read_parquet(args.corpus, columns=["page_id", "candidate_election_id", "text_sha256"])
        key = dict(zip(corpus["page_id"], zip(corpus["candidate_election_id"], corpus["text_sha256"])))
        by_text = {}
        for page_id, (label, _, _) in labels.items():
            by_text.setdefault(key[page_id], set()).add(label)
        for page_id, k in key.items():
            if page_id not in labels and len(by_text.get(k, ())) == 1:
                labels[page_id] = (next(iter(by_text[k])), "same-text-as-labeled-page", None)
                inherited += 1
    counts = Counter()
    with args.output.open("w", newline="") as handle:
        writer = csv.writer(handle)
        writer.writerow(["page_id", "label", "prompt_version", "race_year"])
        for page_id, (label, version, year) in sorted(labels.items()):
            counts[label] += 1
            writer.writerow([page_id, label, version, "" if year is None else year])
    print(json.dumps({"pages": len(labels), "labels": dict(counts), "hand_decisions": len(hand),
                      "inherited_same_text": inherited}, indent=1))


if __name__ == "__main__":
    main()
