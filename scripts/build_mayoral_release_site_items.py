"""Build Hive items for the release site check (mayoral release stage 4).

One item per credited site (finalist x target) in the page corpus: the root-most usable page within
365 days of the election and the longest other usable page, as text. Usable means not excluded and not
a duplicate. The items use the input format of scripts/hive/classify_mayoral_site_attribution.py
(prompt mayoral-site-attribution-v1), so the release check uses the same instrument as the October 4
attribution checks. Known negatives from that calibration (legacy_attribution/items.jsonl, gold "no")
are added with their gold label as a control.

Run on mac2 from ~/candidate_website_extension_stage3:
  .venv/bin/python scripts/build_mayoral_release_site_items.py --corpus release_mayoral/mayoral_page_corpus.parquet \
      --candidates candidate_coverage.csv --calibration legacy_attribution_items.jsonl --output release_mayoral/site_items.jsonl
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit


def depth(url: str) -> int:
    return len([part for part in urlsplit(url).path.split("/") if part])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--calibration", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    import pyarrow.parquet as pq

    people = {r["candidate_election_id"]: r for r in csv.DictReader(args.candidates.open())}
    rows = pq.read_table(args.corpus, columns=["candidate_election_id", "target_id", "original_url", "capture_timestamp",
                                               "in_text_window", "excluded", "duplicate_of", "text", "n_char"]).to_pylist()
    sites = defaultdict(list)
    for r in rows:
        if r["in_text_window"] and not r["excluded"] and not r["duplicate_of"]:
            sites[(r["candidate_election_id"], r["target_id"])].append(r)
    written = 0
    with args.output.open("w") as handle:
        for (finalist, target), pages in sorted(sites.items()):
            person = people[finalist]
            root = min(pages, key=lambda p: (depth(p["original_url"]), len(p["original_url"]), p["capture_timestamp"]))
            others = [p for p in pages if p is not root]
            chosen = [root] + ([max(others, key=lambda p: p["n_char"])] if others else [])
            handle.write(json.dumps({
                "item_id": f"release|{finalist}|{target}", "candidate_name": person["candidate_name"],
                "city_name": person["city_name"], "state_abb": person["state_abb"], "election_date": person["election_date"],
                "url": f"{urlsplit(root['original_url']).scheme}://{urlsplit(root['original_url']).hostname}/",
                "captures": [{"timestamp": p["capture_timestamp"], "url": p["original_url"],
                              "text": p["text"].replace("#+#", "\n")[:6000]} for p in chosen],
                "gold": ""}) + "\n")
            written += 1
        controls = 0
        for line in args.calibration.open():
            item = json.loads(line)
            if item.get("gold") == "no":
                item["item_id"] = "control|" + item["item_id"]
                handle.write(json.dumps(item) + "\n")
                controls += 1
    print(json.dumps({"site_items": written, "finalists": len({f for f, _ in sites}), "negative_controls": controls}))


if __name__ == "__main__":
    main()
