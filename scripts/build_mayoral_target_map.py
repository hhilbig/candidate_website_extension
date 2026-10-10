"""Map every scrape target credited with text to its finalist, for the page manifest.

The coverage build records, for each finalist with downloaded text, the evidence that credits it:
  stage3_pages:<target_id>:<n> and stage3_legacy_pages:<target_id>:<n>   stage 3 batches on mac2 (matched per
                                                                         batch by the finalist's current domains)
  saved_inventory:<path>/municipal_archive_acquisition/<batch>/...       August 26 batches
  saved_inventory:<path>/production_all_inventory/...                    the older droplet corpus
For saved-inventory evidence the target is the batch target with the finalist's candidate_year_id and one
of the finalist's own URL domains, so news sites and other candidates in those batches stay out.
The older corpus maps to the copy on mac2, legacy_all_inventory_20260906.

Run on mac2 from ~/candidate_website_extension_stage3:
  python3 scripts/build_mayoral_target_map.py --candidates candidate_coverage.csv --output target_to_finalist.json
"""
from __future__ import annotations

import argparse
import csv
import json
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

ACQ = Path("data/municipal_archive_acquisition")
OLD_CORPUS_BATCH = "legacy_all_inventory_20260906"


def hosts(person: dict) -> set[str]:
    urls = json.loads(person["exact_urls"] or "[]") + ([person["discovered_campaign_url"]] if person["discovered_campaign_url"] else [])
    return {(urlsplit(u).hostname or "").lower().removeprefix("www.") for u in urls}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--candidates", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    people = [r for r in csv.DictReader(args.candidates.open()) if r["eligible"] == "True" and r["downloaded_content"] == "True"]
    mapping: dict[str, dict[str, str]] = defaultdict(dict)
    targets_cache: dict[str, list[dict]] = {}
    stage3_targets: dict[str, list[dict]] = {}
    unmatched = []
    for person in people:
        found = False
        for ref in json.loads(person["evidence_refs"]):
            if ref.startswith(("stage3_pages:", "stage3_legacy_pages:")):
                # Stage 3 target IDs derive from the finalist alone and can repeat across batches, so each batch's
                # target is kept only if its domain matches the URL that evidence refers to: the current discovered
                # campaign URL for stage3_pages, a roster URL for stage3_legacy_pages. A superseded scrape stays out.
                target_id = ref.split(":")[1]
                if ref.startswith("stage3_pages:"):
                    url = person["discovered_campaign_url"]
                    own = {(urlsplit(url).hostname or "").lower().removeprefix("www.")} if url else set()
                else:
                    own = {(urlsplit(u).hostname or "").lower().removeprefix("www.") for u in json.loads(person["exact_urls"] or "[]")}
                for batch in sorted(ACQ.glob("mayoral_stage3*")):
                    for t in stage3_targets.setdefault(batch.name, list(csv.DictReader((batch / "candidate_site_targets.csv").open()))):
                        domain = t["registrable_domain"].lower()
                        if t["target_id"] == target_id and (domain in own or any(h.endswith("." + domain) or h == domain for h in own)):
                            mapping[batch.name][target_id] = person["candidate_election_id"]
                            found = True
            elif ref.startswith("saved_inventory:"):
                if "/municipal_archive_acquisition/" in ref:
                    batch = ref.split("/municipal_archive_acquisition/")[1].split("/")[0]
                elif "/production_all_inventory/" in ref:
                    batch = OLD_CORPUS_BATCH
                else:
                    continue
                if batch not in targets_cache:
                    path = ACQ / batch / "frozen" / "targets.csv"
                    if not path.exists():
                        path = ACQ / batch / "candidate_site_targets.csv"
                    targets_cache[batch] = list(csv.DictReader(path.open()))
                own = hosts(person)
                for t in targets_cache[batch]:
                    domain = (t.get("registrable_domain") or "").lower()
                    same_year = t.get("candidate_year_id", person["candidate_year_id"]) == person["candidate_year_id"]
                    if same_year and (not domain or domain in own or any(h.endswith("." + domain) for h in own)):
                        mapping[batch][t["target_id"]] = person["candidate_election_id"]
                        found = True
        if not found:
            unmatched.append(person["candidate_election_id"])
    args.output.write_text(json.dumps(mapping, indent=1, sort_keys=True))
    finalists = {f for targets in mapping.values() for f in targets.values()}
    print(json.dumps({"finalists_with_text": len(people), "mapped_finalists": len(finalists),
                      "unmatched": unmatched, "targets_by_source": {k: len(v) for k, v in mapping.items()}}, indent=1))


if __name__ == "__main__":
    main()
