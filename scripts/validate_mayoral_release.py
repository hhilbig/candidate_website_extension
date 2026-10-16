"""Validate a mayoral release folder built by scripts/build_mayoral_release.py (stage 6).

Checks, each recomputed from the release files themselves:
  keys       unique keys in every table; 100 cities, one row per city-year, finalists unique
  links      city-years, elections and finalists point to existing cities and elections
  status     every eligible finalist has exactly one text status; ineligible ones have none;
             a no_url_lead finalist has no URL lead; text_status "text" if and only if a panel row exists
  pages      page_id unique; release_usable equals the documented rule; no excluded, duplicate,
             out-of-window or failed-site page is usable
  panel      each panel row has the number of usable pages and the exact joined text of those pages, and the
             joined text of its pre-election pages
  manifest   manifest.json lists every data file with its row count and SHA-256, and they match
It prints a JSON report and exits 1 if any check fails. Drops above 20 percent are reported, not failed.
"""
from __future__ import annotations

import argparse
import csv
import hashlib
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path
from urllib.parse import urlsplit

csv.field_size_limit(sys.maxsize)
STATUSES = {"text", "excluded_site_check", "no_usable_pages", "site_rejected", "site_not_archived", "check_incomplete",
            "no_url_lead"}
DATA_FILES = ["mayoral_cities.csv", "mayoral_city_years.csv", "mayoral_elections.csv", "mayoral_finalists.csv",
              "mayoral_finalist_panel.csv", "mayoral_page_corpus.parquet"]


def read(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1 << 20), b""):
            digest.update(block)
    return digest.hexdigest()


def depth(url: str) -> int:
    return len([part for part in urlsplit(url).path.split("/") if part])


def validate(release: Path) -> dict:
    import pyarrow.parquet as pq

    errors: list[str] = []
    check = lambda ok, message: ok or errors.append(message)  # noqa: E731
    cities, years = read(release / "mayoral_cities.csv"), read(release / "mayoral_city_years.csv")
    elections, finalists = read(release / "mayoral_elections.csv"), read(release / "mayoral_finalists.csv")
    panel = read(release / "mayoral_finalist_panel.csv")
    city_ids = {c["city_id"] for c in cities}
    check(len(cities) == len(city_ids) == 100, f"expected 100 unique cities, found {len(cities)}")
    for c in cities:
        counties = c["county_fips_all"].split(";")
        check(len(c["county_fips"]) == 5 and counties[0] == c["county_fips"]
              and all(f[:2] == c["city_id"][:2] for f in counties), f"{c['city_id']}: county codes inconsistent")
    check(len({(y["city_id"], y["year"]) for y in years}) == len(years) == 1700, "city-years are not 1,700 unique rows")
    check({y["city_id"] for y in years} <= city_ids, "a city-year names an unknown city")
    election_ids = {e["election_id"] for e in elections}
    check(len(election_ids) == len(elections), "election_id is not unique")
    check({f["city_id"] for f in finalists} <= city_ids, "a finalist names an unknown city")
    check({f["election_id"] for f in finalists} <= election_ids, "a finalist names an unknown election")
    check(len({f["candidate_election_id"] for f in finalists}) == len(finalists), "candidate_election_id is not unique")
    eligible = [f for f in finalists if f["eligible"] == "True"]
    for f in finalists:
        if f["eligible"] == "True":
            check(f["text_status"] in STATUSES, f"{f['candidate_election_id']}: bad text_status {f['text_status']!r}")
        else:
            check(f["text_status"] == "", f"{f['candidate_election_id']}: ineligible finalist has a text status")
        if f["text_status"] == "no_url_lead":
            check(f["url_lead"] != "True", f"{f['candidate_election_id']}: no_url_lead but a URL lead exists")
        if "person_id" in f:
            check(f["person_id"].startswith("person_"), f"{f['candidate_election_id']}: person_id missing")
            check(f["incumbent"] in {"True", "False", ""}, f"{f['candidate_election_id']}: bad incumbent value")
    with_text = {f["candidate_election_id"] for f in eligible if f["text_status"] == "text"}
    panel_ids = [p["candidate_election_id"] for p in panel]
    check(len(panel_ids) == len(set(panel_ids)), "panel has duplicate finalists")
    check(set(panel_ids) == with_text, "panel rows differ from finalists with text_status text")

    corpus = pq.read_table(release / "mayoral_page_corpus.parquet").to_pylist()
    check(len({r["page_id"] for r in corpus}) == len(corpus), "page_id is not unique")
    usable = defaultdict(list)
    by_id = {r["page_id"]: r for r in corpus}
    for r in corpus:
        if r["duplicate_of"]:
            kept = by_id.get(r["duplicate_of"])
            check(kept is not None and kept["candidate_election_id"] == r["candidate_election_id"]
                  and kept["in_text_window"] and not kept["excluded"] and not kept["duplicate_of"],
                  f"{r['page_id']}: duplicate_of does not point to a kept page of the same finalist")
        rule = r["in_text_window"] and not r["excluded"] and not r["duplicate_of"] and r["site_check_passed"]
        check(r["release_usable"] == rule, f"{r['page_id']}: release_usable differs from the rule")
        check(abs(r["days_from_election"]) <= 365 or not r["release_usable"], f"{r['page_id']}: usable outside window")
        if "phase" in r:
            d = r["days_from_election"]
            check(r["phase"] == ("before" if d < 0 else "election_day" if d == 0 else "after"), f"{r['page_id']}: phase")
        check("\x00" not in r["text"] or not r["release_usable"], f"{r['page_id']}: usable page contains NUL bytes")
        if r["release_usable"]:
            usable[r["candidate_election_id"]].append(r)
        if "also_in_election" in r:
            check(not r["also_in_election"] or r["release_usable"], f"{r['page_id']}: also_in_election on an unusable page")
            check(r["candidate_election_id"] not in r["also_in_election"].split(";"), f"{r['page_id']}: also_in_election names itself")
    by_id = {f["candidate_election_id"]: f for f in finalists}
    for p in panel:
        own = sorted(usable[p["candidate_election_id"]],
                     key=lambda r: (r["target_id"], depth(r["original_url"]), r["original_url"], r["capture_timestamp"]))
        check(int(p["n_pages"]) == len(own) == int(by_id[p["candidate_election_id"]]["n_pages_usable"]),
              f"{p['candidate_election_id']}: page count differs between panel, finalists and corpus")
        check(p["text"] == "#+#".join(r["text"] for r in own), f"{p['candidate_election_id']}: panel text differs")
        if "text_before" in p:
            before = [r for r in own if r["phase"] == "before"]
            check(p["text_before"] == "#+#".join(r["text"] for r in before), f"{p['candidate_election_id']}: text_before differs")
            check(int(p["n_pages_before_election"]) == len(before)
                  and int(p["n_pages_after_election"]) == len(own) - len(before),
                  f"{p['candidate_election_id']}: before/after page counts differ")
    check(set(usable) == with_text, "finalists with usable pages differ from finalists with text")

    manifest_path = release / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        for name in DATA_FILES:
            entry = manifest["files"].get(name)
            check(entry is not None, f"manifest lacks {name}")
            if entry:
                check(entry["sha256"] == sha256(release / name), f"{name}: SHA-256 differs from manifest")
    else:
        errors.append("manifest.json is missing")

    excluded = sum(r["excluded"] for r in corpus)
    duplicates = sum(bool(r["duplicate_of"]) for r in corpus)
    failed_site = sum(not r["site_check_passed"] for r in corpus)
    drops = {"pages": len(corpus), "excluded_by_page_rule": excluded, "duplicate_text": duplicates,
             "failed_site_check": failed_site, "outside_window": sum(not r["in_text_window"] for r in corpus),
             "usable": sum(r["release_usable"] for r in corpus)}
    over = {k: round(v / len(corpus), 3) for k, v in drops.items() if k not in {"pages", "usable"} and v / len(corpus) > 0.2}
    return {"valid": not errors, "errors": errors[:50], "error_count": len(errors),
            "eligible_finalists": len(eligible), "text_status": dict(Counter(f["text_status"] for f in eligible)),
            "panel_rows": len(panel), "page_drops": drops, "drops_above_20_percent": over}


def write_manifest(release: Path) -> dict:
    import pyarrow.parquet as pq
    files = {}
    for name in DATA_FILES:
        path = release / name
        if path.suffix == ".parquet":
            meta = pq.ParquetFile(path).metadata
            rows, columns = meta.num_rows, meta.num_columns
        else:
            with path.open(newline="") as handle:
                reader = csv.reader(handle)
                columns = len(next(reader))
                rows = sum(1 for _ in reader)
        files[name] = {"rows": rows, "columns": columns, "bytes": path.stat().st_size, "sha256": sha256(path)}
    manifest = {"dataset": "Campaign websites of mayoral finalists in the 100 largest US cities, 2010-2026", "files": files}
    (release / "manifest.json").write_text(json.dumps(manifest, indent=1, sort_keys=True) + "\n")
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("release", type=Path)
    parser.add_argument("--write-manifest", action="store_true", help="write manifest.json before validating")
    args = parser.parse_args()
    if args.write_manifest:
        write_manifest(args.release)
    report = validate(args.release)
    print(json.dumps(report, indent=1))
    sys.exit(0 if report["valid"] else 1)


if __name__ == "__main__":
    main()
