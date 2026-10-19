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
              "mayoral_finalist_panel.csv", "mayoral_ledb_crosswalk.csv", "mayoral_page_corpus.parquet"]
LEDB_MATCH = {"name_same_month", "name_other_month", "last_name_same_month", "last_name_other_month",
              "outside_ledb_years", "no_ledb_election", "no_ledb_candidate", "ambiguous"}


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
    links = read(release / "mayoral_ledb_crosswalk.csv")
    check(sorted(x["candidate_election_id"] for x in links) == sorted(f["candidate_election_id"] for f in finalists),
          "the ALGED crosswalk does not have one row for each finalist")
    check(all(x["ledb_match"] in LEDB_MATCH and bool(x["ledb_candid"]) == x["ledb_match"].startswith(("name", "last"))
              for x in links), "the ALGED crosswalk has an unknown match type or IDs that disagree with it")
    election_of = {f["candidate_election_id"]: f["election_id"] for f in finalists}
    contests = defaultdict(set)
    for x in links:
        if x["ledb_contest"]:
            contests[election_of[x["candidate_election_id"]]].add(x["ledb_contest"])
    check(all(len(c) == 1 for c in contests.values()), "two finalists of one election link to different ALGED contests")
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
    # v6: the corpus also holds the pages of primary candidates, whose usable pages feed the primary files instead.
    finalist_ids = {f["candidate_election_id"] for f in finalists}
    primary_ids = set()
    if (release / PRIMARY_FILES[1]).exists():
        primary_ids = {p["candidate_election_id"] for p in read(release / PRIMARY_FILES[1])}
    check(set(usable) <= finalist_ids | primary_ids, "a usable page names an unknown candidate")
    check(set(usable) & finalist_ids == with_text, "finalists with usable pages differ from finalists with text")

    primary_report = validate_primaries(release, corpus, finalists, check) if (release / PRIMARY_FILES[1]).exists() else None

    manifest_path = release / "manifest.json"
    if manifest_path.exists():
        manifest = json.loads(manifest_path.read_text())
        for name in data_files(release):
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
            "panel_rows": len(panel), "page_drops": drops, "drops_above_20_percent": over,
            **({"primaries": primary_report} if primary_report else {})}


PRIMARY_FILES = ["mayoral_primary_elections.csv", "mayoral_primary_candidates.csv", "mayoral_primary_panel.csv"]
PRIMARY_STATUSES = {"text", "excluded_site_check", "no_usable_pages", "site_not_archived", "check_incomplete", "no_url_lead"}


def data_files(release: Path) -> list[str]:
    return DATA_FILES + [n for n in PRIMARY_FILES if (release / PRIMARY_FILES[1]).exists()]


def validate_primaries(release: Path, corpus: list[dict], finalists: list[dict], check) -> dict:
    """v6 primary files: keys and links, statuses, the campaign_period column, and the panel text recomputed with the
    primary-period rule of scripts/build_mayoral_release.primary_pages."""
    sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
    from scripts.build_mayoral_release import primary_pages
    elections = read(release / PRIMARY_FILES[0])
    people, panel = read(release / PRIMARY_FILES[1]), read(release / PRIMARY_FILES[2])
    ids = {e["primary_election_id"] for e in elections}
    check(len(ids) == len(elections), "primary_election_id is not unique")
    check(len({p["candidate_election_id"] for p in people}) == len(people), "primary candidate ID is not unique")
    check({p["primary_election_id"] for p in people} <= ids, "a primary candidate names an unknown primary")
    finalist_ids = {f["candidate_election_id"] for f in finalists}
    general_ids = {f["election_id"] for f in finalists}
    check(all(not e["general_election_id"] or e["general_election_id"] in general_ids for e in elections),
          "a primary names an unknown general election")
    check(all(not p["finalist_candidate_election_id"] or p["finalist_candidate_election_id"] in finalist_ids
              for p in people), "a primary candidate links to an unknown finalist")
    check(not ({p["candidate_election_id"] for p in people} & finalist_ids), "a primary candidate ID is also a finalist ID")
    check(all(p["text_status"] in PRIMARY_STATUSES for p in people), "a primary candidate has an unknown text status")
    with_text = {p["candidate_election_id"] for p in people if p["text_status"] == "text"}
    check({p["candidate_election_id"] for p in panel} == with_text, "primary panel rows differ from text_status text")
    pool = defaultdict(list)
    for r in corpus:
        if not r["excluded"] and r["site_check_passed"]:
            pool[r["candidate_election_id"]].append(r)
    by_id = {p["candidate_election_id"]: p for p in people}
    own_dates = {p["candidate_election_id"]: p["election_date"].replace("-", "") for p in people}
    first = {e["election_id"]: e for e in read(release / "mayoral_elections.csv")}
    general_of = {f["candidate_election_id"]: f["election_id"] for f in finalists}
    for e in first.values():
        check(e["rounds"] in {"", "two_rounds", "single_round", "unknown"}, f"{e['election_id']}: rounds")
        check(e["rounds"] != "two_rounds" or e["first_round_date"] < e["election_date"], f"{e['election_id']}: first round date")
    for r in corpus:
        day = r["capture_timestamp"][:8]
        if r["candidate_election_id"] in own_dates:
            want = "first_round" if day <= own_dates[r["candidate_election_id"]] else "after_primary"
        else:
            e = first.get(general_of.get(r["candidate_election_id"]), {})
            want = {"two_rounds": "first_round" if day <= e.get("first_round_date", "").replace("-", "") else "final_round",
                    "single_round": "single_round"}.get(e.get("rounds"), "")
        check(r["campaign_period"] == want, f"{r['page_id']}: campaign_period")
    for p in panel:
        person = by_id[p["candidate_election_id"]]
        source = p["candidate_election_id"] if person["searched"] == "True" else person["finalist_candidate_election_id"]
        own = sorted(primary_pages(pool[source], person["election_date"]),
                     key=lambda r: (r["target_id"], depth(r["original_url"]), r["original_url"], r["capture_timestamp"]))
        check(int(p["n_pages"]) == len(own) == int(person["n_pages_usable"]), f"{p['candidate_election_id']}: primary page count")
        check(p["text"] == "#+#".join(r["text"] for r in own), f"{p['candidate_election_id']}: primary panel text differs")
    return {"primary_elections": len(elections), "primary_candidates": len(people),
            "text_status": dict(Counter(p["text_status"] for p in people)), "panel_rows": len(panel)}


def write_manifest(release: Path) -> dict:
    import pyarrow.parquet as pq
    files = {}
    for name in data_files(release):
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
