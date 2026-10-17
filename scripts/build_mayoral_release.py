"""Build the mayoral campaign website release (stage 5 of the release pipeline in docs/status.md).

Inputs (all hashed in the manifest):
  coverage outputs   data/municipal_discovery/top100_coverage/outputs/ (cities, city_year_audit, elections,
                     candidate_coverage) from scripts/build_mayoral_top_two_coverage.py
  page corpus        mayoral_page_corpus.parquet from scripts/build_mayoral_page_corpus.py (stages 2 and 3)
  site check         site_check.csv: one row per credited site (candidate_election_id, target_id, status
                     pass or excluded, reason, source, pages_until), from the Hive check and the agent review
                     (stage 4); pages_until limits a passing site to pages captured up to that date

Outputs in --out-dir (which must not exist, so no earlier release is overwritten):
  mayoral_cities.csv            100 cities, the fixed 2020 Census population and county codes
  mayoral_city_years.csv        1,700 city-years with their assessment status
  mayoral_elections.csv         every recorded election with its verification status
  mayoral_finalists.csv         every recorded finalist; eligible ones carry a text status and reason
  mayoral_page_corpus.parquet   every fetched page of a credited site with flags and a release_usable column
  mayoral_finalist_panel.csv    one row per finalist with text: all usable in-window pages joined by "#+#"

A page is release_usable when it lies within 365 days of the election, is not excluded by a page rule, is
not a duplicate text of an earlier page of the same finalist, and its site passed the site check.
"""
from __future__ import annotations

import argparse
import csv
import json
import sys
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlsplit

csv.field_size_limit(sys.maxsize)
REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from scripts.icpsr_replicate_coding import icpsr_clean_text  # noqa: E402

COVERAGE = REPO / "data/municipal_discovery/top100_coverage/outputs"
COUNTY_PARTS = REPO / "data/municipal_discovery/top100_coverage/inputs/city_county_parts_2020.csv"  # Census 2020 PL API
DISCOVERY = REPO / "data/municipal_discovery/top100_coverage/url_discovery"
STATUSES = ("text", "excluded_site_check", "no_usable_pages", "site_rejected", "site_not_archived", "check_incomplete",
            "no_url_lead")
REJECTIONS = REPO / "data/municipal_discovery/top100_coverage/release_site_check/roster_url_rejections.csv"
RUN_NAMES = {"production": "first_150_pages", "uncapped": "remaining_pages", "carymoon_extra": "supplementary_query",
             "prefetch": "pre_election_copy"}
NICKNAMES = {"bill": "william", "billy": "william", "will": "william", "bob": "robert", "rob": "robert", "bobby": "robert",
             "jim": "james", "jimmy": "james", "mike": "michael", "tom": "thomas", "dave": "david", "joe": "joseph",
             "rick": "richard", "dick": "richard", "ed": "edward", "eddie": "edward", "steve": "steven", "chris": "christopher",
             "kate": "katherine", "katie": "katherine", "betsy": "elizabeth", "beth": "elizabeth", "liz": "elizabeth",
             "dan": "daniel", "danny": "daniel", "tony": "anthony", "andy": "andrew", "drew": "andrew", "ron": "ronald",
             "don": "donald", "pat": "patrick", "matt": "matthew", "sam": "samuel", "ben": "benjamin", "greg": "gregory",
             "jeff": "jeffrey", "ken": "kenneth", "larry": "lawrence", "jerry": "gerald", "terry": "terrence",
             "sue": "susan", "nick": "nicholas", "rich": "richard", "charlie": "charles", "chuck": "charles"}
FINALIST_COLUMNS = ["candidate_election_id", "person_id", "election_id", "city_id", "city_name", "state_abb", "year",
                    "election_date", "election_type", "candidate_name", "official_result_role", "official_votes",
                    "top_two_vote_share", "incumbent", "result_source_url",
                    "eligible", "verification_status", "url_lead", "url_lead_basis", "campaign_urls", "text_status",
                    "n_sites", "n_pages_usable", "n_char_usable"]
PANEL_COLUMNS = ["candidate_election_id", "person_id", "election_id", "city_id", "city_name", "state_abb", "population_2020",
                 "year", "election_date", "election_type", "stage", "candidate_name", "official_result_role",
                 "top_two_vote_share", "incumbent", "n_sites", "n_pages", "n_pages_before_election", "n_pages_after_election",
                 "first_capture", "last_capture", "n_char", "n_words", "n_tags", "n_clean_tags", "text", "text_clean",
                 "n_char_before", "text_before", "text_clean_before", "text_unique"]


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


def add_counties(cities: list[dict]) -> list[dict]:
    """Add the main county (the city-county part with the largest 2020 population), all counties, and the
    main county's share of the summed city-county parts."""
    parts = defaultdict(list)
    for row in read_csv(COUNTY_PARTS):
        parts[row["city_id"]].append((int(row["population_2020_part"]), row["county_fips"]))
    out = []
    for city in cities:
        own = sorted(parts[city["city_id"]], reverse=True)
        if not own:
            raise ValueError(f"no county parts for {city['city_id']}")
        out.append({**city, "county_fips": own[0][1], "county_fips_all": ";".join(f for _, f in own),
                    "county_population_share": round(own[0][0] / sum(p for p, _ in own), 4)})
    return out


def unique_segments(text: str) -> str:
    """Keep each text segment once, at its first occurrence: menus and footers repeated across a finalist's
    pages then count once (approved October 16, 2026). Segments are compared after collapsing white space."""
    seen, kept = set(), []
    for segment in text.split("#+#"):
        key = " ".join(segment.split())
        if key and key not in seen:
            seen.add(key)
            kept.append(segment)
    return "#+#".join(kept)


def write_csv(path: Path, rows: list[dict], columns: list[str]) -> None:
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def incomplete_checks() -> set[str]:
    """Finalists whose last search pass ended incomplete (repair attempt 3, the after-election sweep and the
    legacy-lead search)."""
    out = set()
    for path in sorted((DISCOVERY / "repair_20261004" / "followup2").glob("*_url_leads.json")):
        out |= {lead["candidate_election_id"] for lead in json.loads(path.read_text()) if lead["status"] == "incomplete"}
    # Legacy-lead search (October 9): incomplete entries that no checked sweep hit resolved later.
    legacy = DISCOVERY / "legacy_search_20261009"
    resolved_path = REPO / "data/municipal_discovery/top100_coverage/linkage/legacy_search_leads.csv"
    resolved = {r["candidate_election_id"] for r in read_csv(resolved_path)} if resolved_path.exists() else set()
    for path in sorted(legacy.glob("*_url_leads.json")):
        out |= {lead["candidate_election_id"] for lead in json.loads(path.read_text())
                if lead["status"] == "incomplete" and lead["candidate_election_id"] not in resolved}
    sweep = DISCOVERY / "sweep_after_20261007" / "sweep_check_verdicts.json"
    if sweep.exists():
        out |= {v["candidate_election_id"] for v in json.loads(sweep.read_text()) if v["status"] == "incomplete"}
    # Fourth pass (October 10-14): the latest search for its 99 finalists, so its status replaces the earlier ones.
    for path in sorted((DISCOVERY / "pass4_20261010").glob("*_url_leads.json")):
        for lead in json.loads(path.read_text()):
            if lead["status"] == "incomplete":
                out.add(lead["candidate_election_id"])
            else:
                out.discard(lead["candidate_election_id"])
    return out


def depth(url: str) -> int:
    return len([part for part in urlsplit(url).path.split("/") if part])


def truth(value: str) -> bool:
    return str(value).strip().lower() == "true"


def text_status(person: dict, usable_pages: int, site_states: list[str], incomplete: set[str],
                rejected: frozenset = frozenset()) -> str:
    if usable_pages:
        return "text"
    if site_states and all(s == "excluded" for s in site_states):
        return "excluded_site_check"
    if truth(person["downloaded_content"]):
        return "no_usable_pages"
    if person["candidate_election_id"] in rejected:
        return "site_rejected"
    if person["candidate_election_id"] in incomplete:
        return "check_incomplete"
    if truth(person["url_lead"]):
        return "site_not_archived"
    return "no_url_lead"


def name_parts(name: str) -> tuple[str, str]:
    """(canonical first name, last name) for person matching within a city."""
    import re
    import unicodedata
    plain = unicodedata.normalize("NFKD", name).encode("ascii", "ignore").decode().lower()
    plain = re.sub(r"\(.*?\)|\".*?\"|'[^']*'", " ", plain)
    words = [w.strip(".,") for w in plain.replace(",", " ").split()]
    words = [w for w in words if w and w not in {"jr", "sr", "ii", "iii", "iv", "dr"} and len(w) > 1]
    if not words:
        return "", ""
    first = NICKNAMES.get(words[0], words[0])
    return first, words[-1]


def person_ids(people: list[dict]) -> dict[str, str]:
    """One ID per person and city: same city, same last name, same canonical first initial (e.g. Ed Lee = Edwin M. Lee)."""
    import hashlib
    out = {}
    for p in people:
        first, last = name_parts(p["candidate_name"])
        key = f"{p['city_id']}|{last}|{first[:1]}"
        out[p["candidate_election_id"]] = "person_" + hashlib.sha256(key.encode()).hexdigest()[:12]
    return out


def build(corpus_path: Path, site_check_path: Path, out_dir: Path) -> dict:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if out_dir.exists():
        raise FileExistsError(f"{out_dir} exists; choose a new release folder")
    out_dir.mkdir(parents=True)
    people = read_csv(COVERAGE / "candidate_coverage.csv")
    pids = person_ids(people)
    # Election and city variables repeated in the panel, so users need no merge (approved October 16, 2026).
    stage = {e["election_id"]: e["stage"] for e in read_csv(COVERAGE / "elections.csv")}
    population = {c["city_id"]: c["population_2020"] for c in read_csv(COVERAGE / "cities.csv")}
    sites = {(r["candidate_election_id"], r["target_id"]): r for r in read_csv(site_check_path)}
    corpus = pq.read_table(corpus_path)
    def site_passes(finalist: str, target: str, stamp: str) -> bool:
        row = sites.get((finalist, target), {})
        if row.get("status") != "pass":
            return False
        # A site can pass only up to a date, when the domain was taken over after the election.
        until = row.get("pages_until", "")
        return not until or stamp[:8] <= until.replace("-", "")

    passed = [site_passes(c, t, s) for c, t, s in zip(corpus["candidate_election_id"].to_pylist(),
                                                     corpus["target_id"].to_pylist(),
                                                     corpus["capture_timestamp"].to_pylist())]
    usable = pc.and_(pc.and_(corpus["in_text_window"], pc.invert(corpus["excluded"])),
                     pc.and_(pc.equal(corpus["duplicate_of"], ""), pa.array(passed)))
    days = corpus["days_from_election"].to_pylist()
    phase = pa.array(["before" if d < 0 else "election_day" if d == 0 else "after" for d in days])
    # Neutral provenance names: collection rounds in date order, and what each run collected.
    rounds = {b: f"round_{i:02d}" for i, b in enumerate(sorted(set(corpus["batch"].to_pylist()),
                                                                key=lambda b: (b.rsplit("_", 1)[-1], b)), 1)}
    corpus = corpus.set_column(corpus.schema.get_field_index("batch"), "batch",
                               pa.array([rounds[b] for b in corpus["batch"].to_pylist()]))
    corpus = corpus.set_column(corpus.schema.get_field_index("run"), "run",
                               pa.array([RUN_NAMES.get(r, r) for r in corpus["run"].to_pylist()]))
    corpus = corpus.append_column("phase", phase)
    corpus = corpus.append_column("site_check_passed", pa.array(passed)).append_column("release_usable", usable)
    # A usable page whose text or capture also counts as usable for another election of the same person, for example
    # an incumbent's platform page from the previous race (approved October 16, 2026). Lists those elections.
    keys = corpus.select(["candidate_election_id", "text_sha256", "snap_url"]).to_pylist()
    owners = defaultdict(set)
    for row, ok in zip(keys, usable.to_pylist()):
        if ok:
            person = pids[row["candidate_election_id"]]
            owners[(person, "text", row["text_sha256"])].add(row["candidate_election_id"])
            owners[(person, "capture", row["snap_url"])].add(row["candidate_election_id"])
    also = []
    for row, ok in zip(keys, usable.to_pylist()):
        person = pids[row["candidate_election_id"]]
        other = (owners[(person, "text", row["text_sha256"])] | owners[(person, "capture", row["snap_url"])]) if ok else set()
        also.append(";".join(sorted(other - {row["candidate_election_id"]})))
    corpus = corpus.append_column("also_in_election", pa.array(also))
    (out_dir.parent / f"{out_dir.name}_round_names.json").write_text(json.dumps(rounds, indent=1))
    pq.write_table(corpus, out_dir / "mayoral_page_corpus.parquet", compression="zstd")

    pages = defaultdict(list)
    for row in corpus.filter(usable).select(["candidate_election_id", "target_id", "original_url", "capture_timestamp",
                                             "text", "phase"]).to_pylist():
        pages[row["candidate_election_id"]].append(row)
    site_states = defaultdict(list)
    for (finalist, _), row in sites.items():
        site_states[finalist].append(row["status"])
    incomplete = incomplete_checks()
    rejected = frozenset(r["candidate_election_id"] for r in read_csv(REJECTIONS)) if REJECTIONS.exists() else frozenset()
    by_election = defaultdict(list)
    for person in people:
        by_election[person["election_id"]].append(person)
    # Incumbency, derived: the finalist won the city's previous decisive election in the data. Unknown ("") for a
    # city's first election in the data, since the earlier mayor is not recorded here.
    decisive = sorted({(p["city_id"], p["election_date"], p["election_id"]) for p in people
                       if truth(p["eligible"])})
    previous_winner, incumbent = {}, {}
    for city_id, date, election_id in decisive:
        winners = [pids[p["candidate_election_id"]] for p in by_election[election_id] if p["official_result_role"] == "winner"]
        prior = previous_winner.get(city_id)
        for p in by_election[election_id]:
            incumbent[p["candidate_election_id"]] = "" if prior is None else str(pids[p["candidate_election_id"]] == prior)
        if winners:
            previous_winner[city_id] = winners[0]
    finalists, panel = [], []
    for person in people:
        votes = [int(p["official_votes"]) for p in by_election[person["election_id"]] if str(p["official_votes"]).isdigit()]
        own_votes = int(person["official_votes"]) if str(person["official_votes"]).isdigit() else None
        person = {**person, "person_id": pids[person["candidate_election_id"]],
                  "incumbent": incumbent.get(person["candidate_election_id"], "") if truth(person["eligible"]) else "",
                  "top_two_vote_share": round(own_votes / sum(votes), 4) if own_votes is not None and len(votes) == 2 and sum(votes) else ""}
        own = sorted(pages.get(person["candidate_election_id"], []),
                     key=lambda p: (p["target_id"], depth(p["original_url"]), p["original_url"], p["capture_timestamp"]))
        urls = json.loads(person["exact_urls"] or "[]") + ([person["discovered_campaign_url"]] if person["discovered_campaign_url"] else [])
        status = text_status(person, len(own), site_states.get(person["candidate_election_id"], []), incomplete,
                             rejected) if truth(person["eligible"]) else ""
        text = "#+#".join(p["text"] for p in own)
        finalists.append({**person, "campaign_urls": json.dumps(urls), "text_status": status,
                          "n_sites": len({p["target_id"] for p in own}), "n_pages_usable": len(own), "n_char_usable": len(text)})
        if own and truth(person["eligible"]):
            clean = icpsr_clean_text(text)
            before = "#+#".join(p["text"] for p in own if p["phase"] == "before")
            stamps = sorted(p["capture_timestamp"] for p in own)
            panel.append({**person, "n_sites": len({p["target_id"] for p in own}), "n_pages": len(own),
                          "n_pages_before_election": sum(p["phase"] == "before" for p in own),
                          "n_pages_after_election": sum(p["phase"] != "before" for p in own),
                          "first_capture": stamps[0], "last_capture": stamps[-1], "n_char": len(text),
                          "n_words": len(text.split()), "n_tags": clean["n_tags"], "n_clean_tags": clean["n_clean_tags"],
                          "text": text, "text_clean": clean["text"], "n_char_before": len(before),
                          "text_before": before, "text_clean_before": icpsr_clean_text(before)["text"] if before else "",
                          "text_unique": unique_segments(text), "stage": stage[person["election_id"]],
                          "population_2020": population[person["city_id"]]})
    write_csv(out_dir / "mayoral_finalists.csv", finalists, FINALIST_COLUMNS)
    write_csv(out_dir / "mayoral_finalist_panel.csv", panel, PANEL_COLUMNS)
    for name, source in (("mayoral_cities.csv", "cities.csv"), ("mayoral_city_years.csv", "city_year_audit.csv"),
                         ("mayoral_elections.csv", "elections.csv")):
        rows = read_csv(COVERAGE / source)
        if name == "mayoral_cities.csv":
            rows = add_counties(rows)
        write_csv(out_dir / name, rows, list(rows[0]))
    counts = defaultdict(int)
    for row in finalists:
        counts[row["text_status"] or "not_eligible"] += 1
    return {"finalists": len(finalists), "text_status": dict(counts), "panel_rows": len(panel),
            "corpus_pages": corpus.num_rows, "usable_pages": int(pc.sum(usable).as_py())}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--corpus", type=Path, required=True)
    parser.add_argument("--site-check", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path, required=True)
    args = parser.parse_args()
    print(json.dumps(build(args.corpus, args.site_check, args.out_dir), indent=1))


if __name__ == "__main__":
    main()
