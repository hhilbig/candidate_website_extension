"""Build the mayoral campaign website release (stage 5 of the release pipeline in docs/status.md).

Inputs (all hashed in the manifest):
  coverage outputs   data/municipal_discovery/top100_coverage/outputs/ (cities, city_year_audit, elections,
                     candidate_coverage) from scripts/build_mayoral_top_two_coverage.py
  page corpus        mayoral_page_corpus.parquet from scripts/build_mayoral_page_corpus.py (stages 2 and 3)
  site check         site_check.csv: one row per credited site (candidate_election_id, target_id, status
                     pass or excluded, reason, source, pages_until), from the Hive check and the agent review
                     (stage 4); pages_until limits a passing site to pages captured up to that date

Outputs in --out-dir (which must not exist, so no earlier release is overwritten):
  mayoral_cities.csv            100 cities, the fixed 2020 Census population
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
DISCOVERY = REPO / "data/municipal_discovery/top100_coverage/url_discovery"
STATUSES = ("text", "excluded_site_check", "no_usable_pages", "site_not_archived", "check_incomplete", "no_url_lead")
FINALIST_COLUMNS = ["candidate_election_id", "election_id", "city_id", "city_name", "state_abb", "year", "election_date",
                    "election_type", "candidate_name", "official_result_role", "official_votes", "result_source_url",
                    "eligible", "verification_status", "url_lead", "url_lead_basis", "campaign_urls", "text_status",
                    "n_sites", "n_pages_usable", "n_char_usable"]
PANEL_COLUMNS = ["candidate_election_id", "election_id", "city_id", "city_name", "state_abb", "year", "election_date",
                 "candidate_name", "official_result_role", "n_sites", "n_pages", "first_capture", "last_capture",
                 "n_char", "n_words", "n_tags", "n_clean_tags", "text", "text_clean"]


def read_csv(path: Path) -> list[dict]:
    with path.open(newline="") as handle:
        return list(csv.DictReader(handle))


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
    return out


def depth(url: str) -> int:
    return len([part for part in urlsplit(url).path.split("/") if part])


def truth(value: str) -> bool:
    return str(value).strip().lower() == "true"


def text_status(person: dict, usable_pages: int, site_states: list[str], incomplete: set[str]) -> str:
    if usable_pages:
        return "text"
    if site_states and all(s == "excluded" for s in site_states):
        return "excluded_site_check"
    if truth(person["downloaded_content"]):
        return "no_usable_pages"
    if person["candidate_election_id"] in incomplete:
        return "check_incomplete"
    if truth(person["url_lead"]):
        return "site_not_archived"
    return "no_url_lead"


def build(corpus_path: Path, site_check_path: Path, out_dir: Path) -> dict:
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    if out_dir.exists():
        raise FileExistsError(f"{out_dir} exists; choose a new release folder")
    out_dir.mkdir(parents=True)
    people = read_csv(COVERAGE / "candidate_coverage.csv")
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
    corpus = corpus.append_column("site_check_passed", pa.array(passed)).append_column("release_usable", usable)
    pq.write_table(corpus, out_dir / "mayoral_page_corpus.parquet", compression="zstd")

    pages = defaultdict(list)
    for row in corpus.filter(usable).select(["candidate_election_id", "target_id", "original_url", "capture_timestamp",
                                             "text"]).to_pylist():
        pages[row["candidate_election_id"]].append(row)
    site_states = defaultdict(list)
    for (finalist, _), row in sites.items():
        site_states[finalist].append(row["status"])
    incomplete = incomplete_checks()
    finalists, panel = [], []
    for person in people:
        own = sorted(pages.get(person["candidate_election_id"], []),
                     key=lambda p: (p["target_id"], depth(p["original_url"]), p["original_url"], p["capture_timestamp"]))
        urls = json.loads(person["exact_urls"] or "[]") + ([person["discovered_campaign_url"]] if person["discovered_campaign_url"] else [])
        status = text_status(person, len(own), site_states.get(person["candidate_election_id"], []), incomplete) \
            if truth(person["eligible"]) else ""
        text = "#+#".join(p["text"] for p in own)
        finalists.append({**person, "campaign_urls": json.dumps(urls), "text_status": status,
                          "n_sites": len({p["target_id"] for p in own}), "n_pages_usable": len(own), "n_char_usable": len(text)})
        if own and truth(person["eligible"]):
            clean = icpsr_clean_text(text)
            stamps = sorted(p["capture_timestamp"] for p in own)
            panel.append({**person, "n_sites": len({p["target_id"] for p in own}), "n_pages": len(own),
                          "first_capture": stamps[0], "last_capture": stamps[-1], "n_char": len(text),
                          "n_words": len(text.split()), "n_tags": clean["n_tags"], "n_clean_tags": clean["n_clean_tags"],
                          "text": text, "text_clean": clean["text"]})
    write_csv(out_dir / "mayoral_finalists.csv", finalists, FINALIST_COLUMNS)
    write_csv(out_dir / "mayoral_finalist_panel.csv", panel, PANEL_COLUMNS)
    for name, source in (("mayoral_cities.csv", "cities.csv"), ("mayoral_city_years.csv", "city_year_audit.csv"),
                         ("mayoral_elections.csv", "elections.csv")):
        rows = read_csv(COVERAGE / source)
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
