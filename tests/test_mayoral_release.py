"""Mayoral release: text status rule and validator detection of inconsistent release files."""
import csv
import json

import pyarrow as pa
import pyarrow.parquet as pq

from scripts.build_mayoral_release import text_status
from scripts.validate_mayoral_release import validate, write_manifest


def person(**kw):
    return {"candidate_election_id": "a", "downloaded_content": "", "url_lead": "", **kw}


def test_text_status_precedence():
    assert text_status(person(), 3, ["pass"], set()) == "text"
    assert text_status(person(downloaded_content="True"), 0, ["excluded"], set()) == "excluded_site_check"
    assert text_status(person(downloaded_content="True"), 0, ["pass"], set()) == "no_usable_pages"
    assert text_status(person(), 0, [], {"a"}) == "check_incomplete"
    assert text_status(person(url_lead="True"), 0, [], set()) == "site_not_archived"
    assert text_status(person(), 0, [], set()) == "no_url_lead"


def write(path, rows):
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def release(tmp_path):
    cities = [{"city_id": f"c{i}"} for i in range(100)]
    years = [{"city_id": f"c{i}", "year": str(y)} for i in range(100) for y in range(2010, 2027)]
    write(tmp_path / "mayoral_cities.csv", cities)
    write(tmp_path / "mayoral_city_years.csv", years)
    write(tmp_path / "mayoral_elections.csv", [{"election_id": "e1"}])
    finalists = [
        {"candidate_election_id": "f1", "election_id": "e1", "city_id": "c0", "eligible": "True", "text_status": "text",
         "url_lead": "True", "n_pages_usable": "2"},
        {"candidate_election_id": "f2", "election_id": "e1", "city_id": "c0", "eligible": "True",
         "text_status": "no_url_lead", "url_lead": "", "n_pages_usable": "0"}]
    write(tmp_path / "mayoral_finalists.csv", finalists)
    page = {"candidate_election_id": "f1", "target_id": "t", "capture_timestamp": "20200101000000",
            "days_from_election": -10, "in_text_window": True, "excluded": False, "duplicate_of": "",
            "site_check_passed": True, "release_usable": True}
    pages = [dict(page, page_id="p1", original_url="https://a.test/", text="Vote A"),
             dict(page, page_id="p2", original_url="https://a.test/issues/", text="Issues"),
             dict(page, page_id="p3", original_url="https://a.test/old/", text="Old", days_from_election=-900,
                  in_text_window=False, release_usable=False)]
    pq.write_table(pa.Table.from_pylist(pages), tmp_path / "mayoral_page_corpus.parquet")
    write(tmp_path / "mayoral_finalist_panel.csv", [{"candidate_election_id": "f1", "n_pages": "2", "text": "Vote A#+#Issues"}])
    write_manifest(tmp_path)
    return tmp_path


def test_valid_release_passes(tmp_path):
    report = validate(release(tmp_path))
    assert report["valid"], report["errors"]


def test_validator_catches_panel_text_and_usable_rule_changes(tmp_path):
    root = release(tmp_path)
    write(root / "mayoral_finalist_panel.csv", [{"candidate_election_id": "f1", "n_pages": "2", "text": "Vote A"}])
    report = validate(root)
    assert not report["valid"] and any("panel text differs" in e for e in report["errors"])
    table = pq.read_table(root / "mayoral_page_corpus.parquet").to_pylist()
    table[2]["release_usable"] = True
    pq.write_table(pa.Table.from_pylist(table), root / "mayoral_page_corpus.parquet")
    errors = validate(root)["errors"]
    assert any("release_usable differs" in e for e in errors) and any("SHA-256 differs" in e for e in errors)


def test_validator_rejects_status_without_panel(tmp_path):
    root = release(tmp_path)
    rows = list(csv.DictReader((root / "mayoral_finalists.csv").open()))
    rows[1]["text_status"] = "text"
    write(root / "mayoral_finalists.csv", rows)
    assert any("panel rows differ" in e for e in validate(root)["errors"])


def test_site_rejected_status_and_person_matching():
    from scripts.build_mayoral_release import name_parts, person_ids
    assert text_status(person(url_lead="True"), 0, [], set(), frozenset({"a"})) == "site_rejected"
    assert name_parts("Ed Lee")[1] == name_parts("Edwin M. Lee")[1] == "lee"
    people = [{"candidate_election_id": i, "city_id": "c", "candidate_name": n} for i, n in
              [("1", "Bill Bell"), ("2", "William V. (Bill) Bell"), ("3", "Anjanee Bell"), ("4", "Ed Lee"), ("5", "Edwin M. Lee")]]
    ids = person_ids(people)
    assert ids["1"] == ids["2"] != ids["3"] and ids["4"] == ids["5"]
